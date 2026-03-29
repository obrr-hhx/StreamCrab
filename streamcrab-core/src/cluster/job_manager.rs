use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use tonic::{Request, Response, Status};

use crate::checkpoint::{
    CheckpointCoordinator, InMemoryCheckpointStorage, TaskCheckpointAbort, TaskCheckpointAck,
};
use crate::cluster::rpc::job_manager_service_server::{JobManagerService, JobManagerServiceServer};
use crate::cluster::rpc::task_manager_service_client::TaskManagerServiceClient;
use crate::cluster::rpc::{
    CheckpointAck, CheckpointBarrier, CheckpointComplete, Empty, FailureReport,
    GetJobStatusRequest, GetJobStatusResponse, HeartbeatRequest, HeartbeatResponse,
    ListTaskManagersRequest, ListTaskManagersResponse, RegisterTmRequest, RegisterTmResponse,
    SubmitJobRequest, SubmitJobResponse, TaskDeploymentDescriptor, TriggerCheckpointRequest,
    TriggerCheckpointResponse, TriggerRescaleRequest, TriggerRescaleResponse,
};
use crate::elastic::{Autoscaler, ScalePolicy, TaskMetrics};
use crate::runtime::descriptors::{JobPlan, OperatorDescriptor, SinkGuarantee};
use crate::runtime::task::{TaskId, VertexId};
use crate::state::{GrpcStateClient, StateMode, StateServiceClient};

use super::{
    ClusterConfig, JobId, JobStatus, Resources, RoundRobinScheduler, TaskLocation, TaskManagerInfo,
    TmId, collect_affected_jobs, current_unix_millis, latest_completed_checkpoint_for_job,
};

pub struct JobManager {
    pub config: ClusterConfig,
    next_job_id: AtomicU64,
    task_managers: RwLock<HashMap<TmId, TaskManagerInfo>>,
    jobs: RwLock<HashMap<JobId, JobStatus>>,
    job_expected_tasks: RwLock<HashMap<JobId, Vec<TaskId>>>,
    job_task_locations: RwLock<HashMap<JobId, Vec<TaskLocation>>>,
    job_parallelism: RwLock<HashMap<JobId, usize>>,
    job_plans: RwLock<HashMap<JobId, Vec<u8>>>,
    job_operator_runtime: RwLock<HashMap<JobId, HashMap<u32, crate::runtime::descriptors::OperatorRuntimeConfig>>>,
    job_source_offsets: RwLock<HashMap<JobId, u64>>,
    job_committed_source_offsets: RwLock<HashMap<JobId, u64>>,
    task_to_job: RwLock<HashMap<String, JobId>>,
    checkpoint_to_job: Mutex<HashMap<u64, JobId>>,
    scheduler: Mutex<RoundRobinScheduler>,
    checkpoint_coordinator: Arc<CheckpointCoordinator<InMemoryCheckpointStorage>>,
    state_service_client: Option<GrpcStateClient>,
    rescale_coordinator: Mutex<super::RescaleCoordinator>,
    autoscaler: Mutex<Autoscaler>,
    last_autoscaler_evaluation: Mutex<Option<Instant>>,
}

impl JobManager {
    pub fn new(config: ClusterConfig) -> Self {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let checkpoint_coordinator =
            Arc::new(CheckpointCoordinator::new(storage).with_retained_checkpoints(3));
        let autoscaler_cooldown = config.autoscaler_cooldown;
        let state_service_client = config
            .state_service_endpoint
            .as_ref()
            .map(|endpoint| GrpcStateClient::new(endpoint.clone()));
        Self {
            config,
            next_job_id: AtomicU64::new(1),
            task_managers: RwLock::new(HashMap::new()),
            jobs: RwLock::new(HashMap::new()),
            job_expected_tasks: RwLock::new(HashMap::new()),
            job_task_locations: RwLock::new(HashMap::new()),
            job_parallelism: RwLock::new(HashMap::new()),
            job_plans: RwLock::new(HashMap::new()),
            job_operator_runtime: RwLock::new(HashMap::new()),
            job_source_offsets: RwLock::new(HashMap::new()),
            job_committed_source_offsets: RwLock::new(HashMap::new()),
            task_to_job: RwLock::new(HashMap::new()),
            checkpoint_to_job: Mutex::new(HashMap::new()),
            scheduler: Mutex::new(RoundRobinScheduler::new()),
            checkpoint_coordinator,
            state_service_client,
            rescale_coordinator: Mutex::new(super::RescaleCoordinator::new()),
            autoscaler: Mutex::new(Autoscaler::new(autoscaler_cooldown)),
            last_autoscaler_evaluation: Mutex::new(None),
        }
    }

    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(JobManagerServiceServer::new(JobManagerRpc::new(self)))
            .serve(addr)
            .await?;
        Ok(())
    }

    pub fn register_task_manager(
        &self,
        tm_id: TmId,
        address: String,
        num_slots: usize,
        resources: Resources,
    ) -> bool {
        let mut guard = self.task_managers.write().expect("task_managers poisoned");
        let existed = guard.contains_key(&tm_id);
        guard.insert(
            tm_id.clone(),
            TaskManagerInfo::new(tm_id, address, num_slots, resources),
        );
        !existed
    }

    pub fn heartbeat(&self, tm_id: &str, queue_usage: u32, throughput: u64) -> bool {
        let mut guard = self.task_managers.write().expect("task_managers poisoned");
        if let Some(info) = guard.get_mut(tm_id) {
            info.heartbeat(queue_usage, throughput);
            true
        } else {
            false
        }
    }

    pub fn evict_stale_task_managers(&self) -> Vec<TmId> {
        let mut removed = Vec::new();
        let timeout = self.config.heartbeat.timeout;
        let mut guard = self.task_managers.write().expect("task_managers poisoned");
        guard.retain(|tm_id, info| {
            let alive = info.is_alive(timeout);
            if !alive {
                removed.push(tm_id.clone());
            }
            alive
        });
        removed
    }

    pub fn list_task_managers(&self) -> Vec<crate::cluster::rpc::TaskManagerSummary> {
        self.task_managers
            .read()
            .expect("task_managers poisoned")
            .values()
            .map(TaskManagerInfo::to_summary)
            .collect()
    }

    pub fn submit_job(&self, parallelism: usize) -> Result<JobId> {
        if parallelism == 0 {
            return Err(anyhow!("parallelism must be greater than 0"));
        }
        let job_id = format!("job-{}", self.next_job_id.fetch_add(1, Ordering::SeqCst));
        self.jobs
            .write()
            .expect("jobs poisoned")
            .insert(job_id.clone(), JobStatus::Created);

        let expected_tasks: Vec<TaskId> = (0..parallelism)
            .map(|i| TaskId::new(VertexId::new(0), i))
            .collect();
        self.job_expected_tasks
            .write()
            .expect("job_expected_tasks poisoned")
            .insert(job_id.clone(), expected_tasks);
        self.job_parallelism
            .write()
            .expect("job_parallelism poisoned")
            .insert(job_id.clone(), parallelism);
        self.job_source_offsets
            .write()
            .expect("job_source_offsets poisoned")
            .insert(job_id.clone(), 0);
        self.job_committed_source_offsets
            .write()
            .expect("job_committed_source_offsets poisoned")
            .insert(job_id.clone(), 0);
        Ok(job_id)
    }

    pub fn get_job_status(&self, job_id: &str) -> Option<JobStatus> {
        self.jobs
            .read()
            .expect("jobs poisoned")
            .get(job_id)
            .copied()
    }

    pub fn report_source_offset(&self, job_id: &str, offset: u64) -> Result<()> {
        if !self
            .jobs
            .read()
            .expect("jobs poisoned")
            .contains_key(job_id)
        {
            return Err(anyhow!("job {} not found", job_id));
        }
        self.job_source_offsets
            .write()
            .expect("job_source_offsets poisoned")
            .insert(job_id.to_string(), offset);
        Ok(())
    }

    fn validate_sink_guarantees(plan: &JobPlan) -> Result<()> {
        for (idx, op) in plan.operators.iter().enumerate() {
            if let OperatorDescriptor::Sink { guarantee, .. } = op
                && matches!(guarantee, SinkGuarantee::AtLeastOnce)
            {
                return Err(anyhow!(
                    "sink operator {} uses AtLeastOnce; exactly-once checkpointing requires idempotent or 2PC sink",
                    idx
                ));
            }
        }
        Ok(())
    }

    fn commit_source_offset_for_checkpoint(&self, checkpoint_id: u64) {
        let job_id = self
            .checkpoint_to_job
            .lock()
            .expect("checkpoint_to_job poisoned")
            .get(&checkpoint_id)
            .cloned();
        let Some(job_id) = job_id else {
            return;
        };
        let current = self
            .job_source_offsets
            .read()
            .expect("job_source_offsets poisoned")
            .get(&job_id)
            .copied();
        if let Some(offset) = current {
            self.job_committed_source_offsets
                .write()
                .expect("job_committed_source_offsets poisoned")
                .insert(job_id, offset);
        }
    }

    fn rollback_source_offset_to_committed(&self, job_id: &str) {
        let committed = self
            .job_committed_source_offsets
            .read()
            .expect("job_committed_source_offsets poisoned")
            .get(job_id)
            .copied();
        if let Some(offset) = committed {
            self.job_source_offsets
                .write()
                .expect("job_source_offsets poisoned")
                .insert(job_id.to_string(), offset);
        }
    }

    fn update_job_operator_runtime(
        &self,
        job_id: &str,
        plan: Option<&JobPlan>,
    ) -> HashMap<u32, crate::runtime::descriptors::OperatorRuntimeConfig> {
        let runtime = plan
            .map(|p| {
                p.operator_runtime
                    .iter()
                    .map(|cfg| (cfg.operator_id, cfg.clone()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        self.job_operator_runtime
            .write()
            .expect("job_operator_runtime poisoned")
            .insert(job_id.to_string(), runtime.clone());
        runtime
    }

    fn autoscaler_tick_due(&self, now: Instant) -> bool {
        let mut guard = self
            .last_autoscaler_evaluation
            .lock()
            .expect("last_autoscaler_evaluation poisoned");
        if let Some(last) = *guard
            && now.duration_since(last) < self.config.autoscaler_interval
        {
            return false;
        }
        *guard = Some(now);
        true
    }

    fn collect_task_metrics(&self) -> HashMap<String, TaskMetrics> {
        self.task_managers
            .read()
            .expect("task_managers poisoned")
            .iter()
            .map(|(tm_id, info)| {
                (
                    tm_id.clone(),
                    TaskMetrics {
                        input_queue_usage: info.last_queue_usage,
                        throughput: info.last_throughput,
                    },
                )
            })
            .collect()
    }

    pub async fn run_autoscaler_once_if_due(&self) -> Result<()> {
        let now = Instant::now();
        if !self.autoscaler_tick_due(now) {
            return Ok(());
        }

        let metrics = self.collect_task_metrics();
        if metrics.is_empty() {
            return Ok(());
        }

        let decisions = {
            let runtime_map = self
                .job_operator_runtime
                .read()
                .expect("job_operator_runtime poisoned");
            let jobs = self.jobs.read().expect("jobs poisoned");
            let parallelism = self.job_parallelism.read().expect("job_parallelism poisoned");
            let mut autoscaler = self.autoscaler.lock().expect("autoscaler poisoned");
            let mut pending = Vec::new();

            for (job_id, operators) in runtime_map.iter() {
                if !matches!(jobs.get(job_id), Some(JobStatus::Running)) {
                    continue;
                }
                let Some(&current_parallelism) = parallelism.get(job_id) else {
                    continue;
                };
                for runtime in operators.values() {
                    let Some(config) = runtime.elastic_config.as_ref() else {
                        continue;
                    };
                    if !matches!(runtime.state_mode, StateMode::Tiered { .. }) {
                        continue;
                    }
                    if matches!(config.scale_policy, ScalePolicy::Manual) {
                        continue;
                    }
                    if let Some(decision) = autoscaler.evaluate_scale_decision(
                        runtime.operator_id,
                        config,
                        current_parallelism,
                        &metrics,
                        now,
                    ) && decision.target_parallelism != current_parallelism
                    {
                        pending.push((
                            job_id.clone(),
                            runtime.operator_id,
                            decision.target_parallelism,
                        ));
                    }
                }
            }
            pending
        };

        for (job_id, operator_id, new_parallelism) in decisions {
            if let Err(err) = self
                .trigger_rescale(&job_id, operator_id, new_parallelism)
                .await
            {
                tracing::warn!(
                    "autoscaler rescale failed: job={} operator={} target_parallelism={} err={}",
                    job_id,
                    operator_id,
                    new_parallelism,
                    err
                );
            } else {
                tracing::info!(
                    "autoscaler rescale applied: job={} operator={} target_parallelism={}",
                    job_id,
                    operator_id,
                    new_parallelism
                );
            }
        }
        Ok(())
    }

    fn validate_rescale_preconditions(&self, job_id: &str, operator_id: u32) -> Result<()> {
        let runtime_map = self
            .job_operator_runtime
            .read()
            .expect("job_operator_runtime poisoned");
        if let Some(job_runtime) = runtime_map.get(job_id)
            && let Some(runtime) = job_runtime.get(&operator_id)
            && !matches!(runtime.state_mode, StateMode::Tiered { .. })
        {
            return Err(anyhow!(
                "operator {} in job {} uses Local state mode; online rescale requires Tiered",
                operator_id,
                job_id
            ));
        }
        drop(runtime_map);

        let plan_bytes = self
            .job_plans
            .read()
            .expect("job_plans poisoned")
            .get(job_id)
            .cloned()
            .unwrap_or_default();
        if plan_bytes.is_empty() {
            return Ok(());
        }
        let plan = JobPlan::from_bytes(&plan_bytes)?;
        if let Some(crate::runtime::descriptors::OperatorDescriptor::Window { assigner, .. }) =
            plan.operators.get(operator_id as usize)
            && assigner.to_ascii_lowercase().contains("session")
        {
            return Err(anyhow!(
                "session window operator {} in job {} does not support online rescale",
                operator_id,
                job_id
            ));
        }
        Ok(())
    }

    pub fn trigger_checkpoint(&self, job_id: &str) -> Result<u64> {
        let expected_tasks = self
            .job_expected_tasks
            .read()
            .expect("job_expected_tasks poisoned")
            .get(job_id)
            .cloned()
            .ok_or_else(|| anyhow!("job {job_id} not found"))?;

        let barrier = self
            .checkpoint_coordinator
            .trigger_checkpoint(current_unix_millis(), expected_tasks)?;
        self.checkpoint_to_job
            .lock()
            .expect("checkpoint_to_job poisoned")
            .insert(barrier.checkpoint_id, job_id.to_string());
        Ok(barrier.checkpoint_id)
    }

    fn expected_task_for_job(&self, job_id: &str) -> Option<TaskId> {
        self.job_expected_tasks
            .read()
            .expect("job_expected_tasks poisoned")
            .get(job_id)
            .and_then(|tasks| tasks.first().copied())
    }

    fn abort_checkpoint_best_effort(&self, checkpoint_id: u64, job_id: &str, reason: &str) {
        if let Some(task_id) = self.expected_task_for_job(job_id) {
            let _ = self
                .checkpoint_coordinator
                .abort_checkpoint(checkpoint_id, task_id, reason);
        }
        self.checkpoint_to_job
            .lock()
            .expect("checkpoint_to_job poisoned")
            .remove(&checkpoint_id);
    }

    pub fn acknowledge_checkpoint(&self, ack: TaskCheckpointAck) -> Result<bool> {
        self.checkpoint_coordinator.acknowledge_checkpoint(ack)
    }

    pub fn abort_checkpoint(&self, abort: TaskCheckpointAbort) -> Result<bool> {
        self.checkpoint_coordinator.abort_checkpoint(
            abort.checkpoint_id,
            abort.task_id,
            &abort.reason,
        )
    }

    pub fn task_manager_count(&self) -> usize {
        self.task_managers
            .read()
            .expect("task_managers poisoned")
            .len()
    }

    pub fn assign_tasks_to_task_managers(
        &self,
        task_ids: &[String],
    ) -> Result<HashMap<String, TmId>> {
        let managers = self.task_managers.read().expect("task_managers poisoned");
        let mut scheduler = self.scheduler.lock().expect("scheduler poisoned");
        scheduler.schedule(task_ids, &managers)
    }

    fn task_manager_endpoint(&self, tm_id: &str) -> Result<String> {
        let managers = self.task_managers.read().expect("task_managers poisoned");
        let info = managers
            .get(tm_id)
            .ok_or_else(|| anyhow!("task manager {} not found", tm_id))?;
        if info.address.starts_with("http://") || info.address.starts_with("https://") {
            Ok(info.address.clone())
        } else {
            Ok(format!("http://{}", info.address))
        }
    }

    async fn deploy_job_to_task_managers(
        &self,
        task_to_tm: &HashMap<String, TmId>,
        job_plan_bytes: Vec<u8>,
    ) -> Result<Vec<TaskLocation>> {
        let mut ordered_assignments: Vec<_> = task_to_tm.iter().collect();
        ordered_assignments.sort_by(|(task_a, _), (task_b, _)| task_a.cmp(task_b));

        let mut task_locations = Vec::with_capacity(ordered_assignments.len());
        let mut deployed_so_far: Vec<(String, String)> = Vec::new();

        for (task_id, tm_id) in ordered_assignments {
            let deploy_result = async {
                let mut client =
                    TaskManagerServiceClient::connect(self.task_manager_endpoint(tm_id)?).await?;
                let task = parse_task_id(task_id)
                    .ok_or_else(|| anyhow!("invalid task id generated: {}", task_id))?;
                let descriptor = TaskDeploymentDescriptor {
                    task_id: task_id.clone(),
                    vertex_id: task.vertex_id.0,
                    subtask_index: task.subtask_index as u32,
                    operator_bytes: job_plan_bytes.clone(),
                    input_channels: vec![],
                    output_channels: vec![],
                    output_peer_tms: vec![],
                };
                client
                    .deploy_task(Request::new(descriptor))
                    .await
                    .map(|r| r.into_inner())
                    .map_err(anyhow::Error::from)
            }
            .await;

            match deploy_result {
                Ok(result) if result.success => {
                    deployed_so_far.push((task_id.clone(), tm_id.clone()));
                    task_locations.push(TaskLocation {
                        task_id: task_id.clone(),
                        tm_id: tm_id.clone(),
                    });
                }
                Ok(result) => {
                    self.cancel_deployed_tasks_best_effort(&deployed_so_far)
                        .await;
                    return Err(anyhow!(
                        "deploy task {} to {} failed: {}",
                        task_id,
                        tm_id,
                        result.error_message
                    ));
                }
                Err(err) => {
                    self.cancel_deployed_tasks_best_effort(&deployed_so_far)
                        .await;
                    return Err(anyhow!(
                        "deploy task {} to {} RPC failed: {}",
                        task_id,
                        tm_id,
                        err
                    ));
                }
            }
        }

        Ok(task_locations)
    }

    async fn cancel_deployed_tasks_best_effort(&self, deployed: &[(String, String)]) {
        for (task_id, tm_id) in deployed {
            let endpoint = match self.task_manager_endpoint(tm_id) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Ok(mut client) = TaskManagerServiceClient::connect(endpoint).await {
                let _ = client
                    .cancel_task(Request::new(crate::cluster::rpc::CancelTaskRequest {
                        task_id: task_id.clone(),
                    }))
                    .await;
            }
        }
    }

    fn latest_completed_checkpoint_for_job(&self, job_id: &str) -> Result<Option<u64>> {
        let completed = self.checkpoint_coordinator.completed_checkpoint_ids()?;
        let cp_to_job = self
            .checkpoint_to_job
            .lock()
            .expect("checkpoint_to_job poisoned");
        Ok(latest_completed_checkpoint_for_job(
            &completed, &cp_to_job, job_id,
        ))
    }

    async fn cancel_job_tasks_best_effort(&self, job_id: &str) {
        let locations = self
            .job_task_locations
            .read()
            .expect("job_task_locations poisoned")
            .get(job_id)
            .cloned()
            .unwrap_or_default();
        for location in locations {
            let endpoint = match self.task_manager_endpoint(&location.tm_id) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Ok(mut client) = TaskManagerServiceClient::connect(endpoint).await {
                let _ = client
                    .cancel_task(Request::new(crate::cluster::rpc::CancelTaskRequest {
                        task_id: location.task_id,
                    }))
                    .await;
            }
        }
    }

    async fn redeploy_job(&self, job_id: &str) -> Result<()> {
        let parallelism = *self
            .job_parallelism
            .read()
            .expect("job_parallelism poisoned")
            .get(job_id)
            .ok_or_else(|| anyhow!("parallelism not found for job {}", job_id))?;
        let task_ids: Vec<String> = (0..parallelism)
            .map(|i| deployment_task_id(job_id, TaskId::new(VertexId::new(0), i)))
            .collect();
        let assignments = self.assign_tasks_to_task_managers(&task_ids)?;
        let plan = self
            .job_plans
            .read()
            .expect("job_plans poisoned")
            .get(job_id)
            .cloned()
            .unwrap_or_default();
        let task_locations = self.deploy_job_to_task_managers(&assignments, plan).await?;
        self.job_task_locations
            .write()
            .expect("job_task_locations poisoned")
            .insert(job_id.to_string(), task_locations);
        let mut task_to_job = self.task_to_job.write().expect("task_to_job poisoned");
        for task_id in task_ids {
            task_to_job.insert(task_id, job_id.to_string());
        }
        Ok(())
    }

    pub async fn submit_job_and_deploy(
        &self,
        job_plan: Vec<u8>,
        parallelism: usize,
    ) -> Result<JobId> {
        let parsed_plan = if job_plan.is_empty() {
            None
        } else {
            let plan = JobPlan::from_bytes(&job_plan)?;
            Self::validate_sink_guarantees(&plan)?;
            Some(plan)
        };

        let job_id = self.submit_job(parallelism)?;
        let task_ids: Vec<String> = (0..parallelism)
            .map(|i| deployment_task_id(&job_id, TaskId::new(VertexId::new(0), i)))
            .collect();
        let assignments = self.assign_tasks_to_task_managers(&task_ids)?;
        let task_locations = match self
            .deploy_job_to_task_managers(&assignments, job_plan.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                self.jobs
                    .write()
                    .expect("jobs poisoned")
                    .insert(job_id.clone(), JobStatus::Failed);
                self.job_task_locations
                    .write()
                    .expect("job_task_locations poisoned")
                    .remove(&job_id);
                return Err(e);
            }
        };

        self.job_plans
            .write()
            .expect("job_plans poisoned")
            .insert(job_id.clone(), job_plan);
        self.update_job_operator_runtime(&job_id, parsed_plan.as_ref());
        self.job_task_locations
            .write()
            .expect("job_task_locations poisoned")
            .insert(job_id.clone(), task_locations);
        self.jobs
            .write()
            .expect("jobs poisoned")
            .insert(job_id.clone(), JobStatus::Running);

        let mut task_to_job = self.task_to_job.write().expect("task_to_job poisoned");
        for task_id in task_ids {
            task_to_job.insert(task_id, job_id.clone());
        }
        Ok(job_id)
    }

    pub async fn trigger_checkpoint_and_dispatch(&self, job_id: &str) -> Result<u64> {
        let checkpoint_id = self.trigger_checkpoint(job_id)?;
        let locations_result = self
            .job_task_locations
            .read()
            .expect("job_task_locations poisoned")
            .get(job_id)
            .cloned()
            .ok_or_else(|| anyhow!("job {} has no deployed tasks", job_id));
        let locations = match locations_result {
            Ok(v) => v,
            Err(err) => {
                self.abort_checkpoint_best_effort(checkpoint_id, job_id, "dispatch failed");
                return Err(err);
            }
        };
        let mut tasks_by_tm: HashMap<String, Vec<String>> = HashMap::new();
        for location in locations {
            tasks_by_tm
                .entry(location.tm_id)
                .or_default()
                .push(location.task_id);
        }
        for (tm_id, task_ids) in tasks_by_tm {
            let dispatch = async {
                let mut client =
                    TaskManagerServiceClient::connect(self.task_manager_endpoint(&tm_id)?).await?;
                client
                    .trigger_checkpoint(Request::new(CheckpointBarrier {
                        checkpoint_id,
                        timestamp: current_unix_millis(),
                        job_id: job_id.to_string(),
                        task_ids,
                    }))
                    .await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(err) = dispatch {
                self.abort_checkpoint_best_effort(checkpoint_id, job_id, "dispatch failed");
                return Err(err);
            }
        }
        Ok(checkpoint_id)
    }

    async fn prepare_new_tasks_for_rescale(
        &self,
        job_id: &str,
        old_parallelism: usize,
        new_parallelism: usize,
    ) -> Result<Vec<TaskLocation>> {
        if new_parallelism <= old_parallelism {
            return Ok(Vec::new());
        }
        let task_ids: Vec<String> = (old_parallelism..new_parallelism)
            .map(|i| deployment_task_id(job_id, TaskId::new(VertexId::new(0), i)))
            .collect();
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let assignments = self.assign_tasks_to_task_managers(&task_ids)?;
        let plan = self
            .job_plans
            .read()
            .expect("job_plans poisoned")
            .get(job_id)
            .cloned()
            .unwrap_or_default();
        self.deploy_job_to_task_managers(&assignments, plan).await
    }

    async fn rollback_prepared_tasks_best_effort(&self, prepared: &[TaskLocation]) {
        for location in prepared {
            let endpoint = match self.task_manager_endpoint(&location.tm_id) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Ok(mut client) = TaskManagerServiceClient::connect(endpoint).await {
                let _ = client
                    .cancel_task(Request::new(crate::cluster::rpc::CancelTaskRequest {
                        task_id: location.task_id.clone(),
                    }))
                    .await;
            }
        }
    }

    async fn activate_rescaled_topology(
        &self,
        job_id: &str,
        old_parallelism: usize,
        new_parallelism: usize,
        prepared_locations: &[TaskLocation],
    ) -> Result<()> {
        let mut removed_task_ids: HashSet<String> = HashSet::new();
        let mut removed_locations: Vec<TaskLocation> = Vec::new();
        {
            let mut locations_guard = self
                .job_task_locations
                .write()
                .expect("job_task_locations poisoned");
            let locations = locations_guard
                .get_mut(job_id)
                .ok_or_else(|| anyhow!("job {} has no deployed task locations", job_id))?;

            if new_parallelism > old_parallelism {
                for loc in prepared_locations {
                    if !locations
                        .iter()
                        .any(|existing| existing.task_id == loc.task_id)
                    {
                        locations.push(loc.clone());
                    }
                }
            } else if new_parallelism < old_parallelism {
                removed_task_ids = (new_parallelism..old_parallelism)
                    .map(|i| deployment_task_id(job_id, TaskId::new(VertexId::new(0), i)))
                    .collect();
                removed_locations = locations
                    .iter()
                    .filter(|loc| removed_task_ids.contains(&loc.task_id))
                    .cloned()
                    .collect();
                locations.retain(|loc| !removed_task_ids.contains(&loc.task_id));
            }
        }

        if !removed_locations.is_empty() {
            self.rollback_prepared_tasks_best_effort(&removed_locations)
                .await;
        }
        if !removed_task_ids.is_empty() {
            let mut task_to_job = self.task_to_job.write().expect("task_to_job poisoned");
            for task_id in removed_task_ids {
                task_to_job.remove(&task_id);
            }
        }

        self.job_parallelism
            .write()
            .expect("job_parallelism poisoned")
            .insert(job_id.to_string(), new_parallelism);
        self.job_expected_tasks
            .write()
            .expect("job_expected_tasks poisoned")
            .insert(
                job_id.to_string(),
                (0..new_parallelism)
                    .map(|i| TaskId::new(VertexId::new(0), i))
                    .collect(),
            );
        let mut task_to_job = self.task_to_job.write().expect("task_to_job poisoned");
        for i in 0..new_parallelism {
            task_to_job.insert(
                deployment_task_id(job_id, TaskId::new(VertexId::new(0), i)),
                job_id.to_string(),
            );
        }
        Ok(())
    }

    async fn wait_checkpoint_completed(&self, checkpoint_id: u64, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self
                .checkpoint_coordinator
                .completed_checkpoint_ids()?
                .contains(&checkpoint_id)
            {
                return Ok(());
            }
            if self
                .checkpoint_coordinator
                .aborted_checkpoint_ids()?
                .contains(&checkpoint_id)
            {
                return Err(anyhow!(
                    "checkpoint {} aborted while waiting for rescale",
                    checkpoint_id
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "checkpoint {} did not complete before timeout {:?}",
                    checkpoint_id,
                    timeout
                ));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn trigger_rescale(
        &self,
        job_id: &str,
        operator_id: u32,
        new_parallelism: usize,
    ) -> Result<u64> {
        let status = self.get_job_status(job_id);
        if status.is_none() {
            return Err(anyhow!("job {job_id} not found"));
        }
        if !matches!(status, Some(JobStatus::Running | JobStatus::Created)) {
            return Err(anyhow!("job {job_id} is not in a rescalable state"));
        }
        self.validate_rescale_preconditions(job_id, operator_id)?;
        let old_parallelism = *self
            .job_parallelism
            .read()
            .expect("job_parallelism poisoned")
            .get(job_id)
            .ok_or_else(|| anyhow!("parallelism not found for job {}", job_id))?;
        if old_parallelism == new_parallelism {
            return Err(anyhow!(
                "job {} already runs at parallelism {}",
                job_id,
                new_parallelism
            ));
        }

        let plan = crate::elastic::RescalePlan {
            job_id: job_id.to_string(),
            operator_id,
            new_parallelism,
        };
        let generation = self
            .rescale_coordinator
            .lock()
            .expect("rescale_coordinator poisoned")
            .begin_rescale(&plan)?;

        let prepared_locations = match self
            .prepare_new_tasks_for_rescale(job_id, old_parallelism, new_parallelism)
            .await
        {
            Ok(locations) => locations,
            Err(err) => {
                let _ = self
                    .rescale_coordinator
                    .lock()
                    .expect("rescale_coordinator poisoned")
                    .mark_failed(job_id, generation, err.to_string());
                return Err(err);
            }
        };

        let mark_prepared_result = {
            self.rescale_coordinator
                .lock()
                .expect("rescale_coordinator poisoned")
                .mark_prepared(
                    job_id,
                    generation,
                    prepared_locations
                        .iter()
                        .map(|loc| loc.task_id.clone())
                        .collect(),
                )
        };
        if let Err(err) = mark_prepared_result {
            self.rollback_prepared_tasks_best_effort(&prepared_locations)
                .await;
            return Err(err);
        }

        let checkpoint_id = match self.trigger_checkpoint_and_dispatch(job_id).await {
            Ok(id) => id,
            Err(err) => {
                let _ = self
                    .rescale_coordinator
                    .lock()
                    .expect("rescale_coordinator poisoned")
                    .mark_failed(job_id, generation, err.to_string());
                self.rollback_prepared_tasks_best_effort(&prepared_locations)
                    .await;
                return Err(err);
            }
        };

        let mark_barrier_result = {
            self.rescale_coordinator
                .lock()
                .expect("rescale_coordinator poisoned")
                .mark_barrier_injected(job_id, generation, checkpoint_id)
        };
        if let Err(err) = mark_barrier_result {
            self.abort_checkpoint_best_effort(checkpoint_id, job_id, "invalid rescale state");
            self.rollback_prepared_tasks_best_effort(&prepared_locations)
                .await;
            return Err(err);
        }

        if let Err(err) = self
            .wait_checkpoint_completed(checkpoint_id, Duration::from_secs(5))
            .await
        {
            let _ = self
                .rescale_coordinator
                .lock()
                .expect("rescale_coordinator poisoned")
                .mark_failed(job_id, generation, err.to_string());
            self.abort_checkpoint_best_effort(checkpoint_id, job_id, "rescale timeout");
            self.rollback_prepared_tasks_best_effort(&prepared_locations)
                .await;
            return Err(err);
        }

        if let Err(err) = {
            let mut coordinator = self
                .rescale_coordinator
                .lock()
                .expect("rescale_coordinator poisoned");
            coordinator.mark_aligned(job_id, generation)?;
            coordinator.mark_flushed(job_id, generation)?;
            coordinator.mark_switched(job_id, generation)?;
            Ok::<(), anyhow::Error>(())
        } {
            let _ = self
                .rescale_coordinator
                .lock()
                .expect("rescale_coordinator poisoned")
                .mark_failed(job_id, generation, err.to_string());
            self.rollback_prepared_tasks_best_effort(&prepared_locations)
                .await;
            return Err(err);
        }

        if let Err(err) = self
            .activate_rescaled_topology(
                job_id,
                old_parallelism,
                new_parallelism,
                &prepared_locations,
            )
            .await
        {
            let _ = self
                .rescale_coordinator
                .lock()
                .expect("rescale_coordinator poisoned")
                .mark_failed(job_id, generation, err.to_string());
            self.rollback_prepared_tasks_best_effort(&prepared_locations)
                .await;
            return Err(err);
        }

        self.rescale_coordinator
            .lock()
            .expect("rescale_coordinator poisoned")
            .mark_activated(job_id, generation)?;
        Ok(generation)
    }

    async fn notify_checkpoint_complete(&self, checkpoint_id: u64) -> Result<()> {
        let job_id = self
            .checkpoint_to_job
            .lock()
            .expect("checkpoint_to_job poisoned")
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| anyhow!("checkpoint {} has no job mapping", checkpoint_id))?;
        let locations = self
            .job_task_locations
            .read()
            .expect("job_task_locations poisoned")
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let unique_tm_ids: HashSet<_> = locations.into_iter().map(|l| l.tm_id).collect();
        for tm_id in unique_tm_ids {
            let mut client =
                TaskManagerServiceClient::connect(self.task_manager_endpoint(&tm_id)?).await?;
            client
                .notify_checkpoint_complete(Request::new(CheckpointComplete { checkpoint_id }))
                .await?;
        }
        Ok(())
    }

    pub async fn acknowledge_checkpoint_and_notify(&self, ack: TaskCheckpointAck) -> Result<bool> {
        let checkpoint_id = ack.checkpoint_id;
        let finished = self.acknowledge_checkpoint(ack)?;
        if finished {
            self.commit_source_offset_for_checkpoint(checkpoint_id);
        }
        if finished
            && let Some(client) = &self.state_service_client
            && let Err(err) = client.seal(checkpoint_id)
        {
            tracing::error!(
                "checkpoint {} finalized but state service seal failed: {}",
                checkpoint_id,
                err
            );
        }
        if finished && let Err(err) = self.notify_checkpoint_complete(checkpoint_id).await {
            tracing::error!(
                "checkpoint {} finalized but notify complete failed: {}",
                checkpoint_id,
                err
            );
        }
        Ok(finished)
    }

    pub async fn handle_task_manager_failures(&self, removed_tm_ids: &[TmId]) -> Result<()> {
        if removed_tm_ids.is_empty() {
            return Ok(());
        }
        let removed: HashSet<TmId> = removed_tm_ids.iter().cloned().collect();
        let affected_jobs: Vec<JobId> = collect_affected_jobs(
            &self
                .job_task_locations
                .read()
                .expect("job_task_locations poisoned"),
            &removed,
        );

        for job_id in affected_jobs {
            self.jobs
                .write()
                .expect("jobs poisoned")
                .insert(job_id.clone(), JobStatus::Cancelling);
            self.rollback_source_offset_to_committed(&job_id);
            self.cancel_job_tasks_best_effort(&job_id).await;
            let latest = self.latest_completed_checkpoint_for_job(&job_id)?;
            match self.redeploy_job(&job_id).await {
                Ok(()) => {
                    self.jobs
                        .write()
                        .expect("jobs poisoned")
                        .insert(job_id.clone(), JobStatus::Running);
                    tracing::info!(
                        "job {} recovered by global rollback, latest_checkpoint={:?}",
                        job_id,
                        latest
                    );
                }
                Err(err) => {
                    self.jobs
                        .write()
                        .expect("jobs poisoned")
                        .insert(job_id.clone(), JobStatus::Failed);
                    tracing::error!("job {} recovery failed: {}", job_id, err);
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_checkpoint_count(&self) -> Result<usize> {
        Ok(self
            .checkpoint_coordinator
            .pending_checkpoints
            .lock()
            .map_err(|_| anyhow!("pending_checkpoints lock poisoned"))?
            .len())
    }

    #[cfg(test)]
    pub(crate) fn aborted_checkpoint_ids(&self) -> Result<Vec<u64>> {
        self.checkpoint_coordinator.aborted_checkpoint_ids()
    }

    #[cfg(test)]
    pub(crate) fn job_parallelism_for_test(&self, job_id: &str) -> Option<usize> {
        self.job_parallelism
            .read()
            .expect("job_parallelism poisoned")
            .get(job_id)
            .copied()
    }

    #[cfg(test)]
    pub(crate) fn latest_rescale_generation_for_test(&self, job_id: &str) -> Option<u64> {
        self.rescale_coordinator
            .lock()
            .expect("rescale_coordinator poisoned")
            .latest_generation(job_id)
    }

    #[cfg(test)]
    pub(crate) fn source_offset_for_test(&self, job_id: &str) -> Option<u64> {
        self.job_source_offsets
            .read()
            .expect("job_source_offsets poisoned")
            .get(job_id)
            .copied()
    }

    #[cfg(test)]
    pub(crate) fn committed_source_offset_for_test(&self, job_id: &str) -> Option<u64> {
        self.job_committed_source_offsets
            .read()
            .expect("job_committed_source_offsets poisoned")
            .get(job_id)
            .copied()
    }
}

#[derive(Clone)]
pub struct JobManagerRpc {
    inner: Arc<JobManager>,
}

impl JobManagerRpc {
    pub fn new(inner: Arc<JobManager>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl JobManagerService for JobManagerRpc {
    async fn register_task_manager(
        &self,
        request: Request<RegisterTmRequest>,
    ) -> Result<Response<RegisterTmResponse>, Status> {
        let req = request.into_inner();
        let inserted = self.inner.register_task_manager(
            req.tm_id.clone(),
            req.address,
            req.num_slots as usize,
            req.resources.into(),
        );
        let message = if inserted {
            "registered".to_string()
        } else {
            "updated".to_string()
        };
        Ok(Response::new(RegisterTmResponse {
            accepted: true,
            message,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let alive = self
            .inner
            .heartbeat(&req.tm_id, req.queue_usage, req.throughput);
        let removed = self.inner.evict_stale_task_managers();
        self.inner
            .handle_task_manager_failures(&removed)
            .await
            .map_err(|e| Status::internal(format!("failure handling failed: {e}")))?;
        if let Err(err) = self.inner.run_autoscaler_once_if_due().await {
            tracing::warn!("autoscaler loop failed in heartbeat: {}", err);
        }
        Ok(Response::new(HeartbeatResponse { alive }))
    }

    async fn ack_checkpoint(
        &self,
        request: Request<CheckpointAck>,
    ) -> Result<Response<Empty>, Status> {
        let ack = request.into_inner();
        let task_id = parse_task_id(&ack.task_id)
            .ok_or_else(|| Status::invalid_argument("invalid task_id format"))?;
        self.inner
            .acknowledge_checkpoint_and_notify(TaskCheckpointAck {
                checkpoint_id: ack.checkpoint_id,
                task_id,
                state: ack.state_bytes,
            })
            .await
            .map_err(|e| Status::failed_precondition(format!("ack failed: {e}")))?;
        Ok(Response::new(Empty {}))
    }

    async fn report_failure(
        &self,
        request: Request<FailureReport>,
    ) -> Result<Response<Empty>, Status> {
        let report = request.into_inner();
        let mut affected_jobs: HashSet<JobId> = HashSet::new();
        if !report.task_id.is_empty() {
            if let Some(job_id) = extract_job_id_from_deployment_task_id(&report.task_id) {
                affected_jobs.insert(job_id.to_string());
            }
            if let Some(mapped_job) = self
                .inner
                .task_to_job
                .read()
                .expect("task_to_job poisoned")
                .get(&report.task_id)
                .cloned()
            {
                affected_jobs.insert(mapped_job);
            }
        }
        if affected_jobs.is_empty() && !report.tm_id.is_empty() {
            let locations = self
                .inner
                .job_task_locations
                .read()
                .expect("job_task_locations poisoned");
            for (job_id, task_locations) in locations.iter() {
                if task_locations.iter().any(|loc| loc.tm_id == report.tm_id) {
                    affected_jobs.insert(job_id.clone());
                }
            }
        }

        if !affected_jobs.is_empty() {
            let mut jobs = self.inner.jobs.write().expect("jobs poisoned");
            for job_id in affected_jobs {
                if let Some(status) = jobs.get_mut(&job_id)
                    && matches!(*status, JobStatus::Created | JobStatus::Running)
                {
                    *status = JobStatus::Failed;
                }
            }
        }
        tracing::warn!(
            "failure reported: tm_id={}, task_id={}, reason={}",
            report.tm_id,
            report.task_id,
            report.reason
        );
        Ok(Response::new(Empty {}))
    }

    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = self
            .inner
            .submit_job_and_deploy(req.job_plan, req.parallelism as usize)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(SubmitJobResponse {
            job_id,
            accepted: true,
            error_message: String::new(),
        }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let req = request.into_inner();
        let status = self
            .inner
            .get_job_status(&req.job_id)
            .unwrap_or(JobStatus::Unspecified);
        Ok(Response::new(GetJobStatusResponse {
            job_id: req.job_id,
            status: i32::from(status),
            message: String::new(),
        }))
    }

    async fn trigger_checkpoint(
        &self,
        request: Request<TriggerCheckpointRequest>,
    ) -> Result<Response<TriggerCheckpointResponse>, Status> {
        let req = request.into_inner();
        match self
            .inner
            .trigger_checkpoint_and_dispatch(&req.job_id)
            .await
        {
            Ok(checkpoint_id) => Ok(Response::new(TriggerCheckpointResponse {
                accepted: true,
                checkpoint_id,
                message: "triggered".to_string(),
            })),
            Err(e) => Ok(Response::new(TriggerCheckpointResponse {
                accepted: false,
                checkpoint_id: 0,
                message: e.to_string(),
            })),
        }
    }

    async fn list_task_managers(
        &self,
        _request: Request<ListTaskManagersRequest>,
    ) -> Result<Response<ListTaskManagersResponse>, Status> {
        Ok(Response::new(ListTaskManagersResponse {
            task_managers: self.inner.list_task_managers(),
        }))
    }

    async fn trigger_rescale(
        &self,
        request: Request<TriggerRescaleRequest>,
    ) -> Result<Response<TriggerRescaleResponse>, Status> {
        let req = request.into_inner();
        match self
            .inner
            .trigger_rescale(&req.job_id, req.operator_id, req.new_parallelism as usize)
            .await
        {
            Ok(generation) => Ok(Response::new(TriggerRescaleResponse {
                accepted: true,
                message: "triggered".to_string(),
                generation,
            })),
            Err(err) => Ok(Response::new(TriggerRescaleResponse {
                accepted: false,
                message: err.to_string(),
                generation: 0,
            })),
        }
    }
}

fn deployment_task_id(job_id: &str, task_id: TaskId) -> String {
    format!("{job_id}::{}", task_id)
}

fn extract_job_id_from_deployment_task_id(task_id: &str) -> Option<&str> {
    task_id.split_once("::").map(|(job_id, _)| job_id)
}

fn parse_task_id(task_id: &str) -> Option<TaskId> {
    let raw = task_id
        .split_once("::")
        .map(|(_, suffix)| suffix)
        .unwrap_or(task_id);
    let suffix = raw.strip_prefix("vertex_")?;
    let (vertex_id, subtask_index) = suffix.rsplit_once('_')?;
    let vertex_id: u32 = vertex_id.parse().ok()?;
    let subtask_index: usize = subtask_index.parse().ok()?;
    Some(TaskId::new(VertexId::new(vertex_id), subtask_index))
}
