use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

use crate::channel::{LocalChannelReceiver, local_channel_default};
use crate::cluster::rpc::job_manager_service_client::JobManagerServiceClient;
use crate::cluster::rpc::task_manager_service_server::{
    TaskManagerService, TaskManagerServiceServer,
};
use crate::cluster::rpc::{
    CancelTaskRequest, CheckpointAck, CheckpointBarrier, CheckpointComplete, DeploymentResult,
    Empty, HeartbeatRequest, RegisterTmRequest,
};
use crate::network::{Frame, NetworkManager, RemoteInputGate, RemoteOutputGate};
use crate::runtime::descriptors::JobPlan;
use crate::types::StreamElement;

use super::{HeartbeatConfig, OperatorFactory, Resources, TmId, current_unix_millis};

pub struct TaskManager {
    pub tm_id: TmId,
    pub jm_endpoint: String,
    pub heartbeat: HeartbeatConfig,
    pub address: String,
    pub num_slots: usize,
    pub resources: Resources,
    data_plane: NetworkManager,
    deployed_tasks: Mutex<HashMap<String, DeployedTaskBridge>>,
    input_channel_to_task: Mutex<HashMap<u32, String>>,
    incoming_pumps: Mutex<HashMap<String, Vec<JoinHandle<()>>>>,
    peer_ingress_states: Mutex<HashMap<String, PeerIngressState>>,
    next_peer_generation: AtomicU64,
    operator_factory: Mutex<OperatorFactory>,
    triggered_checkpoints: Mutex<Vec<u64>>,
    completed_checkpoints: Mutex<Vec<u64>>,
    running_tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    heartbeat_attempts: AtomicU64,
}

struct DeployedTaskBridge {
    remote_input_gate: RemoteInputGate,
    remote_output_gate: RemoteOutputGate,
    input_channel_ids: Vec<u32>,
    input_receivers: HashMap<u32, LocalChannelReceiver<Vec<u8>>>,
}

#[derive(Debug, Default)]
struct PeerIngressState {
    active_generation: Option<u64>,
    active_generation_pumps: usize,
    pending_generation: Option<u64>,
    pending_generation_pumps: usize,
    // Frames from current active generation buffered during promotion drain.
    active_buffer: VecDeque<Frame>,
    // Frames from pending generation before promotion.
    pending_buffer: VecDeque<Frame>,
    // While true, active generation frames must be buffered to preserve switch boundary order.
    pause_active_forwarding: bool,
}

enum PeerFrameAction {
    Forward(Frame),
    Buffered,
    Dropped,
}

impl DeployedTaskBridge {
    fn from_descriptor(descriptor: &crate::cluster::rpc::TaskDeploymentDescriptor) -> Result<Self> {
        let mut input_senders = HashMap::new();
        let mut input_receivers = HashMap::new();
        for channel_id in &descriptor.input_channels {
            if input_senders.contains_key(channel_id) {
                return Err(anyhow!("duplicate input channel id {}", channel_id));
            }
            let (sender, receiver) = local_channel_default();
            input_senders.insert(*channel_id, sender);
            input_receivers.insert(*channel_id, receiver);
        }

        let mut seen_output = HashSet::new();
        for channel_id in &descriptor.output_channels {
            if !seen_output.insert(*channel_id) {
                return Err(anyhow!("duplicate output channel id {}", channel_id));
            }
        }

        Ok(Self {
            remote_input_gate: RemoteInputGate::new(input_senders),
            remote_output_gate: RemoteOutputGate::new(descriptor.output_channels.clone()),
            input_channel_ids: descriptor.input_channels.clone(),
            input_receivers,
        })
    }

    fn ingest_frame(&self, frame: Frame) -> Result<()> {
        self.remote_input_gate.ingest_frame(frame)
    }

    fn try_recv_input(&self, channel_id: u32) -> Result<Option<StreamElement<Vec<u8>>>> {
        let receiver = self
            .input_receivers
            .get(&channel_id)
            .ok_or_else(|| anyhow!("unknown input channel id {}", channel_id))?;
        receiver.try_recv()
    }

    fn encode_output(&self, output_index: usize, element: StreamElement<Vec<u8>>) -> Result<Frame> {
        self.remote_output_gate
            .encode_for_output(output_index, element)
    }

    fn input_channels(&self) -> Vec<u32> {
        self.input_channel_ids.clone()
    }

    fn take_input_receivers(&mut self) -> HashMap<u32, LocalChannelReceiver<Vec<u8>>> {
        std::mem::take(&mut self.input_receivers)
    }
}

impl TaskManager {
    const DEFAULT_DATA_PLANE_CAPACITY: usize = 1024;

    pub fn new(
        tm_id: TmId,
        jm_endpoint: String,
        address: String,
        num_slots: usize,
        resources: Resources,
        heartbeat: HeartbeatConfig,
    ) -> Self {
        Self {
            tm_id,
            jm_endpoint,
            heartbeat,
            address,
            num_slots,
            resources,
            data_plane: NetworkManager::new(Self::DEFAULT_DATA_PLANE_CAPACITY),
            deployed_tasks: Mutex::new(HashMap::new()),
            input_channel_to_task: Mutex::new(HashMap::new()),
            incoming_pumps: Mutex::new(HashMap::new()),
            peer_ingress_states: Mutex::new(HashMap::new()),
            next_peer_generation: AtomicU64::new(1),
            operator_factory: Mutex::new(
                OperatorFactory::new()
                    .with_udf("builtin::identity")
                    .with_udf("builtin::passthrough"),
            ),
            triggered_checkpoints: Mutex::new(Vec::new()),
            completed_checkpoints: Mutex::new(Vec::new()),
            running_tasks: Mutex::new(HashMap::new()),
            heartbeat_attempts: AtomicU64::new(0),
        }
    }

    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(TaskManagerServiceServer::new(TaskManagerRpc::new(self)))
            .serve(addr)
            .await?;
        Ok(())
    }

    pub async fn register_once(&self) -> Result<bool> {
        let mut client = JobManagerServiceClient::connect(self.jm_endpoint.clone()).await?;
        let response = client
            .register_task_manager(Request::new(RegisterTmRequest {
                tm_id: self.tm_id.clone(),
                address: self.address.clone(),
                num_slots: self.num_slots as u32,
                resources: Some(self.resources.clone().into()),
            }))
            .await?;
        Ok(response.into_inner().accepted)
    }

    pub async fn register_with_retry(
        &self,
        max_retries: usize,
        retry_delay: Duration,
    ) -> Result<()> {
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..max_retries {
            match self.register_once().await {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    last_err = Some(anyhow!("registration rejected"));
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
            tokio::time::sleep(retry_delay).await;
        }
        Err(last_err.unwrap_or_else(|| anyhow!("register_with_retry failed")))
    }

    pub async fn send_heartbeat(&self) -> Result<bool> {
        self.heartbeat_attempts.fetch_add(1, Ordering::Relaxed);
        let mut client = JobManagerServiceClient::connect(self.jm_endpoint.clone()).await?;
        let response = client
            .heartbeat(Request::new(HeartbeatRequest {
                tm_id: self.tm_id.clone(),
                timestamp: current_unix_millis(),
                queue_usage: 0,
                throughput: 0,
            }))
            .await?;
        Ok(response.into_inner().alive)
    }

    pub fn spawn_heartbeat_loop(self: Arc<Self>) -> (watch::Sender<bool>, JoinHandle<()>) {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let interval = self.heartbeat.interval;
        let this = Arc::clone(&self);

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let _ = this.send_heartbeat().await;
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_ok() && *shutdown_rx.borrow() {
                            break;
                        }
                        if changed.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        (shutdown_tx, handle)
    }

    pub fn deployed_task_count(&self) -> usize {
        self.deployed_tasks
            .lock()
            .expect("deployed_tasks poisoned")
            .len()
    }

    pub fn heartbeat_attempts(&self) -> u64 {
        self.heartbeat_attempts.load(Ordering::Relaxed)
    }

    pub fn ingest_remote_input_frame(&self, task_id: &str, frame: Frame) -> Result<()> {
        let guard = self.deployed_tasks.lock().expect("deployed_tasks poisoned");
        let bridge = guard
            .get(task_id)
            .ok_or_else(|| anyhow!("task {} not deployed", task_id))?;
        bridge.ingest_frame(frame)
    }

    pub fn try_recv_bridged_input(
        &self,
        task_id: &str,
        channel_id: u32,
    ) -> Result<Option<StreamElement<Vec<u8>>>> {
        let guard = self.deployed_tasks.lock().expect("deployed_tasks poisoned");
        let bridge = guard
            .get(task_id)
            .ok_or_else(|| anyhow!("task {} not deployed", task_id))?;
        bridge.try_recv_input(channel_id)
    }

    pub fn encode_bridged_output(
        &self,
        task_id: &str,
        output_index: usize,
        element: StreamElement<Vec<u8>>,
    ) -> Result<Frame> {
        let guard = self.deployed_tasks.lock().expect("deployed_tasks poisoned");
        let bridge = guard
            .get(task_id)
            .ok_or_else(|| anyhow!("task {} not deployed", task_id))?;
        bridge.encode_output(output_index, element)
    }

    pub fn route_incoming_frame(&self, frame: Frame) -> Result<()> {
        let channel_id = frame.channel_id;
        let task_id = {
            let guard = self
                .input_channel_to_task
                .lock()
                .expect("input_channel_to_task poisoned");
            guard
                .get(&channel_id)
                .cloned()
                .ok_or_else(|| anyhow!("no deployed task for input channel {}", channel_id))?
        };
        self.ingest_remote_input_frame(&task_id, frame)
    }

    fn begin_peer_generation(&self, peer_tm_id: &str) -> u64 {
        let mut states = self
            .peer_ingress_states
            .lock()
            .expect("peer_ingress_states poisoned");
        let state = states.entry(peer_tm_id.to_string()).or_default();

        if state.active_generation.is_none() {
            let generation = self.next_peer_generation.fetch_add(1, Ordering::Relaxed);
            state.active_generation = Some(generation);
            state.active_generation_pumps = 1;
            state.pending_generation = None;
            state.pending_generation_pumps = 0;
            state.active_buffer.clear();
            state.pending_buffer.clear();
            state.pause_active_forwarding = false;
            return generation;
        }

        if state.active_generation_pumps == 0 && state.pending_generation.is_none() {
            let generation = self.next_peer_generation.fetch_add(1, Ordering::Relaxed);
            state.active_generation = Some(generation);
            state.active_generation_pumps = 1;
            state.active_buffer.clear();
            state.pause_active_forwarding = false;
            return generation;
        }

        if let Some(pending_generation) = state.pending_generation {
            state.pending_generation_pumps += 1;
            return pending_generation;
        }

        let generation = self.next_peer_generation.fetch_add(1, Ordering::Relaxed);
        state.pending_generation = Some(generation);
        state.pending_generation_pumps = 1;
        generation
    }

    fn classify_peer_frame(
        &self,
        peer_tm_id: &str,
        generation: u64,
        frame: Frame,
    ) -> PeerFrameAction {
        let mut states = self
            .peer_ingress_states
            .lock()
            .expect("peer_ingress_states poisoned");
        let Some(state) = states.get_mut(peer_tm_id) else {
            return PeerFrameAction::Dropped;
        };

        if state.active_generation == Some(generation) {
            if state.pause_active_forwarding || !state.active_buffer.is_empty() {
                state.active_buffer.push_back(frame);
                return PeerFrameAction::Buffered;
            }
            return PeerFrameAction::Forward(frame);
        }

        if state.pending_generation == Some(generation) {
            state.pending_buffer.push_back(frame);
            return PeerFrameAction::Buffered;
        }

        PeerFrameAction::Dropped
    }

    fn close_peer_generation(&self, peer_tm_id: &str, generation: u64) -> bool {
        let mut states = self
            .peer_ingress_states
            .lock()
            .expect("peer_ingress_states poisoned");
        let mut remove_state = false;
        let mut need_drain = false;

        if let Some(state) = states.get_mut(peer_tm_id) {
            if state.active_generation == Some(generation) && state.active_generation_pumps > 0 {
                state.active_generation_pumps -= 1;
            } else if state.pending_generation == Some(generation)
                && state.pending_generation_pumps > 0
            {
                state.pending_generation_pumps -= 1;
            }

            if state.active_generation_pumps == 0
                && let Some(next_generation) = state.pending_generation.take()
            {
                state.active_generation = Some(next_generation);
                state.active_generation_pumps = state.pending_generation_pumps;
                state.pending_generation_pumps = 0;
                state.active_buffer = std::mem::take(&mut state.pending_buffer);
                state.pause_active_forwarding = !state.active_buffer.is_empty();
                need_drain = state.pause_active_forwarding;
            }

            remove_state = state.active_generation_pumps == 0
                && state.pending_generation.is_none()
                && state.active_buffer.is_empty()
                && state.pending_buffer.is_empty()
                && !state.pause_active_forwarding;
        }

        if remove_state {
            states.remove(peer_tm_id);
        }
        need_drain
    }

    fn take_active_buffered_frames(&self, peer_tm_id: &str) -> Vec<Frame> {
        let mut states = self
            .peer_ingress_states
            .lock()
            .expect("peer_ingress_states poisoned");
        let Some(state) = states.get_mut(peer_tm_id) else {
            return Vec::new();
        };
        if !state.pause_active_forwarding {
            return Vec::new();
        }
        let drained: Vec<Frame> = state.active_buffer.drain(..).collect();
        if drained.is_empty() {
            state.pause_active_forwarding = false;
        }
        drained
    }

    async fn drain_promoted_generation_frames(&self, peer_tm_id: &str) {
        loop {
            let drained = self.take_active_buffered_frames(peer_tm_id);
            if drained.is_empty() {
                break;
            }
            for frame in drained {
                if let Err(err) = self.route_incoming_frame(frame) {
                    tracing::warn!("drop drained promoted frame from {}: {}", peer_tm_id, err);
                }
            }
        }
    }

    pub fn spawn_incoming_frame_pump(
        self: Arc<Self>,
        incoming_rx: mpsc::Receiver<Frame>,
    ) -> JoinHandle<()> {
        let peer_tm_id = "__direct_pump__".to_string();
        let generation = self.begin_peer_generation(&peer_tm_id);
        self.spawn_incoming_frame_pump_with_generation(peer_tm_id, generation, incoming_rx)
    }

    fn spawn_incoming_frame_pump_with_generation(
        self: Arc<Self>,
        peer_tm_id: String,
        generation: u64,
        mut incoming_rx: mpsc::Receiver<Frame>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(frame) = incoming_rx.recv().await {
                match self.classify_peer_frame(&peer_tm_id, generation, frame) {
                    PeerFrameAction::Forward(frame) => {
                        if let Err(err) = self.route_incoming_frame(frame) {
                            tracing::warn!("drop incoming frame: {err}");
                        }
                    }
                    PeerFrameAction::Buffered => {}
                    PeerFrameAction::Dropped => {
                        tracing::warn!(
                            "drop incoming frame from stale generation: peer={}, generation={}",
                            peer_tm_id,
                            generation
                        );
                    }
                }
            }
            let need_drain = self.close_peer_generation(&peer_tm_id, generation);
            if need_drain {
                self.drain_promoted_generation_frames(&peer_tm_id).await;
            }
        })
    }

    fn register_incoming_pump(&self, peer_tm_id: String, pump: JoinHandle<()>) {
        let mut pumps = self.incoming_pumps.lock().expect("incoming_pumps poisoned");
        let entry = pumps.entry(peer_tm_id).or_default();
        entry.retain(|handle| !handle.is_finished());
        entry.push(pump);
    }

    /// Connect to a peer TM data-plane endpoint and start consuming incoming frames.
    pub async fn connect_data_plane_peer(
        self: &Arc<Self>,
        peer_tm_id: String,
        peer_addr: SocketAddr,
    ) -> Result<()> {
        let generation = self.begin_peer_generation(&peer_tm_id);
        let incoming_rx = self
            .data_plane
            .connect(peer_tm_id.clone(), peer_addr)
            .await?;
        let pump = Arc::clone(self).spawn_incoming_frame_pump_with_generation(
            peer_tm_id.clone(),
            generation,
            incoming_rx,
        );
        self.register_incoming_pump(peer_tm_id, pump);
        Ok(())
    }

    /// Attach an accepted incoming data-plane TCP stream from peer TM.
    pub async fn attach_incoming_data_plane_stream(
        self: &Arc<Self>,
        peer_tm_id: String,
        stream: TcpStream,
    ) -> Result<()> {
        let generation = self.begin_peer_generation(&peer_tm_id);
        let (conn, incoming_rx) =
            crate::network::TcpConnection::from_stream(stream, Self::DEFAULT_DATA_PLANE_CAPACITY)
                .await?;
        self.data_plane.insert_connection(peer_tm_id.clone(), conn);
        let pump = Arc::clone(self).spawn_incoming_frame_pump_with_generation(
            peer_tm_id.clone(),
            generation,
            incoming_rx,
        );
        self.register_incoming_pump(peer_tm_id, pump);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn incoming_pump_count_for_peer(&self, peer_tm_id: &str) -> usize {
        self.incoming_pumps
            .lock()
            .expect("incoming_pumps poisoned")
            .get(peer_tm_id)
            .map(|handles| handles.iter().filter(|h| !h.is_finished()).count())
            .unwrap_or(0)
    }

    pub async fn send_data_frame_to_peer(&self, peer_tm_id: &str, frame: Frame) -> Result<()> {
        self.data_plane.send_data(peer_tm_id, frame).await
    }

    pub fn send_control_frame_to_peer(&self, peer_tm_id: &str, frame: Frame) -> Result<()> {
        self.data_plane.send_control(peer_tm_id, frame)
    }

    pub async fn forward_bridged_output_to_peer(
        &self,
        task_id: &str,
        output_index: usize,
        peer_tm_id: &str,
        element: StreamElement<Vec<u8>>,
    ) -> Result<()> {
        let frame = self.encode_bridged_output(task_id, output_index, element)?;
        match frame.frame_type {
            crate::network::FrameType::Data => {
                self.send_data_frame_to_peer(peer_tm_id, frame).await
            }
            _ => self.send_control_frame_to_peer(peer_tm_id, frame),
        }
    }

    pub fn completed_checkpoint_ids(&self) -> Vec<u64> {
        self.completed_checkpoints
            .lock()
            .expect("completed_checkpoints poisoned")
            .clone()
    }

    pub fn triggered_checkpoint_ids(&self) -> Vec<u64> {
        self.triggered_checkpoints
            .lock()
            .expect("triggered_checkpoints poisoned")
            .clone()
    }

    fn spawn_task_runner(
        self: Arc<Self>,
        task_id: String,
        input_receivers: HashMap<u32, LocalChannelReceiver<Vec<u8>>>,
        output_peer_tms: Vec<String>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut receivers: Vec<LocalChannelReceiver<Vec<u8>>> =
                input_receivers.into_values().collect();
            if receivers.is_empty() {
                return;
            }
            let mut ended = vec![false; receivers.len()];
            loop {
                let mut progressed = false;
                for (idx, receiver) in receivers.iter_mut().enumerate() {
                    if ended[idx] {
                        continue;
                    }
                    let maybe_elem = match receiver.try_recv() {
                        Ok(v) => v,
                        Err(err) => {
                            tracing::warn!("task {} input receive failed: {}", task_id, err);
                            ended[idx] = true;
                            continue;
                        }
                    };
                    let Some(elem) = maybe_elem else { continue };
                    progressed = true;
                    if matches!(elem, StreamElement::End) {
                        ended[idx] = true;
                    }
                    for (out_idx, peer_tm_id) in output_peer_tms.iter().enumerate() {
                        if let Err(err) = self
                            .forward_bridged_output_to_peer(
                                &task_id,
                                out_idx,
                                peer_tm_id,
                                elem.clone(),
                            )
                            .await
                        {
                            tracing::warn!(
                                "task {} output forward failed to {}: {}",
                                task_id,
                                peer_tm_id,
                                err
                            );
                        }
                    }
                }
                if ended.iter().all(|v| *v) {
                    break;
                }
                if !progressed {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        })
    }
}

#[derive(Clone)]
pub struct TaskManagerRpc {
    inner: Arc<TaskManager>,
}

impl TaskManagerRpc {
    pub fn new(inner: Arc<TaskManager>) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl TaskManagerService for TaskManagerRpc {
    async fn deploy_task(
        &self,
        request: Request<crate::cluster::rpc::TaskDeploymentDescriptor>,
    ) -> Result<Response<DeploymentResult>, Status> {
        let descriptor = request.into_inner();
        let task_id = descriptor.task_id.clone();

        if descriptor.output_peer_tms.len() != descriptor.output_channels.len() {
            return Err(Status::invalid_argument(format!(
                "output_peer_tms length {} must equal output_channels length {}",
                descriptor.output_peer_tms.len(),
                descriptor.output_channels.len()
            )));
        }

        if !descriptor.operator_bytes.is_empty() {
            let plan = JobPlan::from_bytes(&descriptor.operator_bytes)
                .map_err(|e| Status::invalid_argument(format!("invalid operator bytes: {e}")))?;
            self.inner
                .operator_factory
                .lock()
                .expect("operator_factory poisoned")
                .validate_job_plan(&plan)
                .map_err(|e| Status::invalid_argument(format!("unsupported job plan: {e}")))?;
        }

        let mut bridge = DeployedTaskBridge::from_descriptor(&descriptor)
            .map_err(|e| Status::invalid_argument(format!("invalid deployment descriptor: {e}")))?;
        let input_channels = bridge.input_channels();
        let previous_input_channels = self
            .inner
            .deployed_tasks
            .lock()
            .expect("deployed_tasks poisoned")
            .get(&task_id)
            .map(DeployedTaskBridge::input_channels)
            .unwrap_or_default();
        let runner_input_receivers = if descriptor.operator_bytes.is_empty() {
            None
        } else {
            Some(bridge.take_input_receivers())
        };
        let output_peer_tms = descriptor.output_peer_tms.clone();

        {
            let mut channel_map = self
                .inner
                .input_channel_to_task
                .lock()
                .expect("input_channel_to_task poisoned");
            for channel_id in &input_channels {
                if let Some(existing) = channel_map.get(channel_id) {
                    if existing != &task_id {
                        return Err(Status::already_exists(format!(
                            "input channel {} already assigned to task {}",
                            channel_id, existing
                        )));
                    }
                }
            }
            for channel_id in previous_input_channels {
                if channel_map.get(&channel_id) == Some(&task_id) {
                    channel_map.remove(&channel_id);
                }
            }
            for channel_id in input_channels {
                channel_map.insert(channel_id, task_id.clone());
            }
        }

        if let Some(handle) = self
            .inner
            .running_tasks
            .lock()
            .expect("running_tasks poisoned")
            .remove(&descriptor.task_id)
        {
            handle.abort();
        }

        self.inner
            .deployed_tasks
            .lock()
            .expect("deployed_tasks poisoned")
            .insert(task_id, bridge);

        if let Some(input_receivers) = runner_input_receivers {
            let runner = Arc::clone(&self.inner).spawn_task_runner(
                descriptor.task_id.clone(),
                input_receivers,
                output_peer_tms,
            );
            self.inner
                .running_tasks
                .lock()
                .expect("running_tasks poisoned")
                .insert(descriptor.task_id, runner);
        }
        Ok(Response::new(DeploymentResult {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let removed = self
            .inner
            .deployed_tasks
            .lock()
            .expect("deployed_tasks poisoned")
            .remove(&req.task_id);
        if let Some(bridge) = removed {
            let mut channel_map = self
                .inner
                .input_channel_to_task
                .lock()
                .expect("input_channel_to_task poisoned");
            for channel_id in bridge.input_channels() {
                if channel_map.get(&channel_id) == Some(&req.task_id) {
                    channel_map.remove(&channel_id);
                }
            }
        }
        if let Some(handle) = self
            .inner
            .running_tasks
            .lock()
            .expect("running_tasks poisoned")
            .remove(&req.task_id)
        {
            handle.abort();
        }
        Ok(Response::new(Empty {}))
    }

    async fn trigger_checkpoint(
        &self,
        request: Request<CheckpointBarrier>,
    ) -> Result<Response<Empty>, Status> {
        let barrier = request.into_inner();
        self.inner
            .triggered_checkpoints
            .lock()
            .expect("triggered_checkpoints poisoned")
            .push(barrier.checkpoint_id);

        let task_ids: Vec<String> = if !barrier.task_ids.is_empty() {
            barrier.task_ids.clone()
        } else if !barrier.job_id.is_empty() {
            self.inner
                .deployed_tasks
                .lock()
                .expect("deployed_tasks poisoned")
                .keys()
                .filter(|id| id.starts_with(&format!("{}::", barrier.job_id)))
                .cloned()
                .collect()
        } else {
            self.inner
                .deployed_tasks
                .lock()
                .expect("deployed_tasks poisoned")
                .keys()
                .cloned()
                .collect()
        };
        if task_ids.is_empty() {
            return Ok(Response::new(Empty {}));
        }

        let mut client = JobManagerServiceClient::connect(self.inner.jm_endpoint.clone())
            .await
            .map_err(|e| Status::unavailable(format!("connect jm failed: {e}")))?;
        let deployed_keys: HashSet<String> = self
            .inner
            .deployed_tasks
            .lock()
            .expect("deployed_tasks poisoned")
            .keys()
            .cloned()
            .collect();

        let mut seen_task_ids = HashSet::new();
        for task_id in task_ids {
            if !seen_task_ids.insert(task_id.clone()) {
                continue;
            }
            if !deployed_keys.contains(&task_id) {
                tracing::warn!(
                    "skip checkpoint ack for missing task {}, checkpoint_id={}",
                    task_id,
                    barrier.checkpoint_id
                );
                continue;
            }
            client
                .ack_checkpoint(Request::new(CheckpointAck {
                    checkpoint_id: barrier.checkpoint_id,
                    task_id,
                    state_bytes: Vec::new(),
                }))
                .await
                .map_err(|e| Status::internal(format!("ack checkpoint failed: {e}")))?;
        }
        Ok(Response::new(Empty {}))
    }

    async fn notify_checkpoint_complete(
        &self,
        request: Request<CheckpointComplete>,
    ) -> Result<Response<Empty>, Status> {
        let complete = request.into_inner();
        self.inner
            .completed_checkpoints
            .lock()
            .expect("completed_checkpoints poisoned")
            .push(complete.checkpoint_id);
        Ok(Response::new(Empty {}))
    }
}
