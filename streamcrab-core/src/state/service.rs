use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use tonic::{Request, Response, Status};

use crate::cluster::rpc;
use crate::cluster::rpc::state_service_client::StateServiceClient as RpcStateServiceClient;
use crate::cluster::rpc::state_service_server::{StateService, StateServiceServer};

const TIMER_KEY_PREFIX: &str = "__timer_event_";

#[derive(Debug, Clone)]
pub struct StateServiceCoreConfig {
    pub max_pending_epochs: u64,
    pub max_total_keys: usize,
    pub max_total_bytes: usize,
    pub state_ttl_ms: Option<u64>,
}

impl Default for StateServiceCoreConfig {
    fn default() -> Self {
        Self {
            max_pending_epochs: 2,
            max_total_keys: 1_000_000,
            max_total_bytes: 256 * 1024 * 1024,
            state_ttl_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerIndexEntry {
    pub fire_at: i64,
    pub user_key: Vec<u8>,
    pub state_key: Vec<u8>,
}

/// Epoch-based in-memory state service core.
#[derive(Debug)]
pub struct StateServiceCore {
    committed: HashMap<Vec<u8>, Vec<u8>>,
    pending: BTreeMap<u64, HashMap<Vec<u8>, Vec<u8>>>,
    sealed_epoch: u64,
    triggered_snapshots: BTreeSet<u64>,
    timer_index: BTreeMap<(i64, Vec<u8>), Vec<u8>>,
    timer_lookup: HashMap<Vec<u8>, (i64, Vec<u8>)>,
    access_frequency: HashMap<Vec<u8>, u64>,
    last_write_seq_by_task_epoch: HashMap<(String, u64), u64>,
    last_updated_ms: HashMap<Vec<u8>, i64>,
    config: StateServiceCoreConfig,
}

impl Default for StateServiceCore {
    fn default() -> Self {
        Self::new(StateServiceCoreConfig::default())
    }
}

impl StateServiceCore {
    pub fn new(config: StateServiceCoreConfig) -> Self {
        Self {
            committed: HashMap::new(),
            pending: BTreeMap::new(),
            sealed_epoch: 0,
            triggered_snapshots: BTreeSet::new(),
            timer_index: BTreeMap::new(),
            timer_lookup: HashMap::new(),
            access_frequency: HashMap::new(),
            last_write_seq_by_task_epoch: HashMap::new(),
            last_updated_ms: HashMap::new(),
            config,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, epoch: u64) -> Result<()> {
        self.put_with_meta(key, value, epoch, None, None)?;
        Ok(())
    }

    pub fn put_with_meta(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        epoch: u64,
        task_id: Option<&str>,
        seq_no: Option<u64>,
    ) -> Result<bool> {
        self.cleanup_expired();
        self.validate_epoch(epoch)?;
        if !self.accept_write_sequence(task_id, epoch, seq_no) {
            return Ok(false);
        }
        self.ensure_capacity_for_write(&key, value.len(), epoch)?;
        self.pending
            .entry(epoch)
            .or_default()
            .insert(key.clone(), value);
        self.bump_access(&key);
        self.mark_updated(&key);
        self.update_timer_index(&key);
        Ok(true)
    }

    pub fn batch_put(&mut self, entries: Vec<(Vec<u8>, Vec<u8>)>, epoch: u64) -> Result<usize> {
        self.batch_put_with_meta(entries, epoch, None, None)
    }

    pub fn batch_put_with_meta(
        &mut self,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        epoch: u64,
        task_id: Option<&str>,
        base_seq_no: Option<u64>,
    ) -> Result<usize> {
        self.cleanup_expired();
        self.validate_epoch(epoch)?;
        if entries.is_empty() {
            return Ok(0);
        }
        let mut accepted_entries = Vec::new();
        let start_seq = base_seq_no.unwrap_or(0);
        for (idx, (key, value)) in entries.into_iter().enumerate() {
            let seq = if start_seq == 0 {
                None
            } else {
                Some(start_seq + idx as u64)
            };
            if !self.accept_write_sequence(task_id, epoch, seq) {
                continue;
            }
            self.ensure_capacity_for_write(&key, value.len(), epoch)?;
            accepted_entries.push((key, value));
        }
        if accepted_entries.is_empty() {
            return Ok(0);
        }
        let accepted_count = accepted_entries.len();
        let mut updated_keys = Vec::with_capacity(accepted_count);
        {
            let pending = self.pending.entry(epoch).or_default();
            for (key, value) in accepted_entries {
                pending.insert(key.clone(), value);
                updated_keys.push(key);
            }
        }
        for key in updated_keys {
            self.bump_access(&key);
            self.mark_updated(&key);
            self.update_timer_index(&key);
        }
        Ok(accepted_count)
    }

    pub fn get(&mut self, key: &[u8], epoch: u64) -> Option<Vec<u8>> {
        self.cleanup_expired();
        let value = self
            .pending
            .get(&epoch)
            .and_then(|entries| entries.get(key).cloned())
            .or_else(|| self.committed.get(key).cloned());
        if value.is_some() {
            self.bump_access(key);
        }
        value
    }

    pub fn batch_get(&mut self, keys: &[Vec<u8>], epoch: u64) -> Vec<Option<Vec<u8>>> {
        self.cleanup_expired();
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(key, epoch));
        }
        values
    }

    pub fn top_keys(&self, limit: usize, epoch: u64) -> Vec<Vec<u8>> {
        if limit == 0 {
            return Vec::new();
        }
        let mut scored: Vec<(Vec<u8>, u64)> = self
            .access_frequency
            .iter()
            .filter_map(|(key, count)| {
                let visible = self
                    .pending
                    .get(&epoch)
                    .map(|pending| pending.contains_key(key))
                    .unwrap_or(false)
                    || self.committed.contains_key(key);
                if visible {
                    Some((key.clone(), *count))
                } else {
                    None
                }
            })
            .collect();
        scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.into_iter().take(limit).map(|(key, _)| key).collect()
    }

    pub fn flush(&self, epoch: u64) -> Result<()> {
        self.validate_epoch(epoch)
    }

    pub fn trigger_snapshot(&mut self, epoch: u64) -> Result<()> {
        self.validate_epoch(epoch)?;
        self.triggered_snapshots.insert(epoch);
        Ok(())
    }

    pub fn seal(&mut self, epoch: u64) -> Result<()> {
        self.cleanup_expired();
        if epoch <= self.sealed_epoch {
            return Ok(());
        }
        self.validate_epoch(epoch)?;
        if let Some(entries) = self.pending.remove(&epoch) {
            for (key, value) in entries {
                self.committed.insert(key, value);
            }
        }
        self.sealed_epoch = epoch;
        self.pending
            .retain(|pending_epoch, _| *pending_epoch > epoch);
        self.last_write_seq_by_task_epoch
            .retain(|(_, write_epoch), _| *write_epoch > epoch);
        Ok(())
    }

    pub fn rollback(&mut self) {
        self.pending.clear();
        self.last_write_seq_by_task_epoch.clear();
        self.triggered_snapshots.clear();
        self.rebuild_timer_index_from_committed();
    }

    pub fn sealed_epoch(&self) -> u64 {
        self.sealed_epoch
    }

    pub fn pending_epochs(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }

    pub fn triggered_snapshot_epochs(&self) -> Vec<u64> {
        self.triggered_snapshots.iter().copied().collect()
    }

    pub fn timer_entries(&self) -> Vec<TimerIndexEntry> {
        self.timer_index
            .iter()
            .map(|((fire_at, user_key), state_key)| TimerIndexEntry {
                fire_at: *fire_at,
                user_key: user_key.clone(),
                state_key: state_key.clone(),
            })
            .collect()
    }

    fn validate_epoch(&self, epoch: u64) -> Result<()> {
        if epoch <= self.sealed_epoch {
            return Err(anyhow!(
                "epoch {} must be greater than sealed_epoch {}",
                epoch,
                self.sealed_epoch
            ));
        }
        let upper = self.sealed_epoch + self.config.max_pending_epochs;
        if epoch > upper {
            return Err(anyhow!(
                "epoch {} exceeds pending window (sealed={}, max_pending={})",
                epoch,
                self.sealed_epoch,
                self.config.max_pending_epochs
            ));
        }
        Ok(())
    }

    fn accept_write_sequence(
        &mut self,
        task_id: Option<&str>,
        epoch: u64,
        seq_no: Option<u64>,
    ) -> bool {
        let Some(task_id) = task_id else {
            return true;
        };
        if task_id.is_empty() {
            return true;
        }
        let Some(seq_no) = seq_no else {
            return true;
        };
        if seq_no == 0 {
            return true;
        }
        let entry = self
            .last_write_seq_by_task_epoch
            .entry((task_id.to_string(), epoch))
            .or_insert(0);
        if seq_no <= *entry {
            return false;
        }
        *entry = seq_no;
        true
    }

    fn ensure_capacity_for_write(&self, key: &[u8], value_len: usize, epoch: u64) -> Result<()> {
        let in_epoch = self
            .pending
            .get(&epoch)
            .map(|entries| entries.contains_key(key))
            .unwrap_or(false);
        let in_committed = self.committed.contains_key(key);
        let is_new_key = !in_epoch && !in_committed;
        if is_new_key && self.estimated_total_keys() >= self.config.max_total_keys {
            tracing::warn!(
                "reject state write due to key limit: max_total_keys={}",
                self.config.max_total_keys
            );
            return Err(anyhow!(
                "state key limit exceeded: max_total_keys={}",
                self.config.max_total_keys
            ));
        }

        let existing_value_len = self
            .pending
            .get(&epoch)
            .and_then(|entries| entries.get(key).map(|v| v.len()))
            .or_else(|| self.committed.get(key).map(|v| v.len()))
            .unwrap_or(0);
        let base = self.estimated_total_bytes();
        let added_key_bytes = if is_new_key { key.len() } else { 0 };
        let projected = base
            .saturating_sub(existing_value_len)
            .saturating_add(value_len)
            .saturating_add(added_key_bytes);
        if projected > self.config.max_total_bytes {
            tracing::warn!(
                "reject state write due to memory limit: projected={} max_total_bytes={}",
                projected,
                self.config.max_total_bytes
            );
            return Err(anyhow!(
                "state memory limit exceeded: projected={} max_total_bytes={}",
                projected,
                self.config.max_total_bytes
            ));
        }
        Ok(())
    }

    fn estimated_total_keys(&self) -> usize {
        let mut all_keys = std::collections::HashSet::<Vec<u8>>::new();
        for key in self.committed.keys() {
            all_keys.insert(key.clone());
        }
        for entries in self.pending.values() {
            for key in entries.keys() {
                all_keys.insert(key.clone());
            }
        }
        all_keys.len()
    }

    fn estimated_total_bytes(&self) -> usize {
        let mut total = 0usize;
        for (k, v) in &self.committed {
            total = total.saturating_add(k.len()).saturating_add(v.len());
        }
        for entries in self.pending.values() {
            for (k, v) in entries {
                total = total.saturating_add(k.len()).saturating_add(v.len());
            }
        }
        total
    }

    fn mark_updated(&mut self, key: &[u8]) {
        self.last_updated_ms.insert(key.to_vec(), current_unix_millis());
    }

    fn cleanup_expired(&mut self) {
        let Some(ttl_ms) = self.config.state_ttl_ms else {
            return;
        };
        let now_ms = current_unix_millis();
        let expired_keys: Vec<Vec<u8>> = self
            .last_updated_ms
            .iter()
            .filter_map(|(key, updated_ms)| {
                if now_ms.saturating_sub(*updated_ms) > ttl_ms as i64 {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();
        if expired_keys.is_empty() {
            return;
        }
        for key in expired_keys {
            self.committed.remove(&key);
            for entries in self.pending.values_mut() {
                entries.remove(&key);
            }
            self.access_frequency.remove(&key);
            self.last_updated_ms.remove(&key);
            if let Some(previous) = self.timer_lookup.remove(&key) {
                self.timer_index.remove(&previous);
            }
        }
    }

    fn update_timer_index(&mut self, state_key: &[u8]) {
        if let Some(previous) = self.timer_lookup.remove(state_key) {
            self.timer_index.remove(&previous);
        }

        if let Some((fire_at, user_key)) = parse_timer_key(state_key) {
            self.timer_lookup
                .insert(state_key.to_vec(), (fire_at, user_key.clone()));
            self.timer_index
                .insert((fire_at, user_key), state_key.to_vec());
        }
    }

    fn bump_access(&mut self, key: &[u8]) {
        let counter = self.access_frequency.entry(key.to_vec()).or_insert(0);
        *counter += 1;
    }

    fn rebuild_timer_index_from_committed(&mut self) {
        self.timer_index.clear();
        self.timer_lookup.clear();
        let committed_keys: Vec<Vec<u8>> = self.committed.keys().cloned().collect();
        for key in committed_keys {
            self.update_timer_index(&key);
        }
    }
}

pub trait StateServiceClient: Send + Sync {
    fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Vec<u8>>>;
    fn batch_get(&self, keys: &[Vec<u8>], epoch: u64) -> Result<Vec<Option<Vec<u8>>>>;
    fn put(&self, key: Vec<u8>, value: Vec<u8>, epoch: u64) -> Result<()>;
    fn batch_put(&self, entries: Vec<(Vec<u8>, Vec<u8>)>, epoch: u64) -> Result<()>;
    fn top_keys(&self, limit: usize, epoch: u64) -> Result<Vec<Vec<u8>>>;
    fn flush(&self, epoch: u64) -> Result<()>;
    fn trigger_snapshot(&self, epoch: u64) -> Result<()>;
    fn seal(&self, epoch: u64) -> Result<()>;
    fn rollback(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct InMemoryStateClient {
    core: Arc<Mutex<StateServiceCore>>,
    writer_id: String,
    next_seq: Arc<AtomicU64>,
}

impl InMemoryStateClient {
    pub fn new(core: Arc<Mutex<StateServiceCore>>) -> Self {
        Self::with_writer_id(core, "in-memory-client")
    }

    pub fn with_writer_id(core: Arc<Mutex<StateServiceCore>>, writer_id: impl Into<String>) -> Self {
        Self {
            core,
            writer_id: writer_id.into(),
            next_seq: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl StateServiceClient for InMemoryStateClient {
    fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Vec<u8>>> {
        let mut core = self
            .core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?;
        Ok(core.get(key, epoch))
    }

    fn batch_get(&self, keys: &[Vec<u8>], epoch: u64) -> Result<Vec<Option<Vec<u8>>>> {
        let mut core = self
            .core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?;
        Ok(core.batch_get(keys, epoch))
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>, epoch: u64) -> Result<()> {
        let seq_no = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .put_with_meta(key, value, epoch, Some(&self.writer_id), Some(seq_no))?;
        Ok(())
    }

    fn batch_put(&self, entries: Vec<(Vec<u8>, Vec<u8>)>, epoch: u64) -> Result<()> {
        let base_seq_no = self.next_seq.fetch_add(entries.len() as u64, Ordering::SeqCst);
        self.core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .batch_put_with_meta(
                entries,
                epoch,
                Some(&self.writer_id),
                Some(base_seq_no.max(1)),
            )?;
        Ok(())
    }

    fn top_keys(&self, limit: usize, epoch: u64) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .top_keys(limit, epoch))
    }

    fn flush(&self, epoch: u64) -> Result<()> {
        self.core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .flush(epoch)
    }

    fn trigger_snapshot(&self, epoch: u64) -> Result<()> {
        self.core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .trigger_snapshot(epoch)
    }

    fn seal(&self, epoch: u64) -> Result<()> {
        self.core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .seal(epoch)
    }

    fn rollback(&self) -> Result<()> {
        self.core
            .lock()
            .map_err(|_| anyhow!("state core lock poisoned"))?
            .rollback();
        Ok(())
    }
}

#[derive(Clone)]
pub struct GrpcStateClient {
    endpoint: String,
    writer_id: String,
    next_seq: Arc<AtomicU64>,
}

impl GrpcStateClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        Self {
            endpoint: normalize_endpoint(&endpoint),
            writer_id: format!("grpc-client@{}", endpoint),
            next_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    fn block_on<T, F>(&self, fut: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, tonic::Status>> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            let join = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| anyhow!("create runtime failed: {}", e))?;
                runtime
                    .block_on(fut)
                    .map_err(|status| anyhow!("state service rpc failed: {}", status))
            });
            join.join()
                .map_err(|_| anyhow!("state service rpc worker thread panicked"))?
        } else {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime
                .block_on(fut)
                .map_err(|status| anyhow!("state service rpc failed: {}", status))
        }
    }
}

impl StateServiceClient for GrpcStateClient {
    fn get(&self, key: &[u8], epoch: u64) -> Result<Option<Vec<u8>>> {
        let endpoint = self.endpoint.clone();
        let request_key = key.to_vec();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            let response = client
                .get(Request::new(rpc::GetRequest {
                    key: request_key,
                    epoch,
                }))
                .await?
                .into_inner();
            Ok(if response.found {
                Some(response.value)
            } else {
                None
            })
        })
    }

    fn batch_get(&self, keys: &[Vec<u8>], epoch: u64) -> Result<Vec<Option<Vec<u8>>>> {
        let endpoint = self.endpoint.clone();
        let request_keys = keys.to_vec();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            let response = client
                .batch_get(Request::new(rpc::BatchGetRequest {
                    keys: request_keys,
                    epoch,
                }))
                .await?
                .into_inner();
            Ok(response
                .items
                .into_iter()
                .map(|item| if item.found { Some(item.value) } else { None })
                .collect())
        })
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>, epoch: u64) -> Result<()> {
        let endpoint = self.endpoint.clone();
        let task_id = self.writer_id.clone();
        let seq_no = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            client
                .put(Request::new(rpc::PutRequest {
                    key,
                    value,
                    epoch,
                    task_id,
                    seq_no,
                }))
                .await?;
            Ok(())
        })
    }

    fn batch_put(&self, entries: Vec<(Vec<u8>, Vec<u8>)>, epoch: u64) -> Result<()> {
        let endpoint = self.endpoint.clone();
        let task_id = self.writer_id.clone();
        let base_seq_no = self.next_seq.fetch_add(entries.len() as u64, Ordering::SeqCst);
        let entries = entries
            .into_iter()
            .map(|(key, value)| rpc::StateEntry { key, value })
            .collect();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            client
                .batch_put(Request::new(rpc::BatchPutRequest {
                    entries,
                    epoch,
                    task_id,
                    base_seq_no: base_seq_no.max(1),
                }))
                .await?;
            Ok(())
        })
    }

    fn top_keys(&self, limit: usize, epoch: u64) -> Result<Vec<Vec<u8>>> {
        let endpoint = self.endpoint.clone();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            let response = client
                .top_keys(Request::new(rpc::TopKeysRequest {
                    limit: limit as u32,
                    epoch,
                }))
                .await?
                .into_inner();
            Ok(response.keys)
        })
    }

    fn flush(&self, epoch: u64) -> Result<()> {
        let endpoint = self.endpoint.clone();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            client
                .flush(Request::new(rpc::FlushRequest { epoch }))
                .await?;
            Ok(())
        })
    }

    fn trigger_snapshot(&self, epoch: u64) -> Result<()> {
        let endpoint = self.endpoint.clone();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            client
                .trigger_snapshot(Request::new(rpc::TriggerSnapshotRequest { epoch }))
                .await?;
            Ok(())
        })
    }

    fn seal(&self, epoch: u64) -> Result<()> {
        let endpoint = self.endpoint.clone();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            client
                .seal(Request::new(rpc::SealRequest { epoch }))
                .await?;
            Ok(())
        })
    }

    fn rollback(&self) -> Result<()> {
        let endpoint = self.endpoint.clone();
        self.block_on(async move {
            let mut client = RpcStateServiceClient::connect(endpoint)
                .await
                .map_err(|e| Status::unavailable(e.to_string()))?;
            client
                .rollback(Request::new(rpc::RollbackRequest {}))
                .await?;
            Ok(())
        })
    }
}

pub struct StateServiceHandle {
    core: Arc<Mutex<StateServiceCore>>,
}

impl StateServiceHandle {
    pub fn new(config: StateServiceCoreConfig) -> Self {
        Self {
            core: Arc::new(Mutex::new(StateServiceCore::new(config))),
        }
    }

    pub fn in_memory_client(&self) -> InMemoryStateClient {
        InMemoryStateClient::new(Arc::clone(&self.core))
    }

    pub async fn serve(&self, addr: SocketAddr) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(StateServiceServer::new(StateServiceRpc::new(Arc::clone(
                &self.core,
            ))))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct StateServiceRpc {
    core: Arc<Mutex<StateServiceCore>>,
}

impl StateServiceRpc {
    pub fn new(core: Arc<Mutex<StateServiceCore>>) -> Self {
        Self { core }
    }
}

#[tonic::async_trait]
impl StateService for StateServiceRpc {
    async fn get(
        &self,
        request: Request<rpc::GetRequest>,
    ) -> Result<Response<rpc::GetResponse>, Status> {
        let req = request.into_inner();
        let mut core = self
            .core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?;
        let value = core.get(&req.key, req.epoch);
        Ok(Response::new(rpc::GetResponse {
            found: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    async fn put(
        &self,
        request: Request<rpc::PutRequest>,
    ) -> Result<Response<rpc::PutResponse>, Status> {
        let req = request.into_inner();
        let accepted = self.core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .put_with_meta(
                req.key,
                req.value,
                req.epoch,
                Some(req.task_id.as_str()),
                Some(req.seq_no),
            )
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(rpc::PutResponse { accepted }))
    }

    async fn batch_get(
        &self,
        request: Request<rpc::BatchGetRequest>,
    ) -> Result<Response<rpc::BatchGetResponse>, Status> {
        let req = request.into_inner();
        let mut core = self
            .core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?;
        let items = core
            .batch_get(&req.keys, req.epoch)
            .into_iter()
            .zip(req.keys)
            .map(|(value, key)| rpc::BatchGetItem {
                key,
                found: value.is_some(),
                value: value.unwrap_or_default(),
            })
            .collect();
        Ok(Response::new(rpc::BatchGetResponse { items }))
    }

    async fn top_keys(
        &self,
        request: Request<rpc::TopKeysRequest>,
    ) -> Result<Response<rpc::TopKeysResponse>, Status> {
        let req = request.into_inner();
        let keys = self
            .core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .top_keys(req.limit as usize, req.epoch);
        Ok(Response::new(rpc::TopKeysResponse { keys }))
    }

    async fn batch_put(
        &self,
        request: Request<rpc::BatchPutRequest>,
    ) -> Result<Response<rpc::BatchPutResponse>, Status> {
        let req = request.into_inner();
        let entries = req
            .entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();
        let written = self
            .core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .batch_put_with_meta(
                entries,
                req.epoch,
                Some(req.task_id.as_str()),
                Some(req.base_seq_no),
            )
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(rpc::BatchPutResponse {
            written: written as u32,
        }))
    }

    async fn flush(
        &self,
        request: Request<rpc::FlushRequest>,
    ) -> Result<Response<rpc::FlushResponse>, Status> {
        let req = request.into_inner();
        self.core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .flush(req.epoch)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(rpc::FlushResponse { accepted: true }))
    }

    async fn trigger_snapshot(
        &self,
        request: Request<rpc::TriggerSnapshotRequest>,
    ) -> Result<Response<rpc::TriggerSnapshotResponse>, Status> {
        let req = request.into_inner();
        self.core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .trigger_snapshot(req.epoch)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(rpc::TriggerSnapshotResponse { accepted: true }))
    }

    async fn seal(
        &self,
        request: Request<rpc::SealRequest>,
    ) -> Result<Response<rpc::SealResponse>, Status> {
        let req = request.into_inner();
        self.core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .seal(req.epoch)
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(rpc::SealResponse { accepted: true }))
    }

    async fn rollback(
        &self,
        _request: Request<rpc::RollbackRequest>,
    ) -> Result<Response<rpc::RollbackResponse>, Status> {
        self.core
            .lock()
            .map_err(|_| Status::internal("state core lock poisoned"))?
            .rollback();
        Ok(Response::new(rpc::RollbackResponse { accepted: true }))
    }
}

fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn parse_timer_key(key: &[u8]) -> Option<(i64, Vec<u8>)> {
    let key_str = std::str::from_utf8(key).ok()?;
    let raw = key_str.strip_prefix(TIMER_KEY_PREFIX)?;
    let (ts, user_key) = raw.split_once('|')?;
    let fire_at = ts.parse::<i64>().ok()?;
    Some((fire_at, user_key.as_bytes().to_vec()))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{}", endpoint)
    }
}

#[cfg(test)]
#[path = "tests/service_tests.rs"]
mod tests;
