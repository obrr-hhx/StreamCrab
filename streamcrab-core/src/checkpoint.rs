//! Checkpoint primitives and storage implementations for P3.

use crate::input_gate::ChannelIndex;
use crate::task::TaskId;
use crate::types::{Barrier, CheckpointId, EventTime, StreamElement};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Task-level checkpoint acknowledgement payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCheckpointAck {
    pub checkpoint_id: CheckpointId,
    pub task_id: TaskId,
    pub state: Vec<u8>,
}

/// Task-level checkpoint abort payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCheckpointAbort {
    pub checkpoint_id: CheckpointId,
    pub task_id: TaskId,
    pub reason: String,
}

/// Task -> coordinator checkpoint control event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskCheckpointEvent {
    Ack(TaskCheckpointAck),
    Aborted(TaskCheckpointAbort),
}

/// Persisted checkpoint metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointMetadata {
    pub checkpoint_id: CheckpointId,
    pub timestamp: EventTime,
    pub task_ids: Vec<TaskId>,
}

/// Storage interface for checkpoint metadata and per-task state.
pub trait CheckpointStorage: Send + Sync {
    fn save_checkpoint(&self, metadata: CheckpointMetadata) -> Result<()>;
    fn save_task_state(
        &self,
        checkpoint_id: CheckpointId,
        task_id: TaskId,
        state: Vec<u8>,
    ) -> Result<()>;
    fn load_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<CheckpointMetadata>;
    fn load_task_state(&self, checkpoint_id: CheckpointId, task_id: TaskId) -> Result<Vec<u8>>;
    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>>;
    fn purge(&self, keep_last_n: usize) -> Result<()>;
}

/// In-memory checkpoint storage for tests and local single-process execution.
#[derive(Default)]
pub struct InMemoryCheckpointStorage {
    metadata: Mutex<HashMap<CheckpointId, CheckpointMetadata>>,
    task_states: Mutex<HashMap<(CheckpointId, TaskId), Vec<u8>>>,
}

impl InMemoryCheckpointStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointStorage for InMemoryCheckpointStorage {
    fn save_checkpoint(&self, metadata: CheckpointMetadata) -> Result<()> {
        self.metadata
            .lock()
            .map_err(|_| anyhow!("checkpoint metadata lock poisoned"))?
            .insert(metadata.checkpoint_id, metadata);
        Ok(())
    }

    fn save_task_state(
        &self,
        checkpoint_id: CheckpointId,
        task_id: TaskId,
        state: Vec<u8>,
    ) -> Result<()> {
        self.task_states
            .lock()
            .map_err(|_| anyhow!("checkpoint task-state lock poisoned"))?
            .insert((checkpoint_id, task_id), state);
        Ok(())
    }

    fn load_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<CheckpointMetadata> {
        self.metadata
            .lock()
            .map_err(|_| anyhow!("checkpoint metadata lock poisoned"))?
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| anyhow!("checkpoint {} not found", checkpoint_id))
    }

    fn load_task_state(&self, checkpoint_id: CheckpointId, task_id: TaskId) -> Result<Vec<u8>> {
        self.task_states
            .lock()
            .map_err(|_| anyhow!("checkpoint task-state lock poisoned"))?
            .get(&(checkpoint_id, task_id))
            .cloned()
            .ok_or_else(|| anyhow!("task state not found for checkpoint {}", checkpoint_id))
    }

    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>> {
        let mut ids: Vec<_> = self
            .metadata
            .lock()
            .map_err(|_| anyhow!("checkpoint metadata lock poisoned"))?
            .keys()
            .copied()
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    fn purge(&self, keep_last_n: usize) -> Result<()> {
        let mut ids = self.list_checkpoints()?;
        if ids.len() <= keep_last_n {
            return Ok(());
        }
        let purge_count = ids.len() - keep_last_n;
        ids.truncate(purge_count);

        let mut meta_guard = self
            .metadata
            .lock()
            .map_err(|_| anyhow!("checkpoint metadata lock poisoned"))?;
        let mut state_guard = self
            .task_states
            .lock()
            .map_err(|_| anyhow!("checkpoint task-state lock poisoned"))?;

        for checkpoint_id in ids {
            meta_guard.remove(&checkpoint_id);
            state_guard.retain(|(id, _), _| *id != checkpoint_id);
        }
        Ok(())
    }
}

/// File-system checkpoint storage.
pub struct FsCheckpointStorage {
    base_path: PathBuf,
}

impl FsCheckpointStorage {
    pub fn new(base_path: impl Into<PathBuf>) -> Result<Self> {
        let base_path = base_path.into();
        fs::create_dir_all(&base_path).with_context(|| {
            format!(
                "failed to create checkpoint storage directory {}",
                base_path.display()
            )
        })?;
        Ok(Self { base_path })
    }

    fn checkpoint_dir(&self, checkpoint_id: CheckpointId) -> PathBuf {
        self.base_path.join(format!("chk-{checkpoint_id}"))
    }

    fn metadata_path(&self, checkpoint_id: CheckpointId) -> PathBuf {
        self.checkpoint_dir(checkpoint_id).join("metadata.bin")
    }

    fn task_state_path(&self, checkpoint_id: CheckpointId, task_id: TaskId) -> PathBuf {
        self.checkpoint_dir(checkpoint_id)
            .join(format!("task-{}.bin", task_id))
    }
}

impl CheckpointStorage for FsCheckpointStorage {
    fn save_checkpoint(&self, metadata: CheckpointMetadata) -> Result<()> {
        let checkpoint_dir = self.checkpoint_dir(metadata.checkpoint_id);
        fs::create_dir_all(&checkpoint_dir).with_context(|| {
            format!(
                "failed to create checkpoint dir {}",
                checkpoint_dir.display()
            )
        })?;
        let bytes =
            bincode::serialize(&metadata).context("serialize checkpoint metadata failed")?;
        fs::write(self.metadata_path(metadata.checkpoint_id), bytes)
            .context("write checkpoint metadata failed")
    }

    fn save_task_state(
        &self,
        checkpoint_id: CheckpointId,
        task_id: TaskId,
        state: Vec<u8>,
    ) -> Result<()> {
        let checkpoint_dir = self.checkpoint_dir(checkpoint_id);
        fs::create_dir_all(&checkpoint_dir).with_context(|| {
            format!(
                "failed to create checkpoint dir {}",
                checkpoint_dir.display()
            )
        })?;
        fs::write(self.task_state_path(checkpoint_id, task_id), state)
            .context("write checkpoint task state failed")
    }

    fn load_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<CheckpointMetadata> {
        let bytes = fs::read(self.metadata_path(checkpoint_id))
            .context("read checkpoint metadata failed")?;
        bincode::deserialize(&bytes).context("deserialize checkpoint metadata failed")
    }

    fn load_task_state(&self, checkpoint_id: CheckpointId, task_id: TaskId) -> Result<Vec<u8>> {
        fs::read(self.task_state_path(checkpoint_id, task_id))
            .context("read checkpoint task state failed")
    }

    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.base_path)
            .with_context(|| format!("read_dir failed for {}", self.base_path.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id_part) = name.strip_prefix("chk-") {
                if let Ok(id) = id_part.parse::<CheckpointId>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    fn purge(&self, keep_last_n: usize) -> Result<()> {
        let ids = self.list_checkpoints()?;
        if ids.len() <= keep_last_n {
            return Ok(());
        }
        let purge_count = ids.len() - keep_last_n;
        for checkpoint_id in ids.into_iter().take(purge_count) {
            let dir = self.checkpoint_dir(checkpoint_id);
            if dir.exists() {
                fs::remove_dir_all(&dir)
                    .with_context(|| format!("failed to remove {}", dir.display()))?;
            }
        }
        Ok(())
    }
}

/// Barrier alignment result.
#[derive(Debug)]
pub enum BarrierAlignResult<T> {
    Forward(StreamElement<T>),
    Buffering,
    Aligned {
        barrier: Barrier,
        buffered: Vec<(ChannelIndex, StreamElement<T>)>,
    },
    Aborted {
        checkpoint_id: CheckpointId,
        drained: Vec<(ChannelIndex, StreamElement<T>)>,
    },
}

/// Barrier aligner state for multi-input tasks.
pub struct BarrierAligner<T> {
    pub num_inputs: usize,
    pub max_buffer_size: usize,
    current_barrier: Option<Barrier>,
    barriers_received: Vec<bool>,
    blocked_channels: Vec<bool>,
    buffered: VecDeque<(ChannelIndex, StreamElement<T>)>,
    last_cleared_checkpoint_id: Option<CheckpointId>,
}

impl<T> BarrierAligner<T> {
    pub fn new(num_inputs: usize) -> Self {
        Self {
            num_inputs,
            max_buffer_size: 10_000,
            current_barrier: None,
            barriers_received: vec![false; num_inputs],
            blocked_channels: vec![false; num_inputs],
            buffered: VecDeque::new(),
            last_cleared_checkpoint_id: None,
        }
    }

    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    pub fn process_element(
        &mut self,
        channel_idx: ChannelIndex,
        element: StreamElement<T>,
    ) -> Result<BarrierAlignResult<T>> {
        if channel_idx >= self.num_inputs {
            return Err(anyhow!("channel index {} out of bounds", channel_idx));
        }

        match element {
            StreamElement::CheckpointBarrier(barrier) => self.process_barrier(channel_idx, barrier),
            other => self.process_non_barrier(channel_idx, other),
        }
    }

    fn process_barrier(
        &mut self,
        channel_idx: ChannelIndex,
        barrier: Barrier,
    ) -> Result<BarrierAlignResult<T>> {
        if self
            .last_cleared_checkpoint_id
            .is_some_and(|cleared| barrier.checkpoint_id <= cleared)
        {
            // Late barrier from a checkpoint that has already completed/aborted.
            return Ok(BarrierAlignResult::Buffering);
        }

        if let Some(current) = self.current_barrier {
            if barrier.checkpoint_id != current.checkpoint_id {
                return Err(anyhow!(
                    "out-of-order barrier: current={}, incoming={}",
                    current.checkpoint_id,
                    barrier.checkpoint_id
                ));
            }
        } else {
            self.current_barrier = Some(barrier);
            self.barriers_received.fill(false);
            self.blocked_channels.fill(false);
            self.buffered.clear();
        }

        if self.barriers_received[channel_idx] {
            return Err(anyhow!(
                "duplicate barrier {} on channel {}",
                barrier.checkpoint_id,
                channel_idx
            ));
        }

        self.barriers_received[channel_idx] = true;
        self.blocked_channels[channel_idx] = true;

        if self.barriers_received.iter().all(|v| *v) {
            let aligned_barrier = self
                .current_barrier
                .take()
                .ok_or_else(|| anyhow!("internal aligner state is inconsistent"))?;
            let buffered = self.buffered.drain(..).collect();
            self.barriers_received.fill(false);
            self.blocked_channels.fill(false);
            self.mark_checkpoint_cleared(aligned_barrier.checkpoint_id);
            return Ok(BarrierAlignResult::Aligned {
                barrier: aligned_barrier,
                buffered,
            });
        }

        Ok(BarrierAlignResult::Buffering)
    }

    fn process_non_barrier(
        &mut self,
        channel_idx: ChannelIndex,
        element: StreamElement<T>,
    ) -> Result<BarrierAlignResult<T>> {
        if self.current_barrier.is_some() && self.blocked_channels[channel_idx] {
            if self.buffered.len() >= self.max_buffer_size {
                let checkpoint_id = self
                    .current_barrier
                    .take()
                    .ok_or_else(|| anyhow!("internal aligner state is inconsistent"))?
                    .checkpoint_id;
                let mut drained: Vec<(ChannelIndex, StreamElement<T>)> =
                    self.buffered.drain(..).collect();
                drained.push((channel_idx, element));
                self.barriers_received.fill(false);
                self.blocked_channels.fill(false);
                self.mark_checkpoint_cleared(checkpoint_id);
                return Ok(BarrierAlignResult::Aborted {
                    checkpoint_id,
                    drained,
                });
            }
            self.buffered.push_back((channel_idx, element));
            return Ok(BarrierAlignResult::Buffering);
        }

        Ok(BarrierAlignResult::Forward(element))
    }

    fn mark_checkpoint_cleared(&mut self, checkpoint_id: CheckpointId) {
        self.last_cleared_checkpoint_id = Some(
            self.last_cleared_checkpoint_id
                .map_or(checkpoint_id, |prev| prev.max(checkpoint_id)),
        );
    }
}

/// Pending checkpoint tracked by the coordinator.
#[derive(Debug)]
pub struct PendingCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub timestamp: EventTime,
    pub expected_tasks: HashSet<TaskId>,
    pub acknowledged_tasks: HashSet<TaskId>,
    pub task_states: HashMap<TaskId, Vec<u8>>,
}

/// Checkpoint coordinator skeleton for single-process mode.
pub struct CheckpointCoordinator<S: CheckpointStorage> {
    pub next_checkpoint_id: AtomicU64,
    pub pending_checkpoints: Mutex<HashMap<CheckpointId, PendingCheckpoint>>,
    pub completed_checkpoints: Mutex<VecDeque<CheckpointMetadata>>,
    pub aborted_checkpoints: Mutex<HashSet<CheckpointId>>,
    pub checkpoint_storage: Arc<S>,
    pub retained_checkpoints: usize,
}

impl<S: CheckpointStorage> CheckpointCoordinator<S> {
    pub fn new(checkpoint_storage: Arc<S>) -> Self {
        Self {
            next_checkpoint_id: AtomicU64::new(1),
            pending_checkpoints: Mutex::new(HashMap::new()),
            completed_checkpoints: Mutex::new(VecDeque::new()),
            aborted_checkpoints: Mutex::new(HashSet::new()),
            checkpoint_storage,
            retained_checkpoints: 3,
        }
    }

    pub fn with_retained_checkpoints(mut self, retained_checkpoints: usize) -> Self {
        self.retained_checkpoints = retained_checkpoints.max(1);
        self
    }

    pub fn trigger_checkpoint(
        &self,
        timestamp: EventTime,
        expected_tasks: Vec<TaskId>,
    ) -> Result<Barrier> {
        if expected_tasks.is_empty() {
            return Err(anyhow!("expected_tasks must not be empty"));
        }

        let checkpoint_id = self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);
        let expected_tasks_set: HashSet<TaskId> = expected_tasks.into_iter().collect();
        let pending = PendingCheckpoint {
            checkpoint_id,
            timestamp,
            expected_tasks: expected_tasks_set,
            acknowledged_tasks: HashSet::new(),
            task_states: HashMap::new(),
        };

        self.pending_checkpoints
            .lock()
            .map_err(|_| anyhow!("pending_checkpoints lock poisoned"))?
            .insert(checkpoint_id, pending);

        Ok(Barrier::with_timestamp(checkpoint_id, timestamp))
    }

    pub fn acknowledge_checkpoint(&self, ack: TaskCheckpointAck) -> Result<bool> {
        if self
            .aborted_checkpoints
            .lock()
            .map_err(|_| anyhow!("aborted_checkpoints lock poisoned"))?
            .contains(&ack.checkpoint_id)
        {
            // Ignore late acks for aborted checkpoints.
            return Ok(false);
        }

        let mut pending_guard = self
            .pending_checkpoints
            .lock()
            .map_err(|_| anyhow!("pending_checkpoints lock poisoned"))?;

        let pending = pending_guard
            .get_mut(&ack.checkpoint_id)
            .ok_or_else(|| anyhow!("checkpoint {} is not pending", ack.checkpoint_id))?;

        if !pending.expected_tasks.contains(&ack.task_id) {
            return Err(anyhow!(
                "task {} is not expected for checkpoint {}",
                ack.task_id,
                ack.checkpoint_id
            ));
        }
        if pending.acknowledged_tasks.contains(&ack.task_id) {
            return Err(anyhow!(
                "duplicate ack from task {} for checkpoint {}",
                ack.task_id,
                ack.checkpoint_id
            ));
        }

        pending.acknowledged_tasks.insert(ack.task_id);
        pending.task_states.insert(ack.task_id, ack.state);

        if pending.acknowledged_tasks.len() != pending.expected_tasks.len() {
            return Ok(false);
        }

        let finished = pending_guard
            .remove(&ack.checkpoint_id)
            .ok_or_else(|| anyhow!("checkpoint {} disappeared", ack.checkpoint_id))?;
        drop(pending_guard);
        self.finalize_checkpoint(finished)?;
        Ok(true)
    }

    pub fn abort_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        task_id: TaskId,
        _reason: &str,
    ) -> Result<bool> {
        let removed = {
            let mut pending_guard = self
                .pending_checkpoints
                .lock()
                .map_err(|_| anyhow!("pending_checkpoints lock poisoned"))?;
            if let Some(pending) = pending_guard.get(&checkpoint_id) {
                if !pending.expected_tasks.contains(&task_id) {
                    return Err(anyhow!(
                        "task {} is not expected for checkpoint {}",
                        task_id,
                        checkpoint_id
                    ));
                }
            } else {
                return Ok(false);
            }
            pending_guard.remove(&checkpoint_id).is_some()
        };

        if removed {
            self.aborted_checkpoints
                .lock()
                .map_err(|_| anyhow!("aborted_checkpoints lock poisoned"))?
                .insert(checkpoint_id);
        }
        Ok(removed)
    }

    pub fn completed_checkpoint_ids(&self) -> Result<Vec<CheckpointId>> {
        let guard = self
            .completed_checkpoints
            .lock()
            .map_err(|_| anyhow!("completed_checkpoints lock poisoned"))?;
        Ok(guard.iter().map(|m| m.checkpoint_id).collect())
    }

    pub fn aborted_checkpoint_ids(&self) -> Result<Vec<CheckpointId>> {
        let mut ids: Vec<_> = self
            .aborted_checkpoints
            .lock()
            .map_err(|_| anyhow!("aborted_checkpoints lock poisoned"))?
            .iter()
            .copied()
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    fn finalize_checkpoint(&self, pending: PendingCheckpoint) -> Result<()> {
        let mut task_ids: Vec<TaskId> = pending.expected_tasks.iter().copied().collect();
        task_ids.sort_by_key(|t| (t.vertex_id.0, t.subtask_index));

        let metadata = CheckpointMetadata {
            checkpoint_id: pending.checkpoint_id,
            timestamp: pending.timestamp,
            task_ids,
        };

        for (task_id, state) in pending.task_states {
            self.checkpoint_storage
                .save_task_state(metadata.checkpoint_id, task_id, state)?;
        }
        self.checkpoint_storage.save_checkpoint(metadata.clone())?;
        self.checkpoint_storage.purge(self.retained_checkpoints)?;

        let mut completed_guard = self
            .completed_checkpoints
            .lock()
            .map_err(|_| anyhow!("completed_checkpoints lock poisoned"))?;
        completed_guard.push_back(metadata);
        while completed_guard.len() > self.retained_checkpoints {
            completed_guard.pop_front();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{TaskId, VertexId};
    use crate::types::StreamElement;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "streamcrab-{prefix}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn test_checkpoint_barrier_with_timestamp_roundtrip() {
        let elem = StreamElement::<i32>::barrier_with_timestamp(10, 123);
        match elem {
            StreamElement::CheckpointBarrier(barrier) => {
                assert_eq!(barrier.checkpoint_id, 10);
                assert_eq!(barrier.timestamp, 123);
            }
            _ => panic!("expected barrier element"),
        }
    }

    #[test]
    fn test_in_memory_checkpoint_storage_roundtrip() {
        let storage = InMemoryCheckpointStorage::new();
        let t0 = TaskId::new(VertexId::new(1), 0);
        let meta = CheckpointMetadata {
            checkpoint_id: 7,
            timestamp: 42,
            task_ids: vec![t0],
        };

        storage.save_checkpoint(meta.clone()).unwrap();
        storage.save_task_state(7, t0, vec![1, 2, 3]).unwrap();

        assert_eq!(storage.load_checkpoint(7).unwrap(), meta);
        assert_eq!(storage.load_task_state(7, t0).unwrap(), vec![1, 2, 3]);
        assert_eq!(storage.list_checkpoints().unwrap(), vec![7]);
    }

    #[test]
    fn test_fs_checkpoint_storage_roundtrip() {
        let path = unique_temp_dir("checkpoint-storage");
        let storage = FsCheckpointStorage::new(&path).unwrap();

        let t0 = TaskId::new(VertexId::new(2), 1);
        let meta = CheckpointMetadata {
            checkpoint_id: 9,
            timestamp: 77,
            task_ids: vec![t0],
        };

        storage.save_checkpoint(meta.clone()).unwrap();
        storage.save_task_state(9, t0, vec![9, 8, 7]).unwrap();

        assert_eq!(storage.load_checkpoint(9).unwrap(), meta);
        assert_eq!(storage.load_task_state(9, t0).unwrap(), vec![9, 8, 7]);
        assert_eq!(storage.list_checkpoints().unwrap(), vec![9]);

        fs::remove_dir_all(&path).unwrap();
    }

    #[test]
    fn test_in_memory_storage_purge_keeps_latest_n() {
        let storage = InMemoryCheckpointStorage::new();
        let t0 = TaskId::new(VertexId::new(1), 0);
        for id in 1..=4 {
            storage
                .save_checkpoint(CheckpointMetadata {
                    checkpoint_id: id,
                    timestamp: id as i64,
                    task_ids: vec![t0],
                })
                .unwrap();
        }

        storage.purge(2).unwrap();
        assert_eq!(storage.list_checkpoints().unwrap(), vec![3, 4]);
    }

    #[test]
    fn test_barrier_aligner_single_input_aligned_immediately() {
        let mut aligner = BarrierAligner::<i32>::new(1);
        let result = aligner
            .process_element(0, StreamElement::barrier_with_timestamp(1, 100))
            .unwrap();
        match result {
            BarrierAlignResult::Aligned { barrier, buffered } => {
                assert_eq!(barrier.checkpoint_id, 1);
                assert!(buffered.is_empty());
            }
            _ => panic!("expected aligned result"),
        }
    }

    #[test]
    fn test_barrier_aligner_channel_out_of_bounds_error() {
        let mut aligner = BarrierAligner::<i32>::new(2);
        let err = aligner
            .process_element(2, StreamElement::record(1))
            .unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_barrier_aligner_duplicate_barrier_error() {
        let mut aligner = BarrierAligner::<i32>::new(2);
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(8, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));

        let err = aligner
            .process_element(0, StreamElement::barrier_with_timestamp(8, 100))
            .unwrap_err();
        assert!(
            err.to_string().contains("duplicate barrier"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_barrier_aligner_multi_input_blocks_and_releases() {
        let mut aligner = BarrierAligner::<i32>::new(2);
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(7, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::record(10))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        match aligner
            .process_element(1, StreamElement::record(20))
            .unwrap()
        {
            BarrierAlignResult::Forward(StreamElement::Record(r)) => assert_eq!(r.value, 20),
            _ => panic!("expected forward record"),
        }

        match aligner
            .process_element(1, StreamElement::barrier_with_timestamp(7, 100))
            .unwrap()
        {
            BarrierAlignResult::Aligned { barrier, buffered } => {
                assert_eq!(barrier.checkpoint_id, 7);
                assert_eq!(buffered.len(), 1);
                assert_eq!(buffered[0].0, 0);
            }
            _ => panic!("expected aligned"),
        }

        match aligner
            .process_element(0, StreamElement::record(30))
            .unwrap()
        {
            BarrierAlignResult::Forward(StreamElement::Record(r)) => assert_eq!(r.value, 30),
            _ => panic!("expected forward after alignment"),
        }
    }

    #[test]
    fn test_barrier_aligner_abort_on_buffer_overflow() {
        let mut aligner = BarrierAligner::<i32>::new(2).with_max_buffer_size(1);
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(9, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::record(1))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));

        match aligner
            .process_element(0, StreamElement::record(2))
            .unwrap()
        {
            BarrierAlignResult::Aborted {
                checkpoint_id,
                drained,
            } => {
                assert_eq!(checkpoint_id, 9);
                assert_eq!(drained.len(), 2);
                match &drained[0].1 {
                    StreamElement::Record(r) => assert_eq!(r.value, 1),
                    _ => panic!("expected record"),
                }
                match &drained[1].1 {
                    StreamElement::Record(r) => assert_eq!(r.value, 2),
                    _ => panic!("expected record"),
                }
            }
            _ => panic!("expected aborted"),
        }
    }

    #[test]
    fn test_barrier_aligner_ignores_late_barrier_after_abort() {
        let mut aligner = BarrierAligner::<i32>::new(2).with_max_buffer_size(1);
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(9, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::record(1))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::record(2))
                .unwrap(),
            BarrierAlignResult::Aborted {
                checkpoint_id: 9,
                ..
            }
        ));

        // Late barrier from the aborted checkpoint must be ignored.
        assert!(matches!(
            aligner
                .process_element(1, StreamElement::barrier_with_timestamp(9, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));

        // New checkpoint can still align normally.
        assert!(matches!(
            aligner
                .process_element(1, StreamElement::barrier_with_timestamp(10, 200))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        match aligner
            .process_element(0, StreamElement::barrier_with_timestamp(10, 200))
            .unwrap()
        {
            BarrierAlignResult::Aligned { barrier, buffered } => {
                assert_eq!(barrier.checkpoint_id, 10);
                assert!(buffered.is_empty());
            }
            _ => panic!("expected aligned result"),
        }
    }

    #[test]
    fn test_barrier_aligner_ignores_late_barrier_after_aligned() {
        let mut aligner = BarrierAligner::<i32>::new(2);
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(5, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        assert!(matches!(
            aligner
                .process_element(1, StreamElement::barrier_with_timestamp(5, 100))
                .unwrap(),
            BarrierAlignResult::Aligned {
                barrier: Barrier {
                    checkpoint_id: 5,
                    ..
                },
                ..
            }
        ));

        // Late barrier from completed checkpoint must be ignored.
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(5, 100))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));

        // Next checkpoint should still align.
        assert!(matches!(
            aligner
                .process_element(0, StreamElement::barrier_with_timestamp(6, 110))
                .unwrap(),
            BarrierAlignResult::Buffering
        ));
        assert!(matches!(
            aligner
                .process_element(1, StreamElement::barrier_with_timestamp(6, 110))
                .unwrap(),
            BarrierAlignResult::Aligned {
                barrier: Barrier {
                    checkpoint_id: 6,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn test_barrier_aligner_out_of_order_barrier_error() {
        let mut aligner = BarrierAligner::<i32>::new(2);
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(10, 100))
            .unwrap();

        let err = aligner
            .process_element(1, StreamElement::barrier_with_timestamp(11, 110))
            .unwrap_err();
        assert!(
            err.to_string().contains("out-of-order barrier"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_checkpoint_coordinator_trigger_ack_finalize() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage.clone());

        let t0 = TaskId::new(VertexId::new(1), 0);
        let t1 = TaskId::new(VertexId::new(1), 1);
        let barrier = coordinator.trigger_checkpoint(1234, vec![t0, t1]).unwrap();
        assert_eq!(barrier.checkpoint_id, 1);
        assert_eq!(barrier.timestamp, 1234);

        let done = coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: 1,
                task_id: t0,
                state: vec![1, 2],
            })
            .unwrap();
        assert!(!done);

        let done = coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: 1,
                task_id: t1,
                state: vec![3, 4],
            })
            .unwrap();
        assert!(done);

        assert_eq!(coordinator.completed_checkpoint_ids().unwrap(), vec![1]);
        let meta = storage.load_checkpoint(1).unwrap();
        assert_eq!(meta.checkpoint_id, 1);
        assert_eq!(meta.timestamp, 1234);
        assert_eq!(meta.task_ids.len(), 2);
        assert_eq!(storage.load_task_state(1, t0).unwrap(), vec![1, 2]);
        assert_eq!(storage.load_task_state(1, t1).unwrap(), vec![3, 4]);
    }

    #[test]
    fn test_checkpoint_coordinator_trigger_rejects_empty_expected_tasks() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage);
        let err = coordinator.trigger_checkpoint(123, Vec::new()).unwrap_err();
        assert!(
            err.to_string().contains("expected_tasks"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_checkpoint_coordinator_rejects_duplicate_ack() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage);
        let t0 = TaskId::new(VertexId::new(2), 0);

        coordinator.trigger_checkpoint(10, vec![t0]).unwrap();
        coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: 1,
                task_id: t0,
                state: vec![9],
            })
            .unwrap();

        let err = coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: 1,
                task_id: t0,
                state: vec![9],
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("not pending") || err.to_string().contains("duplicate ack"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_checkpoint_coordinator_retention_purges_old_checkpoints() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage.clone()).with_retained_checkpoints(2);
        let t0 = TaskId::new(VertexId::new(3), 0);

        for ts in [100, 200, 300] {
            let barrier = coordinator.trigger_checkpoint(ts, vec![t0]).unwrap();
            coordinator
                .acknowledge_checkpoint(TaskCheckpointAck {
                    checkpoint_id: barrier.checkpoint_id,
                    task_id: t0,
                    state: vec![ts as u8],
                })
                .unwrap();
        }

        assert_eq!(coordinator.completed_checkpoint_ids().unwrap(), vec![2, 3]);
        assert_eq!(storage.list_checkpoints().unwrap(), vec![2, 3]);
    }

    #[test]
    fn test_checkpoint_coordinator_abort_removes_pending_and_ignores_late_ack() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage);
        let t0 = TaskId::new(VertexId::new(5), 0);
        let t1 = TaskId::new(VertexId::new(5), 1);

        coordinator.trigger_checkpoint(50, vec![t0, t1]).unwrap();
        assert!(
            coordinator
                .abort_checkpoint(1, t0, "alignment buffer overflow")
                .unwrap()
        );
        assert_eq!(
            coordinator.completed_checkpoint_ids().unwrap(),
            Vec::<u64>::new()
        );
        assert_eq!(coordinator.aborted_checkpoint_ids().unwrap(), vec![1]);

        let pending_guard = coordinator.pending_checkpoints.lock().unwrap();
        assert!(!pending_guard.contains_key(&1));
        drop(pending_guard);

        // Acks arriving after abort should be ignored.
        let done = coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: 1,
                task_id: t1,
                state: vec![1],
            })
            .unwrap();
        assert!(!done);
    }

    #[test]
    fn test_checkpoint_coordinator_abort_non_pending_returns_false() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage);
        let t0 = TaskId::new(VertexId::new(6), 0);
        assert!(!coordinator.abort_checkpoint(42, t0, "noop").unwrap());
    }

    #[test]
    fn test_checkpoint_coordinator_abort_rejects_unexpected_task() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage);
        let t0 = TaskId::new(VertexId::new(7), 0);
        let t1 = TaskId::new(VertexId::new(7), 1);

        coordinator.trigger_checkpoint(88, vec![t0]).unwrap();
        let err = coordinator
            .abort_checkpoint(1, t1, "unexpected")
            .unwrap_err();
        assert!(
            err.to_string().contains("is not expected"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_checkpoint_coordinator_retained_minimum_is_one() {
        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let coordinator = CheckpointCoordinator::new(storage.clone()).with_retained_checkpoints(0);
        let t0 = TaskId::new(VertexId::new(8), 0);

        for ts in [11, 22] {
            let barrier = coordinator.trigger_checkpoint(ts, vec![t0]).unwrap();
            coordinator
                .acknowledge_checkpoint(TaskCheckpointAck {
                    checkpoint_id: barrier.checkpoint_id,
                    task_id: t0,
                    state: vec![ts as u8],
                })
                .unwrap();
        }

        assert_eq!(coordinator.completed_checkpoint_ids().unwrap(), vec![2]);
        assert_eq!(storage.list_checkpoints().unwrap(), vec![2]);
    }
}
