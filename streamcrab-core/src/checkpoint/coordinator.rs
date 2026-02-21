use super::*;

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
