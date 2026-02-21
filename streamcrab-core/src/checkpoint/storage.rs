use super::*;

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
