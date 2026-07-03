use super::*;
use opendal::Operator;
use std::future::Future;

/// Object-store checkpoint storage backed by Apache OpenDAL.
///
/// Works with any OpenDAL service (S3, OSS, GCS, memory, ...). This is the
/// storage backend for cluster deployments: checkpoints live outside the
/// TaskManager hosts, so a task rescheduled to a new machine can still
/// recover its state.
///
/// Object layout mirrors [`FsCheckpointStorage`]:
/// `chk-{id}/metadata.bin`, `chk-{id}/task-{task_id}.bin`.
pub struct ObjectStoreCheckpointStorage {
    op: Operator,
    // Option so Drop can take ownership for `shutdown_background()`; a plain
    // Runtime drop panics when the storage is dropped inside a tokio context.
    rt: Option<tokio::runtime::Runtime>,
}

impl ObjectStoreCheckpointStorage {
    pub fn new(op: Operator) -> Result<Self> {
        // A dedicated worker thread must keep running between calls: hyper's
        // pooled keep-alive connections and timers are tasks on this runtime,
        // and an undriven runtime (current-thread + intermittent block_on)
        // deadlocks the next request on pool checkout.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("checkpoint-io")
            .enable_all()
            .build()
            .context("failed to build checkpoint IO runtime")?;
        Ok(Self { op, rt: Some(rt) })
    }

    /// Build an S3-backed storage (AWS S3, MinIO, or any S3-compatible store).
    /// Credentials are resolved from the standard AWS credential chain
    /// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars, etc.).
    pub fn s3(
        bucket: &str,
        root: &str,
        endpoint: Option<&str>,
        region: Option<&str>,
    ) -> Result<Self> {
        let mut builder = opendal::services::S3::default().bucket(bucket).root(root);
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(region) = region {
            builder = builder.region(region);
        }
        let op = Operator::new(builder)
            .context("failed to configure S3 checkpoint storage")?
            .finish();
        Self::new(op)
    }

    /// Run an OpenDAL future to completion from the sync trait methods.
    /// Spawns onto the dedicated runtime and blocks on a channel, which is
    /// safe from both sync and async callers (a nested `block_on` on the
    /// caller's runtime would panic).
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = self.rt.as_ref().expect("runtime taken only in Drop");
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        rt.spawn(async move {
            let _ = tx.send(fut.await);
        });
        rx.recv().expect("checkpoint IO task panicked")
    }

    fn metadata_path(checkpoint_id: CheckpointId) -> String {
        format!("chk-{checkpoint_id}/metadata.bin")
    }

    fn task_state_path(checkpoint_id: CheckpointId, task_id: TaskId) -> String {
        format!("chk-{checkpoint_id}/task-{task_id}.bin")
    }
}

impl CheckpointStorage for ObjectStoreCheckpointStorage {
    fn save_checkpoint(&self, metadata: CheckpointMetadata) -> Result<()> {
        let bytes =
            bincode::serialize(&metadata).context("serialize checkpoint metadata failed")?;
        let op = self.op.clone();
        let path = Self::metadata_path(metadata.checkpoint_id);
        self.block_on(async move { op.write(&path, bytes).await })
            .context("write checkpoint metadata to object store failed")?;
        Ok(())
    }

    fn save_task_state(
        &self,
        checkpoint_id: CheckpointId,
        task_id: TaskId,
        state: Vec<u8>,
    ) -> Result<()> {
        let op = self.op.clone();
        let path = Self::task_state_path(checkpoint_id, task_id);
        self.block_on(async move { op.write(&path, state).await })
            .context("write checkpoint task state to object store failed")?;
        Ok(())
    }

    fn load_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<CheckpointMetadata> {
        let op = self.op.clone();
        let path = Self::metadata_path(checkpoint_id);
        let buf = self
            .block_on(async move { op.read(&path).await })
            .context("read checkpoint metadata from object store failed")?;
        bincode::deserialize(&buf.to_vec()).context("deserialize checkpoint metadata failed")
    }

    fn load_task_state(&self, checkpoint_id: CheckpointId, task_id: TaskId) -> Result<Vec<u8>> {
        let op = self.op.clone();
        let path = Self::task_state_path(checkpoint_id, task_id);
        let buf = self
            .block_on(async move { op.read(&path).await })
            .context("read checkpoint task state from object store failed")?;
        Ok(buf.to_vec())
    }

    fn list_checkpoints(&self) -> Result<Vec<CheckpointId>> {
        let op = self.op.clone();
        let entries = self
            .block_on(async move { op.list_with("").recursive(true).await })
            .context("list checkpoints in object store failed")?;
        let mut ids: Vec<CheckpointId> = entries
            .iter()
            .filter_map(|e| e.path().strip_prefix("chk-"))
            .filter_map(|rest| rest.split('/').next())
            .filter_map(|id| id.parse().ok())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    fn purge(&self, keep_last_n: usize) -> Result<()> {
        let ids = self.list_checkpoints()?;
        if ids.len() <= keep_last_n {
            return Ok(());
        }
        let purge_count = ids.len() - keep_last_n;
        for checkpoint_id in ids.into_iter().take(purge_count) {
            let op = self.op.clone();
            let path = format!("chk-{checkpoint_id}/");
            self.block_on(async move { op.delete_with(&path).recursive(true).await })
                .with_context(|| format!("purge checkpoint {checkpoint_id} failed"))?;
        }
        Ok(())
    }
}

impl Drop for ObjectStoreCheckpointStorage {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

#[cfg(test)]
#[path = "tests/opendal_storage_tests.rs"]
mod tests;
