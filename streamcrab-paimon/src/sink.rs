//! Two-phase-commit Paimon sink.
//!
//! Lifecycle mirrors the `KafkaSink` convention and maps 1:1 onto paimon's
//! write path:
//!
//! | StreamCrab 2PC        | Paimon                                  |
//! |-----------------------|-----------------------------------------|
//! | `begin_transaction`   | fresh `TableWrite`                      |
//! | `write_batch`         | `TableWrite::write_arrow_batch`         |
//! | `pre_commit(epoch)`   | `prepare_commit()` → hold messages      |
//! | `commit_transaction`  | `TableCommit::commit(messages)`         |
//! | `abort_transaction`   | drop writer + pending (files orphaned,  |
//! |                       | never referenced by any snapshot)       |
//!
//! # Exactly-once scope (v1)
//!
//! Failures before an epoch is sealed abort cleanly: nothing was committed,
//! the source replays, no duplicates and no loss. The remaining gap is a
//! crash between JM seal and `commit_transaction`: prepared messages live
//! only in memory because paimon 0.2's `CommitMessage` does not implement
//! serde — once it does (upstream PR candidate), pending epochs move into
//! the sink's checkpoint snapshot and recovery re-commits them.
//!
//! Call from plain (non-async) sink threads; methods drive paimon's async
//! API on a dedicated single-worker runtime.

use anyhow::{Context, Result, bail};
use arrow::record_batch::RecordBatch;
use paimon::catalog::Identifier;
use paimon::table::{CommitMessage, Table, TableCommit, TableWrite};
use paimon::{Catalog, FileSystemCatalog, Options};

pub struct PaimonSink {
    warehouse: String,
    database: String,
    table_name: String,
    commit_user: String,
    rt: Option<tokio::runtime::Runtime>,
    table: Option<Table>,
    write: Option<TableWrite>,
    /// Prepared-but-uncommitted epochs, oldest first.
    pending: Vec<(u64, Vec<CommitMessage>)>,
}

impl PaimonSink {
    pub fn new(
        warehouse: impl Into<String>,
        database: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            warehouse: warehouse.into(),
            database: database.into(),
            table_name: table_name.into(),
            commit_user: "streamcrab".to_string(),
            rt: None,
            table: None,
            write: None,
            pending: Vec::new(),
        }
    }

    pub fn with_commit_user(mut self, user: impl Into<String>) -> Self {
        self.commit_user = user.into();
        self
    }

    /// Open the table and start the first transaction.
    pub fn open(&mut self) -> Result<()> {
        // Single worker thread keeps driving connection/timer tasks between
        // the blocking calls below (an undriven runtime deadlocks pooled
        // HTTP transports on S3 warehouses).
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("paimon-sink-io")
            .enable_all()
            .build()
            .context("build paimon sink runtime")?;

        let mut options = Options::new();
        options.set("warehouse", &self.warehouse);
        let catalog = FileSystemCatalog::new(options).context("open filesystem catalog")?;
        let identifier = Identifier::new(&self.database, &self.table_name);
        let table = rt
            .block_on(catalog.get_table(&identifier))
            .with_context(|| format!("get table {identifier}"))?;

        self.rt = Some(rt);
        self.table = Some(table);
        self.begin_transaction()?;
        Ok(())
    }

    fn rt(&self) -> Result<&tokio::runtime::Runtime> {
        self.rt.as_ref().context("sink not opened; call open() first")
    }

    fn new_write(&self) -> Result<TableWrite> {
        self.table
            .as_ref()
            .context("sink not opened")?
            .new_write_builder()
            .new_write()
            .context("create table writer")
    }

    /// Ensure a writer exists for the current epoch.
    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.write.is_none() {
            self.write = Some(self.new_write()?);
        }
        Ok(())
    }

    /// Buffer a batch inside the current transaction.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let rt = self
            .rt
            .as_ref()
            .context("sink not opened; call open() first")?;
        let write = self
            .write
            .as_mut()
            .context("no open transaction; call begin_transaction()")?;
        rt.block_on(write.write_arrow_batch(batch))
            .context("write batch to paimon")?;
        Ok(())
    }

    /// Phase 1: flush the current writer into commit messages held for
    /// `epoch`, and start the next transaction. Called on barrier alignment
    /// before the checkpoint ack (invariant I1).
    pub fn pre_commit(&mut self, epoch: u64) -> Result<()> {
        let mut write = self
            .write
            .take()
            .context("no open transaction to pre-commit")?;
        let messages = self
            .rt()?
            .block_on(write.prepare_commit())
            .context("prepare_commit failed")?;
        if let Some((last_epoch, _)) = self.pending.last() {
            if *last_epoch >= epoch {
                bail!("pre_commit epochs must be monotonic: {last_epoch} >= {epoch}");
            }
        }
        if !messages.is_empty() {
            self.pending.push((epoch, messages));
        }
        self.write = Some(self.new_write()?);
        Ok(())
    }

    /// Phase 2: commit every prepared epoch `<= up_to_epoch` as Paimon
    /// snapshots. Called on notifyCheckpointComplete (after JM seal).
    pub fn commit_transaction(&mut self, up_to_epoch: u64) -> Result<()> {
        while let Some((epoch, _)) = self.pending.first() {
            if *epoch > up_to_epoch {
                break;
            }
            let (_, messages) = self.pending.remove(0);
            let table = self.table.as_ref().context("sink not opened")?.clone();
            let commit = TableCommit::new(table, self.commit_user.clone());
            self.rt()?
                .block_on(commit.commit(messages))
                .context("paimon snapshot commit failed")?;
        }
        Ok(())
    }

    /// Void the in-flight transaction and all prepared-but-uncommitted
    /// epochs (checkpoint abort / recovery rollback). Written data files
    /// become unreferenced orphans and are never visible to readers.
    pub fn abort_transaction(&mut self) -> Result<()> {
        self.write = None;
        self.pending.clear();
        self.begin_transaction()
    }

    /// Number of prepared-but-uncommitted epochs (for tests/monitoring).
    pub fn pending_epochs(&self) -> usize {
        self.pending.len()
    }
}

impl Drop for PaimonSink {
    fn drop(&mut self) {
        // Writers/tables must go before the runtime they were created on.
        self.write = None;
        self.table = None;
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}
