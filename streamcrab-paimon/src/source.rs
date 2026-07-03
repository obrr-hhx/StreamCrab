//! Bounded Paimon table source.

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::{Catalog, FileSystemCatalog, Options};

/// Reads a Paimon table snapshot into Arrow `RecordBatch`es.
///
/// This is the Arrow-native entry into StreamCrab's vectorized pipeline:
/// batches go straight through `VectorOp`/`DfFilterOp`/... with no row
/// conversion. v1 is a bounded scan (latest snapshot); incremental/changelog
/// streaming follows once paimon-rust exposes it.
pub struct PaimonSource {
    warehouse: String,
    database: String,
    table: String,
}

impl PaimonSource {
    pub fn new(
        warehouse: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            warehouse: warehouse.into(),
            database: database.into(),
            table: table.into(),
        }
    }

    /// Scan the latest snapshot of the table into memory.
    pub async fn scan(&self) -> Result<Vec<RecordBatch>> {
        let mut options = Options::new();
        options.set("warehouse", &self.warehouse);
        let catalog = FileSystemCatalog::new(options).context("open filesystem catalog")?;
        let identifier = Identifier::new(&self.database, &self.table);
        let table = catalog
            .get_table(&identifier)
            .await
            .with_context(|| format!("get table {identifier}"))?;

        let read_builder = table.new_read_builder();
        let plan = read_builder
            .new_scan()
            .plan()
            .await
            .context("plan table scan")?;
        let read = read_builder.new_read().context("create table reader")?;
        let stream = read
            .to_arrow(plan.splits())
            .context("open arrow record batch stream")?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.context("read batches")?;
        Ok(batches)
    }

    /// Blocking variant for synchronous runtime contexts (e.g. driving an
    /// operator chain from a source thread).
    pub fn scan_blocking(&self) -> Result<Vec<RecordBatch>> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build source runtime")?
            .block_on(self.scan())
    }
}
