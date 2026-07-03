//! PaimonSink two-phase-commit semantics against a real local warehouse.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use paimon::catalog::Identifier;
use paimon::spec::{BigIntType, DataType, Schema};
use paimon::{Catalog, FileSystemCatalog, Options};
use streamcrab_paimon::{PaimonSink, PaimonSource};

fn unique_warehouse(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("streamcrab-sink-{tag}-{}-{nanos}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

async fn create_table(warehouse: &str) {
    let mut options = Options::new();
    options.set("warehouse", warehouse);
    let catalog = FileSystemCatalog::new(options).unwrap();
    catalog
        .create_database("db", true, std::collections::HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .column("k", DataType::BigInt(BigIntType::new()))
        .column("v", DataType::BigInt(BigIntType::new()))
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("db", "t"), schema, false)
        .await
        .unwrap();
}

fn batch(rows: std::ops::Range<i64>) -> RecordBatch {
    let ks: Vec<i64> = rows.clone().collect();
    let vs: Vec<i64> = rows.map(|i| i * 10).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("k", ArrowDataType::Int64, true),
            Field::new("v", ArrowDataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ks)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

fn scan_keys(warehouse: &str) -> Vec<i64> {
    let batches = PaimonSource::new(warehouse, "db", "t").scan_blocking().unwrap();
    let mut keys: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    keys.sort_unstable();
    keys
}

#[test]
fn two_phase_commit_visibility_and_ordering() {
    let warehouse = unique_warehouse("2pc");
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(create_table(&warehouse));

    let mut sink = PaimonSink::new(&warehouse, "db", "t").with_commit_user("test-2pc");
    sink.open().unwrap();

    // Epoch 1: written + prepared but NOT committed -> invisible.
    sink.write_batch(&batch(0..10)).unwrap();
    sink.pre_commit(1).unwrap();
    assert_eq!(sink.pending_epochs(), 1);
    assert!(scan_keys(&warehouse).is_empty(), "uncommitted data must be invisible");

    // Epoch 2 prepared as well; commit through epoch 2 publishes both in order.
    sink.write_batch(&batch(10..20)).unwrap();
    sink.pre_commit(2).unwrap();
    assert_eq!(sink.pending_epochs(), 2);
    sink.commit_transaction(2).unwrap();
    assert_eq!(sink.pending_epochs(), 0);
    assert_eq!(scan_keys(&warehouse), (0..20).collect::<Vec<_>>());

    std::fs::remove_dir_all(&warehouse).ok();
}

#[test]
fn abort_discards_inflight_and_prepared_epochs() {
    let warehouse = unique_warehouse("abort");
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(create_table(&warehouse));

    let mut sink = PaimonSink::new(&warehouse, "db", "t").with_commit_user("test-abort");
    sink.open().unwrap();

    // Commit a baseline epoch.
    sink.write_batch(&batch(0..5)).unwrap();
    sink.pre_commit(1).unwrap();
    sink.commit_transaction(1).unwrap();

    // In-flight writes + a prepared epoch, then abort (recovery rollback).
    sink.write_batch(&batch(100..110)).unwrap();
    sink.pre_commit(2).unwrap();
    sink.write_batch(&batch(200..210)).unwrap();
    sink.abort_transaction().unwrap();
    assert_eq!(sink.pending_epochs(), 0);

    // Only the committed baseline is visible; aborted files are orphans.
    assert_eq!(scan_keys(&warehouse), (0..5).collect::<Vec<_>>());

    // The sink stays usable after abort (fresh transaction).
    sink.write_batch(&batch(5..8)).unwrap();
    sink.pre_commit(3).unwrap();
    sink.commit_transaction(3).unwrap();
    assert_eq!(scan_keys(&warehouse), (0..8).collect::<Vec<_>>());

    std::fs::remove_dir_all(&warehouse).ok();
}
