//! Self-contained Paimon roundtrip: create a table with paimon-rust's write
//! path, commit a snapshot, then scan it back through `PaimonSource`.
//!
//! This doubles as the P12 feasibility probe: `prepare_commit` /
//! `TableCommit::commit` map directly onto StreamCrab's 2PC sink protocol.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use paimon::catalog::Identifier;
use paimon::spec::{BigIntType, DataType, Schema};
use paimon::table::TableCommit;
use paimon::{Catalog, FileSystemCatalog, Options};
use streamcrab_paimon::PaimonSource;

fn unique_warehouse() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("streamcrab-paimon-{}-{nanos}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

fn test_batch(rows: std::ops::Range<i64>) -> RecordBatch {
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

#[tokio::test]
async fn paimon_write_commit_then_scan() {
    let warehouse = unique_warehouse();

    let mut options = Options::new();
    options.set("warehouse", &warehouse);
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
    let identifier = Identifier::new("db", "t");
    catalog.create_table(&identifier, schema, false).await.unwrap();
    let table = catalog.get_table(&identifier).await.unwrap();

    // Write two batches and commit one snapshot (2PC shape:
    // prepare_commit -> commit).
    let write_builder = table.new_write_builder();
    let mut write = write_builder.new_write().unwrap();
    write.write_arrow_batch(&test_batch(0..60)).await.unwrap();
    write.write_arrow_batch(&test_batch(60..100)).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert!(!messages.is_empty(), "prepare_commit must yield messages");
    TableCommit::new(table.clone(), "streamcrab-test".to_string())
        .commit(messages)
        .await
        .unwrap();

    // Read back through the source.
    let source = PaimonSource::new(&warehouse, "db", "t");
    let batches = source.scan().await.unwrap();

    let mut rows: Vec<(i64, i64)> = Vec::new();
    for b in &batches {
        let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let vs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((ks.value(i), vs.value(i)));
        }
    }
    rows.sort_unstable();

    let expected: Vec<(i64, i64)> = (0..100).map(|i| (i, i * 10)).collect();
    assert_eq!(rows, expected);

    std::fs::remove_dir_all(&warehouse).ok();
}
