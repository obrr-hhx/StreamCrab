//! SQL subset end-to-end: in-memory tables and a real Paimon table.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use streamcrab_sql::SqlContext;

fn table_t() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    (0..100i64)
        .collect::<Vec<_>>()
        .chunks(17)
        .map(|chunk| {
            let ks: Vec<i64> = chunk.iter().map(|i| i % 5).collect();
            let vs: Vec<i64> = chunk.to_vec();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn i64_rows(batches: &[RecordBatch]) -> Vec<Vec<i64>> {
    let mut rows = Vec::new();
    for b in batches {
        for i in 0..b.num_rows() {
            rows.push(
                (0..b.num_columns())
                    .map(|c| {
                        b.column(c)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(i)
                    })
                    .collect(),
            );
        }
    }
    rows.sort_unstable();
    rows
}

#[test]
fn select_where_filters_rows() {
    let mut ctx = SqlContext::new();
    ctx.register_batches("t", table_t()).unwrap();

    let out = ctx.sql("SELECT k, v FROM t WHERE v >= 95").unwrap();
    let rows = i64_rows(&out);
    assert_eq!(
        rows,
        (95..100i64).map(|v| vec![v % 5, v]).collect::<Vec<_>>()
    );
}

#[test]
fn select_projection_expressions() {
    let mut ctx = SqlContext::new();
    ctx.register_batches("t", table_t()).unwrap();

    let out = ctx
        .sql("SELECT v * 2 AS doubled FROM t WHERE v < 3")
        .unwrap();
    assert_eq!(out[0].schema().field(0).name(), "doubled");
    assert_eq!(i64_rows(&out), vec![vec![0], vec![2], vec![4]]);
}

#[test]
fn group_by_sum() {
    let mut ctx = SqlContext::new();
    ctx.register_batches("t", table_t()).unwrap();

    let out = ctx
        .sql("SELECT k, SUM(v) AS total FROM t WHERE v >= 50 GROUP BY k")
        .unwrap();

    let mut expected: std::collections::HashMap<i64, f64> = Default::default();
    for v in 50..100i64 {
        *expected.entry(v % 5).or_default() += v as f64;
    }

    let mut seen = 0;
    for b in &out {
        let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(sums.value(i), expected[&keys.value(i)]);
            seen += 1;
        }
    }
    assert_eq!(seen, 5);
}

#[test]
fn unsupported_features_fail_clearly() {
    let mut ctx = SqlContext::new();
    ctx.register_batches("t", table_t()).unwrap();
    let err = ctx.sql("SELECT k FROM t ORDER BY k").unwrap_err();
    assert!(err.to_string().contains("unsupported"), "got: {err:#}");
}

mod paimon_e2e {
    use super::*;
    use paimon::catalog::Identifier;
    use paimon::spec::{BigIntType, DataType as PaimonType, Schema as PaimonSchema};
    use paimon::{Catalog, FileSystemCatalog, Options};
    use streamcrab_paimon::{PaimonSink, PaimonSource};

    #[test]
    fn sql_over_paimon_table() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let warehouse = std::env::temp_dir()
            .join(format!("streamcrab-sql-{}-{nanos}", std::process::id()))
            .to_string_lossy()
            .into_owned();

        // Create the table and land two committed epochs through the 2PC sink.
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let mut options = Options::new();
            options.set("warehouse", &warehouse);
            let catalog = FileSystemCatalog::new(options).unwrap();
            catalog
                .create_database("db", true, std::collections::HashMap::new())
                .await
                .unwrap();
            let schema = PaimonSchema::builder()
                .column("k", PaimonType::BigInt(BigIntType::new()))
                .column("v", PaimonType::BigInt(BigIntType::new()))
                .build()
                .unwrap();
            catalog
                .create_table(&Identifier::new("db", "t"), schema, false)
                .await
                .unwrap();
        });

        let mut sink = PaimonSink::new(&warehouse, "db", "t");
        sink.open().unwrap();
        for (epoch, range) in [(1u64, 0..50i64), (2, 50..100)] {
            let ks: Vec<i64> = range.clone().map(|i| i % 5).collect();
            let vs: Vec<i64> = range.collect();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, true),
                    Field::new("v", DataType::Int64, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Int64Array::from(vs)),
                ],
            )
            .unwrap();
            sink.write_batch(&batch).unwrap();
            sink.pre_commit(epoch).unwrap();
        }
        sink.commit_transaction(2).unwrap();

        // Kafka-less lakehouse loop: scan Paimon, register, query with SQL.
        let batches = PaimonSource::new(&warehouse, "db", "t").scan_blocking().unwrap();
        let mut ctx = SqlContext::new();
        ctx.register_batches("paimon_t", batches).unwrap();

        let out = ctx
            .sql("SELECT k, SUM(v) AS total FROM paimon_t GROUP BY k")
            .unwrap();
        let mut expected: std::collections::HashMap<i64, f64> = Default::default();
        for v in 0..100i64 {
            *expected.entry(v % 5).or_default() += v as f64;
        }
        let mut seen = 0;
        for b in &out {
            let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                assert_eq!(sums.value(i), expected[&keys.value(i)]);
                seen += 1;
            }
        }
        assert_eq!(seen, 5);

        std::fs::remove_dir_all(&warehouse).ok();
    }
}
