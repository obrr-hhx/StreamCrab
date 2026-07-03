//! Integration tests: vectorized operators running through StreamCrab's own
//! operator chain and the DataStream API (P9).

use arrow::array::{Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use streamcrab_api::environment::StreamExecutionEnvironment;
use streamcrab_core::operator_chain::{Chain, ChainEnd, Operator, TimerDomain};
use streamcrab_core::time::EVENT_TIME_MAX;
use streamcrab_vectorized::bridge::{Batcher, Unbatcher, VectorOp};
use streamcrab_vectorized::expression::{col, gt, lit_i64};
use streamcrab_vectorized::operators::{
    AggregateDescriptor, AggregateFunction, FilterOperator, HashAggregateOperator,
};

/// Feed rows through a chain and fire the end-of-stream timer, the same way
/// the runtime drives a bounded pipeline.
fn drain<C: Operator<(i64, i64)>>(chain: &mut C, rows: &[(i64, i64)]) -> Vec<C::OUT> {
    let mut out = Vec::new();
    chain.process_batch(rows, &mut out).unwrap();
    chain
        .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
        .unwrap();
    out
}

#[test]
fn batcher_filter_unbatcher_roundtrip() {
    let mut chain = Chain::new(
        Batcher::<(i64, i64)>::new(4),
        Chain::new(
            VectorOp::new(FilterOperator::new(gt(col(1), lit_i64(50)))),
            Chain::new(Unbatcher::<(i64, i64)>::new(), ChainEnd),
        ),
    );

    let rows: Vec<(i64, i64)> = (1..=10).map(|i| (i, i * 10)).collect();
    let out = drain(&mut chain, &rows);
    assert_eq!(out, vec![(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)]);
}

fn agg_chain() -> impl Operator<(i64, i64), OUT = RecordBatch> {
    Chain::new(
        Batcher::<(i64, i64)>::new(3),
        Chain::new(
            VectorOp::new(HashAggregateOperator::new(
                vec![0],
                vec![AggregateDescriptor {
                    function: AggregateFunction::Sum,
                    input_col: 1,
                    output_name: "sum".into(),
                }],
            )),
            ChainEnd,
        ),
    )
}

fn agg_results(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
    let mut rows = Vec::new();
    for b in batches {
        let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            rows.push((keys.value(i), sums.value(i)));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

#[test]
fn hash_aggregate_checkpoint_restore_resumes_exactly() {
    let part_a: Vec<(i64, i64)> = (0..50).map(|i| (i % 5, i)).collect();
    let part_b: Vec<(i64, i64)> = (50..100).map(|i| (i % 5, i)).collect();

    // Reference: continuous run over A ++ B.
    let mut continuous = agg_chain();
    let mut all = part_a.clone();
    all.extend(&part_b);
    let expected = agg_results(&drain(&mut continuous, &all));

    // Run A, snapshot mid-stream (barrier point: Batcher has pending rows,
    // HashAggregate has partial accumulators), restore into a fresh chain
    // and finish with B.
    let mut first = agg_chain();
    let mut sink = Vec::new();
    first.process_batch(&part_a, &mut sink).unwrap();
    assert!(sink.is_empty(), "aggregate must not emit before watermark");
    let snapshot = first.snapshot_state().unwrap();

    let mut second = agg_chain();
    second.restore_state(&snapshot).unwrap();
    let resumed = agg_results(&drain(&mut second, &part_b));

    assert_eq!(resumed, expected);
    assert_eq!(resumed.len(), 5);
    let total: f64 = resumed.iter().map(|(_, s)| s).sum();
    assert_eq!(total, (0..100).sum::<i64>() as f64);
}

#[test]
fn vectorized_segment_in_datastream_pipeline() {
    let rows: Vec<(i64, i64)> = (1..=1000).map(|i| (i % 10, i)).collect();
    let mut expected: std::collections::HashMap<i64, i64> = Default::default();
    for &(k, v) in &rows {
        if v > 500 {
            *expected.entry(k).or_default() += v;
        }
    }

    let env = StreamExecutionEnvironment::new("bridge-e2e");
    let results = env
        .from_iter(rows)
        .transform(Batcher::<(i64, i64)>::new(64))
        .transform(VectorOp::new(FilterOperator::new(gt(col(1), lit_i64(500)))))
        .transform(Unbatcher::<(i64, i64)>::new())
        .key_by(|r: &(i64, i64)| r.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(2)
        .unwrap();

    let results = results.lock().unwrap();
    assert_eq!(results.len(), expected.len());
    for (k, sum) in expected {
        assert_eq!(results.get(&k).map(|r| r.1), Some(sum), "key {k}");
    }
}
