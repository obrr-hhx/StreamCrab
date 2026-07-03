//! P9 T8: row-oriented vs vectorized pipeline throughput, driven through the
//! same operator-chain machinery the runtime uses.
//!
//! Run for real numbers:
//!   cargo test -p streamcrab-vectorized --release --test bridge_benchmark -- --nocapture

use std::time::Instant;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::DataType;
use streamcrab_core::operator_chain::{Chain, ChainEnd, Operator, TimerDomain};
use streamcrab_core::time::EVENT_TIME_MAX;
use streamcrab_vectorized::bridge::{Batcher, Unbatcher, VectorOp};
use streamcrab_vectorized::expression::{col, gt, lit_i64, mul};
use streamcrab_vectorized::operators::{
    AggregateDescriptor, AggregateFunction, FilterOperator, HashAggregateOperator,
    ProjectOperator, Projection,
};

// Row-oriented reference operators (same shape as the api's FilterOp/MapOp).
struct RowFilter<F>(F);
impl<T, F> Operator<T> for RowFilter<F>
where
    T: Clone + Send,
    F: FnMut(&T) -> bool + Send,
{
    type OUT = T;
    fn process_batch(&mut self, input: &[T], output: &mut Vec<T>) -> anyhow::Result<()> {
        for item in input {
            if (self.0)(item) {
                output.push(item.clone());
            }
        }
        Ok(())
    }
}

struct RowMap<F>(F);
impl<T, U, F> Operator<T> for RowMap<F>
where
    T: Send,
    U: Send,
    F: FnMut(&T) -> U + Send,
{
    type OUT = U;
    fn process_batch(&mut self, input: &[T], output: &mut Vec<U>) -> anyhow::Result<()> {
        for item in input {
            output.push((self.0)(item));
        }
        Ok(())
    }
}

fn row_count() -> i64 {
    if cfg!(debug_assertions) {
        1_000_000
    } else {
        10_000_000
    }
}

#[test]
fn bench_row_vs_vectorized_filter_project() {
    let n = row_count();
    let rows: Vec<(i64, i64)> = (0..n).map(|i| (i % 1024, i)).collect();

    let mut row_chain = Chain::new(
        RowFilter(move |r: &(i64, i64)| r.1 > n / 2),
        Chain::new(RowMap(|r: &(i64, i64)| (r.0, r.1 * 2)), ChainEnd),
    );
    let start = Instant::now();
    let mut row_out = Vec::new();
    row_chain.process_batch(&rows, &mut row_out).unwrap();
    let row_time = start.elapsed();

    let mut vec_chain = Chain::new(
        Batcher::<(i64, i64)>::new(1024),
        Chain::new(
            VectorOp::new(FilterOperator::new(gt(col(1), lit_i64(n / 2)))),
            Chain::new(
                VectorOp::new(ProjectOperator::new(vec![
                    Projection::Column(0),
                    Projection::Computed {
                        expr: mul(col(1), lit_i64(2)),
                        name: "f1".into(),
                        data_type: DataType::Int64,
                    },
                ])),
                Chain::new(Unbatcher::<(i64, i64)>::new(), ChainEnd),
            ),
        ),
    );
    let start = Instant::now();
    let mut vec_out = Vec::new();
    vec_chain.process_batch(&rows, &mut vec_out).unwrap();
    vec_chain
        .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut vec_out)
        .unwrap();
    let vec_time = start.elapsed();

    row_out.sort_unstable();
    vec_out.sort_unstable();
    assert_eq!(row_out, vec_out);

    let row_tp = n as f64 / row_time.as_secs_f64() / 1e6;
    let vec_tp = n as f64 / vec_time.as_secs_f64() / 1e6;
    println!("\n=== P9 bench: filter+project over {n} rows ===");
    println!(
        "  row-oriented : {:>8.1} ms  ({row_tp:.1} M rec/s)",
        row_time.as_secs_f64() * 1e3
    );
    println!(
        "  vectorized   : {:>8.1} ms  ({vec_tp:.1} M rec/s)",
        vec_time.as_secs_f64() * 1e3
    );
    println!(
        "  speedup      : {:.2}x",
        row_time.as_secs_f64() / vec_time.as_secs_f64()
    );
}

#[test]
fn bench_row_vs_vectorized_keyed_sum() {
    let n = row_count();
    let rows: Vec<(i64, i64)> = (0..n).map(|i| (i % 1024, i)).collect();

    let start = Instant::now();
    let mut map: ahash::AHashMap<i64, f64> = ahash::AHashMap::default();
    for &(k, v) in &rows {
        *map.entry(k).or_default() += v as f64;
    }
    let row_time = start.elapsed();

    let mut chain = Chain::new(
        Batcher::<(i64, i64)>::new(1024),
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
    );
    let start = Instant::now();
    let mut out = Vec::new();
    chain.process_batch(&rows, &mut out).unwrap();
    chain
        .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
        .unwrap();
    let vec_time = start.elapsed();

    // Correctness: every group's sum must match the row-oriented reference.
    let mut groups = 0usize;
    for b in &out {
        let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(sums.value(i), map[&keys.value(i)]);
            groups += 1;
        }
    }
    assert_eq!(groups, 1024);

    let row_tp = n as f64 / row_time.as_secs_f64() / 1e6;
    let vec_tp = n as f64 / vec_time.as_secs_f64() / 1e6;
    println!("\n=== P9 bench: keyed sum over {n} rows (1024 keys) ===");
    println!(
        "  row hashmap  : {:>8.1} ms  ({row_tp:.1} M rec/s)",
        row_time.as_secs_f64() * 1e3
    );
    println!(
        "  vectorized   : {:>8.1} ms  ({vec_tp:.1} M rec/s)",
        vec_time.as_secs_f64() * 1e3
    );
    println!(
        "  speedup      : {:.2}x",
        row_time.as_secs_f64() / vec_time.as_secs_f64()
    );
}
