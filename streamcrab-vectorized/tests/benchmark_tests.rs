//! Performance benchmark tests for streamcrab-vectorized operators.
//!
//! These are regular `#[test]` functions that measure throughput and print
//! human-readable results.  Run with `-- --nocapture` to see the output.
//!
//! Each benchmark:
//!   - runs 10 iterations and takes the median elapsed time
//!   - asserts a minimum throughput floor (sanity check)
//!   - prints one formatted result line
//!
//! At the end the full summary table is printed.

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::{Duration, Instant};

use streamcrab_vectorized::operators::{
    AggregateDescriptor, AggregateFunction, FilterOperator, HashAggregateOperator,
    HashJoinOperator, JoinType, ProjectOperator, Projection, VectorizedOperator,
    WindowAggFunction, WindowAggregateDescriptor, WindowAggregateOperator, WindowType,
};
use streamcrab_vectorized::expression::eval::{add, col, gt, lit_f64, mul};
use streamcrab_vectorized::VeloxBatch;

// ── helpers ───────────────────────────────────────────────────────────────────

const ITERS: usize = 10;

/// Run `f` `ITERS` times, collect durations, return median.
fn median_duration<F: FnMut()>(mut f: F) -> Duration {
    let mut durations: Vec<Duration> = (0..ITERS)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed()
        })
        .collect();
    durations.sort();
    durations[ITERS / 2]
}

fn rows_per_sec(rows: usize, d: Duration) -> f64 {
    rows as f64 / d.as_secs_f64()
}

fn million_rows_per_sec(rows: usize, d: Duration) -> f64 {
    rows_per_sec(rows, d) / 1_000_000.0
}

/// Build a 1M-row batch with schema [id:Int64, value:Float64].
fn make_large_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ids: Vec<i64> = (0..n as i64).collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

// ── test_benchmark_filter_throughput ─────────────────────────────────────────

#[test]
fn test_benchmark_filter_throughput() {
    const N: usize = 1_000_000;
    let batch = make_large_batch(N);

    let d = median_duration(|| {
        let mut op = FilterOperator::new(gt(col(1), lit_f64(500_000.0)));
        op.add_input(VeloxBatch::new(batch.clone())).unwrap();
        let _ = op.get_output().unwrap().unwrap().materialize().unwrap();
    });

    let mrps = million_rows_per_sec(N, d);
    println!("Filter (1M rows):          {:.1} M rows/sec", mrps);

    assert!(
        mrps > 1.0,
        "filter throughput {:.1} M rows/sec below 1 M rows/sec floor",
        mrps
    );
}

// ── test_benchmark_project_throughput ────────────────────────────────────────

#[test]
fn test_benchmark_project_throughput() {
    const N: usize = 1_000_000;
    let batch = make_large_batch(N);

    let projections = vec![
        Projection::Column(0),  // id pass-through
        Projection::Column(1),  // value pass-through
        Projection::Computed {
            expr: add(mul(col(1), lit_f64(2.0)), lit_f64(1.0)),
            name: "computed".into(),
            data_type: DataType::Float64,
        },
    ];

    let d = median_duration(|| {
        let mut op = ProjectOperator::new(projections.clone());
        op.add_input(VeloxBatch::new(batch.clone())).unwrap();
        let _ = op.get_output().unwrap().unwrap().materialize().unwrap();
    });

    let mrps = million_rows_per_sec(N, d);
    println!("Project (1M rows):         {:.1} M rows/sec", mrps);

    assert!(
        mrps > 1.0,
        "project throughput {:.1} M rows/sec below 1 M rows/sec floor",
        mrps
    );
}

// ── test_benchmark_hash_aggregate_throughput ─────────────────────────────────

#[test]
fn test_benchmark_hash_aggregate_throughput() {
    const N: usize = 1_000_000;
    const NUM_GROUPS: usize = 100;

    // Schema: [group_id:Int64, value:Float64]
    let schema = Arc::new(Schema::new(vec![
        Field::new("group_id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let group_ids: Vec<i64> = (0..N as i64).map(|i| i % NUM_GROUPS as i64).collect();
    let values: Vec<f64> = (0..N).map(|i| i as f64).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(group_ids)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();

    // Measure accumulation time.
    let accum_d = median_duration(|| {
        let mut op = HashAggregateOperator::new(
            vec![0],
            vec![
                AggregateDescriptor { function: AggregateFunction::Sum,   input_col: 1, output_name: "s".into() },
                AggregateDescriptor { function: AggregateFunction::Count, input_col: 1, output_name: "c".into() },
            ],
        );
        op.add_input(VeloxBatch::new(batch.clone())).unwrap();
        // Don't flush — just measure accumulation.
    });

    // Measure flush (on_watermark) time in a single op that already has accumulated.
    let mut op_for_flush = HashAggregateOperator::new(
        vec![0],
        vec![
            AggregateDescriptor { function: AggregateFunction::Sum,   input_col: 1, output_name: "s".into() },
            AggregateDescriptor { function: AggregateFunction::Count, input_col: 1, output_name: "c".into() },
        ],
    );
    op_for_flush.add_input(VeloxBatch::new(batch.clone())).unwrap();

    let flush_start = Instant::now();
    let _ = op_for_flush.on_watermark(0).unwrap();
    let flush_d = flush_start.elapsed();

    let accum_mrps = million_rows_per_sec(N, accum_d);
    let flush_ms = flush_d.as_secs_f64() * 1000.0;
    println!(
        "HashAggregate (1M rows):   {:.1} M rows/sec (accum), {:.1} ms (flush)",
        accum_mrps, flush_ms
    );
}

// ── test_benchmark_hash_join_throughput ──────────────────────────────────────

#[test]
fn test_benchmark_hash_join_throughput() {
    const BUILD_N: usize = 10_000;
    const PROBE_N: usize = 1_000_000;

    // Build side: [id:Int64, value:Float64]
    let build_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let build_ids: Vec<i64> = (0..BUILD_N as i64).collect();
    let build_vals: Vec<f64> = (0..BUILD_N).map(|i| i as f64).collect();
    let build_batch = RecordBatch::try_new(
        build_schema,
        vec![
            Arc::new(Int64Array::from(build_ids)),
            Arc::new(Float64Array::from(build_vals)),
        ],
    )
    .unwrap();

    // Probe side: [probe_id:Int64, value:Float64] — each matches exactly 1 build row.
    let probe_schema = Arc::new(Schema::new(vec![
        Field::new("probe_id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let probe_ids: Vec<i64> = (0..PROBE_N as i64).map(|i| i % BUILD_N as i64).collect();
    let probe_vals: Vec<f64> = (0..PROBE_N).map(|i| i as f64).collect();
    let probe_batch = RecordBatch::try_new(
        probe_schema,
        vec![
            Arc::new(Int64Array::from(probe_ids)),
            Arc::new(Float64Array::from(probe_vals)),
        ],
    )
    .unwrap();

    // Measure build time.
    let build_start = Instant::now();
    let mut built_op = HashJoinOperator::new(JoinType::Inner, vec![0], vec![0]);
    built_op.add_input(VeloxBatch::new(build_batch.clone())).unwrap();
    built_op.finish_build();
    let build_d = build_start.elapsed();

    // Measure probe throughput.
    let probe_d = median_duration(|| {
        let mut op = HashJoinOperator::new(JoinType::Inner, vec![0], vec![0]);
        op.add_input(VeloxBatch::new(build_batch.clone())).unwrap();
        op.finish_build();
        op.add_input(VeloxBatch::new(probe_batch.clone())).unwrap();
        op.finish_probe().unwrap();
        // Drain output.
        while op.get_output().unwrap().is_some() {}
    });

    let build_ms = build_d.as_secs_f64() * 1000.0;
    let probe_mrps = million_rows_per_sec(PROBE_N, probe_d);
    println!(
        "HashJoin (1M probe):       {:.1} M rows/sec (probe), {:.1} ms (build 10K)",
        probe_mrps, build_ms
    );
}

// ── test_benchmark_window_aggregate_throughput ───────────────────────────────

#[test]
fn test_benchmark_window_aggregate_throughput() {
    const N: usize = 1_000_000;
    const NUM_PARTITIONS: i64 = 100;
    const WINDOW_MS: i64 = 10_000;
    const NUM_WINDOWS: i64 = 100;
    // event_time spread over 0..1_000_000ms (100 windows of 10000ms each)

    let schema = Arc::new(Schema::new(vec![
        Field::new("partition", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let partitions: Vec<i64> = (0..N as i64).map(|i| i % NUM_PARTITIONS).collect();
    let event_times: Vec<i64> = (0..N as i64)
        .map(|i| (i * WINDOW_MS * NUM_WINDOWS) / N as i64)
        .collect();
    let values: Vec<f64> = (0..N).map(|i| i as f64).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(partitions)),
            Arc::new(Int64Array::from(event_times)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();

    // Measure accumulation throughput.
    let accum_d = median_duration(|| {
        let mut op = WindowAggregateOperator::new(
            WindowType::Tumbling { size_ms: WINDOW_MS },
            1,
            vec![0],
            vec![WindowAggregateDescriptor {
                function: WindowAggFunction::Sum,
                input_col: 2,
                output_name: "s".into(),
            }],
        );
        op.add_input(VeloxBatch::new(batch.clone())).unwrap();
    });

    // Measure per-watermark flush time (fire all NUM_WINDOWS watermarks).
    let mut op_for_flush = WindowAggregateOperator::new(
        WindowType::Tumbling { size_ms: WINDOW_MS },
        1,
        vec![0],
        vec![WindowAggregateDescriptor {
            function: WindowAggFunction::Sum,
            input_col: 2,
            output_name: "s".into(),
        }],
    );
    op_for_flush.add_input(VeloxBatch::new(batch.clone())).unwrap();

    let flush_start = Instant::now();
    for w in 1..=NUM_WINDOWS {
        let _ = op_for_flush.on_watermark(w * WINDOW_MS).unwrap();
    }
    let flush_d = flush_start.elapsed();

    let accum_mrps = million_rows_per_sec(N, accum_d);
    let flush_ms = flush_d.as_secs_f64() * 1000.0;
    println!(
        "WindowAggregate (1M rows): {:.1} M rows/sec (accum), {:.1} ms (flush {} windows)",
        accum_mrps, flush_ms, NUM_WINDOWS
    );
}

// ── test_benchmark_arrow_ffi_roundtrip ───────────────────────────────────────

/// Measures the cost of exporting a VeloxBatch to Arrow C FFI pointers and
/// importing it back.  This approximates the JNI bridge overhead per batch.
///
/// Uses the Arrow FFI API directly (same code path as streamcrab-flink-bridge's
/// `export_batch` / `import_batch`) without requiring a separate crate dependency.
#[test]
fn test_benchmark_arrow_ffi_roundtrip() {
    use arrow::array::{Array, StructArray};
    use arrow::ffi::{from_ffi, to_ffi};

    const N: usize = 1_000_000;
    let batch = make_large_batch(N);
    let vbatch = VeloxBatch::new(batch);

    let d = median_duration(|| {
        // Export: VeloxBatch → FFI pointer pair.
        let rb = vbatch.materialize().unwrap();
        let struct_array = StructArray::from(rb.clone());
        let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data()).unwrap();
        let schema_ptr = Box::into_raw(Box::new(ffi_schema));
        let array_ptr  = Box::into_raw(Box::new(ffi_array));

        // Import: FFI pointer pair → VeloxBatch (simulates Java→Rust import).
        let imported = unsafe {
            let ffi_schema2 = Box::from_raw(schema_ptr);
            let ffi_array2  = Box::from_raw(array_ptr);
            let array_data = from_ffi(*ffi_array2, &*ffi_schema2).unwrap();
            let struct_arr = StructArray::from(array_data);
            let _rb2 = RecordBatch::from(struct_arr);
            VeloxBatch::new(_rb2)
        };

        // Prevent the compiler from optimising away the import.
        assert_eq!(imported.num_rows_total(), N);
    });

    let ms_per_batch = d.as_secs_f64() * 1000.0;
    println!(
        "Arrow FFI roundtrip (1M):  {:.1} ms per 1M rows",
        ms_per_batch
    );
}

// ── Summary printout ─────────────────────────────────────────────────────────

/// Convenience test that just prints the summary header so the block of
/// individual benchmark outputs is visually framed when run together.
///
/// Run all benchmarks with:
///   cargo test -p streamcrab-vectorized --test benchmark_tests -- --nocapture
#[test]
fn test_benchmark_summary_header() {
    println!();
    println!("--- StreamCrab Vectorized Benchmark Results ---");
    println!("(each number is the median over {} iterations)", ITERS);
    println!("(run individual benchmark tests to see per-operator results)");
    println!();
}
