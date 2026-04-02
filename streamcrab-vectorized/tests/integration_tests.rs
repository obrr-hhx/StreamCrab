//! Integration tests for streamcrab-vectorized.
//!
//! Each test is self-contained and exercises one or more operators in combination,
//! mirroring Nexmark query semantics where indicated.

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use streamcrab_vectorized::VeloxBatch;
use streamcrab_vectorized::expression::eval::{col, eq, gt, lit_f64, lit_i64, lt, mul};
use streamcrab_vectorized::operators::VectorizedOperator;
use streamcrab_vectorized::operators::{
    AggregateDescriptor, AggregateFunction, FilterOperator, HashAggregateOperator,
    HashJoinOperator, JoinType, ProjectOperator, Projection, WindowAggFunction,
    WindowAggregateDescriptor, WindowAggregateOperator, WindowType,
};

// ── Data generation helpers ──────────────────────────────────────────────────

/// schema: auction_id:Int64, price:Float64, seller:Utf8
/// Generates `n` rows: auction_id = 0..n, price = row as f64, seller = "seller_{row % 10}"
fn make_auction_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction_id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("seller", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..n as i64).collect();
    let prices: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let sellers: Vec<String> = (0..n).map(|i| format!("seller_{}", i % 10)).collect();
    let sellers_ref: Vec<&str> = sellers.iter().map(String::as_str).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(StringArray::from(sellers_ref)),
        ],
    )
    .unwrap()
}

/// schema: auction_id:Int64, event_time:Int64, bid_amount:Float64
/// Generates `n` rows with event_time spanning 0..max_time_ms uniformly.
fn make_bid_batch(n: usize, max_time_ms: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("bid_amount", DataType::Float64, false),
    ]));
    let step = if n > 1 { max_time_ms / n as i64 } else { 0 };
    let auction_ids: Vec<i64> = (0..n as i64).map(|i| i % 5).collect();
    let times: Vec<i64> = (0..n as i64).map(|i| i * step).collect();
    let amounts: Vec<f64> = (0..n).map(|i| (i as f64) + 1.0).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction_ids)),
            Arc::new(Int64Array::from(times)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap()
}

/// schema: person_id:Int64, name:Utf8
fn make_person_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = (0..n).map(|i| format!("person_{}", i)).collect();
    let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names_ref)),
        ],
    )
    .unwrap()
}

/// schema: auction_id:Int64, seller_id:Int64
/// seller_id = row % num_persons so there are always matches.
fn make_auction_seller_batch(n: usize, num_persons: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction_id", DataType::Int64, false),
        Field::new("seller_id", DataType::Int64, false),
    ]));
    let auction_ids: Vec<i64> = (0..n as i64).collect();
    let seller_ids: Vec<i64> = (0..n as i64).map(|i| i % num_persons as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction_ids)),
            Arc::new(Int64Array::from(seller_ids)),
        ],
    )
    .unwrap()
}

/// schema: dept:Utf8, amount:Float64
/// Generates `n` rows: dept cycles through 5 departments, amount = row index as f64.
fn make_dept_amount_batch(n: usize) -> RecordBatch {
    let depts = ["alpha", "beta", "gamma", "delta", "epsilon"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("dept", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let dept_vals: Vec<&str> = (0..n).map(|i| depts[i % depts.len()]).collect();
    let amounts: Vec<f64> = (0..n).map(|i| i as f64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(dept_vals)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap()
}

// ── Test 1: Nexmark Q1 — currency conversion ─────────────────────────────────

/// Nexmark Q1: multiply auction price by 0.908 to convert USD → EUR.
///
/// Pipeline: ProjectOperator with Computed { expr: mul(col(1), lit_f64(0.908)) }
/// Verifies: all 1000 rows present, price_euro = price * 0.908 for every row.
#[test]
fn test_nexmark_q1_currency_conversion() {
    const N: usize = 1000;
    let input = make_auction_batch(N);

    let projections = vec![
        Projection::Column(0), // auction_id
        Projection::Column(2), // seller
        Projection::Computed {
            expr: mul(col(1), lit_f64(0.908)),
            name: "price_euro".to_owned(),
            data_type: DataType::Float64,
        },
    ];
    let mut op = ProjectOperator::new(projections);
    op.add_input(VeloxBatch::new(input.clone())).unwrap();
    let output = op.get_output().unwrap().unwrap();
    let rb = output.materialize().unwrap();

    assert_eq!(rb.num_rows(), N, "all 1000 rows must be present");
    assert_eq!(rb.num_columns(), 3);
    assert_eq!(rb.schema().field(2).name(), "price_euro");

    let original_prices = input
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let euro_prices = rb
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    for i in 0..N {
        let expected = original_prices.value(i) * 0.908;
        let actual = euro_prices.value(i);
        assert!(
            (actual - expected).abs() < 1e-9,
            "row {}: expected {}, got {}",
            i,
            expected,
            actual
        );
    }
}

// ── Test 2: Nexmark Q2 — filter auction by ID ────────────────────────────────

/// Nexmark Q2: keep only auctions with auction_id < 10.
///
/// Pipeline: FilterOperator with predicate lt(col(0), lit_i64(10))
/// Verifies: exactly 10 rows (ids 0..9), all have auction_id < 10.
#[test]
fn test_nexmark_q2_filter_auction() {
    const N: usize = 1000;
    let input = make_auction_batch(N);

    let mut op = FilterOperator::new(lt(col(0), lit_i64(10)));
    op.add_input(VeloxBatch::new(input)).unwrap();
    let output = op.get_output().unwrap().unwrap();
    let rb = output.materialize().unwrap();

    assert_eq!(rb.num_rows(), 10, "only rows 0..9 should pass the filter");

    let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..rb.num_rows() {
        assert!(
            ids.value(i) < 10,
            "row {} has auction_id {} which should be < 10",
            i,
            ids.value(i)
        );
    }
}

// ── Test 3: Nexmark Q5 — window aggregation (count bids per window) ──────────

/// Nexmark Q5: count bids per 1-second tumbling window, partitioned by auction_id.
///
/// 100 rows, event_time 0..5000ms (step 50ms), auction_id cycles 0..5.
/// Windows: [0,1000), [1000,2000), [2000,3000), [3000,4000), [4000,5000).
/// Fire watermarks at 1000, 2000, 3000, 4000, 5000.
/// Verifies: each fired window has rows, counts are positive integers.
#[test]
fn test_nexmark_q5_window_aggregation() {
    // 100 rows, event_time step = 5000/100 = 50ms, so 20 rows per 1000ms window.
    const N: usize = 100;
    let input = make_bid_batch(N, 5000);

    let mut op = WindowAggregateOperator::new(
        WindowType::Tumbling { size_ms: 1000 },
        1,       // event_time col
        vec![0], // partition by auction_id
        vec![WindowAggregateDescriptor {
            function: WindowAggFunction::Count,
            input_col: 2, // bid_amount
            output_name: "bid_count".to_owned(),
        }],
    );

    op.add_input(VeloxBatch::new(input)).unwrap();

    let mut total_window_rows = 0usize;
    let mut total_count_sum = 0i64;

    for watermark in [1000i64, 2000, 3000, 4000, 5000] {
        let batches = op.on_watermark(watermark).unwrap();
        for batch in &batches {
            let rb = batch.inner();
            assert!(
                rb.num_rows() > 0,
                "fired window at wm={} must have rows",
                watermark
            );

            // col 0 = part_0 (auction_id, Int64)
            // col 1 = bid_count (Int64, Count returns Int64)
            // col 2 = window_start, col 3 = window_end
            let counts = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..rb.num_rows() {
                let c = counts.value(i);
                assert!(c > 0, "count must be positive, got {} at row {}", c, i);
                total_count_sum += c;
            }
            total_window_rows += rb.num_rows();
        }
    }

    // 100 events total, each assigned to exactly 1 tumbling window.
    assert_eq!(
        total_count_sum, N as i64,
        "sum of all window counts must equal total input rows ({} vs {})",
        total_count_sum, N
    );
    assert!(
        total_window_rows > 0,
        "at least some window result rows must have been produced"
    );
}

// ── Test 4: Nexmark Q8 — join persons with auctions ──────────────────────────

/// Nexmark Q8: inner join persons (build) with auctions (probe) on person_id = seller_id.
///
/// 50 persons, 200 auctions (seller_id = auction_idx % 50, so all match once).
/// Verifies: 200 output rows (each auction matches exactly one person),
/// output contains a non-empty name column.
#[test]
fn test_nexmark_q8_join() {
    const NUM_PERSONS: usize = 50;
    const NUM_AUCTIONS: usize = 200;

    let persons = make_person_batch(NUM_PERSONS);
    let auctions = make_auction_seller_batch(NUM_AUCTIONS, NUM_PERSONS);

    // build on person_id (col 0 of persons), probe on seller_id (col 1 of auctions)
    let mut op = HashJoinOperator::new(JoinType::Inner, vec![0], vec![1]);

    // Feed build side
    op.add_input(VeloxBatch::new(persons)).unwrap();
    op.finish_build();

    // Feed probe side
    op.add_input(VeloxBatch::new(auctions)).unwrap();
    op.finish_probe().unwrap();

    // Drain all output
    let mut total_rows = 0usize;
    while let Some(batch) = op.get_output().unwrap() {
        let rb = batch.materialize().unwrap();
        total_rows += rb.num_rows();

        // Output schema: build cols (person_id, name) ++ probe cols (auction_id, seller_id)
        // Verify name column (col 1 from build side) is non-null strings
        let names = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..rb.num_rows() {
            assert!(
                !names.is_null(i),
                "name at row {} must not be null in inner join",
                i
            );
            assert!(
                names.value(i).starts_with("person_"),
                "name '{}' at row {} must start with 'person_'",
                names.value(i),
                i
            );
        }
    }

    assert_eq!(
        total_rows, NUM_AUCTIONS,
        "inner join should produce exactly {} rows (one per auction), got {}",
        NUM_AUCTIONS, total_rows
    );
}

// ── Test 5: Multi-operator pipeline — filter then aggregate ──────────────────

/// Filter high-value transactions (amount > 250), then group by dept and SUM + COUNT.
///
/// 500 rows: dept cycles 5 ways, amount = row index (0..499).
/// After filter: rows with amount > 250 → amounts 251..499 = 249 rows.
/// Verify: each dept's sum and count match the expected filtered values.
#[test]
fn test_pipeline_filter_then_aggregate() {
    const N: usize = 500;
    let input = make_dept_amount_batch(N);

    // Step 1: filter amount > 250 (col 1)
    let mut filter = FilterOperator::new(gt(col(1), lit_f64(250.0)));
    filter.add_input(VeloxBatch::new(input)).unwrap();
    let filtered_batch = filter.get_output().unwrap().unwrap();

    // After filter, dept is still col 0, amount is col 1 in the materialized batch.
    // Step 2: hash aggregate: group by dept (col 0), SUM(amount col 1), COUNT(amount col 1)
    let mut agg = HashAggregateOperator::new(
        vec![0], // group by dept
        vec![
            AggregateDescriptor {
                function: AggregateFunction::Sum,
                input_col: 1,
                output_name: "sum_amount".to_owned(),
            },
            AggregateDescriptor {
                function: AggregateFunction::Count,
                input_col: 1,
                output_name: "cnt".to_owned(),
            },
        ],
    );

    agg.add_input(filtered_batch).unwrap();
    let results = agg.on_watermark(0).unwrap();

    assert_eq!(results.len(), 1, "watermark must flush one batch");
    let rb = results[0].inner();

    // 5 departments
    assert_eq!(rb.num_rows(), 5, "5 departments expected");

    // Compute expected per-dept values for amounts 251..499
    // dept[i % 5] gets amount i for all i in 251..499
    let depts = ["alpha", "beta", "gamma", "delta", "epsilon"];
    let mut expected_sum = [0.0f64; 5];
    let mut expected_cnt = [0usize; 5];
    for i in 0..N {
        if i as f64 > 250.0 {
            let d = i % 5;
            expected_sum[d] += i as f64;
            expected_cnt[d] += 1;
        }
    }

    // Output schema: group_0 (Utf8), sum_amount (Float64), cnt (Float64)
    let dept_col = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let sum_col = rb
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let cnt_col = rb
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    for row in 0..rb.num_rows() {
        let dept_name = dept_col.value(row);
        let dept_idx = depts
            .iter()
            .position(|&d| d == dept_name)
            .expect("dept name should be one of the 5 known departments");
        let actual_sum = sum_col.value(row);
        let actual_cnt = cnt_col.value(row) as usize;
        assert!(
            (actual_sum - expected_sum[dept_idx]).abs() < 1e-6,
            "dept {}: expected sum {}, got {}",
            dept_name,
            expected_sum[dept_idx],
            actual_sum
        );
        assert_eq!(
            actual_cnt, expected_cnt[dept_idx],
            "dept {}: expected count {}, got {}",
            dept_name, expected_cnt[dept_idx], actual_cnt
        );
    }
}

// ── Test 6: Checkpoint / restore mid-stream ──────────────────────────────────

/// Feed 50 rows into HashAggregateOperator, snapshot, restore into a new operator,
/// feed 50 more rows, watermark to flush, verify all 100 rows are aggregated.
///
/// Uses a single group (no group-by column variance) to keep math simple:
/// all rows go to the same group, SUM = sum(0..99) = 4950.
#[test]
fn test_checkpoint_restore_full_pipeline() {
    // Single-group schema: group_key:Int64 (always 0), value:Float64
    let make_batch = |start: i64, end: i64| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_key", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let keys: Vec<i64> = vec![0i64; (end - start) as usize];
        let values: Vec<f64> = (start..end).map(|i| i as f64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    };

    let make_op = || {
        HashAggregateOperator::new(
            vec![0], // group by group_key
            vec![
                AggregateDescriptor {
                    function: AggregateFunction::Sum,
                    input_col: 1,
                    output_name: "total".to_owned(),
                },
                AggregateDescriptor {
                    function: AggregateFunction::Count,
                    input_col: 1,
                    output_name: "cnt".to_owned(),
                },
            ],
        )
    };

    // Phase 1: feed first 50 rows (values 0..49)
    let mut op1 = make_op();
    op1.add_input(VeloxBatch::new(make_batch(0, 50))).unwrap();

    // Snapshot mid-stream
    let snapshot = op1.snapshot_state().unwrap();
    assert!(!snapshot.is_empty(), "snapshot must not be empty");

    // Phase 2: restore into a new operator, feed remaining 50 rows (values 50..99)
    let mut op2 = make_op();
    op2.restore_state(&snapshot).unwrap();
    op2.add_input(VeloxBatch::new(make_batch(50, 100))).unwrap();

    // Flush via watermark
    let results = op2.on_watermark(0).unwrap();
    assert_eq!(results.len(), 1, "watermark must produce one batch");

    let rb = results[0].inner();
    assert_eq!(rb.num_rows(), 1, "single group expected");

    // sum(0..99) = 99*100/2 = 4950
    let sum_col = rb
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let cnt_col = rb
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let actual_sum = sum_col.value(0);
    let actual_cnt = cnt_col.value(0) as i64;

    assert!(
        (actual_sum - 4950.0).abs() < 1e-6,
        "expected sum 4950 (0..99), got {}",
        actual_sum
    );
    assert_eq!(actual_cnt, 100, "expected count 100, got {}", actual_cnt);
}

// ── Test 7: Large batch performance smoke test ────────────────────────────────

/// Runs 100,000 rows through FilterOperator + ProjectOperator and verifies
/// the pipeline completes in under 1 second with correct row count.
#[test]
fn test_large_batch_performance() {
    use std::time::Instant;

    const N: usize = 100_000;

    // Build a large batch: id:Int64, value:Float64
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ids: Vec<i64> = (0..N as i64).collect();
    let values: Vec<f64> = (0..N).map(|i| i as f64).collect();
    let large_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap();

    let start = Instant::now();

    // Filter: id >= 0 (passes all rows — just exercises the code path)
    let mut filter = FilterOperator::new(
        // id >= 0 expressed as NOT (id < 0)
        // Simpler: use gt(col(0), lit_i64(-1)) which passes all rows
        gt(col(0), lit_i64(-1)),
    );
    filter.add_input(VeloxBatch::new(large_batch)).unwrap();
    let filtered = filter.get_output().unwrap().unwrap();

    // Project: keep id, compute value * 2.0
    let mut project = ProjectOperator::new(vec![
        Projection::Column(0),
        Projection::Computed {
            expr: mul(col(1), lit_f64(2.0)),
            name: "value_doubled".to_owned(),
            data_type: DataType::Float64,
        },
    ]);
    project.add_input(filtered).unwrap();
    let output = project.get_output().unwrap().unwrap();
    let rb = output.materialize().unwrap();

    let elapsed = start.elapsed();

    assert_eq!(rb.num_rows(), N, "all {} rows must pass through", N);
    assert_eq!(rb.num_columns(), 2);

    // Spot-check: last row's value_doubled should be (N-1)*2
    let doubled = rb
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let expected_last = (N as f64 - 1.0) * 2.0;
    assert!(
        (doubled.value(N - 1) - expected_last).abs() < 1e-6,
        "last row value_doubled should be {}, got {}",
        expected_last,
        doubled.value(N - 1)
    );

    assert!(
        elapsed.as_secs() < 1,
        "pipeline over {} rows took {:?}, expected < 1s",
        N,
        elapsed
    );
}

// ── Test 8: eq filter verifies exact match semantics ────────────────────────

/// Sanity check for eq predicate: filter auction_id == 42 yields exactly 1 row.
#[test]
fn test_filter_eq_single_row() {
    const N: usize = 1000;
    let input = make_auction_batch(N);

    let mut op = FilterOperator::new(eq(col(0), lit_i64(42)));
    op.add_input(VeloxBatch::new(input)).unwrap();
    let output = op.get_output().unwrap().unwrap();
    let rb = output.materialize().unwrap();

    assert_eq!(rb.num_rows(), 1, "exactly one row with auction_id == 42");
    let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.value(0), 42);
}
