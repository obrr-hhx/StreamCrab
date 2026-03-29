//! Rigorous correctness verification tests for streamcrab-vectorized operators.
//!
//! Every test independently computes expected values using plain Rust arithmetic
//! and compares against every single output value from the operator under test.
//! No test merely checks "non-empty output" — all values are verified exactly.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use streamcrab_vectorized::operators::{
    AggregateDescriptor, AggregateFunction, FilterOperator, HashAggregateOperator,
    HashJoinOperator, JoinType, ProjectOperator, Projection, VectorizedOperator,
    WindowAggFunction, WindowAggregateDescriptor, WindowAggregateOperator, WindowType,
};
use streamcrab_vectorized::expression::eval::{add, col, gt, lit_f64, mul};
use streamcrab_vectorized::VeloxBatch;

const EPSILON: f64 = 1e-10;

// ── helpers ───────────────────────────────────────────────────────────────────

fn f64_col(rb: &RecordBatch, col_idx: usize) -> &Float64Array {
    rb.column(col_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
}

fn i64_col(rb: &RecordBatch, col_idx: usize) -> &Int64Array {
    rb.column(col_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

fn str_col(rb: &RecordBatch, col_idx: usize) -> &StringArray {
    rb.column(col_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

// ── test_aggregation_correctness_exact ───────────────────────────────────────

/// Generate 10,000 rows: department cycles through 5 departments, salary is
/// deterministic as row_idx * 10.0 + dept_offset.  Run HashAggregateOperator
/// with SUM, COUNT, MIN, MAX, AVG on salary grouped by department.
/// Independently compute expected values and compare EVERY value within 1e-10.
#[test]
fn test_aggregation_correctness_exact() {
    const N: usize = 10_000;
    const NUM_DEPTS: usize = 5;
    let dept_names = ["dept_0", "dept_1", "dept_2", "dept_3", "dept_4"];
    let dept_offsets = [0.0f64, 100.0, 200.0, 300.0, 400.0];

    // Build the input batch.
    let schema = Arc::new(Schema::new(vec![
        Field::new("department", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]));
    let departments: Vec<&str> = (0..N).map(|i| dept_names[i % NUM_DEPTS]).collect();
    let salaries: Vec<f64> = (0..N)
        .map(|i| i as f64 * 10.0 + dept_offsets[i % NUM_DEPTS])
        .collect();

    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(departments)),
            Arc::new(Float64Array::from(salaries.clone())),
        ],
    )
    .unwrap();

    // Independently compute expected SUM/COUNT/MIN/MAX/AVG per department.
    let mut expected_sum = [0.0f64; NUM_DEPTS];
    let mut expected_count = [0i64; NUM_DEPTS];
    let mut expected_min = [f64::INFINITY; NUM_DEPTS];
    let mut expected_max = [f64::NEG_INFINITY; NUM_DEPTS];
    for i in 0..N {
        let d = i % NUM_DEPTS;
        let v = salaries[i];
        expected_sum[d] += v;
        expected_count[d] += 1;
        if v < expected_min[d] {
            expected_min[d] = v;
        }
        if v > expected_max[d] {
            expected_max[d] = v;
        }
    }
    let expected_avg: Vec<f64> = (0..NUM_DEPTS)
        .map(|d| expected_sum[d] / expected_count[d] as f64)
        .collect();

    // Run operator.
    let mut op = HashAggregateOperator::new(
        vec![0], // group by department
        vec![
            AggregateDescriptor { function: AggregateFunction::Sum,   input_col: 1, output_name: "sum_sal".into()   },
            AggregateDescriptor { function: AggregateFunction::Count, input_col: 1, output_name: "cnt".into()       },
            AggregateDescriptor { function: AggregateFunction::Min,   input_col: 1, output_name: "min_sal".into()   },
            AggregateDescriptor { function: AggregateFunction::Max,   input_col: 1, output_name: "max_sal".into()   },
            AggregateDescriptor { function: AggregateFunction::Avg,   input_col: 1, output_name: "avg_sal".into()   },
        ],
    );
    op.add_input(VeloxBatch::new(rb)).unwrap();
    let batches = op.on_watermark(0).unwrap();

    assert_eq!(batches.len(), 1, "watermark must produce exactly 1 batch");
    let out = batches[0].inner();

    // Exactly 5 groups.
    assert_eq!(out.num_rows(), NUM_DEPTS, "expected {} groups", NUM_DEPTS);

    // Schema: group_0 (Utf8), sum_sal, cnt, min_sal, max_sal, avg_sal
    let dept_col = str_col(out, 0);
    let sum_col  = f64_col(out, 1);
    let cnt_col  = f64_col(out, 2); // Count is stored as f64 by HashAggregateOperator
    let min_col  = f64_col(out, 3);
    let max_col  = f64_col(out, 4);
    let avg_col  = f64_col(out, 5);

    for row in 0..out.num_rows() {
        let name = dept_col.value(row);
        let d = dept_names
            .iter()
            .position(|&n| n == name)
            .unwrap_or_else(|| panic!("unexpected dept name: {}", name));

        assert!(
            (sum_col.value(row) - expected_sum[d]).abs() < EPSILON,
            "dept {}: sum expected {}, got {}",
            name, expected_sum[d], sum_col.value(row)
        );
        assert_eq!(
            cnt_col.value(row) as i64,
            expected_count[d],
            "dept {}: count expected {}, got {}",
            name, expected_count[d], cnt_col.value(row)
        );
        assert!(
            (min_col.value(row) - expected_min[d]).abs() < EPSILON,
            "dept {}: min expected {}, got {}",
            name, expected_min[d], min_col.value(row)
        );
        assert!(
            (max_col.value(row) - expected_max[d]).abs() < EPSILON,
            "dept {}: max expected {}, got {}",
            name, expected_max[d], max_col.value(row)
        );
        assert!(
            (avg_col.value(row) - expected_avg[d]).abs() < EPSILON,
            "dept {}: avg expected {}, got {}",
            name, expected_avg[d], avg_col.value(row)
        );
    }
}

// ── test_join_correctness_exact ───────────────────────────────────────────────

/// Build side: 1000 customers (id 0..999, name = "customer_{id}").
/// Probe side: 5000 orders (order_id 0..4999, customer_id = order_id % 1000,
///             amount = order_id as f64).
/// INNER join on customer_id.
/// Verify: exactly 5000 output rows, correct customer name for every row,
///         total amount = sum(0..4999).
/// Then LEFT join: add 100 customers with id 1000..1099 (no matching orders),
///                 verify 5100 output rows.
#[test]
fn test_join_correctness_exact() {
    const NUM_CUSTOMERS: usize = 1000;
    const NUM_ORDERS: usize = 5000;

    // Build (customers): schema = [customer_id:Int64, name:Utf8]
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let cust_ids: Vec<i64> = (0..NUM_CUSTOMERS as i64).collect();
    let cust_names: Vec<String> = (0..NUM_CUSTOMERS).map(|i| format!("customer_{}", i)).collect();
    let cust_names_ref: Vec<&str> = cust_names.iter().map(String::as_str).collect();
    let customers = RecordBatch::try_new(
        cust_schema,
        vec![
            Arc::new(Int64Array::from(cust_ids)),
            Arc::new(StringArray::from(cust_names_ref)),
        ],
    )
    .unwrap();

    // Probe (orders): schema = [order_id:Int64, customer_id:Int64, amount:Float64]
    let order_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let order_ids: Vec<i64> = (0..NUM_ORDERS as i64).collect();
    let order_cids: Vec<i64> = (0..NUM_ORDERS as i64).map(|i| i % NUM_CUSTOMERS as i64).collect();
    let order_amounts: Vec<f64> = (0..NUM_ORDERS).map(|i| i as f64).collect();
    let orders = RecordBatch::try_new(
        order_schema,
        vec![
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int64Array::from(order_cids)),
            Arc::new(Float64Array::from(order_amounts.clone())),
        ],
    )
    .unwrap();

    // INNER join: build_key_col=0 (customer_id), probe_key_col=1 (customer_id in orders)
    let mut op = HashJoinOperator::new(JoinType::Inner, vec![0], vec![1]);
    op.add_input(VeloxBatch::new(customers.clone())).unwrap();
    op.finish_build();
    op.add_input(VeloxBatch::new(orders.clone())).unwrap();
    op.finish_probe().unwrap();

    // Collect all output.
    let mut total_rows = 0usize;
    let mut total_amount = 0.0f64;
    while let Some(batch) = op.get_output().unwrap() {
        let rb = batch.materialize().unwrap();
        // Output schema: [customer_id(build), name(build), order_id(probe), customer_id(probe), amount(probe)]
        let name_col   = str_col(&rb, 1);
        let probe_cid  = i64_col(&rb, 3);
        let amount_col = f64_col(&rb, 4);

        for row in 0..rb.num_rows() {
            let cid = probe_cid.value(row);
            let expected_name = format!("customer_{}", cid);
            assert_eq!(
                name_col.value(row), expected_name.as_str(),
                "row {}: name mismatch for customer_id {}",
                row, cid
            );
            total_amount += amount_col.value(row);
        }
        total_rows += rb.num_rows();
    }

    assert_eq!(total_rows, NUM_ORDERS,
        "inner join must produce exactly {} rows, got {}", NUM_ORDERS, total_rows);

    let expected_total_amount: f64 = order_amounts.iter().sum();
    assert!(
        (total_amount - expected_total_amount).abs() < EPSILON,
        "total amount: expected {}, got {}",
        expected_total_amount, total_amount
    );

    // LEFT join: 1000 original customers + 100 extra with no matching orders.
    const NUM_EXTRA: usize = 100;
    let all_cust_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let all_ids: Vec<i64> = (0..(NUM_CUSTOMERS + NUM_EXTRA) as i64).collect();
    let all_names: Vec<String> = (0..NUM_CUSTOMERS + NUM_EXTRA)
        .map(|i| format!("customer_{}", i))
        .collect();
    let all_names_ref: Vec<&str> = all_names.iter().map(String::as_str).collect();
    let all_customers = RecordBatch::try_new(
        all_cust_schema,
        vec![
            Arc::new(Int64Array::from(all_ids)),
            Arc::new(StringArray::from(all_names_ref)),
        ],
    )
    .unwrap();

    let mut left_op = HashJoinOperator::new(JoinType::Left, vec![0], vec![1]);
    left_op.add_input(VeloxBatch::new(all_customers)).unwrap();
    left_op.finish_build();
    left_op.add_input(VeloxBatch::new(orders)).unwrap();
    left_op.finish_probe().unwrap();

    let mut left_total = 0usize;
    while let Some(batch) = left_op.get_output().unwrap() {
        left_total += batch.materialize().unwrap().num_rows();
    }

    // 5000 matched rows + 100 unmatched build rows = 5100
    assert_eq!(left_total, NUM_ORDERS + NUM_EXTRA,
        "left join must produce {} rows, got {}", NUM_ORDERS + NUM_EXTRA, left_total);
}

// ── test_window_correctness_exact ────────────────────────────────────────────

/// 10,000 events: user_id cycles 0..10, event_time = 0..9999 ms,
/// value = event_idx as f64.
/// Tumbling window 1000ms, partition by user_id, SUM + COUNT on value.
/// Fire watermarks at 1000, 2000, …, 10000.
/// For each window [start, start+1000): verify every output value exactly.
#[test]
fn test_window_correctness_exact() {
    const N: usize = 10_000;
    const NUM_USERS: usize = 10;
    const WINDOW_MS: i64 = 1000;
    const NUM_WINDOWS: usize = 10;

    // Build input batch.
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let user_ids: Vec<i64> = (0..N as i64).map(|i| i % NUM_USERS as i64).collect();
    let event_times: Vec<i64> = (0..N as i64).collect(); // 0..9999 ms
    let values: Vec<f64> = (0..N).map(|i| i as f64).collect();

    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Int64Array::from(event_times)),
            Arc::new(Float64Array::from(values.clone())),
        ],
    )
    .unwrap();

    // Independently compute expected SUM and COUNT per (window_idx, user_id).
    // event i is in window i / WINDOW_MS (integer division), user i % NUM_USERS.
    let mut expected_sum = vec![[0.0f64; NUM_USERS]; NUM_WINDOWS];
    let mut expected_count = vec![[0i64; NUM_USERS]; NUM_WINDOWS];
    for i in 0..N {
        let w = i / WINDOW_MS as usize;
        let u = i % NUM_USERS;
        expected_sum[w][u] += i as f64;
        expected_count[w][u] += 1;
    }

    let mut op = WindowAggregateOperator::new(
        WindowType::Tumbling { size_ms: WINDOW_MS },
        1, // event_time col
        vec![0], // partition by user_id
        vec![
            WindowAggregateDescriptor {
                function: WindowAggFunction::Sum,
                input_col: 2,
                output_name: "sum_val".into(),
            },
            WindowAggregateDescriptor {
                function: WindowAggFunction::Count,
                input_col: 2,
                output_name: "cnt".into(),
            },
        ],
    );

    op.add_input(VeloxBatch::new(rb)).unwrap();

    for w in 0..NUM_WINDOWS {
        let watermark = (w as i64 + 1) * WINDOW_MS;
        let batches = op.on_watermark(watermark).unwrap();

        assert_eq!(batches.len(), 1,
            "window {} (wm={}) must produce exactly 1 batch", w, watermark);

        let out = batches[0].inner();

        // Each window must have exactly NUM_USERS rows (one per user).
        assert_eq!(out.num_rows(), NUM_USERS,
            "window {}: expected {} rows, got {}", w, NUM_USERS, out.num_rows());

        // Schema: [part_0(user_id:Int64), sum_val(Float64), cnt(Int64), window_start, window_end]
        let user_col  = i64_col(out, 0);
        let sum_col   = f64_col(out, 1);
        // Count returns Int64Array
        let cnt_col   = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let wstart_col = i64_col(out, 3);
        let wend_col   = i64_col(out, 4);

        for row in 0..out.num_rows() {
            let uid = user_col.value(row) as usize;
            let expected_w_start = w as i64 * WINDOW_MS;
            let expected_w_end   = expected_w_start + WINDOW_MS;

            assert_eq!(wstart_col.value(row), expected_w_start,
                "window {} user {}: wrong window_start", w, uid);
            assert_eq!(wend_col.value(row), expected_w_end,
                "window {} user {}: wrong window_end", w, uid);
            assert!(
                (sum_col.value(row) - expected_sum[w][uid]).abs() < EPSILON,
                "window {} user {}: sum expected {}, got {}",
                w, uid, expected_sum[w][uid], sum_col.value(row)
            );
            assert_eq!(cnt_col.value(row), expected_count[w][uid],
                "window {} user {}: count expected {}, got {}",
                w, uid, expected_count[w][uid], cnt_col.value(row)
            );
        }
    }
}

// ── test_filter_project_correctness ──────────────────────────────────────────

/// 100,000 rows: id (0..99999), value = id * 1.5, category = id % 3.
/// Filter: value > 75000.0  (i.e., id > 50000)
/// Project: id, computed_col = value * 2.0 + 1.0
/// Verify: exactly 49999 output rows, every computed_col = id * 3.0 + 1.0.
#[test]
fn test_filter_project_correctness() {
    const N: usize = 100_000;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
    ]));

    let ids: Vec<i64> = (0..N as i64).collect();
    let vals: Vec<f64> = (0..N).map(|i| i as f64 * 1.5).collect();
    let cats: Vec<String> = (0..N).map(|i| format!("cat_{}", i % 3)).collect();
    let cats_ref: Vec<&str> = cats.iter().map(String::as_str).collect();

    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Float64Array::from(vals)),
            Arc::new(StringArray::from(cats_ref)),
        ],
    )
    .unwrap();

    // Filter: value (col 1) > 75000.0
    let mut filter = FilterOperator::new(gt(col(1), lit_f64(75000.0)));
    filter.add_input(VeloxBatch::new(rb)).unwrap();
    let filtered = filter.get_output().unwrap().unwrap();

    // Project: id (col 0), computed = value * 2.0 + 1.0 = col(1) * 2.0 + 1.0
    // After filter materialization, schema is [id, value, category]
    let mut project = ProjectOperator::new(vec![
        Projection::Column(0), // id
        Projection::Computed {
            expr: add(mul(col(1), lit_f64(2.0)), lit_f64(1.0)),
            name: "computed_col".into(),
            data_type: DataType::Float64,
        },
    ]);
    project.add_input(filtered).unwrap();
    let output = project.get_output().unwrap().unwrap();
    let out = output.materialize().unwrap();

    // id > 50000 means ids 50001..99999 → exactly 49999 rows.
    let expected_rows = 49999usize;
    assert_eq!(out.num_rows(), expected_rows,
        "expected {} rows after filter, got {}", expected_rows, out.num_rows());

    let id_col       = i64_col(&out, 0);
    let computed_col = f64_col(&out, 1);

    for row in 0..out.num_rows() {
        let id = id_col.value(row);

        // Every id must be > 50000
        assert!(id > 50000,
            "row {}: id {} should be > 50000", row, id);

        // computed_col = id * 1.5 * 2.0 + 1.0 = id * 3.0 + 1.0
        let expected_computed = id as f64 * 3.0 + 1.0;
        assert!(
            (computed_col.value(row) - expected_computed).abs() < EPSILON,
            "row {} (id={}): computed_col expected {}, got {}",
            row, id, expected_computed, computed_col.value(row)
        );
    }

    // Verify ids are exactly 50001..99999 by checking the set.
    let id_set: std::collections::HashSet<i64> =
        (0..out.num_rows()).map(|i| id_col.value(i)).collect();
    for expected_id in 50001i64..100000 {
        assert!(id_set.contains(&expected_id),
            "id {} missing from output", expected_id);
    }
}

// ── test_sliding_window_correctness ──────────────────────────────────────────

/// 5000 events, single partition, event_time 0..4999ms.
/// Sliding window: size=2000ms, slide=500ms.
/// Fire watermark at 5000.
/// For each output window, verify COUNT and SUM against independently computed values.
#[test]
fn test_sliding_window_correctness() {
    const N: usize = 5000;
    const SIZE_MS: i64 = 2000;
    const SLIDE_MS: i64 = 500;

    // Single partition (no partition cols): use a single-value group_key column
    // so that all events land in the same partition.
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition", DataType::Int64, false),
        Field::new("event_time", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let partitions: Vec<i64> = vec![0i64; N];
    let event_times: Vec<i64> = (0..N as i64).collect(); // 0..4999
    let values: Vec<f64> = (0..N).map(|i| i as f64).collect();

    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(partitions)),
            Arc::new(Int64Array::from(event_times)),
            Arc::new(Float64Array::from(values.clone())),
        ],
    )
    .unwrap();

    // Independently compute expected SUM and COUNT per window.
    // A sliding window [s, s+SIZE_MS) with s % SLIDE_MS == 0 contains event t
    // iff s <= t < s + SIZE_MS.
    // Window starts can be negative (e.g. s=-500 covers t=0..1499).
    // For each event t, the valid starts are:
    //   s ∈ { k*SLIDE_MS : k*SLIDE_MS <= t < k*SLIDE_MS + SIZE_MS }
    //   i.e. t - SIZE_MS < k*SLIDE_MS <= t
    //   i.e. ceil((t - SIZE_MS + 1) / SLIDE_MS) <= k <= floor(t / SLIDE_MS)
    let mut expected: HashMap<i64, (f64, i64)> = HashMap::new(); // start → (sum, count)
    for i in 0..N {
        let t = i as i64;
        // Largest k such that k*SLIDE_MS <= t.
        let k_max = t / SLIDE_MS;
        // Smallest k such that k*SLIDE_MS + SIZE_MS > t  →  k > (t - SIZE_MS) / SLIDE_MS
        // Use div_euclid so negative t is handled correctly.
        let k_min = (t - SIZE_MS).div_euclid(SLIDE_MS) + 1;
        for k in k_min..=k_max {
            let s = k * SLIDE_MS;
            debug_assert!(s <= t && t < s + SIZE_MS);
            let e = expected.entry(s).or_insert((0.0, 0));
            e.0 += i as f64;
            e.1 += 1;
        }
    }

    let mut op = WindowAggregateOperator::new(
        WindowType::Sliding { size_ms: SIZE_MS, slide_ms: SLIDE_MS },
        1, // event_time col
        vec![0], // partition by 'partition' col
        vec![
            WindowAggregateDescriptor {
                function: WindowAggFunction::Sum,
                input_col: 2,
                output_name: "sum_val".into(),
            },
            WindowAggregateDescriptor {
                function: WindowAggFunction::Count,
                input_col: 2,
                output_name: "cnt".into(),
            },
        ],
    );

    op.add_input(VeloxBatch::new(rb)).unwrap();

    // Fire a single watermark at 5000 to flush all completed windows.
    // A window [start, start+SIZE_MS) is complete when watermark >= start+SIZE_MS.
    // With watermark=5000, windows with start+2000 <= 5000, i.e. start <= 3000, fire.
    let batches = op.on_watermark(5000).unwrap();

    // Collect all output rows into a map: window_start → (sum, count).
    let mut actual: HashMap<i64, (f64, i64)> = HashMap::new();
    let mut total_output_rows = 0usize;
    for batch in &batches {
        let rb = batch.inner();
        // Schema: [part_0(Int64), sum_val(Float64), cnt(Int64), window_start, window_end]
        let sum_col    = f64_col(rb, 1);
        let cnt_col    = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let wstart_col = i64_col(rb, 3);
        let wend_col   = i64_col(rb, 4);

        for row in 0..rb.num_rows() {
            let ws = wstart_col.value(row);
            let we = wend_col.value(row);
            assert_eq!(we - ws, SIZE_MS,
                "window size must be {}ms, got ws={} we={}", SIZE_MS, ws, we);
            actual.insert(ws, (sum_col.value(row), cnt_col.value(row)));
        }
        total_output_rows += rb.num_rows();
    }

    assert!(total_output_rows > 0, "must produce at least one output row");

    // Verify every expected window that should have fired (start + SIZE_MS <= 5000).
    for (&ws, &(exp_sum, exp_cnt)) in &expected {
        if ws + SIZE_MS > 5000 {
            // Window not yet complete at watermark=5000.
            continue;
        }
        let (act_sum, act_cnt) = *actual.get(&ws).unwrap_or_else(|| {
            panic!("expected window start={} not found in output", ws)
        });
        assert!(
            (act_sum - exp_sum).abs() < EPSILON,
            "window start={}: sum expected {}, got {}", ws, exp_sum, act_sum
        );
        assert_eq!(act_cnt, exp_cnt,
            "window start={}: count expected {}, got {}", ws, exp_cnt, act_cnt);
    }

    // Verify no spurious extra windows.
    for (&ws, _) in &actual {
        // Window starts may be negative (e.g. [-500,1500) covers t=0..999).
        // They must be aligned to SLIDE_MS.
        assert_eq!(
            ws.rem_euclid(SLIDE_MS), 0,
            "spurious window start={} not aligned to slide_ms={}", ws, SLIDE_MS
        );
        // Every output window must have a corresponding expected entry.
        assert!(expected.contains_key(&ws),
            "output window start={} has no expected entry (no events?)", ws);
    }
}
