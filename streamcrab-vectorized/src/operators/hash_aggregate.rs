//! Hash aggregation operator for streaming grouped aggregation.
//!
//! Implements incremental aggregation using an in-memory hash table keyed on
//! serialized group-by column values. State is flushed via `on_watermark()`,
//! which emits one output batch per window and resets the accumulator state.

use crate::batch::VeloxBatch;
use crate::operators::VectorizedOperator;
use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

// ── AggregateFunction ────────────────────────────────────────────────────────

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum AggregateFunction {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

// ── AggregateDescriptor ──────────────────────────────────────────────────────

/// Describes one aggregate output column.
pub struct AggregateDescriptor {
    pub function: AggregateFunction,
    /// Column index in the input batch.
    pub input_col: usize,
    pub output_name: String,
}

// ── Internal accumulator state ───────────────────────────────────────────────

/// Per-group, per-aggregate running state stored as f64 for uniformity.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AccumulatorState {
    sum: f64,
    count: i64,
    min: f64,
    max: f64,
}

impl AccumulatorState {
    fn new() -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Update with a single f64 value from the input column.
    fn update(&mut self, v: f64) {
        self.sum += v;
        self.count += 1;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
    }
}

// ── GroupKeyValue ────────────────────────────────────────────────────────────

/// Scalar value stored to reconstruct group-by columns on output.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum GroupKeyValue {
    Int64(i64),
    /// Stored as raw bits to allow Serialize/Deserialize without ordered_float.
    Float64Bits(u64),
    Utf8(String),
    Bool(bool),
    Null,
}

// ── Key serialization ────────────────────────────────────────────────────────

/// Serialize the group-by columns for a single row into a byte key.
///
/// Each column value is written as a type tag byte followed by the value
/// bytes, giving an unambiguous representation for HashMap lookup.
fn serialize_row_keys(columns: &[ArrayRef], row: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(columns.len() * 16);
    for col in columns {
        if col.is_null(row) {
            buf.push(0u8); // tag: Null
            continue;
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            buf.push(1u8);
            buf.extend_from_slice(&arr.value(row).to_le_bytes());
        } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            buf.push(2u8);
            buf.extend_from_slice(&arr.value(row).to_bits().to_le_bytes());
        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            buf.push(3u8);
            let s = arr.value(row).as_bytes();
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s);
        } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            buf.push(4u8);
            buf.push(arr.value(row) as u8);
        } else {
            // Unsupported type — use a stable sentinel so the key is still valid.
            buf.push(0xFFu8);
        }
    }
    buf
}

/// Extract the scalar value from a column at a given row for key storage.
fn extract_key_value(col: &ArrayRef, row: usize) -> GroupKeyValue {
    if col.is_null(row) {
        return GroupKeyValue::Null;
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return GroupKeyValue::Int64(arr.value(row));
    }
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return GroupKeyValue::Float64Bits(arr.value(row).to_bits());
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return GroupKeyValue::Utf8(arr.value(row).to_owned());
    }
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        return GroupKeyValue::Bool(arr.value(row));
    }
    GroupKeyValue::Null
}

// ── HashAggregateOperator ────────────────────────────────────────────────────

/// Snapshot-serializable state (groups, accumulators, keys).
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotData {
    groups: Vec<(Vec<u8>, usize)>,
    accumulators: Vec<Vec<AccumulatorState>>,
    group_keys: Vec<Vec<GroupKeyValue>>,
}

/// Streaming hash aggregation operator.
///
/// Accumulates rows into per-group accumulators on each `add_input()` call.
/// `on_watermark()` flushes accumulated state as a single output batch and
/// resets the hash table, ready for the next window.
pub struct HashAggregateOperator {
    group_by_cols: Vec<usize>,
    aggregates: Vec<AggregateDescriptor>,
    /// Maps serialized group key → group index.
    groups: HashMap<Vec<u8>, usize, ahash::RandomState>,
    /// `accumulators[group_idx][agg_idx]`
    accumulators: Vec<Vec<AccumulatorState>>,
    /// `group_keys[group_idx][key_col_idx]` — stored for output reconstruction.
    group_keys: Vec<Vec<GroupKeyValue>>,
    /// Pending output produced by `on_watermark` / `add_input` after flush.
    pending_output: Option<VeloxBatch>,
    finished: bool,
    flush_requested: bool,
}

impl HashAggregateOperator {
    pub fn new(group_by_cols: Vec<usize>, aggregates: Vec<AggregateDescriptor>) -> Self {
        Self {
            group_by_cols,
            aggregates,
            groups: HashMap::with_hasher(ahash::RandomState::new()),
            accumulators: Vec::new(),
            group_keys: Vec::new(),
            pending_output: None,
            finished: false,
            flush_requested: false,
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Look up or create a group for the given key bytes.
    /// Returns the group index.
    fn get_or_create_group(&mut self, key: Vec<u8>, key_values: Vec<GroupKeyValue>) -> usize {
        if let Some(&idx) = self.groups.get(&key) {
            return idx;
        }
        let idx = self.accumulators.len();
        self.accumulators
            .push(vec![AccumulatorState::new(); self.aggregates.len()]);
        self.group_keys.push(key_values);
        self.groups.insert(key, idx);
        idx
    }

    /// Extract an f64 value from an array at the given row.
    fn extract_f64(col: &ArrayRef, row: usize) -> f64 {
        if col.is_null(row) {
            return 0.0;
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            return arr.value(row);
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return arr.value(row) as f64;
        }
        0.0
    }

    /// Build the output RecordBatch from current accumulator state.
    fn build_output_batch(&self) -> Result<RecordBatch> {
        let n = self.group_keys.len();

        // Build group-by output columns.
        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        for (key_col_pos, &src_col_idx) in self.group_by_cols.iter().enumerate() {
            // Determine the Arrow type from the stored key values (first non-null).
            let sample = self
                .group_keys
                .iter()
                .map(|kv| &kv[key_col_pos])
                .find(|v| !matches!(v, GroupKeyValue::Null));

            let (data_type, col_arr): (DataType, ArrayRef) = match sample {
                Some(GroupKeyValue::Int64(_)) | None => {
                    let mut b = Int64Builder::with_capacity(n);
                    for kv in &self.group_keys {
                        match &kv[key_col_pos] {
                            GroupKeyValue::Int64(v) => b.append_value(*v),
                            _ => b.append_null(),
                        }
                    }
                    (DataType::Int64, Arc::new(b.finish()) as ArrayRef)
                }
                Some(GroupKeyValue::Float64Bits(_)) => {
                    let mut b = Float64Builder::with_capacity(n);
                    for kv in &self.group_keys {
                        match &kv[key_col_pos] {
                            GroupKeyValue::Float64Bits(bits) => {
                                b.append_value(f64::from_bits(*bits))
                            }
                            _ => b.append_null(),
                        }
                    }
                    (DataType::Float64, Arc::new(b.finish()) as ArrayRef)
                }
                Some(GroupKeyValue::Utf8(_)) => {
                    let mut b = StringBuilder::with_capacity(n, n * 8);
                    for kv in &self.group_keys {
                        match &kv[key_col_pos] {
                            GroupKeyValue::Utf8(s) => b.append_value(s),
                            _ => b.append_null(),
                        }
                    }
                    (DataType::Utf8, Arc::new(b.finish()) as ArrayRef)
                }
                Some(GroupKeyValue::Bool(_)) => {
                    let mut b = BooleanBuilder::with_capacity(n);
                    for kv in &self.group_keys {
                        match &kv[key_col_pos] {
                            GroupKeyValue::Bool(v) => b.append_value(*v),
                            _ => b.append_null(),
                        }
                    }
                    (DataType::Boolean, Arc::new(b.finish()) as ArrayRef)
                }
                Some(GroupKeyValue::Null) => {
                    let mut b = Int64Builder::with_capacity(n);
                    for _ in 0..n {
                        b.append_null();
                    }
                    (DataType::Int64, Arc::new(b.finish()) as ArrayRef)
                }
            };

            let field_name = format!("group_{}", src_col_idx);
            fields.push(Field::new(field_name, data_type, true));
            columns.push(col_arr);
        }

        // Build aggregate output columns.
        for (agg_idx, agg) in self.aggregates.iter().enumerate() {
            let mut b = Float64Builder::with_capacity(n);
            for group_acc in &self.accumulators {
                let acc = &group_acc[agg_idx];
                let v = match agg.function {
                    AggregateFunction::Sum => acc.sum,
                    AggregateFunction::Count => acc.count as f64,
                    AggregateFunction::Min => {
                        if acc.count == 0 {
                            f64::NAN
                        } else {
                            acc.min
                        }
                    }
                    AggregateFunction::Max => {
                        if acc.count == 0 {
                            f64::NAN
                        } else {
                            acc.max
                        }
                    }
                    AggregateFunction::Avg => {
                        if acc.count == 0 {
                            f64::NAN
                        } else {
                            acc.sum / acc.count as f64
                        }
                    }
                };
                b.append_value(v);
            }
            fields.push(Field::new(&agg.output_name, DataType::Float64, false));
            columns.push(Arc::new(b.finish()) as ArrayRef);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).context("HashAggregateOperator: build output batch")
    }

    /// Flush accumulated state to `pending_output` and reset the hash table.
    fn flush(&mut self) -> Result<()> {
        if self.group_keys.is_empty() {
            return Ok(());
        }
        let rb = self.build_output_batch()?;
        self.pending_output = Some(VeloxBatch::new(rb));

        // Reset for next window.
        self.groups.clear();
        self.accumulators.clear();
        self.group_keys.clear();

        Ok(())
    }
}

impl VectorizedOperator for HashAggregateOperator {
    fn add_input(&mut self, batch: VeloxBatch) -> Result<()> {
        let rb = batch
            .materialize()
            .context("HashAggregateOperator: materialize input")?;

        let num_rows = rb.num_rows();

        // Pre-extract group-by columns and aggregate input columns.
        let group_cols: Vec<ArrayRef> = self
            .group_by_cols
            .iter()
            .map(|&idx| Arc::clone(rb.column(idx)))
            .collect();

        let agg_cols: Vec<ArrayRef> = self
            .aggregates
            .iter()
            .map(|a| Arc::clone(rb.column(a.input_col)))
            .collect();

        for row in 0..num_rows {
            // Serialize group key.
            let key = serialize_row_keys(&group_cols, row);

            // Extract scalar values for key storage (only on new group creation).
            let key_values: Vec<GroupKeyValue> = group_cols
                .iter()
                .map(|c| extract_key_value(c, row))
                .collect();

            let group_idx = self.get_or_create_group(key, key_values);

            // Update each aggregate.
            for (agg_idx, agg_col) in agg_cols.iter().enumerate() {
                let v = Self::extract_f64(agg_col, row);
                self.accumulators[group_idx][agg_idx].update(v);
            }
        }

        if self.flush_requested {
            self.flush()?;
            self.flush_requested = false;
        }

        Ok(())
    }

    fn get_output(&mut self) -> Result<Option<VeloxBatch>> {
        Ok(self.pending_output.take())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn on_watermark(&mut self, _watermark: i64) -> Result<Vec<VeloxBatch>> {
        self.flush()?;
        Ok(self
            .pending_output
            .take()
            .map(|b| vec![b])
            .unwrap_or_default())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        let snap = SnapshotData {
            groups: self.groups.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            accumulators: self.accumulators.clone(),
            group_keys: self.group_keys.clone(),
        };
        bincode::serialize(&snap).context("HashAggregateOperator: snapshot_state serialize")
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let snap: SnapshotData =
            bincode::deserialize(data).context("HashAggregateOperator: restore_state deserialize")?;
        self.groups = snap
            .groups
            .into_iter()
            .collect::<HashMap<Vec<u8>, usize, _>>();
        // Rebuild with ahash hasher.
        let pairs: Vec<(Vec<u8>, usize)> = self.groups.drain().collect();
        self.groups = HashMap::with_hasher(ahash::RandomState::new());
        self.groups.extend(pairs);
        self.accumulators = snap.accumulators;
        self.group_keys = snap.group_keys;
        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::VeloxBatch;
    use crate::operators::VectorizedOperator;
    use arrow::array::{Array, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// schema: department:Utf8, salary:Float64
    /// rows: ("eng",100), ("sales",200), ("eng",150), ("sales",50), ("eng",200)
    fn dept_salary_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("department", DataType::Utf8, false),
            Field::new("salary", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["eng", "sales", "eng", "sales", "eng"])),
                Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0, 50.0, 200.0])),
            ],
        )
        .unwrap()
    }

    /// Helper: find the row index in a StringArray for a given department name.
    fn find_row(rb: &RecordBatch, dept_col: usize, dept: &str) -> Option<usize> {
        let arr = rb
            .column(dept_col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..arr.len()).find(|&i| arr.value(i) == dept)
    }

    /// Helper: get Float64 value at (row, col).
    fn f64_val(rb: &RecordBatch, col: usize, row: usize) -> f64 {
        rb.column(col)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
    }

    // ── Test 1: Basic SUM + COUNT over 2 groups ──────────────────────────────

    #[test]
    fn test_basic_sum_count() {
        let mut op = HashAggregateOperator::new(
            vec![0], // group by department
            vec![
                AggregateDescriptor {
                    function: AggregateFunction::Sum,
                    input_col: 1,
                    output_name: "sum_salary".into(),
                },
                AggregateDescriptor {
                    function: AggregateFunction::Count,
                    input_col: 1,
                    output_name: "cnt".into(),
                },
            ],
        );

        op.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();
        let batches = op.on_watermark(0).unwrap();
        assert_eq!(batches.len(), 1);

        let rb = batches[0].inner().clone();
        assert_eq!(rb.num_rows(), 2); // 2 groups: eng, sales

        let eng_row = find_row(&rb, 0, "eng").unwrap();
        let sales_row = find_row(&rb, 0, "sales").unwrap();

        // eng: 100+150+200 = 450, count=3
        assert_eq!(f64_val(&rb, 1, eng_row), 450.0);
        assert_eq!(f64_val(&rb, 2, eng_row), 3.0);

        // sales: 200+50 = 250, count=2
        assert_eq!(f64_val(&rb, 1, sales_row), 250.0);
        assert_eq!(f64_val(&rb, 2, sales_row), 2.0);
    }

    // ── Test 2: Multiple aggregates: SUM + COUNT + MIN + MAX ─────────────────

    #[test]
    fn test_multiple_aggregates() {
        let mut op = HashAggregateOperator::new(
            vec![0],
            vec![
                AggregateDescriptor {
                    function: AggregateFunction::Sum,
                    input_col: 1,
                    output_name: "sum_salary".into(),
                },
                AggregateDescriptor {
                    function: AggregateFunction::Count,
                    input_col: 1,
                    output_name: "cnt".into(),
                },
                AggregateDescriptor {
                    function: AggregateFunction::Min,
                    input_col: 1,
                    output_name: "min_salary".into(),
                },
                AggregateDescriptor {
                    function: AggregateFunction::Max,
                    input_col: 1,
                    output_name: "max_salary".into(),
                },
            ],
        );

        op.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();
        let batches = op.on_watermark(0).unwrap();
        let rb = batches[0].inner().clone();

        let eng_row = find_row(&rb, 0, "eng").unwrap();
        let sales_row = find_row(&rb, 0, "sales").unwrap();

        // eng: sum=450, count=3, min=100, max=200
        assert_eq!(f64_val(&rb, 1, eng_row), 450.0);
        assert_eq!(f64_val(&rb, 2, eng_row), 3.0);
        assert_eq!(f64_val(&rb, 3, eng_row), 100.0);
        assert_eq!(f64_val(&rb, 4, eng_row), 200.0);

        // sales: sum=250, count=2, min=50, max=200
        assert_eq!(f64_val(&rb, 1, sales_row), 250.0);
        assert_eq!(f64_val(&rb, 2, sales_row), 2.0);
        assert_eq!(f64_val(&rb, 3, sales_row), 50.0);
        assert_eq!(f64_val(&rb, 4, sales_row), 200.0);
    }

    // ── Test 3: AVG aggregation ───────────────────────────────────────────────

    #[test]
    fn test_avg_aggregation() {
        let mut op = HashAggregateOperator::new(
            vec![0],
            vec![AggregateDescriptor {
                function: AggregateFunction::Avg,
                input_col: 1,
                output_name: "avg_salary".into(),
            }],
        );

        op.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();
        let batches = op.on_watermark(0).unwrap();
        let rb = batches[0].inner().clone();

        let eng_row = find_row(&rb, 0, "eng").unwrap();
        let sales_row = find_row(&rb, 0, "sales").unwrap();

        // eng avg: 450 / 3 = 150.0
        let eng_avg = f64_val(&rb, 1, eng_row);
        assert!((eng_avg - 150.0).abs() < 1e-9, "eng avg = {}", eng_avg);

        // sales avg: 250 / 2 = 125.0
        let sales_avg = f64_val(&rb, 1, sales_row);
        assert!((sales_avg - 125.0).abs() < 1e-9, "sales avg = {}", sales_avg);
    }

    // ── Test 4: on_watermark triggers output and resets state ─────────────────

    #[test]
    fn test_on_watermark_resets_state() {
        let mut op = HashAggregateOperator::new(
            vec![0],
            vec![AggregateDescriptor {
                function: AggregateFunction::Sum,
                input_col: 1,
                output_name: "sum_salary".into(),
            }],
        );

        op.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();
        let first = op.on_watermark(100).unwrap();
        assert_eq!(first.len(), 1);

        // After watermark, state should be reset — second watermark yields nothing.
        let second = op.on_watermark(200).unwrap();
        assert!(second.is_empty(), "state should be cleared after first flush");

        // Adding new data after reset should produce fresh results.
        let schema = Arc::new(Schema::new(vec![
            Field::new("department", DataType::Utf8, false),
            Field::new("salary", DataType::Float64, false),
        ]));
        let rb2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["eng"])),
                Arc::new(Float64Array::from(vec![999.0])),
            ],
        )
        .unwrap();
        op.add_input(VeloxBatch::new(rb2)).unwrap();
        let third = op.on_watermark(300).unwrap();
        assert_eq!(third.len(), 1);
        let rb = third[0].inner().clone();
        let eng_row = find_row(&rb, 0, "eng").unwrap();
        assert_eq!(f64_val(&rb, 1, eng_row), 999.0);
    }

    // ── Test 5: Checkpoint / restore round-trip ───────────────────────────────

    #[test]
    fn test_checkpoint_restore() {
        let make_op = || {
            HashAggregateOperator::new(
                vec![0],
                vec![AggregateDescriptor {
                    function: AggregateFunction::Sum,
                    input_col: 1,
                    output_name: "sum_salary".into(),
                }],
            )
        };

        let mut op1 = make_op();
        op1.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();

        // Snapshot mid-accumulation (before flush).
        let snapshot = op1.snapshot_state().unwrap();

        // Restore into a fresh operator.
        let mut op2 = make_op();
        op2.restore_state(&snapshot).unwrap();

        // Both operators should yield the same output.
        let out1 = op1.on_watermark(0).unwrap();
        let out2 = op2.on_watermark(0).unwrap();

        let rb1 = out1[0].inner().clone();
        let rb2 = out2[0].inner().clone();
        assert_eq!(rb1.num_rows(), rb2.num_rows());

        let eng1 = find_row(&rb1, 0, "eng").unwrap();
        let eng2 = find_row(&rb2, 0, "eng").unwrap();
        assert_eq!(f64_val(&rb1, 1, eng1), f64_val(&rb2, 1, eng2));
    }

    // ── Test 6: Multiple add_input calls accumulate correctly ─────────────────

    #[test]
    fn test_multiple_add_input_accumulates() {
        let mut op = HashAggregateOperator::new(
            vec![0],
            vec![AggregateDescriptor {
                function: AggregateFunction::Sum,
                input_col: 1,
                output_name: "sum_salary".into(),
            }],
        );

        // Send the same batch twice — totals should double.
        op.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();
        op.add_input(VeloxBatch::new(dept_salary_batch())).unwrap();

        let batches = op.on_watermark(0).unwrap();
        let rb = batches[0].inner().clone();

        let eng_row = find_row(&rb, 0, "eng").unwrap();
        let sales_row = find_row(&rb, 0, "sales").unwrap();

        // eng: (450) * 2 = 900
        assert_eq!(f64_val(&rb, 1, eng_row), 900.0);
        // sales: (250) * 2 = 500
        assert_eq!(f64_val(&rb, 1, sales_row), 500.0);
    }
}
