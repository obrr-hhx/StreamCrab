//! Vectorized window aggregation operator.
//!
//! Implements tumbling and sliding event-time windows with multiple aggregate
//! functions per window. Windows fire when the watermark advances past
//! window_end, following standard Flink semantics.
//!
//! Output schema:
//!   [part_0..part_N (partition cols), agg_output_cols..., window_start:Int64, window_end:Int64]

use crate::batch::VeloxBatch;
use crate::operators::VectorizedOperator;
use ahash::AHashMap;
use anyhow::{anyhow, Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::sync::Arc;

// ── Window type ──────────────────────────────────────────────────────────────

/// Defines the windowing strategy.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WindowType {
    /// Fixed-size, non-overlapping windows.
    Tumbling { size_ms: i64 },
    /// Fixed-size, overlapping windows.
    Sliding { size_ms: i64, slide_ms: i64 },
}

// ── Aggregate descriptor ─────────────────────────────────────────────────────

/// Aggregate function applied to one input column.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum WindowAggFunction {
    Sum,
    Count,
    Min,
    Max,
}

/// Describes a single aggregate computation.
#[derive(Debug, Clone)]
pub struct WindowAggregateDescriptor {
    pub function: WindowAggFunction,
    /// Input column index.
    pub input_col: usize,
    /// Name of the output column.
    pub output_name: String,
}

// ── Window key ───────────────────────────────────────────────────────────────

/// Identifies one specific window instance for one partition.
#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct WindowKey {
    /// Type-tagged serialized bytes of the partition columns for this row.
    partition_key: Vec<u8>,
    /// Window start timestamp (ms, inclusive).
    window_start: i64,
    /// Window end timestamp (ms, exclusive).
    window_end: i64,
}

// ── Partition key value ──────────────────────────────────────────────────────

/// Typed value stored for output reconstruction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum GroupKeyValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    Null,
}

// ── Window accumulator ───────────────────────────────────────────────────────

/// Per-window accumulated state.
///
/// `accumulators[i]` holds `(sum, count, min, max)` for aggregate `i`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WindowBuffer {
    /// One `(sum, count, min, max)` tuple per aggregate descriptor.
    accumulators: Vec<(f64, i64, f64, f64)>,
    /// Actual partition column values for output reconstruction.
    partition_values: Vec<GroupKeyValue>,
}

impl WindowBuffer {
    fn new(num_aggs: usize, partition_values: Vec<GroupKeyValue>) -> Self {
        Self {
            // min initialised to +inf, max to -inf so the first update wins.
            accumulators: vec![(0.0, 0, f64::INFINITY, f64::NEG_INFINITY); num_aggs],
            partition_values,
        }
    }

    /// Incorporate one numeric value into accumulator at `idx`.
    fn update(&mut self, idx: usize, value: f64) {
        let acc = &mut self.accumulators[idx];
        acc.0 += value;
        acc.1 += 1;
        if value < acc.2 {
            acc.2 = value;
        }
        if value > acc.3 {
            acc.3 = value;
        }
    }
}

// ── Output column builders ───────────────────────────────────────────────────

enum PartitionColBuilder {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
}

impl PartitionColBuilder {
    fn from_value(val: &GroupKeyValue, capacity: usize) -> Self {
        match val {
            GroupKeyValue::Int64(_) => Self::Int64(Vec::with_capacity(capacity)),
            GroupKeyValue::Float64(_) => Self::Float64(Vec::with_capacity(capacity)),
            GroupKeyValue::Utf8(_) => Self::Utf8(Vec::with_capacity(capacity)),
            GroupKeyValue::Bool(_) => Self::Bool(Vec::with_capacity(capacity)),
            GroupKeyValue::Null => Self::Utf8(Vec::with_capacity(capacity)),
        }
    }

    fn push(&mut self, val: &GroupKeyValue) {
        match (self, val) {
            (Self::Int64(v), GroupKeyValue::Int64(x)) => v.push(Some(*x)),
            (Self::Float64(v), GroupKeyValue::Float64(x)) => v.push(Some(*x)),
            (Self::Utf8(v), GroupKeyValue::Utf8(s)) => v.push(Some(s.clone())),
            (Self::Bool(v), GroupKeyValue::Bool(b)) => v.push(Some(*b)),
            // Null or type mismatch.
            (Self::Int64(v), _) => v.push(None),
            (Self::Float64(v), _) => v.push(None),
            (Self::Utf8(v), _) => v.push(None),
            (Self::Bool(v), _) => v.push(None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Int64(v) => Arc::new(Int64Array::from(v)),
            Self::Float64(v) => Arc::new(Float64Array::from(v)),
            Self::Utf8(v) => Arc::new(StringArray::from(v)),
            Self::Bool(v) => Arc::new(BooleanArray::from(v)),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Int64(_) => DataType::Int64,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Bool(_) => DataType::Boolean,
        }
    }
}

struct TypedAggBuffer {
    func: WindowAggFunction,
    floats: Vec<f64>,
    ints: Vec<i64>,
}

impl TypedAggBuffer {
    fn new(func: WindowAggFunction, capacity: usize) -> Self {
        Self {
            func,
            floats: Vec::with_capacity(capacity),
            ints: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, acc: (f64, i64, f64, f64)) {
        match self.func {
            WindowAggFunction::Sum => self.floats.push(acc.0),
            WindowAggFunction::Count => self.ints.push(acc.1),
            WindowAggFunction::Min => {
                self.floats.push(if acc.2 == f64::INFINITY { 0.0 } else { acc.2 })
            }
            WindowAggFunction::Max => {
                self.floats.push(if acc.3 == f64::NEG_INFINITY { 0.0 } else { acc.3 })
            }
        }
    }

    fn finish(self) -> (ArrayRef, DataType) {
        match self.func {
            WindowAggFunction::Count => {
                (Arc::new(Int64Array::from(self.ints)), DataType::Int64)
            }
            _ => (Arc::new(Float64Array::from(self.floats)), DataType::Float64),
        }
    }
}

// ── Serialisable checkpoint state ─────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointState {
    windows: Vec<(WindowKey, WindowBuffer)>,
    current_watermark: i64,
    window_type: WindowType,
}

// ── Operator ─────────────────────────────────────────────────────────────────

/// Vectorized event-time window aggregation operator.
pub struct WindowAggregateOperator {
    window_type: WindowType,
    /// Column index of the Int64 event-time timestamp (milliseconds).
    event_time_col: usize,
    /// Column indices used as the partition (group-by) key.
    partition_cols: Vec<usize>,
    aggregates: Vec<WindowAggregateDescriptor>,
    /// Active (not yet fired) windows, keyed by (partition_key, start, end).
    windows: AHashMap<WindowKey, WindowBuffer>,
    /// Current watermark (ms).
    current_watermark: i64,
    /// Output batches waiting to be consumed via get_output.
    pending_output: VecDeque<VeloxBatch>,
    finished: bool,
}

impl WindowAggregateOperator {
    /// Create a new operator.
    pub fn new(
        window_type: WindowType,
        event_time_col: usize,
        partition_cols: Vec<usize>,
        aggregates: Vec<WindowAggregateDescriptor>,
    ) -> Self {
        Self {
            window_type,
            event_time_col,
            partition_cols,
            aggregates,
            windows: AHashMap::new(),
            current_watermark: i64::MIN,
            pending_output: VecDeque::new(),
            finished: false,
        }
    }

    // ── Window assignment ────────────────────────────────────────────────────

    /// Return all (start, end) window intervals that contain `event_time`.
    fn assign_windows(&self, event_time: i64) -> Vec<(i64, i64)> {
        match &self.window_type {
            WindowType::Tumbling { size_ms } => {
                let size = *size_ms;
                let start = event_time.div_euclid(size) * size;
                vec![(start, start + size)]
            }
            WindowType::Sliding { size_ms, slide_ms } => {
                let size = *size_ms;
                let slide = *slide_ms;
                // Latest window whose start <= event_time.
                let last_start = event_time.div_euclid(slide) * slide;
                let mut result = Vec::new();
                let mut start = last_start;
                // Walk backwards until the window can no longer contain event_time.
                while event_time < start + size {
                    if start <= event_time {
                        result.push((start, start + size));
                    }
                    start -= slide;
                    if start + size <= event_time.saturating_sub(size) {
                        break;
                    }
                    // Safety: stop if we've gone back far enough.
                    if start < event_time - size {
                        break;
                    }
                }
                result
            }
        }
    }

    // ── Key serialisation ────────────────────────────────────────────────────

    /// Serialize partition columns for row `row` into a type-tagged byte key.
    fn serialize_partition_key(columns: &[ArrayRef], row: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        for col in columns {
            if col.is_null(row) {
                buf.push(0u8);
            } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
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
                buf.push(0u8); // unknown type → null tag
            }
        }
        buf
    }

    /// Extract typed partition values from row `row` for output reconstruction.
    fn extract_partition_values(columns: &[ArrayRef], row: usize) -> Vec<GroupKeyValue> {
        columns
            .iter()
            .map(|col| {
                if col.is_null(row) {
                    GroupKeyValue::Null
                } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    GroupKeyValue::Int64(arr.value(row))
                } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                    GroupKeyValue::Float64(arr.value(row))
                } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    GroupKeyValue::Utf8(arr.value(row).to_owned())
                } else if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                    GroupKeyValue::Bool(arr.value(row))
                } else {
                    GroupKeyValue::Null
                }
            })
            .collect()
    }

    // ── Output batch construction ────────────────────────────────────────────

    /// Build the output RecordBatch from a set of completed (WindowKey, WindowBuffer) pairs.
    ///
    /// Schema: [part_0..part_N, agg_cols..., window_start:Int64, window_end:Int64]
    fn build_completed_batch(
        &self,
        completed: &[(WindowKey, WindowBuffer)],
    ) -> Result<RecordBatch> {
        debug_assert!(!completed.is_empty());
        let n = completed.len();

        // Partition builders — derive types from the first buffer's stored values.
        let sample_parts = &completed[0].1.partition_values;
        let mut part_builders: Vec<PartitionColBuilder> = sample_parts
            .iter()
            .map(|val| PartitionColBuilder::from_value(val, n))
            .collect();

        // Aggregate buffers.
        let mut agg_bufs: Vec<TypedAggBuffer> = self
            .aggregates
            .iter()
            .map(|agg| TypedAggBuffer::new(agg.function, n))
            .collect();

        let mut starts: Vec<i64> = Vec::with_capacity(n);
        let mut ends: Vec<i64> = Vec::with_capacity(n);

        for (key, buf) in completed {
            for (i, val) in buf.partition_values.iter().enumerate() {
                part_builders[i].push(val);
            }
            for (i, &acc) in buf.accumulators.iter().enumerate() {
                agg_bufs[i].push(acc);
            }
            starts.push(key.window_start);
            ends.push(key.window_end);
        }

        let mut fields: Vec<Field> = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        for (i, builder) in part_builders.into_iter().enumerate() {
            let dtype = builder.data_type();
            fields.push(Field::new(format!("part_{}", i), dtype, true));
            columns.push(builder.finish());
        }

        for (i, buf) in agg_bufs.into_iter().enumerate() {
            let name = &self.aggregates[i].output_name;
            let (arr, dtype) = buf.finish();
            fields.push(Field::new(name, dtype, false));
            columns.push(arr);
        }

        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .context("WindowAggregateOperator: build_completed_batch")
    }
}

// ── VectorizedOperator impl ───────────────────────────────────────────────────

impl VectorizedOperator for WindowAggregateOperator {
    fn add_input(&mut self, batch: VeloxBatch) -> Result<()> {
        let rb = batch.materialize().context("WindowAggregateOperator: materialize")?;
        let num_rows = rb.num_rows();

        let time_col = rb
            .column(self.event_time_col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                anyhow!("event_time column {} is not Int64", self.event_time_col)
            })?;

        let part_cols: Vec<ArrayRef> = self
            .partition_cols
            .iter()
            .map(|&idx| Arc::clone(rb.column(idx)))
            .collect();

        let agg_input_cols: Vec<ArrayRef> = self
            .aggregates
            .iter()
            .map(|agg| Arc::clone(rb.column(agg.input_col)))
            .collect();

        for row in 0..num_rows {
            let event_time = time_col.value(row);
            let windows = self.assign_windows(event_time);
            let part_key = Self::serialize_partition_key(&part_cols, row);
            let part_values = Self::extract_partition_values(&part_cols, row);

            for (ws, we) in windows {
                // Skip events whose window has already been flushed (late event).
                if we <= self.current_watermark {
                    continue;
                }

                let key = WindowKey {
                    partition_key: part_key.clone(),
                    window_start: ws,
                    window_end: we,
                };

                let num_aggs = self.aggregates.len();
                let buf = self
                    .windows
                    .entry(key)
                    .or_insert_with(|| WindowBuffer::new(num_aggs, part_values.clone()));

                for (agg_idx, col) in agg_input_cols.iter().enumerate() {
                    if col.is_null(row) {
                        continue; // NULL inputs are ignored (SQL semantics).
                    }
                    let value: f64 = if let Some(arr) =
                        col.as_any().downcast_ref::<Float64Array>()
                    {
                        arr.value(row)
                    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        arr.value(row) as f64
                    } else {
                        continue;
                    };
                    buf.update(agg_idx, value);
                }
            }
        }

        Ok(())
    }

    fn get_output(&mut self) -> Result<Option<VeloxBatch>> {
        Ok(self.pending_output.pop_front())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn on_watermark(&mut self, watermark: i64) -> Result<Vec<VeloxBatch>> {
        self.current_watermark = watermark;

        // Collect completed windows (window_end <= watermark) without drain_filter
        // (which is nightly-only).  We extract keys first, then remove.
        let completed_keys: Vec<WindowKey> = self
            .windows
            .keys()
            .filter(|k| k.window_end <= watermark)
            .cloned()
            .collect();

        if completed_keys.is_empty() {
            return Ok(Vec::new());
        }

        let completed: Vec<(WindowKey, WindowBuffer)> = completed_keys
            .into_iter()
            .filter_map(|k| self.windows.remove(&k).map(|v| (k, v)))
            .collect();

        let rb = self.build_completed_batch(&completed)?;
        Ok(vec![VeloxBatch::new(rb)])
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        let state = CheckpointState {
            windows: self.windows.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            current_watermark: self.current_watermark,
            window_type: self.window_type.clone(),
        };
        bincode::serialize(&state).map_err(|e| anyhow!("snapshot_state: {}", e))
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        let state: CheckpointState =
            bincode::deserialize(data).map_err(|e| anyhow!("restore_state: {}", e))?;
        self.windows = state.windows.into_iter().collect();
        self.current_watermark = state.current_watermark;
        self.window_type = state.window_type;
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Build a test batch with schema: user_id:Utf8, event_time:Int64, value:Float64
    fn make_batch(rows: &[(&str, i64, f64)]) -> VeloxBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("event_time", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let users: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let times: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let values: Vec<f64> = rows.iter().map(|r| r.2).collect();
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(users)),
                Arc::new(Int64Array::from(times)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();
        VeloxBatch::new(rb)
    }

    fn sum_op() -> WindowAggregateOperator {
        // event_time col=1, partition on user_id (col=0), sum value (col=2)
        WindowAggregateOperator::new(
            WindowType::Tumbling { size_ms: 1000 },
            1,
            vec![0],
            vec![WindowAggregateDescriptor {
                function: WindowAggFunction::Sum,
                input_col: 2,
                output_name: "sum_value".to_owned(),
            }],
        )
    }

    /// Collect rows from an output batch sorted by partition value then window_start.
    /// Returns Vec<(user_id, agg_f64, window_start, window_end)>.
    fn collect_rows_sum(rb: &RecordBatch) -> Vec<(String, f64, i64, i64)> {
        let n = rb.num_rows();
        // Columns: part_0 (Utf8), sum_value (Float64), window_start (Int64), window_end (Int64)
        let users = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let sums = rb.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let ws = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let we = rb.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut rows: Vec<(String, f64, i64, i64)> = (0..n)
            .map(|i| (users.value(i).to_owned(), sums.value(i), ws.value(i), we.value(i)))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
        rows
    }

    // ── Test 1: Tumbling — 3 events in same window, watermark fires ───────────

    #[test]
    fn test_tumbling_single_window_fires() {
        let mut op = sum_op();

        op.add_input(make_batch(&[
            ("alice", 100, 1.0),
            ("alice", 500, 2.0),
            ("alice", 900, 3.0),
        ]))
        .unwrap();

        // Watermark at 999: window [0,1000) not yet complete.
        let out = op.on_watermark(999).unwrap();
        assert!(out.is_empty(), "must not fire before window_end");

        // Watermark at 1000: fires [0,1000).
        let out = op.on_watermark(1000).unwrap();
        assert_eq!(out.len(), 1);
        let rb = out[0].inner();
        assert_eq!(rb.num_rows(), 1);
        let rows = collect_rows_sum(rb);
        assert_eq!(rows[0].0, "alice");
        assert!((rows[0].1 - 6.0).abs() < 1e-10, "sum should be 6.0, got {}", rows[0].1);
        assert_eq!(rows[0].2, 0);
        assert_eq!(rows[0].3, 1000);
    }

    // ── Test 2: Tumbling — events across 2 windows, first fires only ──────────

    #[test]
    fn test_tumbling_two_windows_partial_fire() {
        let mut op = sum_op();

        op.add_input(make_batch(&[
            ("alice", 100, 1.0),
            ("alice", 500, 2.0),
            ("alice", 1500, 4.0), // window [1000, 2000)
        ]))
        .unwrap();

        let out = op.on_watermark(1000).unwrap();
        assert_eq!(out.len(), 1);
        let rows = collect_rows_sum(out[0].inner());
        assert!((rows[0].1 - 3.0).abs() < 1e-10, "first window sum: 1+2=3");
        assert_eq!(rows[0].2, 0);

        // Second window still pending.
        assert_eq!(op.windows.len(), 1);

        let out2 = op.on_watermark(2000).unwrap();
        assert_eq!(out2.len(), 1);
        let rows2 = collect_rows_sum(out2[0].inner());
        assert!((rows2[0].1 - 4.0).abs() < 1e-10, "second window sum: 4");
    }

    // ── Test 3: Sliding — one event contributes to multiple windows ───────────

    #[test]
    fn test_sliding_event_in_multiple_windows() {
        // size=1000ms, slide=500ms → event at t=700 falls in [0,1000) and [500,1500).
        let mut op = WindowAggregateOperator::new(
            WindowType::Sliding { size_ms: 1000, slide_ms: 500 },
            1,
            vec![0],
            vec![WindowAggregateDescriptor {
                function: WindowAggFunction::Count,
                input_col: 2,
                output_name: "cnt".to_owned(),
            }],
        );

        op.add_input(make_batch(&[("alice", 700, 1.0)])).unwrap();

        // Both windows should be active.
        assert_eq!(op.windows.len(), 2, "event at 700 should open 2 sliding windows");

        // Watermark 1000 fires [0,1000).
        let out = op.on_watermark(1000).unwrap();
        assert_eq!(out.len(), 1);
        let rb = out[0].inner();
        let cnt = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt.value(0), 1, "counted in [0,1000)");

        // [500,1500) still active.
        assert_eq!(op.windows.len(), 1);

        // Watermark 1500 fires [500,1500).
        let out2 = op.on_watermark(1500).unwrap();
        assert_eq!(out2.len(), 1);
        let rb2 = out2[0].inner();
        let cnt2 = rb2.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt2.value(0), 1, "counted in [500,1500)");
    }

    // ── Test 4: Partitioned windows — different partition keys separate ────────

    #[test]
    fn test_partitioned_windows() {
        let mut op = sum_op();

        op.add_input(make_batch(&[
            ("alice", 100, 1.0),
            ("alice", 500, 2.0),
            ("bob", 200, 3.0),
        ]))
        .unwrap();

        let out = op.on_watermark(1000).unwrap();
        assert_eq!(out.len(), 1);
        let rb = out[0].inner();
        assert_eq!(rb.num_rows(), 2, "one row per partition key");

        let rows = collect_rows_sum(rb);
        // sorted: alice then bob
        assert_eq!(rows[0].0, "alice");
        assert!((rows[0].1 - 3.0).abs() < 1e-10, "alice sum: 1+2=3");
        assert_eq!(rows[1].0, "bob");
        assert!((rows[1].1 - 3.0).abs() < 1e-10, "bob sum: 3");
    }

    // ── Test 5: Checkpoint / restore ─────────────────────────────────────────

    #[test]
    fn test_checkpoint_restore() {
        let mut op = sum_op();

        op.add_input(make_batch(&[("alice", 100, 1.0), ("alice", 500, 2.0)])).unwrap();

        let snap = op.snapshot_state().unwrap();

        // Restore into a fresh operator.
        let mut op2 = sum_op();
        op2.restore_state(&snap).unwrap();

        assert_eq!(op.windows.len(), op2.windows.len(), "window count must match after restore");

        let out = op2.on_watermark(1000).unwrap();
        assert_eq!(out.len(), 1);
        let rows = collect_rows_sum(out[0].inner());
        assert!((rows[0].1 - 3.0).abs() < 1e-10, "restored sum: 1+2=3");
    }

    // ── Test 6: Late event skipped ────────────────────────────────────────────

    #[test]
    fn test_late_event_skipped() {
        let mut op = sum_op();

        op.add_input(make_batch(&[("alice", 100, 1.0)])).unwrap();
        op.on_watermark(1000).unwrap(); // fires and removes [0,1000)
        assert_eq!(op.windows.len(), 0);

        // Late event for the already-fired window.
        op.add_input(make_batch(&[("alice", 200, 99.0)])).unwrap();
        assert_eq!(op.windows.len(), 0, "late event must not reopen a flushed window");

        let out = op.on_watermark(2000).unwrap();
        assert!(out.is_empty());
    }

    // ── Extra: min / max aggregates ───────────────────────────────────────────

    #[test]
    fn test_min_max_aggregates() {
        let mut op = WindowAggregateOperator::new(
            WindowType::Tumbling { size_ms: 1000 },
            1,
            vec![0],
            vec![
                WindowAggregateDescriptor {
                    function: WindowAggFunction::Min,
                    input_col: 2,
                    output_name: "min_v".to_owned(),
                },
                WindowAggregateDescriptor {
                    function: WindowAggFunction::Max,
                    input_col: 2,
                    output_name: "max_v".to_owned(),
                },
            ],
        );

        op.add_input(make_batch(&[
            ("alice", 100, 5.0),
            ("alice", 200, 1.0),
            ("alice", 300, 9.0),
        ]))
        .unwrap();

        let out = op.on_watermark(1000).unwrap();
        assert_eq!(out.len(), 1);
        let rb = out[0].inner();
        // Columns: part_0, min_v, max_v, window_start, window_end
        let min_col = rb.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_col = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((min_col.value(0) - 1.0).abs() < 1e-10, "min should be 1.0");
        assert!((max_col.value(0) - 9.0).abs() < 1e-10, "max should be 9.0");
    }
}
