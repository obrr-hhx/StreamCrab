//! Two-phase hash join operator.
//!
//! Implements a classic build-then-probe hash join supporting INNER, LEFT, RIGHT,
//! and FULL join semantics. The operator is split into two phases:
//!
//! 1. **Build phase**: accumulate all build-side batches and index them by key.
//! 2. **Probe phase**: for each probe row, look up matching build rows and emit joined output.
//!
//! Call `finish_build()` to transition from Build to Probe phase. Unmatched rows
//! for LEFT/FULL joins are emitted lazily when `add_input` is called in Probe phase
//! (RIGHT/FULL unmatched probe rows) and via a dedicated `flush_unmatched_build`
//! call at the end of probing (LEFT/FULL unmatched build rows). The operator
//! signals completion via `is_finished()` once `finished` is set and all pending
//! output has been drained.

use crate::batch::VeloxBatch;
use crate::operators::VectorizedOperator;
use ahash::RandomState;
use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, NullArray, StringBuilder,
};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

// ── JoinType ─────────────────────────────────────────────────────────────────

/// Supported join types.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

// ── JoinPhase ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinPhase {
    /// Accumulating build-side batches.
    Build,
    /// Probing with probe-side batches.
    Probe,
}

// ── Key serialization ────────────────────────────────────────────────────────

/// Serialize a single row's key columns into a type-tagged byte string.
///
/// Supports Int64, Float64, Utf8, and Boolean columns. Each value is prefixed
/// with a one-byte type tag so keys of different types never collide.
fn serialize_row_key(columns: &[&ArrayRef], row: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    for col in columns {
        if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
            buf.push(0u8); // tag: Int64
            if arr.is_null(row) {
                buf.extend_from_slice(&i64::MIN.to_le_bytes()); // null sentinel
                buf.push(0u8); // is_null flag
            } else {
                buf.extend_from_slice(&arr.value(row).to_le_bytes());
                buf.push(1u8);
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
            buf.push(1u8); // tag: Float64
            if arr.is_null(row) {
                buf.extend_from_slice(&0u64.to_le_bytes());
                buf.push(0u8);
            } else {
                buf.extend_from_slice(&arr.value(row).to_bits().to_le_bytes());
                buf.push(1u8);
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
            buf.push(2u8); // tag: Utf8
            if arr.is_null(row) {
                buf.push(0u8);
            } else {
                let s = arr.value(row).as_bytes();
                buf.push(1u8);
                let len = s.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(s);
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
            buf.push(3u8); // tag: Boolean
            if arr.is_null(row) {
                buf.push(0u8);
            } else {
                buf.push(1u8);
                buf.push(arr.value(row) as u8);
            }
        }
        // Unknown types are silently skipped — keys will still be partitioned
        // by whatever typed columns were recognized.
    }
    buf
}

// ── Snapshot state ────────────────────────────────────────────────────────────

/// Serializable snapshot of HashJoinOperator build state.
#[derive(serde::Serialize, serde::Deserialize)]
struct JoinSnapshot {
    join_type: JoinType,
    build_key_cols: Vec<usize>,
    probe_key_cols: Vec<usize>,
    /// Each element is an IPC-serialized RecordBatch.
    build_batches_ipc: Vec<Vec<u8>>,
}

// ── HashJoinOperator ──────────────────────────────────────────────────────────

/// Two-phase hash join operator.
///
/// Feed build-side batches via `add_input` while in Build phase, then call
/// `finish_build()` to switch to Probe phase. Feed probe-side batches via
/// `add_input` while in Probe phase. Collect output via `get_output`.
pub struct HashJoinOperator {
    join_type: JoinType,
    build_key_cols: Vec<usize>,
    probe_key_cols: Vec<usize>,
    phase: JoinPhase,

    /// Build side stored as a Vec of RecordBatches.
    build_batches: Vec<RecordBatch>,

    /// Hash map: serialized key bytes → Vec<(batch_idx, row_idx)>.
    build_map: HashMap<Vec<u8>, Vec<(usize, usize)>, RandomState>,

    /// Track which build rows were matched (for LEFT/FULL joins).
    build_matched: Vec<Vec<bool>>,

    /// Pending output batches.
    pending_output: VecDeque<VeloxBatch>,

    /// Output schema: build columns followed by probe columns.
    output_schema: Option<SchemaRef>,

    finished: bool,
}

impl HashJoinOperator {
    /// Create a new HashJoinOperator in Build phase.
    pub fn new(
        join_type: JoinType,
        build_key_cols: Vec<usize>,
        probe_key_cols: Vec<usize>,
    ) -> Self {
        Self {
            join_type,
            build_key_cols,
            probe_key_cols,
            phase: JoinPhase::Build,
            build_batches: Vec::new(),
            build_map: HashMap::with_hasher(RandomState::new()),
            build_matched: Vec::new(),
            pending_output: VecDeque::new(),
            output_schema: None,
            finished: false,
        }
    }

    /// Transition from Build to Probe phase.
    ///
    /// Initializes the `build_matched` tracking vectors (used for LEFT/FULL joins
    /// to detect unmatched build rows at the end of probing).
    pub fn finish_build(&mut self) {
        self.build_matched = self
            .build_batches
            .iter()
            .map(|b| vec![false; b.num_rows()])
            .collect();
        self.phase = JoinPhase::Probe;
    }

    /// Call after all probe batches have been fed (RIGHT/FULL: unmatched probe rows
    /// are already emitted inline). Emits unmatched build rows for LEFT/FULL joins
    /// and sets `finished = true`.
    pub fn finish_probe(&mut self) -> Result<()> {
        if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            self.emit_unmatched_build()?;
        }
        self.finished = true;
        Ok(())
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Index all rows in `batch` (which has already been appended to
    /// `build_batches`) at index `batch_idx` into `build_map`.
    fn index_batch(&mut self, batch_idx: usize) {
        let batch = &self.build_batches[batch_idx];
        let key_cols: Vec<&ArrayRef> = self
            .build_key_cols
            .iter()
            .map(|&i| batch.column(i))
            .collect();

        for row in 0..batch.num_rows() {
            let key = serialize_row_key(&key_cols, row);
            self.build_map
                .entry(key)
                .or_default()
                .push((batch_idx, row));
        }
    }

    /// Build (or cache) the output schema from two independent schemas.
    ///
    /// All output fields are marked nullable because outer joins can pad
    /// either side with nulls.
    fn ensure_output_schema_from_schemas(
        &mut self,
        build_schema: &Schema,
        probe_schema: &Schema,
    ) {
        if self.output_schema.is_none() {
            let fields: Vec<Field> = build_schema
                .fields()
                .iter()
                .chain(probe_schema.fields().iter())
                .map(|f| Field::new(f.name(), f.data_type().clone(), true))
                .collect();
            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
    }

    /// Emit matched (build_row, probe_row) pairs as a RecordBatch.
    fn emit_matched_pairs(
        &mut self,
        pairs: &[(usize, usize, usize, usize)], // (batch_idx, build_row, probe_batch_idx_unused, probe_row)
        probe_batch: &RecordBatch,
    ) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        // We need at least one build batch to determine the schema.
        {
            let build_schema = self.build_batches[pairs[0].0].schema();
            self.ensure_output_schema_from_schemas(&build_schema, &probe_batch.schema());
        }
        let schema = Arc::clone(self.output_schema.as_ref().unwrap());

        // Gather column data by building one array per output column.
        let num_build_cols = self.build_batches[pairs[0].0].num_columns();
        let num_probe_cols = probe_batch.num_columns();
        let n = pairs.len();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_build_cols + num_probe_cols);

        // Build columns.
        for col_idx in 0..num_build_cols {
            let arr = build_column_from_pairs(
                &self.build_batches,
                pairs,
                col_idx,
                schema.field(col_idx).data_type(),
                n,
            )?;
            columns.push(arr);
        }

        // Probe columns.
        for col_idx in 0..num_probe_cols {
            let probe_col = probe_batch.column(col_idx);
            let probe_rows: Vec<usize> = pairs.iter().map(|&(_, _, _, pr)| pr).collect();
            let arr = gather_column(probe_col, &probe_rows, schema.field(num_build_cols + col_idx).data_type())?;
            columns.push(arr);
        }

        let rb = RecordBatch::try_new(schema, columns)
            .context("HashJoinOperator: build matched output batch")?;
        self.pending_output.push_back(VeloxBatch::new(rb));
        Ok(())
    }

    /// Emit unmatched probe rows (RIGHT/FULL) with nulls for build columns.
    fn emit_unmatched_probe_rows(
        &mut self,
        probe_batch: &RecordBatch,
        unmatched: &[usize],
    ) -> Result<()> {
        if unmatched.is_empty() {
            return Ok(());
        }

        // Need a representative build batch for schema. Use first available or
        // construct null schema from output_schema.
        let schema = if let Some(s) = &self.output_schema {
            Arc::clone(s)
        } else {
            // No output schema yet — build one from the probe batch only
            // (we cannot know build schema without a build batch; if there are
            // no build batches, the build side is empty and unmatched probe
            // rows need null build columns).
            return Ok(());
        };

        let num_build_cols = schema.fields().len() - probe_batch.num_columns();
        let n = unmatched.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        // Null arrays for build columns.
        for col_idx in 0..num_build_cols {
            let arr = null_array(schema.field(col_idx).data_type(), n)?;
            columns.push(arr);
        }

        // Actual probe column values.
        for col_idx in 0..probe_batch.num_columns() {
            let probe_col = probe_batch.column(col_idx);
            let arr = gather_column(
                probe_col,
                unmatched,
                schema.field(num_build_cols + col_idx).data_type(),
            )?;
            columns.push(arr);
        }

        let rb = RecordBatch::try_new(schema, columns)
            .context("HashJoinOperator: build unmatched probe output batch")?;
        self.pending_output.push_back(VeloxBatch::new(rb));
        Ok(())
    }

    /// Emit unmatched build rows (LEFT/FULL) with nulls for probe columns.
    fn emit_unmatched_build(&mut self) -> Result<()> {
        let schema = match &self.output_schema {
            Some(s) => Arc::clone(s),
            None => return Ok(()), // nothing was ever matched → nothing to emit
        };

        // Collect all unmatched build row references.
        let mut unmatched: Vec<(usize, usize)> = Vec::new();
        for (bi, matched_rows) in self.build_matched.iter().enumerate() {
            for (ri, &matched) in matched_rows.iter().enumerate() {
                if !matched {
                    unmatched.push((bi, ri));
                }
            }
        }
        if unmatched.is_empty() {
            return Ok(());
        }

        let sample_build = &self.build_batches[unmatched[0].0];
        let num_build_cols = sample_build.num_columns();
        let num_probe_cols = schema.fields().len() - num_build_cols;
        let n = unmatched.len();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        // Actual build column values.
        for col_idx in 0..num_build_cols {
            // Re-use emit_matched_pairs logic by constructing pseudo-pairs.
            let pseudo_pairs: Vec<(usize, usize, usize, usize)> =
                unmatched.iter().map(|&(bi, ri)| (bi, ri, 0, 0)).collect();
            let arr = build_column_from_pairs(
                &self.build_batches,
                &pseudo_pairs,
                col_idx,
                schema.field(col_idx).data_type(),
                n,
            )?;
            columns.push(arr);
        }

        // Null arrays for probe columns.
        for col_idx in 0..num_probe_cols {
            let arr = null_array(schema.field(num_build_cols + col_idx).data_type(), n)?;
            columns.push(arr);
        }

        let rb = RecordBatch::try_new(schema, columns)
            .context("HashJoinOperator: build unmatched build output batch")?;
        self.pending_output.push_back(VeloxBatch::new(rb));
        Ok(())
    }
}

// ── VectorizedOperator impl ───────────────────────────────────────────────────

impl VectorizedOperator for HashJoinOperator {
    fn add_input(&mut self, batch: VeloxBatch) -> Result<()> {
        let rb = batch.materialize().context("HashJoinOperator: materialize input")?;

        match self.phase {
            JoinPhase::Build => {
                let batch_idx = self.build_batches.len();
                self.build_batches.push(rb);
                self.index_batch(batch_idx);
            }
            JoinPhase::Probe => {
                // Ensure output schema is initialized (may not be if no build rows).
                if !self.build_batches.is_empty() {
                    let build_schema = self.build_batches[0].schema();
                    self.ensure_output_schema_from_schemas(&build_schema, &rb.schema());
                }

                let key_cols: Vec<&ArrayRef> = self
                    .probe_key_cols
                    .iter()
                    .map(|&i| rb.column(i))
                    .collect();

                let mut matched_pairs: Vec<(usize, usize, usize, usize)> = Vec::new();
                let mut unmatched_probe: Vec<usize> = Vec::new();

                for probe_row in 0..rb.num_rows() {
                    let key = serialize_row_key(&key_cols, probe_row);
                    if let Some(build_rows) = self.build_map.get(&key) {
                        for &(batch_idx, build_row) in build_rows {
                            matched_pairs.push((batch_idx, build_row, 0, probe_row));
                            // Mark build row as matched.
                            if let Some(matched) = self
                                .build_matched
                                .get_mut(batch_idx)
                                .and_then(|v| v.get_mut(build_row))
                            {
                                *matched = true;
                            }
                        }
                    } else {
                        // No match for this probe row.
                        unmatched_probe.push(probe_row);
                    }
                }

                // Emit matched pairs.
                self.emit_matched_pairs(&matched_pairs, &rb)?;

                // Emit unmatched probe rows for RIGHT/FULL joins.
                if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                    self.emit_unmatched_probe_rows(&rb, &unmatched_probe)?;
                }
            }
        }
        Ok(())
    }

    fn get_output(&mut self) -> Result<Option<VeloxBatch>> {
        Ok(self.pending_output.pop_front())
    }

    fn is_finished(&self) -> bool {
        self.finished && self.pending_output.is_empty()
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        // Only meaningful in Build phase (we checkpoint before probing begins).
        let mut batches_ipc: Vec<Vec<u8>> = Vec::with_capacity(self.build_batches.len());

        for batch in &self.build_batches {
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                    .context("HashJoinOperator snapshot: create IPC writer")?;
                writer
                    .write(batch)
                    .context("HashJoinOperator snapshot: write batch")?;
                writer
                    .finish()
                    .context("HashJoinOperator snapshot: finish IPC writer")?;
            }
            batches_ipc.push(buf);
        }

        let snapshot = JoinSnapshot {
            join_type: self.join_type,
            build_key_cols: self.build_key_cols.clone(),
            probe_key_cols: self.probe_key_cols.clone(),
            build_batches_ipc: batches_ipc,
        };

        bincode::serialize(&snapshot).context("HashJoinOperator snapshot: bincode serialize")
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        let snapshot: JoinSnapshot =
            bincode::deserialize(data).context("HashJoinOperator restore: bincode deserialize")?;

        self.join_type = snapshot.join_type;
        self.build_key_cols = snapshot.build_key_cols;
        self.probe_key_cols = snapshot.probe_key_cols;
        self.build_batches.clear();
        self.build_map.clear();

        for (batch_idx, ipc_bytes) in snapshot.build_batches_ipc.iter().enumerate() {
            let cursor = Cursor::new(ipc_bytes.as_slice());
            let mut reader =
                StreamReader::try_new(cursor, None).context("HashJoinOperator restore: IPC reader")?;
            let batch = reader
                .next()
                .context("HashJoinOperator restore: missing batch in IPC stream")?
                .context("HashJoinOperator restore: IPC read error")?;
            self.build_batches.push(batch);
            self.index_batch(batch_idx);
        }

        self.phase = JoinPhase::Build;
        self.build_matched.clear();
        self.pending_output.clear();
        self.finished = false;
        Ok(())
    }
}

// ── Column-gathering utilities ────────────────────────────────────────────────

/// Gather rows from build batches for a single column.
fn build_column_from_pairs(
    build_batches: &[RecordBatch],
    pairs: &[(usize, usize, usize, usize)],
    col_idx: usize,
    data_type: &DataType,
    n: usize,
) -> Result<ArrayRef> {
    // Collect per-pair source arrays and row indices.
    let sources: Vec<(&ArrayRef, usize)> = pairs
        .iter()
        .map(|&(bi, ri, _, _)| (build_batches[bi].column(col_idx), ri))
        .collect();

    gather_column_mixed(&sources, data_type, n)
}

/// Gather `rows` indices from a single array into a new array of the same type.
fn gather_column(col: &ArrayRef, rows: &[usize], data_type: &DataType) -> Result<ArrayRef> {
    let sources: Vec<(&ArrayRef, usize)> = rows.iter().map(|&r| (col, r)).collect();
    gather_column_mixed(&sources, data_type, rows.len())
}

/// Build a new array by gathering (array, row_index) pairs.
fn gather_column_mixed(
    sources: &[(&ArrayRef, usize)],
    data_type: &DataType,
    n: usize,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(n);
            for &(arr, row) in sources {
                if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
                    if a.is_null(row) {
                        b.append_null();
                    } else {
                        b.append_value(a.value(row));
                    }
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(n);
            for &(arr, row) in sources {
                if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    if a.is_null(row) {
                        b.append_null();
                    } else {
                        b.append_value(a.value(row));
                    }
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(n, n * 8);
            for &(arr, row) in sources {
                if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
                    if a.is_null(row) {
                        b.append_null();
                    } else {
                        b.append_value(a.value(row));
                    }
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for &(arr, row) in sources {
                if let Some(a) = arr.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                    if a.is_null(row) {
                        b.append_null();
                    } else {
                        b.append_value(a.value(row));
                    }
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => anyhow::bail!("HashJoinOperator: unsupported column type {:?}", other),
    }
}

/// Build a null-only array of the given type and length (for outer join padding).
fn null_array(data_type: &DataType, n: usize) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(n);
            for _ in 0..n {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(n);
            for _ in 0..n {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(n, 0);
            for _ in 0..n {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for _ in 0..n {
                b.append_null();
            }
            Ok(Arc::new(b.finish()))
        }
        // Fallback: use Arrow's NullArray (all values are null by definition).
        _ => Ok(Arc::new(NullArray::new(n))),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── Test data helpers ─────────────────────────────────────────────────────

    /// Build side: orders(order_id:Int64, customer_id:Int64, amount:Float64)
    /// Rows: (1,100,10.0), (2,200,20.0), (3,100,30.0), (4,300,40.0)
    fn orders_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![100i64, 200, 100, 300])),
                Arc::new(Float64Array::from(vec![10.0f64, 20.0, 30.0, 40.0])),
            ],
        )
        .unwrap()
    }

    /// Probe side: customers(customer_id:Int64, name:Utf8)
    /// Rows: (100,"Alice"), (200,"Bob"), (400,"Dave")
    fn customers_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100i64, 200, 400])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Dave"])),
            ],
        )
        .unwrap()
    }

    /// Collect all output batches from an operator into a single concatenated list
    /// of (order_id, customer_id_build, amount, customer_id_probe, name) rows —
    /// actually returns the raw batches for the caller to inspect.
    fn drain_output(op: &mut HashJoinOperator) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(batch) = op.get_output().unwrap() {
            let rb = batch.materialize().unwrap();
            if rb.num_rows() > 0 {
                out.push(rb);
            }
        }
        out
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ── Test 1: Inner join ────────────────────────────────────────────────────

    /// Inner join on customer_id. Probe (100,Alice),(200,Bob),(400,Dave).
    /// Expected matches: (1,100,10.0) x Alice, (3,100,30.0) x Alice, (2,200,20.0) x Bob → 3 rows.
    #[test]
    fn test_inner_join() {
        // build_key_cols=[1] (customer_id in orders), probe_key_cols=[0] (customer_id in customers)
        let mut op = HashJoinOperator::new(JoinType::Inner, vec![1], vec![0]);

        op.add_input(VeloxBatch::new(orders_batch())).unwrap();
        op.finish_build();
        op.add_input(VeloxBatch::new(customers_batch())).unwrap();
        op.finish_probe().unwrap();

        let batches = drain_output(&mut op);
        assert_eq!(total_rows(&batches), 3, "expected 3 matched rows");

        // Collect all order_ids from output.
        let mut order_ids: Vec<i64> = batches
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
        order_ids.sort_unstable();
        assert_eq!(order_ids, vec![1, 2, 3]);

        // Collect all names from output (column index 4 = probe col 1 = name).
        let mut names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let arr = b
                    .column(4)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..arr.len()).map(|i| arr.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect();
        names.sort_unstable();
        assert_eq!(names, vec!["Alice", "Alice", "Bob"]);
    }

    // ── Test 2: Left join ─────────────────────────────────────────────────────

    /// Left join: all build rows must appear, including customer_id=300 (no probe match).
    /// Expected: 3 matched rows + 1 unmatched build row = 4 rows total.
    #[test]
    fn test_left_join() {
        let mut op = HashJoinOperator::new(JoinType::Left, vec![1], vec![0]);

        op.add_input(VeloxBatch::new(orders_batch())).unwrap();
        op.finish_build();
        op.add_input(VeloxBatch::new(customers_batch())).unwrap();
        op.finish_probe().unwrap();

        let batches = drain_output(&mut op);
        assert_eq!(total_rows(&batches), 4, "expected 4 rows (3 matched + 1 unmatched build)");

        // The unmatched build row should have null for the name column (col 4).
        let has_null_name = batches.iter().any(|b| {
            let arr = b.column(4).as_any().downcast_ref::<StringArray>().unwrap();
            (0..arr.len()).any(|i| arr.is_null(i))
        });
        assert!(has_null_name, "expected at least one null name for unmatched build row");

        // order_id=4 (customer_id=300) should appear.
        let order_ids: Vec<i64> = batches
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
        assert!(order_ids.contains(&4), "order_id=4 must appear in left join output");
    }

    // ── Test 3: Right join ────────────────────────────────────────────────────

    /// Right join: all probe rows must appear. "Dave" (customer_id=400) has no match.
    /// Expected: 3 matched rows + 1 unmatched probe row = 4 rows.
    #[test]
    fn test_right_join() {
        let mut op = HashJoinOperator::new(JoinType::Right, vec![1], vec![0]);

        op.add_input(VeloxBatch::new(orders_batch())).unwrap();
        op.finish_build();
        op.add_input(VeloxBatch::new(customers_batch())).unwrap();
        op.finish_probe().unwrap();

        let batches = drain_output(&mut op);
        assert_eq!(total_rows(&batches), 4, "expected 4 rows (3 matched + 1 unmatched probe)");

        // "Dave" row should appear with null order_id (col 0).
        let has_dave = batches.iter().any(|b| {
            let names = b.column(4).as_any().downcast_ref::<StringArray>().unwrap();
            let order_ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..names.len()).any(|i| names.value(i) == "Dave" && order_ids.is_null(i))
        });
        assert!(has_dave, "Dave must appear with null order_id in right join");
    }

    // ── Test 4: Multiple matches ──────────────────────────────────────────────

    /// Probe row customer_id=100 matches two build rows (order_id=1 and order_id=3).
    #[test]
    fn test_multiple_matches() {
        let mut op = HashJoinOperator::new(JoinType::Inner, vec![1], vec![0]);

        op.add_input(VeloxBatch::new(orders_batch())).unwrap();
        op.finish_build();

        // Only probe with Alice (customer_id=100).
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let probe = RecordBatch::try_new(
            probe_schema,
            vec![
                Arc::new(Int64Array::from(vec![100i64])),
                Arc::new(StringArray::from(vec!["Alice"])),
            ],
        )
        .unwrap();

        op.add_input(VeloxBatch::new(probe)).unwrap();
        op.finish_probe().unwrap();

        let batches = drain_output(&mut op);
        assert_eq!(total_rows(&batches), 2, "customer_id=100 matches 2 build rows");

        let mut order_ids: Vec<i64> = batches
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
        order_ids.sort_unstable();
        assert_eq!(order_ids, vec![1, 3]);
    }

    // ── Test 5: Checkpoint / restore ─────────────────────────────────────────

    /// Build side snapshot → restore into a new operator → finish_build → probe → same results.
    #[test]
    fn test_checkpoint_restore() {
        // Phase 1: build and snapshot.
        let mut op1 = HashJoinOperator::new(JoinType::Inner, vec![1], vec![0]);
        op1.add_input(VeloxBatch::new(orders_batch())).unwrap();
        let snapshot = op1.snapshot_state().unwrap();

        // Phase 2: restore into a fresh operator and probe.
        let mut op2 = HashJoinOperator::new(JoinType::Inner, vec![1], vec![0]);
        op2.restore_state(&snapshot).unwrap();
        op2.finish_build();
        op2.add_input(VeloxBatch::new(customers_batch())).unwrap();
        op2.finish_probe().unwrap();

        let batches = drain_output(&mut op2);
        assert_eq!(total_rows(&batches), 3, "restored operator must produce same 3 matched rows");

        let mut order_ids: Vec<i64> = batches
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
        order_ids.sort_unstable();
        assert_eq!(order_ids, vec![1, 2, 3]);
    }

    // ── Test 6: No matches ────────────────────────────────────────────────────

    /// Probe with keys not present in the build side → 0 output rows for INNER.
    #[test]
    fn test_no_matches_inner() {
        let mut op = HashJoinOperator::new(JoinType::Inner, vec![1], vec![0]);
        op.add_input(VeloxBatch::new(orders_batch())).unwrap();
        op.finish_build();

        // Probe with customer_id=999 (not in build side).
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let probe = RecordBatch::try_new(
            probe_schema,
            vec![
                Arc::new(Int64Array::from(vec![999i64])),
                Arc::new(StringArray::from(vec!["Nobody"])),
            ],
        )
        .unwrap();

        op.add_input(VeloxBatch::new(probe)).unwrap();
        op.finish_probe().unwrap();

        let batches = drain_output(&mut op);
        assert_eq!(total_rows(&batches), 0, "no matches → 0 output rows");
    }
}
