//! DataFusion-backed operators: physical expressions for filter/project and
//! grouped aggregation on DataFusion's `GroupsAccumulator` fast path.
//!
//! These are drop-in alternatives to the self-built `VectorOp(FilterOperator)`
//! etc. — same `Operator<RecordBatch>` protocol, but expression evaluation and
//! accumulation run on DataFusion machinery. The P11 SQL layer compiles
//! plans down to exactly these building blocks.

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_common::DFSchema;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{ColumnarValue, EmitTo, Expr as DfExpr, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use streamcrab_core::operator_chain::{Operator, TimerDomain};
use streamcrab_core::types::EventTime;

fn compile_expr(expr: &DfExpr, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    let df_schema = DFSchema::try_from(schema.clone()).context("build DFSchema")?;
    create_physical_expr(expr, &df_schema, &ExecutionProps::new())
        .context("compile DataFusion physical expr")
}

fn evaluate_to_array(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Result<ArrayRef> {
    let value: ColumnarValue = expr.evaluate(batch).context("evaluate physical expr")?;
    value
        .into_array(batch.num_rows())
        .context("materialize expr result")
}

/// Filter batches with a DataFusion boolean expression,
/// e.g. `col("f1").gt(lit(50i64))`.
pub struct DfFilterOp {
    predicate: Arc<dyn PhysicalExpr>,
}

impl DfFilterOp {
    pub fn try_new(predicate: DfExpr, input_schema: &Schema) -> Result<Self> {
        Ok(Self {
            predicate: compile_expr(&predicate, input_schema)?,
        })
    }
}

impl Operator<RecordBatch> for DfFilterOp {
    type OUT = RecordBatch;

    fn process_batch(&mut self, input: &[RecordBatch], output: &mut Vec<RecordBatch>) -> Result<()> {
        for batch in input {
            let evaluated = evaluate_to_array(&self.predicate, batch)?;
            let mask = evaluated
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("filter predicate must evaluate to boolean")?;
            let filtered = filter_record_batch(batch, mask).context("filter batch")?;
            if filtered.num_rows() > 0 {
                output.push(filtered);
            }
        }
        Ok(())
    }
}

/// Project batches through DataFusion expressions; output schema is derived
/// from the expressions' types.
pub struct DfProjectOp {
    exprs: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
}

impl DfProjectOp {
    pub fn try_new(exprs: Vec<(DfExpr, &str)>, input_schema: &Schema) -> Result<Self> {
        let mut physical = Vec::with_capacity(exprs.len());
        let mut fields = Vec::with_capacity(exprs.len());
        for (expr, name) in &exprs {
            let p = compile_expr(expr, input_schema)?;
            let dt = p.data_type(input_schema).context("expr output type")?;
            let nullable = p.nullable(input_schema).context("expr nullability")?;
            fields.push(Field::new(*name, dt, nullable));
            physical.push(p);
        }
        Ok(Self {
            exprs: physical,
            schema: Arc::new(Schema::new(fields)),
        })
    }
}

impl Operator<RecordBatch> for DfProjectOp {
    type OUT = RecordBatch;

    fn process_batch(&mut self, input: &[RecordBatch], output: &mut Vec<RecordBatch>) -> Result<()> {
        for batch in input {
            let columns = self
                .exprs
                .iter()
                .map(|e| evaluate_to_array(e, batch))
                .collect::<Result<Vec<_>>>()?;
            output.push(
                RecordBatch::try_new(Arc::clone(&self.schema), columns)
                    .context("build projected batch")?,
            );
        }
        Ok(())
    }
}

type SumAccumulator = PrimitiveGroupsAccumulator<Float64Type, fn(&mut f64, f64)>;

fn new_sum_accumulator() -> SumAccumulator {
    PrimitiveGroupsAccumulator::new(&DataType::Float64, (|acc, v| *acc += v) as fn(&mut f64, f64))
}

struct SumInner {
    converter: Option<RowConverter>,
    key_fields: Vec<Field>,
    group_index: AHashMap<Vec<u8>, usize>,
    group_rows: Vec<OwnedRow>,
    acc: SumAccumulator,
    group_indices_buf: Vec<usize>,
}

impl SumInner {
    fn group_count(&self) -> usize {
        self.group_rows.len()
    }
}

/// Grouped SUM on DataFusion's `PrimitiveGroupsAccumulator` — the columnar
/// fast path: group keys map to dense indices once per batch (arrow row
/// format), then accumulation is a tight `values[group] += v` loop.
///
/// Emits `(key columns..., <output_name>: Float64)` on event-time timers and
/// resets, mirroring `HashAggregateOperator`'s emit-per-watermark semantics.
///
/// Checkpointable: `snapshot_state` drains the accumulator via
/// `state(EmitTo::All)`, immediately merges it back (state extraction is
/// destructive in DataFusion), and encodes keys + state as an arrow IPC
/// stream.
pub struct DfGroupedSumOp {
    key_cols: Vec<usize>,
    value_col: usize,
    output_name: String,
    inner: Mutex<SumInner>,
}

impl DfGroupedSumOp {
    pub fn new(key_cols: Vec<usize>, value_col: usize) -> Self {
        assert!(!key_cols.is_empty(), "key_cols must not be empty");
        Self {
            key_cols,
            value_col,
            output_name: "sum".to_string(),
            inner: Mutex::new(SumInner {
                converter: None,
                key_fields: Vec::new(),
                group_index: AHashMap::default(),
                group_rows: Vec::new(),
                acc: new_sum_accumulator(),
                group_indices_buf: Vec::new(),
            }),
        }
    }

    /// Name of the aggregate output column (the SQL planner aligns this with
    /// the logical plan's field name, e.g. `sum(t.v)`).
    pub fn with_output_name(mut self, name: impl Into<String>) -> Self {
        self.output_name = name.into();
        self
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, SumInner>> {
        self.inner
            .lock()
            .map_err(|_| anyhow!("DfGroupedSumOp state lock poisoned"))
    }

    /// Extract (keys + accumulator state) as one snapshot batch, restoring
    /// the accumulator in place since `state()` drains it.
    fn snapshot_batch(inner: &mut SumInner) -> Result<Option<RecordBatch>> {
        let n = inner.group_count();
        if n == 0 {
            return Ok(None);
        }
        let converter = inner.converter.as_ref().context("groups imply converter")?;

        let state = inner
            .acc
            .state(EmitTo::All)
            .context("extract accumulator state")?;
        let indices: Vec<usize> = (0..n).collect();
        inner
            .acc
            .merge_batch(&state, &indices, None, n)
            .context("restore accumulator after state extraction")?;

        let mut columns = converter
            .convert_rows(inner.group_rows.iter().map(|r| r.row()))
            .context("rebuild key columns")?;
        let mut fields = inner.key_fields.clone();
        for (i, arr) in state.iter().enumerate() {
            fields.push(Field::new(
                format!("__state_{i}"),
                arr.data_type().clone(),
                true,
            ));
        }
        columns.extend(state);
        Ok(Some(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .context("build snapshot batch")?,
        ))
    }
}

impl Operator<RecordBatch> for DfGroupedSumOp {
    type OUT = RecordBatch;

    fn process_batch(&mut self, input: &[RecordBatch], _output: &mut Vec<RecordBatch>) -> Result<()> {
        let inner = &mut *self.lock()?;
        for batch in input {
            if inner.converter.is_none() {
                let fields: Vec<SortField> = self
                    .key_cols
                    .iter()
                    .map(|&i| SortField::new(batch.column(i).data_type().clone()))
                    .collect();
                inner.converter =
                    Some(RowConverter::new(fields).context("build key row converter")?);
                inner.key_fields = self
                    .key_cols
                    .iter()
                    .map(|&i| batch.schema().field(i).clone())
                    .collect();
            }
            let converter = inner.converter.as_ref().expect("converter initialized above");

            let key_arrays: Vec<ArrayRef> = self
                .key_cols
                .iter()
                .map(|&i| Arc::clone(batch.column(i)))
                .collect();
            let rows = converter
                .convert_columns(&key_arrays)
                .context("convert key columns")?;

            let mut group_indices = std::mem::take(&mut inner.group_indices_buf);
            group_indices.clear();
            for row in rows.iter() {
                let idx = match inner.group_index.get(row.as_ref()) {
                    Some(&i) => i,
                    None => {
                        let i = inner.group_index.len();
                        inner.group_index.insert(row.as_ref().to_vec(), i);
                        inner.group_rows.push(row.owned());
                        i
                    }
                };
                group_indices.push(idx);
            }

            let values = batch.column(self.value_col);
            let values_f64 = if values.data_type() == &DataType::Float64 {
                Arc::clone(values)
            } else {
                cast(values, &DataType::Float64).context("cast sum input to f64")?
            };
            let total = inner.group_index.len();
            inner
                .acc
                .update_batch(&[values_f64], &group_indices, None, total)
                .context("GroupsAccumulator update_batch")?;
            inner.group_indices_buf = group_indices;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timestamp: EventTime,
        domain: TimerDomain,
        output: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if domain != TimerDomain::EventTime {
            return Ok(());
        }
        let inner = &mut *self.lock()?;
        if inner.group_rows.is_empty() {
            return Ok(());
        }
        let converter = inner.converter.as_ref().expect("groups imply converter");

        let sums = inner
            .acc
            .evaluate(EmitTo::All)
            .context("GroupsAccumulator evaluate")?;
        let mut columns = converter
            .convert_rows(inner.group_rows.iter().map(|r| r.row()))
            .context("rebuild key columns")?;
        columns.push(sums);

        let mut fields = inner.key_fields.clone();
        fields.push(Field::new(&self.output_name, DataType::Float64, true));
        output.push(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .context("build aggregate output batch")?,
        );

        inner.group_index.clear();
        inner.group_rows.clear();
        inner.acc = new_sum_accumulator();
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        let inner = &mut *self.lock()?;
        let Some(batch) = Self::snapshot_batch(inner)? else {
            return Ok(Vec::new());
        };
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
                .context("open IPC snapshot writer")?;
            writer.write(&batch).context("write snapshot batch")?;
            writer.finish().context("finish snapshot stream")?;
        }
        Ok(buf)
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut reader =
            StreamReader::try_new(Cursor::new(data), None).context("open IPC snapshot reader")?;
        let batch = reader
            .next()
            .context("snapshot stream is empty")?
            .context("read snapshot batch")?;

        let key_count = self.key_cols.len();
        let schema = batch.schema();
        let key_fields: Vec<Field> = schema.fields()[..key_count]
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let key_arrays: Vec<ArrayRef> = (0..key_count).map(|i| Arc::clone(batch.column(i))).collect();
        let state: Vec<ArrayRef> = (key_count..batch.num_columns())
            .map(|i| Arc::clone(batch.column(i)))
            .collect();

        let converter = RowConverter::new(
            key_arrays
                .iter()
                .map(|a| SortField::new(a.data_type().clone()))
                .collect(),
        )
        .context("rebuild key row converter")?;
        let rows = converter
            .convert_columns(&key_arrays)
            .context("convert snapshot keys")?;

        let inner = &mut *self.lock()?;
        inner.group_index.clear();
        inner.group_rows.clear();
        inner.acc = new_sum_accumulator();
        for row in rows.iter() {
            let idx = inner.group_index.len();
            inner.group_index.insert(row.as_ref().to_vec(), idx);
            inner.group_rows.push(row.owned());
        }
        let n = inner.group_rows.len();
        let indices: Vec<usize> = (0..n).collect();
        inner
            .acc
            .merge_batch(&state, &indices, None, n)
            .context("merge snapshot state")?;
        inner.converter = Some(converter);
        inner.key_fields = key_fields;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::ArrowConvertible;
    use datafusion_expr::{col, lit};
    use streamcrab_core::time::EVENT_TIME_MAX;

    #[test]
    fn df_filter_and_project() {
        let rows: Vec<(i64, i64)> = (1..=10).map(|i| (i, i * 10)).collect();
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();
        let schema = <(i64, i64)>::schema();

        let mut filter = DfFilterOp::try_new(col("f1").gt(lit(50i64)), &schema).unwrap();
        let mut filtered = Vec::new();
        filter.process_batch(&[batch], &mut filtered).unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            <(i64, i64)>::from_batch(&filtered[0]).unwrap(),
            vec![(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)]
        );

        let mut project = DfProjectOp::try_new(
            vec![(col("f0"), "f0"), (col("f1") * lit(2i64), "f1")],
            &schema,
        )
        .unwrap();
        let mut projected = Vec::new();
        project.process_batch(&filtered, &mut projected).unwrap();
        assert_eq!(
            <(i64, i64)>::from_batch(&projected[0]).unwrap(),
            vec![(6, 120), (7, 140), (8, 160), (9, 180), (10, 200)]
        );
    }

    fn agg_results(batches: &[RecordBatch]) -> Vec<(i64, f64)> {
        let mut rows = Vec::new();
        for b in batches {
            let keys = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let sums = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                rows.push((keys.value(i), sums.value(i)));
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    #[test]
    fn df_grouped_sum_emits_and_resets_on_watermark() {
        let rows: Vec<(i64, i64)> = (0..100).map(|i| (i % 4, i)).collect();
        let batches: Vec<RecordBatch> = rows
            .chunks(7)
            .map(|c| <(i64, i64)>::to_batch(c).unwrap())
            .collect();

        let mut op = DfGroupedSumOp::new(vec![0], 1);
        let mut out = Vec::new();
        op.process_batch(&batches, &mut out).unwrap();
        assert!(out.is_empty(), "sum must hold until watermark");

        op.on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
            .unwrap();
        assert_eq!(out.len(), 1);

        let mut expected: std::collections::HashMap<i64, f64> = Default::default();
        for &(k, v) in &rows {
            *expected.entry(k).or_default() += v as f64;
        }
        let result = agg_results(&out);
        assert_eq!(result.len(), 4);
        for (k, s) in result {
            assert_eq!(s, expected[&k]);
        }

        // Reset semantics: nothing left after the flush.
        let mut again = Vec::new();
        op.on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut again)
            .unwrap();
        assert!(again.is_empty());
    }

    #[test]
    fn df_grouped_sum_checkpoint_roundtrip() {
        let part_a: Vec<(i64, i64)> = (0..50).map(|i| (i % 5, i)).collect();
        let part_b: Vec<(i64, i64)> = (50..100).map(|i| (i % 5, i)).collect();

        // Continuous reference.
        let mut continuous = DfGroupedSumOp::new(vec![0], 1);
        let mut all: Vec<(i64, i64)> = part_a.clone();
        all.extend(&part_b);
        let mut expected = Vec::new();
        continuous
            .process_batch(&[<(i64, i64)>::to_batch(&all).unwrap()], &mut expected)
            .unwrap();
        continuous
            .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut expected)
            .unwrap();
        let expected = agg_results(&expected);

        // Snapshot mid-stream, restore into a fresh operator, finish there.
        let mut first = DfGroupedSumOp::new(vec![0], 1);
        let mut sink = Vec::new();
        first
            .process_batch(&[<(i64, i64)>::to_batch(&part_a).unwrap()], &mut sink)
            .unwrap();
        let snapshot = first.snapshot_state().unwrap();
        assert!(!snapshot.is_empty());

        // Snapshot must not disturb the running operator.
        let mut first_out = Vec::new();
        first
            .process_batch(&[<(i64, i64)>::to_batch(&part_b).unwrap()], &mut first_out)
            .unwrap();
        first
            .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut first_out)
            .unwrap();
        assert_eq!(agg_results(&first_out), expected);

        let mut second = DfGroupedSumOp::new(vec![0], 1);
        second.restore_state(&snapshot).unwrap();
        let mut resumed = Vec::new();
        second
            .process_batch(&[<(i64, i64)>::to_batch(&part_b).unwrap()], &mut resumed)
            .unwrap();
        second
            .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut resumed)
            .unwrap();
        assert_eq!(agg_results(&resumed), expected);
    }

    #[test]
    fn df_grouped_sum_custom_output_name() {
        let rows = vec![(1i64, 2i64), (1, 3)];
        let mut op = DfGroupedSumOp::new(vec![0], 1).with_output_name("sum(t.v)");
        let mut out = Vec::new();
        op.process_batch(&[<(i64, i64)>::to_batch(&rows).unwrap()], &mut out)
            .unwrap();
        op.on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
            .unwrap();
        assert_eq!(out[0].schema().field(1).name(), "sum(t.v)");
    }
}
