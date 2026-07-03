//! DataFusion-backed operators: physical expressions for filter/project and
//! grouped aggregation on DataFusion's `GroupsAccumulator` fast path.
//!
//! These are drop-in alternatives to the self-built `VectorOp(FilterOperator)`
//! etc. — same `Operator<RecordBatch>` protocol, but expression evaluation and
//! accumulation run on DataFusion machinery. The P11 SQL layer will compile
//! plans down to exactly these building blocks.

use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef};
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

/// Grouped SUM on DataFusion's `PrimitiveGroupsAccumulator` — the columnar
/// fast path: group keys map to dense indices once per batch (arrow row
/// format), then accumulation is a tight `values[group] += v` loop.
///
/// Emits `(key columns..., sum: Float64)` on event-time timers and resets,
/// mirroring `HashAggregateOperator`'s emit-per-watermark semantics.
pub struct DfGroupedSumOp {
    key_cols: Vec<usize>,
    value_col: usize,
    converter: Option<RowConverter>,
    key_fields: Vec<Field>,
    group_index: AHashMap<Vec<u8>, usize>,
    group_rows: Vec<OwnedRow>,
    acc: PrimitiveGroupsAccumulator<Float64Type, fn(&mut f64, f64)>,
    group_indices_buf: Vec<usize>,
}

impl DfGroupedSumOp {
    pub fn new(key_cols: Vec<usize>, value_col: usize) -> Self {
        assert!(!key_cols.is_empty(), "key_cols must not be empty");
        Self {
            key_cols,
            value_col,
            converter: None,
            key_fields: Vec::new(),
            group_index: AHashMap::default(),
            group_rows: Vec::new(),
            acc: new_sum_accumulator(),
            group_indices_buf: Vec::new(),
        }
    }
}

fn new_sum_accumulator() -> PrimitiveGroupsAccumulator<Float64Type, fn(&mut f64, f64)> {
    PrimitiveGroupsAccumulator::new(&DataType::Float64, (|acc, v| *acc += v) as fn(&mut f64, f64))
}

impl Operator<RecordBatch> for DfGroupedSumOp {
    type OUT = RecordBatch;

    fn process_batch(&mut self, input: &[RecordBatch], output: &mut Vec<RecordBatch>) -> Result<()> {
        let _ = &mut *output;
        for batch in input {
            if self.converter.is_none() {
                let fields: Vec<SortField> = self
                    .key_cols
                    .iter()
                    .map(|&i| SortField::new(batch.column(i).data_type().clone()))
                    .collect();
                self.converter =
                    Some(RowConverter::new(fields).context("build key row converter")?);
                self.key_fields = self
                    .key_cols
                    .iter()
                    .map(|&i| batch.schema().field(i).clone())
                    .collect();
            }
            let converter = self.converter.as_ref().expect("converter initialized above");

            let key_arrays: Vec<ArrayRef> = self
                .key_cols
                .iter()
                .map(|&i| Arc::clone(batch.column(i)))
                .collect();
            let rows = converter
                .convert_columns(&key_arrays)
                .context("convert key columns")?;

            self.group_indices_buf.clear();
            for row in rows.iter() {
                let idx = match self.group_index.get(row.as_ref()) {
                    Some(&i) => i,
                    None => {
                        let i = self.group_index.len();
                        self.group_index.insert(row.as_ref().to_vec(), i);
                        self.group_rows.push(row.owned());
                        i
                    }
                };
                self.group_indices_buf.push(idx);
            }

            let values = batch.column(self.value_col);
            let values_f64 = if values.data_type() == &DataType::Float64 {
                Arc::clone(values)
            } else {
                cast(values, &DataType::Float64).context("cast sum input to f64")?
            };
            self.acc
                .update_batch(
                    &[values_f64],
                    &self.group_indices_buf,
                    None,
                    self.group_index.len(),
                )
                .context("GroupsAccumulator update_batch")?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timestamp: EventTime,
        domain: TimerDomain,
        output: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if domain != TimerDomain::EventTime || self.group_rows.is_empty() {
            return Ok(());
        }
        let converter = self.converter.as_ref().expect("groups imply converter");

        let sums = self
            .acc
            .evaluate(EmitTo::All)
            .context("GroupsAccumulator evaluate")?;
        let mut columns = converter
            .convert_rows(self.group_rows.iter().map(|r| r.row()))
            .context("rebuild key columns")?;
        columns.push(sums);

        let mut fields = self.key_fields.clone();
        fields.push(Field::new("sum", DataType::Float64, true));
        output.push(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .context("build aggregate output batch")?,
        );

        self.group_index.clear();
        self.group_rows.clear();
        self.acc = new_sum_accumulator();
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        // GroupsAccumulator only exposes state destructively; wiring its
        // state()/merge_batch into checkpoints lands with P11. Fail fast
        // instead of silently dropping state on recovery.
        bail!("DfGroupedSumOp does not support checkpointing yet; use VectorOp(HashAggregateOperator) for checkpointed pipelines")
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
        let keys = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let sums = out[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(out[0].num_rows(), 4);
        for i in 0..out[0].num_rows() {
            assert_eq!(sums.value(i), expected[&keys.value(i)]);
        }

        // Reset semantics: nothing left after the flush.
        let mut again = Vec::new();
        op.on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut again)
            .unwrap();
        assert!(again.is_empty());
    }

    #[test]
    fn df_grouped_sum_rejects_checkpoint() {
        let op = DfGroupedSumOp::new(vec![0], 1);
        assert!(op.snapshot_state().is_err());
    }
}
