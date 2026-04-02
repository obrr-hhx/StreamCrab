//! Filter and Project vectorized operators.
//!
//! FilterOperator applies a predicate expression using VeloxBatch's selection
//! vector mechanism — no immediate materialization needed downstream.
//!
//! ProjectOperator selects a subset of columns and/or computes new expressions,
//! always materializing the result as a compact RecordBatch.

use crate::batch::VeloxBatch;
use crate::expression::eval::{Expr, evaluate, evaluate_bool};
use crate::operators::VectorizedOperator;
use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// ── Projection item ──────────────────────────────────────────────────────────

/// Describes one output column of a ProjectOperator.
#[derive(Debug, Clone)]
pub enum Projection {
    /// Pass through the column at the given input index unchanged.
    Column(usize),
    /// Evaluate an expression and name the resulting column.
    Computed {
        expr: Expr,
        name: String,
        data_type: DataType,
    },
}

// ── FilterOperator ───────────────────────────────────────────────────────────

/// Applies a boolean predicate to incoming batches using a selection vector.
///
/// The filter is lazy: rather than copying rows, it attaches (or ANDs into)
/// the selection vector on the materialized batch and hands it downstream.
/// Downstream operators that call `batch.materialize()` pay the copy cost once.
pub struct FilterOperator {
    predicate: Expr,
    pending_output: Option<VeloxBatch>,
    finished: bool,
}

impl FilterOperator {
    pub fn new(predicate: Expr) -> Self {
        Self {
            predicate,
            pending_output: None,
            finished: false,
        }
    }
}

impl VectorizedOperator for FilterOperator {
    fn add_input(&mut self, batch: VeloxBatch) -> Result<()> {
        // Materialize any existing selection so the predicate sees a dense batch.
        let materialized = batch
            .materialize()
            .context("FilterOperator: materialize input")?;

        // Evaluate the predicate to get a BooleanArray mask.
        let mask = evaluate_bool(&self.predicate, &materialized)
            .context("FilterOperator: evaluate predicate")?;

        // Wrap in a new VeloxBatch carrying the selection vector.
        let output = VeloxBatch::with_selection(materialized, mask);
        self.pending_output = Some(output);
        Ok(())
    }

    fn get_output(&mut self) -> Result<Option<VeloxBatch>> {
        Ok(self.pending_output.take())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

// ── ProjectOperator ──────────────────────────────────────────────────────────

/// Produces a new batch with a subset (or transformation) of the input columns.
///
/// Each `Projection` either passes a column through by index or evaluates an
/// expression and places the result as a named column. The result is always
/// materialized (selection vector from upstream is consumed here).
pub struct ProjectOperator {
    projections: Vec<Projection>,
    pending_output: Option<VeloxBatch>,
    finished: bool,
}

impl ProjectOperator {
    pub fn new(projections: Vec<Projection>) -> Self {
        Self {
            projections,
            pending_output: None,
            finished: false,
        }
    }
}

impl VectorizedOperator for ProjectOperator {
    fn add_input(&mut self, batch: VeloxBatch) -> Result<()> {
        // Materialize so expressions operate on a dense, selection-free batch.
        let materialized = batch
            .materialize()
            .context("ProjectOperator: materialize input")?;

        let mut fields: Vec<Field> = Vec::with_capacity(self.projections.len());
        let mut columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(self.projections.len());

        for projection in &self.projections {
            match projection {
                Projection::Column(idx) => {
                    let col = Arc::clone(materialized.column(*idx));
                    let field = materialized.schema().field(*idx).clone();
                    fields.push(field);
                    columns.push(col);
                }
                Projection::Computed {
                    expr,
                    name,
                    data_type,
                } => {
                    let arr = evaluate(expr, &materialized)
                        .with_context(|| format!("ProjectOperator: evaluate '{name}'"))?;
                    fields.push(Field::new(name, data_type.clone(), true));
                    columns.push(arr);
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let result = RecordBatch::try_new(schema, columns)
            .context("ProjectOperator: build output RecordBatch")?;

        self.pending_output = Some(VeloxBatch::new(result));
        Ok(())
    }

    fn get_output(&mut self) -> Result<Option<VeloxBatch>> {
        Ok(self.pending_output.take())
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::eval::{and, col, gt, lit_f64, lit_i64, lt, mul};
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// schema: id:Int64, name:Utf8, value:Float64
    /// rows:   (1,"a",10.0), (2,"b",20.0), (3,"c",30.0), (4,"d",40.0), (5,"e",50.0)
    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Float64Array::from(vec![10.0f64, 20.0, 30.0, 40.0, 50.0])),
            ],
        )
        .unwrap()
    }

    // ── FilterOperator tests ─────────────────────────────────────────────────

    /// Filter id > 2 → rows 3, 4, 5 (3 rows selected).
    #[test]
    fn test_filter_gt_simple() {
        let predicate = gt(col(0), lit_i64(2));
        let mut op = FilterOperator::new(predicate);

        let input = VeloxBatch::new(sample_batch());
        op.add_input(input).unwrap();

        let output = op.get_output().unwrap().unwrap();
        assert_eq!(output.num_rows_selected(), 3);

        let materialized = output.materialize().unwrap();
        assert_eq!(materialized.num_rows(), 3);

        let ids = materialized
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[3i64, 4, 5]);
    }

    /// Filter col(0) > 2 AND col(2) < 40.0 → rows with id=3 (value=30.0).
    #[test]
    fn test_filter_complex_predicate() {
        let predicate = and(gt(col(0), lit_i64(2)), lt(col(2), lit_f64(40.0)));
        let mut op = FilterOperator::new(predicate);

        op.add_input(VeloxBatch::new(sample_batch())).unwrap();
        let output = op.get_output().unwrap().unwrap();

        let materialized = output.materialize().unwrap();
        assert_eq!(materialized.num_rows(), 1);

        let ids = materialized
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[3i64]);
    }

    /// Filter that selects no rows → materialized batch has 0 rows.
    #[test]
    fn test_filter_empty_result() {
        let predicate = gt(col(0), lit_i64(100));
        let mut op = FilterOperator::new(predicate);

        op.add_input(VeloxBatch::new(sample_batch())).unwrap();
        let output = op.get_output().unwrap().unwrap();

        assert!(output.is_empty());
        let materialized = output.materialize().unwrap();
        assert_eq!(materialized.num_rows(), 0);
    }

    // ── ProjectOperator tests ────────────────────────────────────────────────

    /// Project only id and value columns (indices 0 and 2).
    #[test]
    fn test_project_column_subset() {
        let projections = vec![Projection::Column(0), Projection::Column(2)];
        let mut op = ProjectOperator::new(projections);

        op.add_input(VeloxBatch::new(sample_batch())).unwrap();
        let output = op.get_output().unwrap().unwrap();

        let rb = output.materialize().unwrap();
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.schema().field(0).name(), "id");
        assert_eq!(rb.schema().field(1).name(), "value");
        assert_eq!(rb.num_rows(), 5);
    }

    /// Project a computed column: value * 2.0.
    #[test]
    fn test_project_computed_column() {
        let projections = vec![
            Projection::Column(0), // id
            Projection::Computed {
                expr: mul(col(2), lit_f64(2.0)),
                name: "value_doubled".into(),
                data_type: DataType::Float64,
            },
        ];
        let mut op = ProjectOperator::new(projections);

        op.add_input(VeloxBatch::new(sample_batch())).unwrap();
        let output = op.get_output().unwrap().unwrap();

        let rb = output.materialize().unwrap();
        assert_eq!(rb.num_columns(), 2);
        assert_eq!(rb.schema().field(1).name(), "value_doubled");

        let doubled = rb
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(doubled.values(), &[20.0f64, 40.0, 60.0, 80.0, 100.0]);
    }

    // ── Pipeline test ────────────────────────────────────────────────────────

    /// Chain: Project(id, value) → Filter(id > 2).
    /// Expected: 3 rows with id in {3,4,5}.
    #[test]
    fn test_pipeline_project_then_filter() {
        // Step 1: project id (col 0) and value (col 2).
        let mut project = ProjectOperator::new(vec![Projection::Column(0), Projection::Column(2)]);
        project.add_input(VeloxBatch::new(sample_batch())).unwrap();
        let projected = project.get_output().unwrap().unwrap();

        // Step 2: filter id > 2 (now at index 0 in the projected batch).
        let mut filter = FilterOperator::new(gt(col(0), lit_i64(2)));
        filter.add_input(projected).unwrap();
        let output = filter.get_output().unwrap().unwrap();

        let rb = output.materialize().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 2);

        let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[3i64, 4, 5]);
    }
}
