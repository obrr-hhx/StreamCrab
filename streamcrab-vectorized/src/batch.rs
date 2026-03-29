//! VeloxBatch: Arrow RecordBatch with selection vector support.
//!
//! Inspired by Velox's vector model, VeloxBatch wraps an Arrow RecordBatch
//! and adds a selection vector (boolean mask) that enables filter operations
//! without materializing a new batch. Downstream operators read through the
//! selection vector, processing only selected rows.

use arrow::array::BooleanArray;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;

/// A vectorized batch of columnar data with optional selection vector.
///
/// This is the fundamental data unit flowing through the vectorized execution
/// pipeline. It mirrors Velox's addInput/getOutput model where operators
/// push batches through without unnecessary materialization.
#[derive(Debug, Clone)]
pub struct VeloxBatch {
    /// The underlying Arrow RecordBatch (columnar data).
    inner: RecordBatch,
    /// Optional selection vector (boolean mask). When present, only rows where
    /// the mask is `true` are logically visible to downstream operators.
    selection: Option<BooleanArray>,
}

impl VeloxBatch {
    /// Create a new VeloxBatch from an Arrow RecordBatch.
    pub fn new(batch: RecordBatch) -> Self {
        Self {
            inner: batch,
            selection: None,
        }
    }

    /// Create a VeloxBatch with a pre-computed selection vector.
    pub fn with_selection(batch: RecordBatch, selection: BooleanArray) -> Self {
        debug_assert_eq!(batch.num_rows(), selection.len());
        Self {
            inner: batch,
            selection: Some(selection),
        }
    }

    /// Returns the underlying RecordBatch (ignoring selection).
    pub fn inner(&self) -> &RecordBatch {
        &self.inner
    }

    /// Returns the schema of the batch.
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// Returns the selection vector, if any.
    pub fn selection(&self) -> Option<&BooleanArray> {
        self.selection.as_ref()
    }

    /// Total number of rows in the underlying batch (before selection).
    pub fn num_rows_total(&self) -> usize {
        self.inner.num_rows()
    }

    /// Number of logically selected rows (after applying selection vector).
    pub fn num_rows_selected(&self) -> usize {
        match &self.selection {
            None => self.inner.num_rows(),
            Some(sel) => sel.true_count(),
        }
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Returns true if the batch has zero selected rows.
    pub fn is_empty(&self) -> bool {
        self.num_rows_selected() == 0
    }

    /// Materialize: apply the selection vector to produce a compact RecordBatch
    /// with only the selected rows. If no selection vector, returns the batch as-is.
    pub fn materialize(&self) -> anyhow::Result<RecordBatch> {
        match &self.selection {
            None => Ok(self.inner.clone()),
            Some(sel) => {
                let filtered = compute::filter_record_batch(&self.inner, sel)?;
                Ok(filtered)
            }
        }
    }

    /// Consume self and return the inner RecordBatch, materializing if needed.
    pub fn into_record_batch(self) -> anyhow::Result<RecordBatch> {
        match self.selection {
            None => Ok(self.inner),
            Some(ref sel) => {
                let filtered = compute::filter_record_batch(&self.inner, sel)?;
                Ok(filtered)
            }
        }
    }

    /// Apply an additional selection (AND with existing selection).
    pub fn apply_selection(&mut self, new_selection: BooleanArray) {
        debug_assert_eq!(self.inner.num_rows(), new_selection.len());
        self.selection = Some(match self.selection.take() {
            None => new_selection,
            Some(existing) => {
                // AND the two boolean arrays
                compute::and(&existing, &new_selection)
                    .expect("boolean AND should not fail on same-length arrays")
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_new_batch_no_selection() {
        let rb = sample_batch();
        let batch = VeloxBatch::new(rb.clone());
        assert_eq!(batch.num_rows_total(), 5);
        assert_eq!(batch.num_rows_selected(), 5);
        assert_eq!(batch.num_columns(), 3);
        assert!(!batch.is_empty());
        assert!(batch.selection().is_none());
    }

    #[test]
    fn test_batch_with_selection() {
        let rb = sample_batch();
        let sel = BooleanArray::from(vec![true, false, true, false, true]);
        let batch = VeloxBatch::with_selection(rb, sel);
        assert_eq!(batch.num_rows_total(), 5);
        assert_eq!(batch.num_rows_selected(), 3);
    }

    #[test]
    fn test_materialize_with_selection() {
        let rb = sample_batch();
        let sel = BooleanArray::from(vec![true, false, true, false, true]);
        let batch = VeloxBatch::with_selection(rb, sel);
        let materialized = batch.materialize().unwrap();
        assert_eq!(materialized.num_rows(), 3);

        let ids = materialized
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 3, 5]);
    }

    #[test]
    fn test_apply_selection_combines_masks() {
        let rb = sample_batch();
        let sel1 = BooleanArray::from(vec![true, true, true, false, false]);
        let mut batch = VeloxBatch::with_selection(rb, sel1);

        let sel2 = BooleanArray::from(vec![true, false, true, true, true]);
        batch.apply_selection(sel2);

        // AND of [T,T,T,F,F] and [T,F,T,T,T] = [T,F,T,F,F]
        assert_eq!(batch.num_rows_selected(), 2);
        let materialized = batch.materialize().unwrap();
        let ids = materialized
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 3]);
    }

    #[test]
    fn test_empty_batch() {
        let rb = sample_batch();
        let sel = BooleanArray::from(vec![false, false, false, false, false]);
        let batch = VeloxBatch::with_selection(rb, sel);
        assert!(batch.is_empty());
        assert_eq!(batch.num_rows_selected(), 0);
    }

    #[test]
    fn test_into_record_batch_no_selection() {
        let rb = sample_batch();
        let batch = VeloxBatch::new(rb.clone());
        let result = batch.into_record_batch().unwrap();
        assert_eq!(result.num_rows(), 5);
    }
}
