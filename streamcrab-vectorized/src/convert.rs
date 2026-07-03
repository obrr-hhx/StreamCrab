//! Row ↔ Arrow conversion for bridging StreamCrab's row-oriented DataStream
//! runtime into the vectorized engine (P9 boundary operators).
//!
//! `Batcher` buffers rows and calls [`ArrowConvertible::to_batch`];
//! `Unbatcher` calls [`ArrowConvertible::from_batch`] to emit rows downstream.
//! Sources that natively produce Arrow (e.g. Paimon) skip this entirely.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

/// Row types that can cross the row↔column boundary.
///
/// v1 covers scalars and small tuples; rows are non-nullable (a null column
/// in an incoming batch is a conversion error, not a `None` row).
pub trait ArrowConvertible: Sized {
    /// Arrow schema describing one row of this type.
    fn schema() -> SchemaRef;
    /// Convert a slice of rows into a single RecordBatch.
    fn to_batch(rows: &[Self]) -> Result<RecordBatch>;
    /// Convert a RecordBatch back into rows.
    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>>;
}

fn column_as<'a, T: 'static>(batch: &'a RecordBatch, idx: usize, expected: &str) -> Result<&'a T> {
    let col = batch.columns().get(idx).with_context(|| {
        format!(
            "batch has {} columns, expected at least {}",
            batch.num_columns(),
            idx + 1
        )
    })?;
    col.as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("column {idx} is not {expected}"))
}

fn ensure_no_nulls(col: &dyn Array, idx: usize) -> Result<()> {
    if col.null_count() > 0 {
        bail!(
            "column {idx} contains {} null(s); rows are non-nullable",
            col.null_count()
        );
    }
    Ok(())
}

impl ArrowConvertible for i64 {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn to_batch(rows: &[Self]) -> Result<RecordBatch> {
        let array = Int64Array::from(rows.to_vec());
        RecordBatch::try_new(Self::schema(), vec![Arc::new(array)])
            .context("build i64 batch failed")
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let col = column_as::<Int64Array>(batch, 0, "Int64")?;
        ensure_no_nulls(col, 0)?;
        Ok(col.values().to_vec())
    }
}

impl ArrowConvertible for f64 {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]))
    }

    fn to_batch(rows: &[Self]) -> Result<RecordBatch> {
        let array = Float64Array::from(rows.to_vec());
        RecordBatch::try_new(Self::schema(), vec![Arc::new(array)])
            .context("build f64 batch failed")
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let col = column_as::<Float64Array>(batch, 0, "Float64")?;
        ensure_no_nulls(col, 0)?;
        Ok(col.values().to_vec())
    }
}

impl ArrowConvertible for (i64, i64) {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("f0", DataType::Int64, false),
            Field::new("f1", DataType::Int64, false),
        ]))
    }

    fn to_batch(rows: &[Self]) -> Result<RecordBatch> {
        let f0 = Int64Array::from_iter_values(rows.iter().map(|r| r.0));
        let f1 = Int64Array::from_iter_values(rows.iter().map(|r| r.1));
        RecordBatch::try_new(Self::schema(), vec![Arc::new(f0), Arc::new(f1)])
            .context("build (i64, i64) batch failed")
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        let f0 = column_as::<Int64Array>(batch, 0, "Int64")?;
        let f1 = column_as::<Int64Array>(batch, 1, "Int64")?;
        ensure_no_nulls(f0, 0)?;
        ensure_no_nulls(f1, 1)?;
        Ok(f0
            .values()
            .iter()
            .zip(f1.values())
            .map(|(a, b)| (*a, *b))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i64_roundtrip() {
        let rows = vec![1i64, -2, 3];
        let batch = i64::to_batch(&rows).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.schema(), <i64 as ArrowConvertible>::schema());
        assert_eq!(i64::from_batch(&batch).unwrap(), rows);
    }

    #[test]
    fn f64_roundtrip() {
        let rows = vec![1.5f64, -0.25];
        let batch = f64::to_batch(&rows).unwrap();
        assert_eq!(f64::from_batch(&batch).unwrap(), rows);
    }

    #[test]
    fn tuple_roundtrip() {
        let rows = vec![(1i64, 10i64), (2, 20), (3, 30)];
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(<(i64, i64)>::from_batch(&batch).unwrap(), rows);
    }

    #[test]
    fn empty_rows_produce_empty_batch() {
        let batch = i64::to_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(i64::from_batch(&batch).unwrap().is_empty());
    }

    #[test]
    fn null_column_rejected() {
        let array = Int64Array::from(vec![Some(1), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        assert!(i64::from_batch(&batch).is_err());
    }

    #[test]
    fn wrong_column_type_rejected() {
        let batch = f64::to_batch(&[1.0]).unwrap();
        assert!(i64::from_batch(&batch).is_err());
    }

    #[test]
    fn missing_column_rejected() {
        let batch = i64::to_batch(&[1]).unwrap();
        assert!(<(i64, i64)>::from_batch(&batch).is_err());
    }
}
