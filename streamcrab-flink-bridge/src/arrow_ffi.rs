//! Arrow C Data Interface (FFI) helpers.
//!
//! Java passes Arrow batches as raw pointer pairs (schema_ptr, array_ptr) that
//! point to heap-allocated `FFI_ArrowSchema` / `FFI_ArrowArray` structs.
//! These functions import/export those pointers into Rust `VeloxBatch` values.

use anyhow::{Context, Result};
use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow::record_batch::RecordBatch;
use streamcrab_vectorized::VeloxBatch;

// ── Import ────────────────────────────────────────────────────────────────────

/// Import a `VeloxBatch` from FFI pointer pair produced by the Java side.
///
/// # Safety
/// `schema_ptr` and `array_ptr` must be valid, non-null pointers to
/// `FFI_ArrowSchema` and `FFI_ArrowArray` instances that were populated by
/// Arrow Java's `Data.exportVectorSchemaRoot`.
///
/// This function reads the FFI structs in place (via `std::ptr::read`) so that
/// Arrow's `from_ffi` can invoke the C release callbacks.  The original memory
/// at the pointer addresses is left in an indeterminate state — the Java side
/// should close its `ArrowArray`/`ArrowSchema` wrappers afterwards (the close
/// is a no-op once the release callback has fired).
pub unsafe fn import_batch(schema_ptr: i64, array_ptr: i64) -> Result<VeloxBatch> {
    let array_data = unsafe {
        // Read (bitwise copy) the structs out of Java-owned memory.  `from_ffi`
        // will invoke the release callback on the *copied* struct, which signals
        // Arrow Java to free its buffers.  We do NOT use `Box::from_raw` because
        // the memory was allocated by Java's Arrow allocator, not by Rust's
        // global allocator.
        let ffi_schema = std::ptr::read(schema_ptr as *const FFI_ArrowSchema);
        let ffi_array = std::ptr::read(array_ptr as *const FFI_ArrowArray);
        from_ffi(ffi_array, &ffi_schema).context("arrow_ffi: from_ffi")?
    };

    let struct_array = StructArray::from(array_data);
    let batch = RecordBatch::from(struct_array);
    Ok(VeloxBatch::new(batch))
}

// ── Export ────────────────────────────────────────────────────────────────────

/// Export a `VeloxBatch` to a FFI pointer pair that Java can import.
///
/// The returned pointers are heap-allocated and owned by the caller.
/// The Java side must eventually call `freeArrowResult` so that Rust can
/// reclaim the memory via `free_ffi_pointers`.
pub fn export_batch(batch: &VeloxBatch) -> Result<(i64, i64)> {
    let rb = batch.materialize().context("arrow_ffi: materialize")?;

    // Convert RecordBatch → StructArray → ArrayData, then export.
    let struct_array = StructArray::from(rb);
    let (ffi_array, ffi_schema) =
        arrow::ffi::to_ffi(&struct_array.into_data()).context("arrow_ffi: to_ffi")?;

    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    Ok((schema_ptr, array_ptr))
}

// ── Free ──────────────────────────────────────────────────────────────────────

/// Free FFI pointer pair previously returned by `export_batch`.
///
/// # Safety
/// Both pointers must have been produced by `export_batch` and must not have
/// been freed already.  Passing 0 for either pointer is safe and a no-op.
pub unsafe fn free_ffi_pointers(schema_ptr: i64, array_ptr: i64) {
    if schema_ptr != 0 {
        unsafe { drop(Box::from_raw(schema_ptr as *mut FFI_ArrowSchema)) };
    }
    if array_ptr != 0 {
        unsafe { drop(Box::from_raw(array_ptr as *mut FFI_ArrowArray)) };
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Float64Array::from(vec![10.0f64, 20.0, 30.0])),
            ],
        )
        .unwrap()
    }

    /// Round-trip: export a batch, then import the same pointer pair back.
    #[test]
    fn test_export_import_roundtrip() {
        let original = VeloxBatch::new(sample_batch());

        // Export.
        let (schema_ptr, array_ptr) = export_batch(&original).unwrap();
        assert_ne!(schema_ptr, 0);
        assert_ne!(array_ptr, 0);

        // Import — this consumes the pointers.
        let roundtripped = unsafe { import_batch(schema_ptr, array_ptr) }.unwrap();

        let rb = roundtripped.materialize().unwrap();
        assert_eq!(rb.num_rows(), 3);
        assert_eq!(rb.num_columns(), 2);

        let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1i64, 2, 3]);

        let vals = rb
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(vals.values(), &[10.0f64, 20.0, 30.0]);
    }

    /// Exporting a batch with a selection vector materializes correctly.
    #[test]
    fn test_export_with_selection() {
        use arrow::array::BooleanArray;

        let rb = sample_batch();
        let sel = BooleanArray::from(vec![true, false, true]);
        let batch = VeloxBatch::with_selection(rb, sel);

        let (schema_ptr, array_ptr) = export_batch(&batch).unwrap();
        let roundtripped = unsafe { import_batch(schema_ptr, array_ptr) }.unwrap();

        let rb = roundtripped.materialize().unwrap();
        assert_eq!(rb.num_rows(), 2, "selection should be applied on export");

        let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1i64, 3]);
    }

    /// `free_ffi_pointers` with zero pointers must not crash.
    #[test]
    fn test_free_null_pointers_is_safe() {
        unsafe { free_ffi_pointers(0, 0) };
    }
}
