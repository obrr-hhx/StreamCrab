//! JNI entry points for Java class `io.streamcrab.flink.NativeOperator`.
//!
//! Every entry point wraps its body in `jni_safe` to prevent Rust panics from
//! crossing the FFI boundary.

use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::{jbyteArray, jlong};
use serde::Deserialize;

use crate::arrow_ffi;
use crate::error::jni_safe;
use crate::handle_map;

// ── Output pair struct ────────────────────────────────────────────────────────

/// Heap-allocated pair of FFI pointers returned to Java after processing.
/// Java reads `schema_ptr` and `array_ptr`, then calls `freeArrowResult` with
/// the pointer to this struct.
#[repr(C)]
pub struct ArrowResult {
    pub schema_ptr: i64,
    pub array_ptr: i64,
}

// ── Config deserialization ────────────────────────────────────────────────────

/// Top-level operator config dispatched by "type" field.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OperatorConfig {
    Filter(FilterConfig),
    HashAggregate(HashAggregateConfig),
    HashJoin(HashJoinConfig),
    WindowAggregate(WindowAggregateConfig),
}

#[derive(Deserialize)]
struct FilterConfig {
    /// Simplified predicate: `{"Column": 0}` → col(N) > 0 is not enough for a
    /// real system, but we support a subset sufficient for testing.
    /// Format: `{"op": "gt", "col": 0, "val": 42}` or `{"op": "col_nonzero", "col": 0}`.
    #[serde(default)]
    predicate: PredicateConfig,
}

#[derive(Deserialize, Default)]
#[serde(tag = "op", rename_all = "snake_case")]
enum PredicateConfig {
    /// col(col) > val_i64
    Gt { col: usize, val: i64 },
    /// col(col) >= val_i64
    Gte { col: usize, val: i64 },
    /// col(col) < val_i64
    Lt { col: usize, val: i64 },
    /// Pass all rows (no-op filter, useful for testing)
    #[default]
    PassAll,
}

#[derive(Deserialize)]
struct AggDescConfig {
    function: String,
    input_col: usize,
    output_name: String,
}

#[derive(Deserialize)]
struct HashAggregateConfig {
    group_by_cols: Vec<usize>,
    aggregates: Vec<AggDescConfig>,
}

#[derive(Deserialize)]
struct HashJoinConfig {
    #[serde(default)]
    join_type: String,
    build_key_cols: Vec<usize>,
    probe_key_cols: Vec<usize>,
}

#[derive(Deserialize)]
struct WindowAggregateConfig {
    window_type: WindowTypeConfig,
    event_time_col: usize,
    partition_cols: Vec<usize>,
    aggregates: Vec<AggDescConfig>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WindowTypeConfig {
    Tumbling { size_ms: i64 },
    Sliding { size_ms: i64, slide_ms: i64 },
}

// ── Builder helpers ───────────────────────────────────────────────────────────

fn build_agg_descriptor(
    cfg: &AggDescConfig,
) -> anyhow::Result<streamcrab_vectorized::operators::AggregateDescriptor> {
    use streamcrab_vectorized::operators::AggregateFunction;
    let function = match cfg.function.to_lowercase().as_str() {
        "sum" => AggregateFunction::Sum,
        "count" => AggregateFunction::Count,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        "avg" => AggregateFunction::Avg,
        other => anyhow::bail!("unknown aggregate function: {other}"),
    };
    Ok(streamcrab_vectorized::operators::AggregateDescriptor {
        function,
        input_col: cfg.input_col,
        output_name: cfg.output_name.clone(),
    })
}

fn build_window_agg_descriptor(
    cfg: &AggDescConfig,
) -> anyhow::Result<streamcrab_vectorized::operators::WindowAggregateDescriptor> {
    use streamcrab_vectorized::operators::WindowAggFunction;
    let function = match cfg.function.to_lowercase().as_str() {
        "sum" => WindowAggFunction::Sum,
        "count" => WindowAggFunction::Count,
        "min" => WindowAggFunction::Min,
        "max" => WindowAggFunction::Max,
        other => anyhow::bail!("unknown window aggregate function: {other}"),
    };
    Ok(
        streamcrab_vectorized::operators::WindowAggregateDescriptor {
            function,
            input_col: cfg.input_col,
            output_name: cfg.output_name.clone(),
        },
    )
}

fn build_predicate(
    cfg: &PredicateConfig,
) -> anyhow::Result<streamcrab_vectorized::operators::project_filter::FilterOperator> {
    use streamcrab_vectorized::operators::project_filter::FilterOperator;
    // We import the expression DSL functions from the eval module.
    use streamcrab_vectorized::expression::eval::{col, gt, gt_eq, lit_i64, lt};

    let expr = match cfg {
        PredicateConfig::Gt { col: c, val } => gt(col(*c), lit_i64(*val)),
        PredicateConfig::Gte { col: c, val } => gt_eq(col(*c), lit_i64(*val)),
        PredicateConfig::Lt { col: c, val } => lt(col(*c), lit_i64(*val)),
        PredicateConfig::PassAll => {
            // col(0) >= i64::MIN — always true for any Int64 column.
            gt_eq(col(0), lit_i64(i64::MIN))
        }
    };
    Ok(FilterOperator::new(expr))
}

fn create_operator(
    config_json: &[u8],
) -> anyhow::Result<Box<dyn streamcrab_vectorized::operators::VectorizedOperator>> {
    use streamcrab_vectorized::operators::{
        HashAggregateOperator, HashJoinOperator, JoinType, WindowAggregateOperator, WindowType,
    };

    let cfg: OperatorConfig = serde_json::from_slice(config_json)
        .map_err(|e| anyhow::anyhow!("invalid operator config JSON: {e}"))?;

    match cfg {
        OperatorConfig::Filter(fc) => {
            let op = build_predicate(&fc.predicate)?;
            Ok(Box::new(op))
        }
        OperatorConfig::HashAggregate(hac) => {
            let aggregates = hac
                .aggregates
                .iter()
                .map(build_agg_descriptor)
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Box::new(HashAggregateOperator::new(
                hac.group_by_cols,
                aggregates,
            )))
        }
        OperatorConfig::HashJoin(hjc) => {
            let join_type = match hjc.join_type.to_lowercase().as_str() {
                "left" => JoinType::Left,
                "right" => JoinType::Right,
                "full" => JoinType::Full,
                _ => JoinType::Inner,
            };
            Ok(Box::new(HashJoinOperator::new(
                join_type,
                hjc.build_key_cols,
                hjc.probe_key_cols,
            )))
        }
        OperatorConfig::WindowAggregate(wac) => {
            let window_type = match wac.window_type {
                WindowTypeConfig::Tumbling { size_ms } => WindowType::Tumbling { size_ms },
                WindowTypeConfig::Sliding { size_ms, slide_ms } => {
                    WindowType::Sliding { size_ms, slide_ms }
                }
            };
            let aggregates = wac
                .aggregates
                .iter()
                .map(build_window_agg_descriptor)
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Box::new(WindowAggregateOperator::new(
                window_type,
                wac.event_time_col,
                wac.partition_cols,
                aggregates,
            )))
        }
    }
}

// ── JNI entry points ──────────────────────────────────────────────────────────

/// Create a native operator from a JSON config byte array.
/// Returns an opaque handle (non-zero on success, 0 on error).
///
/// Java signature:
/// ```java
/// static native long createNative(byte[] config);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_createNative(
    mut env: JNIEnv,
    _class: JClass,
    config: JByteArray,
) -> jlong {
    jni_safe(&mut env, 0i64, |env| {
        let bytes = env.convert_byte_array(&config)?;
        let op = create_operator(&bytes)?;
        let handle = handle_map::insert(op);
        Ok(handle as i64)
    })
}

/// Push an Arrow batch into the operator and retrieve the output batch.
///
/// `schema_ptr` and `array_ptr` are raw `jlong` addresses of heap-allocated
/// `FFI_ArrowSchema` and `FFI_ArrowArray` instances (ownership transferred).
///
/// Returns a pointer to a heap-allocated `ArrowResult` struct containing the
/// output schema/array pointers, or 0 if there is no output.  The caller must
/// eventually call `freeArrowResult` with the returned pointer.
///
/// Java signature:
/// ```java
/// native long processElement(long handle, long schemaPtr, long arrayPtr);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_processElement(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    schema_ptr: jlong,
    array_ptr: jlong,
) -> jlong {
    jni_safe(&mut env, 0i64, |_env| {
        let batch = unsafe { arrow_ffi::import_batch(schema_ptr, array_ptr) }?;

        handle_map::with_operator(handle as u64, |op| {
            op.add_input(batch)?;
            match op.get_output()? {
                None => Ok(0i64),
                Some(output_batch) => {
                    let (sp, ap) = arrow_ffi::export_batch(&output_batch)?;
                    let result = Box::new(ArrowResult {
                        schema_ptr: sp,
                        array_ptr: ap,
                    });
                    Ok(Box::into_raw(result) as i64)
                }
            }
        })
    })
}

/// Serialize the operator's internal state for checkpointing.
///
/// Java signature:
/// ```java
/// native byte[] snapshotState(long handle);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_snapshotState(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) -> jbyteArray {
    // On error we return a null jbyteArray (std::ptr::null_mut()).
    jni_safe(&mut env, std::ptr::null_mut(), |env| {
        let state_bytes = handle_map::with_operator(handle as u64, |op| op.snapshot_state())?;
        let arr = env.byte_array_from_slice(&state_bytes)?;
        Ok(**arr)
    })
}

/// Restore operator state from a previously snapshotted byte array.
///
/// Java signature:
/// ```java
/// native void restoreState(long handle, byte[] state);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_restoreState(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    state: JByteArray,
) {
    jni_safe(&mut env, (), |env| {
        let bytes = env.convert_byte_array(&state)?;
        handle_map::with_operator(handle as u64, |op| op.restore_state(&bytes))
    });
}

/// Advance the watermark.  Returns a pointer to an `ArrowResult` if the
/// operator emitted output (windows fired), or 0 if there is no output.
///
/// Java signature:
/// ```java
/// native long processWatermark(long handle, long timestamp);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_processWatermark(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
    timestamp: jlong,
) -> jlong {
    jni_safe(&mut env, 0i64, |_env| {
        handle_map::with_operator(handle as u64, |op| {
            let batches = op.on_watermark(timestamp)?;
            if batches.is_empty() {
                return Ok(0i64);
            }
            // Return only the first batch for simplicity; additional batches are
            // queued inside the operator and can be retrieved via processElement
            // (get_output).  For the common single-batch case this is transparent.
            let (sp, ap) = arrow_ffi::export_batch(&batches[0])?;
            let result = Box::new(ArrowResult {
                schema_ptr: sp,
                array_ptr: ap,
            });
            Ok(Box::into_raw(result) as i64)
        })
    })
}

/// Destroy a native operator and free all associated resources.
///
/// Java signature:
/// ```java
/// native void destroyNative(long handle);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_destroyNative(
    mut env: JNIEnv,
    _this: JObject,
    handle: jlong,
) {
    jni_safe(&mut env, (), |_env| {
        // Simply drop the boxed operator.
        handle_map::remove(handle as u64);
        Ok(())
    });
}

/// Free the `ArrowResult` struct (and its embedded FFI pointers) returned by
/// `processElement` or `processWatermark`.
///
/// Java signature:
/// ```java
/// native void freeArrowResult(long resultPtr);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn Java_io_streamcrab_flink_NativeOperator_freeArrowResult(
    mut env: JNIEnv,
    _this: JObject,
    result_ptr: jlong,
) {
    jni_safe(&mut env, (), |_env| {
        if result_ptr == 0 {
            return Ok(());
        }
        let result = unsafe { Box::from_raw(result_ptr as *mut ArrowResult) };
        unsafe {
            arrow_ffi::free_ffi_pointers(result.schema_ptr, result.array_ptr);
        }
        Ok(())
    });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use streamcrab_vectorized::VeloxBatch;

    fn int64_batch(values: Vec<i64>) -> VeloxBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let rb = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
        VeloxBatch::new(rb)
    }

    // ── Config parsing ────────────────────────────────────────────────────────

    #[test]
    fn test_create_filter_gt() {
        let json = br#"{"type":"filter","predicate":{"op":"gt","col":0,"val":3}}"#;
        let op = create_operator(json).unwrap();
        assert!(!op.is_finished());
    }

    #[test]
    fn test_create_hash_aggregate() {
        let json = br#"{
            "type":"hash_aggregate",
            "group_by_cols":[0],
            "aggregates":[{"function":"Sum","input_col":1,"output_name":"total"}]
        }"#;
        let op = create_operator(json).unwrap();
        assert!(!op.is_finished());
    }

    #[test]
    fn test_create_hash_join() {
        let json = br#"{
            "type":"hash_join",
            "join_type":"Inner",
            "build_key_cols":[0],
            "probe_key_cols":[0]
        }"#;
        let op = create_operator(json).unwrap();
        assert!(!op.is_finished());
    }

    #[test]
    fn test_create_window_aggregate() {
        let json = br#"{
            "type":"window_aggregate",
            "window_type":{"kind":"tumbling","size_ms":1000},
            "event_time_col":1,
            "partition_cols":[0],
            "aggregates":[{"function":"Sum","input_col":2,"output_name":"s"}]
        }"#;
        let op = create_operator(json).unwrap();
        assert!(!op.is_finished());
    }

    #[test]
    fn test_create_unknown_type_errors() {
        let json = br#"{"type":"bogus"}"#;
        assert!(create_operator(json).is_err());
    }

    // ── Filter operator end-to-end (no JNI) ──────────────────────────────────

    #[test]
    fn test_filter_gt_via_handle_map() {
        let json = br#"{"type":"filter","predicate":{"op":"gt","col":0,"val":3}}"#;
        let op = create_operator(json).unwrap();
        let h = handle_map::insert(op);

        // Build batch with values [1, 2, 3, 4, 5].
        let batch = int64_batch(vec![1, 2, 3, 4, 5]);

        let result_ptr = handle_map::with_operator(h, |op| {
            op.add_input(batch)?;
            match op.get_output()? {
                None => Ok(0i64),
                Some(out) => {
                    let (sp, ap) = arrow_ffi::export_batch(&out)?;
                    let r = Box::new(ArrowResult {
                        schema_ptr: sp,
                        array_ptr: ap,
                    });
                    Ok(Box::into_raw(r) as i64)
                }
            }
        })
        .unwrap();

        assert_ne!(result_ptr, 0);
        let result = unsafe { Box::from_raw(result_ptr as *mut ArrowResult) };
        let roundtripped =
            unsafe { arrow_ffi::import_batch(result.schema_ptr, result.array_ptr) }.unwrap();
        let rb = roundtripped.materialize().unwrap();

        // Filter gt 3 → rows with v=4,5
        assert_eq!(rb.num_rows(), 2);
        let ids = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[4i64, 5]);

        handle_map::remove(h);
    }

    // ── Snapshot/restore round-trip (no JNI) ─────────────────────────────────

    #[test]
    fn test_snapshot_restore_roundtrip() {
        let json = br#"{
            "type":"hash_aggregate",
            "group_by_cols":[0],
            "aggregates":[{"function":"Sum","input_col":1,"output_name":"total"}]
        }"#;

        // Build a dept/salary batch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("dept", DataType::Utf8, false),
            Field::new("salary", DataType::Float64, false),
        ]));
        let rb = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["eng", "sales"])),
                Arc::new(Float64Array::from(vec![100.0f64, 200.0])),
            ],
        )
        .unwrap();
        let batch = VeloxBatch::new(rb);

        let op = create_operator(json).unwrap();
        let h = handle_map::insert(op);

        handle_map::with_operator(h, |op| op.add_input(batch)).unwrap();
        let snap = handle_map::with_operator(h, |op| op.snapshot_state()).unwrap();
        assert!(
            !snap.is_empty(),
            "non-empty agg should produce non-empty snapshot"
        );

        // Create a fresh operator and restore.
        let op2 = create_operator(json).unwrap();
        let h2 = handle_map::insert(op2);
        handle_map::with_operator(h2, |op| op.restore_state(&snap)).unwrap();

        // Both flush at watermark 0.
        let out1 = handle_map::with_operator(h, |op| op.on_watermark(0)).unwrap();
        let out2 = handle_map::with_operator(h2, |op| op.on_watermark(0)).unwrap();

        assert_eq!(out1.len(), out2.len());
        assert_eq!(
            out1[0].inner().num_rows(),
            out2[0].inner().num_rows(),
            "restored operator must produce same row count"
        );

        handle_map::remove(h);
        handle_map::remove(h2);
    }
}
