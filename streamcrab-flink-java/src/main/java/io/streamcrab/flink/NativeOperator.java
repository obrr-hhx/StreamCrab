package io.streamcrab.flink;

/**
 * JNI bridge to StreamCrab's Rust vectorized operators.
 * Native methods map to streamcrab-flink-bridge cdylib.
 */
public class NativeOperator {

    // --- Native methods ---

    /**
     * Create a native operator from JSON config.
     * @param config JSON bytes describing operator type and parameters
     * @return native handle (opaque pointer)
     */
    public static native long createNative(byte[] config);

    /**
     * Process an Arrow batch through the operator.
     * @param handle native operator handle
     * @param schemaPtr Arrow C Data Interface schema pointer
     * @param arrayPtr Arrow C Data Interface array pointer
     * @return pointer to ArrowResult struct (schema_ptr + array_ptr), or 0 if no output
     */
    public static native long processElement(long handle, long schemaPtr, long arrayPtr);

    /**
     * Snapshot operator state for checkpointing.
     * @return serialized state bytes
     */
    public static native byte[] snapshotState(long handle);

    /**
     * Restore operator state from checkpoint.
     */
    public static native void restoreState(long handle, byte[] state);

    /**
     * Process a watermark, potentially triggering window output.
     * @return pointer to ArrowResult, or 0 if no output
     */
    public static native long processWatermark(long handle, long timestamp);

    /**
     * Destroy the native operator and free resources.
     */
    public static native void destroyNative(long handle);

    /**
     * Free an ArrowResult returned by processElement or processWatermark.
     */
    public static native void freeArrowResult(long resultPtr);
}
