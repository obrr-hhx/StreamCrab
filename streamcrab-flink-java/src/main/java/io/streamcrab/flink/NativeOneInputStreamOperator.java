package io.streamcrab.flink;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink streaming operator that delegates processing to a native Rust
 * vectorized operator via JNI.
 *
 * <p>Implements mini-batch accumulation: collects N records before
 * sending a batch to native code, amortizing JNI overhead.</p>
 *
 * <p>Supports exactly-once via checkpoint/restore of native state.</p>
 */
public class NativeOneInputStreamOperator
        extends AbstractStreamOperator<Row>
        implements OneInputStreamOperator<Row, Row>, CheckpointedFunction {

    private static final int DEFAULT_BATCH_SIZE = 256;

    private final byte[] operatorConfig;
    private final int batchSize;

    private transient long nativeHandle;
    private transient List<Row> pendingRows;
    private transient ArrowBatchConverter converter;

    // Checkpoint state
    private transient ListState<byte[]> nativeStateStore;
    private byte[] pendingRestoreState;

    public NativeOneInputStreamOperator(byte[] operatorConfig) {
        this(operatorConfig, DEFAULT_BATCH_SIZE);
    }

    public NativeOneInputStreamOperator(byte[] operatorConfig, int batchSize) {
        this.operatorConfig = operatorConfig;
        this.batchSize = batchSize;
    }

    @Override
    public void open() throws Exception {
        super.open();
        NativeLibLoader.ensureLoaded();
        nativeHandle = NativeOperator.createNative(operatorConfig);
        if (nativeHandle == 0) {
            throw new RuntimeException("Failed to create native operator");
        }
        pendingRows = new ArrayList<>(batchSize);
        converter = new ArrowBatchConverter();

        // Restore state if recovering from checkpoint
        if (pendingRestoreState != null) {
            NativeOperator.restoreState(nativeHandle, pendingRestoreState);
            pendingRestoreState = null;
        }
    }

    @Override
    public void processElement(StreamRecord<Row> element) throws Exception {
        pendingRows.add(element.getValue());

        if (pendingRows.size() >= batchSize) {
            flushBatch();
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // Flush any pending rows first
        if (!pendingRows.isEmpty()) {
            flushBatch();
        }

        long resultPtr = NativeOperator.processWatermark(nativeHandle, mark.getTimestamp());
        if (resultPtr != 0) {
            try {
                emitArrowResult(resultPtr);
            } finally {
                NativeOperator.freeArrowResult(resultPtr);
            }
        }

        super.processWatermark(mark);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Flush pending rows before snapshot
        if (!pendingRows.isEmpty()) {
            flushBatch();
        }

        byte[] state = NativeOperator.snapshotState(nativeHandle);
        nativeStateStore.clear();
        nativeStateStore.add(state);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nativeStateStore = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("native-state", BytePrimitiveArraySerializer.INSTANCE));

        if (context.isRestored()) {
            for (byte[] state : nativeStateStore.get()) {
                pendingRestoreState = state;
                break; // Only one state entry
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (nativeHandle != 0) {
            NativeOperator.destroyNative(nativeHandle);
            nativeHandle = 0;
        }
        if (converter != null) {
            converter.close();
        }
        super.close();
    }

    private void flushBatch() throws Exception {
        if (pendingRows.isEmpty()) return;

        // Convert rows to Arrow and get FFI pointers
        long[] ffiPtrs = converter.rowsToArrowFfi(pendingRows);
        long schemaPtr = ffiPtrs[0];
        long arrayPtr = ffiPtrs[1];

        try {
            long resultPtr = NativeOperator.processElement(nativeHandle, schemaPtr, arrayPtr);
            if (resultPtr != 0) {
                try {
                    emitArrowResult(resultPtr);
                } finally {
                    NativeOperator.freeArrowResult(resultPtr);
                }
            }
        } finally {
            pendingRows.clear();
        }
    }

    private void emitArrowResult(long resultPtr) throws Exception {
        // The ArrowResult struct layout: { i64 schema_ptr, i64 array_ptr }
        long[] ptrs = converter.readArrowResultStruct(resultPtr);
        long schemaPtr = ptrs[0];
        long arrayPtr = ptrs[1];

        if (schemaPtr != 0 && arrayPtr != 0) {
            List<Row> rows = converter.arrowFfiToRows(schemaPtr, arrayPtr);
            for (Row row : rows) {
                output.collect(new StreamRecord<>(row));
            }
        }
    }
}
