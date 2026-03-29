package io.streamcrab.flink;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * End-to-end integration tests for the Rust JNI bridge.
 *
 * The Rust cdylib is loaded from target/release/libstreamcrab_flink_bridge.dylib.
 *
 * Arrow C Data Interface ownership contract:
 *   - After calling Data.exportVectorSchemaRoot the Java ArrowArray/ArrowSchema
 *     buffers are populated and their memory addresses are valid.
 *   - processElement() passes those addresses to Rust. Rust calls Box::from_raw,
 *     taking ownership and freeing the memory. The Java wrappers must NOT be
 *     closed after the JNI call (their underlying memory is gone).
 *   - processElement() / processWatermark() return a resultPtr that points to a
 *     heap-allocated ArrowResult { schema_ptr: i64, array_ptr: i64 } (repr(C)).
 *     We use sun.misc.Unsafe to read the two i64 fields at offsets 0 and 8.
 *   - ArrowArray.wrap(ptr) / ArrowSchema.wrap(ptr) wrap those raw Rust pointers
 *     so Data.importVectorSchemaRoot can decode them.
 *   - freeArrowResult(resultPtr) drops the ArrowResult box AND the embedded FFI
 *     pointers. Do NOT close the wrapped ArrowArray/ArrowSchema after that call.
 */
public class NativeOperatorIntegrationTest {

    private static sun.misc.Unsafe UNSAFE;

    @BeforeClass
    public static void loadNativeLib() throws Exception {
        // Resolve relative path: test cwd is streamcrab-flink-java/, dylib is at ../target/release/
        String libPath = System.getProperty("user.dir")
                + "/../target/release/libstreamcrab_flink_bridge.dylib";
        System.load(java.nio.file.Paths.get(libPath).toAbsolutePath().normalize().toString());

        // Obtain Unsafe for reading the ArrowResult struct fields.
        java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);

        UNSAFE = (sun.misc.Unsafe) f.get(null);
    }

    private BufferAllocator allocator;

    @Before
    public void setUp() {
        allocator = new RootAllocator();
    }

    @After
    public void tearDown() {
        allocator.close();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Read the schema_ptr from a heap-allocated ArrowResult (offset 0).
     */
    private long resultSchemaPtr(long resultPtr) {
        return UNSAFE.getLong(resultPtr);
    }

    /**
     * Read the array_ptr from a heap-allocated ArrowResult (offset 8).
     */
    private long resultArrayPtr(long resultPtr) {
        return UNSAFE.getLong(resultPtr + 8);
    }

    /**
     * Import a VectorSchemaRoot from an ArrowResult pointer.
     * Wraps the raw Rust FFI pointers with ArrowArray.wrap / ArrowSchema.wrap
     * and delegates to Data.importVectorSchemaRoot.
     *
     * NOTE: After calling this, the wrapped pointers are owned by the returned
     * VectorSchemaRoot. Call freeArrowResult(resultPtr) AFTER you are done with
     * the VectorSchemaRoot to free Rust-side memory.
     */
    private VectorSchemaRoot importResult(long resultPtr) {
        long sp = resultSchemaPtr(resultPtr);
        long ap = resultArrayPtr(resultPtr);
        ArrowSchema schema = ArrowSchema.wrap(sp);
        ArrowArray array = ArrowArray.wrap(ap);
        return Data.importVectorSchemaRoot(allocator, array, schema, new CDataDictionaryProvider());
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Test 1: Filter operator - col(0) > 3, input [1,2,3,4,5], expect output [4,5].
     */
    @Test
    public void testFilterOperatorEndToEnd() {
        // Rust config format: {"op":"gt","col":0,"val":3}
        String config = "{\"type\":\"filter\",\"predicate\":{\"op\":\"gt\",\"col\":0,\"val\":3}}";
        long handle = NativeOperator.createNative(config.getBytes(StandardCharsets.UTF_8));
        assertTrue("handle should be non-zero", handle != 0);

        try {
            Schema schema = new Schema(Arrays.asList(
                    Field.nullable("id", new ArrowType.Int(64, true))
            ));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector idVec = (BigIntVector) root.getVector("id");
                idVec.allocateNew(5);
                for (int i = 0; i < 5; i++) {
                    idVec.setSafe(i, i + 1); // values: 1,2,3,4,5
                }
                idVec.setValueCount(5);
                root.setRowCount(5);

                // Export to C Data Interface - Rust takes ownership of these.
                ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);

                long schemaPtr = arrowSchema.memoryAddress();
                long arrayPtr = arrowArray.memoryAddress();

                // Process through native filter. Rust takes ownership of schemaPtr/arrayPtr.
                long resultPtr = NativeOperator.processElement(handle, schemaPtr, arrayPtr);

                // Filter gt 3: rows with id=4,5 pass, so resultPtr must be non-zero.
                assertTrue("filter gt 3 should produce output (rows 4 and 5)", resultPtr != 0);

                try (VectorSchemaRoot result = importResult(resultPtr)) {
                    assertEquals("should have 2 rows (id=4 and id=5)", 2, result.getRowCount());
                    BigIntVector outVec = (BigIntVector) result.getVector(0);
                    assertEquals(4L, outVec.get(0));
                    assertEquals(5L, outVec.get(1));
                    System.out.println("[testFilterOperatorEndToEnd] PASS: 2 rows [4, 5]");
                }

                // Free the Rust-side ArrowResult struct. The FFI pointers inside are
                // now freed by Rust; the Java ArrowArray/ArrowSchema wrappers above
                // have already had their memory consumed by Rust.
                NativeOperator.freeArrowResult(resultPtr);
                // Rust read the FFI structs via ptr::read; the Java wrappers still own
                // their 256-byte C struct allocation and must be closed.
                arrowArray.close();
                arrowSchema.close();
            }
        } finally {
            NativeOperator.destroyNative(handle);
        }
    }

    /**
     * Test 2: Hash aggregate - group by dept (Utf8), SUM salary (Float64).
     * Flush via processWatermark. Expect eng=450, sales=250.
     */
    @Test
    public void testHashAggregateEndToEnd() {
        String config = "{\"type\":\"hash_aggregate\",\"group_by_cols\":[0],"
                + "\"aggregates\":[{\"function\":\"Sum\",\"input_col\":1,\"output_name\":\"total\"}]}";
        long handle = NativeOperator.createNative(config.getBytes(StandardCharsets.UTF_8));
        assertTrue("aggregate handle should be non-zero", handle != 0);

        try {
            Schema schema = new Schema(Arrays.asList(
                    Field.nullable("dept", new ArrowType.Utf8()),
                    Field.nullable("salary", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
            ));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                VarCharVector deptVec = (VarCharVector) root.getVector("dept");
                Float8Vector salaryVec = (Float8Vector) root.getVector("salary");

                String[] depts = {"eng", "sales", "eng", "sales", "eng"};
                double[] salaries = {100.0, 200.0, 150.0, 50.0, 200.0};

                deptVec.allocateNew();
                salaryVec.allocateNew(5);
                for (int i = 0; i < 5; i++) {
                    deptVec.setSafe(i, depts[i].getBytes(StandardCharsets.UTF_8));
                    salaryVec.setSafe(i, salaries[i]);
                }
                deptVec.setValueCount(5);
                salaryVec.setValueCount(5);
                root.setRowCount(5);

                // Feed data - HashAggregate buffers internally, processElement returns 0.
                ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);

                long feedResult = NativeOperator.processElement(
                        handle, arrowSchema.memoryAddress(), arrowArray.memoryAddress());
                arrowArray.close();
                arrowSchema.close();
                // HashAggregate buffers rows; no output until watermark flush.
                if (feedResult != 0) {
                    NativeOperator.freeArrowResult(feedResult);
                }

                // Trigger flush via watermark. HashAggregate emits all groups.
                long resultPtr = NativeOperator.processWatermark(handle, 999L);
                assertTrue("watermark should flush aggregate output", resultPtr != 0);

                try (VectorSchemaRoot result = importResult(resultPtr)) {
                    int numRows = result.getRowCount();
                    assertTrue("should have 2 groups (eng, sales)", numRows == 2);

                    // Find eng and sales rows and verify sums.
                    VarCharVector outDept = (VarCharVector) result.getVector(0);
                    Float8Vector outTotal = (Float8Vector) result.getVector(1);

                    double engTotal = 0, salesTotal = 0;
                    for (int i = 0; i < numRows; i++) {
                        String dept = new String(outDept.get(i), StandardCharsets.UTF_8);
                        double total = outTotal.get(i);
                        System.out.printf("[testHashAggregateEndToEnd] row %d: dept=%s total=%.1f%n",
                                i, dept, total);
                        if ("eng".equals(dept)) engTotal = total;
                        else if ("sales".equals(dept)) salesTotal = total;
                    }
                    assertEquals("eng total should be 450", 450.0, engTotal, 0.001);
                    assertEquals("sales total should be 250", 250.0, salesTotal, 0.001);
                    System.out.println("[testHashAggregateEndToEnd] PASS: eng=450, sales=250");
                }

                NativeOperator.freeArrowResult(resultPtr);
            }
        } finally {
            NativeOperator.destroyNative(handle);
        }
    }

    /**
     * Test 3: Checkpoint / restore round-trip.
     * Feed data to operator 1, snapshot, restore to operator 2, verify state survives.
     */
    @Test
    public void testCheckpointRestoreEndToEnd() {
        String config = "{\"type\":\"hash_aggregate\",\"group_by_cols\":[0],"
                + "\"aggregates\":[{\"function\":\"Sum\",\"input_col\":1,\"output_name\":\"total\"},"
                + "{\"function\":\"Count\",\"input_col\":1,\"output_name\":\"cnt\"}]}";

        long handle1 = NativeOperator.createNative(config.getBytes(StandardCharsets.UTF_8));
        assertTrue(handle1 != 0);

        // Feed batch 1: dept=[eng, sales], salary=[100, 200]
        Schema schema = new Schema(Arrays.asList(
                Field.nullable("dept", new ArrowType.Utf8()),
                Field.nullable("salary", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector deptVec = (VarCharVector) root.getVector("dept");
            Float8Vector salaryVec = (Float8Vector) root.getVector("salary");

            deptVec.allocateNew();
            salaryVec.allocateNew(2);
            deptVec.setSafe(0, "eng".getBytes(StandardCharsets.UTF_8));
            deptVec.setSafe(1, "sales".getBytes(StandardCharsets.UTF_8));
            salaryVec.setSafe(0, 100.0);
            salaryVec.setSafe(1, 200.0);
            deptVec.setValueCount(2);
            salaryVec.setValueCount(2);
            root.setRowCount(2);

            ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);
            long r = NativeOperator.processElement(
                    handle1, arrowSchema.memoryAddress(), arrowArray.memoryAddress());
            arrowArray.close();
            arrowSchema.close();
            if (r != 0) NativeOperator.freeArrowResult(r);
        }

        // Snapshot state from handle1.
        byte[] state = NativeOperator.snapshotState(handle1);
        assertNotNull("snapshot state must not be null", state);
        assertTrue("snapshot state must be non-empty", state.length > 0);
        System.out.printf("[testCheckpointRestoreEndToEnd] snapshot size: %d bytes%n", state.length);
        NativeOperator.destroyNative(handle1);

        // Restore to a fresh operator.
        long handle2 = NativeOperator.createNative(config.getBytes(StandardCharsets.UTF_8));
        assertTrue(handle2 != 0);
        NativeOperator.restoreState(handle2, state);

        // Flush handle2 via watermark - should emit the restored state.
        long resultPtr = NativeOperator.processWatermark(handle2, 999L);
        assertTrue("restored operator watermark should produce output", resultPtr != 0);

        try (VectorSchemaRoot result = importResult(resultPtr)) {
            int numRows = result.getRowCount();
            assertEquals("restored state should have 2 groups", 2, numRows);

            VarCharVector outDept = (VarCharVector) result.getVector(0);
            Float8Vector outTotal = (Float8Vector) result.getVector(1);

            for (int i = 0; i < numRows; i++) {
                String dept = new String(outDept.get(i), StandardCharsets.UTF_8);
                double total = outTotal.get(i);
                System.out.printf("[testCheckpointRestoreEndToEnd] row %d: dept=%s total=%.1f%n",
                        i, dept, total);
            }
            System.out.println("[testCheckpointRestoreEndToEnd] PASS: state restored successfully");
        }

        NativeOperator.freeArrowResult(resultPtr);
        NativeOperator.destroyNative(handle2);
    }

    /**
     * Test 4: Performance - filter 1M rows through JNI, measure throughput.
     */
    @Test
    public void testPerformanceEndToEnd() {
        String config = "{\"type\":\"filter\",\"predicate\":{\"op\":\"gte\",\"col\":0,\"val\":500000}}";
        long handle = NativeOperator.createNative(config.getBytes(StandardCharsets.UTF_8));
        assertTrue(handle != 0);

        try {
            int numRows = 1_000_000;
            Schema schema = new Schema(Arrays.asList(
                    Field.nullable("id", new ArrowType.Int(64, true))
            ));

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector idVec = (BigIntVector) root.getVector("id");
                idVec.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                    idVec.setSafe(i, i);
                }
                idVec.setValueCount(numRows);
                root.setRowCount(numRows);

                ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);

                long start = System.nanoTime();
                long resultPtr = NativeOperator.processElement(
                        handle, arrowSchema.memoryAddress(), arrowArray.memoryAddress());
                long elapsed = System.nanoTime() - start;
                arrowArray.close();
                arrowSchema.close();

                double mRowsPerSec = (numRows / 1_000_000.0) / (elapsed / 1_000_000_000.0);
                System.out.printf("[testPerformanceEndToEnd] JNI Filter E2E (1M rows): "
                        + "%.1f M rows/sec (%.2f ms total)%n", mRowsPerSec, elapsed / 1_000_000.0);

                // gte 500000: rows 500000..999999 pass = 500000 rows
                assertTrue("filter gte 500000 should produce output", resultPtr != 0);

                try (VectorSchemaRoot result = importResult(resultPtr)) {
                    assertEquals("should have 500000 output rows", 500_000, result.getRowCount());
                    System.out.println("[testPerformanceEndToEnd] PASS: 500000 rows output");
                }

                NativeOperator.freeArrowResult(resultPtr);
            }
        } finally {
            NativeOperator.destroyNative(handle);
        }
    }
}
