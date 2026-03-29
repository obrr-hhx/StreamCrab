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
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.types.Row;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import sun.misc.Unsafe;

/**
 * Converts between Flink Row objects and Arrow C Data Interface pointers.
 *
 * <p>Uses Arrow Java's C Data Interface to achieve zero-copy exchange
 * with the native Rust operator.</p>
 */
public class ArrowBatchConverter implements Closeable {

    private static final Unsafe UNSAFE;

    static {
        try {
            java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to obtain Unsafe", e);
        }
    }

    private final BufferAllocator allocator;
    private Schema schema;

    public ArrowBatchConverter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Set the Arrow schema to use for conversions.
     * Call this once during operator open() rather than inferring per batch.
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    /**
     * Convert a list of Flink Rows to Arrow C Data Interface pointers.
     * If schema has been set via setSchema(), uses that; otherwise infers from first row.
     *
     * @param rows the rows to convert (must be non-empty)
     * @return array of [schemaPtr, arrayPtr] for FFI
     */
    public long[] rowsToArrowFfi(List<Row> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("rows must be non-empty");
        }

        Schema effectiveSchema = this.schema != null ? this.schema : inferSchema(rows.get(0));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(effectiveSchema, allocator)) {
            root.allocateNew();
            fillVectors(root, rows);
            root.setRowCount(rows.size());

            // Allocate C structs — must NOT be closed before Rust consumes them
            ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);

            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider();
            Data.exportVectorSchemaRoot(allocator, root, dictProvider, arrowArray, arrowSchema);

            long arrayPtr = arrowArray.memoryAddress();
            long schemaPtr = arrowSchema.memoryAddress();

            // Do NOT close arrowArray / arrowSchema here — Rust side owns them after export
            return new long[]{schemaPtr, arrayPtr};
        }
    }

    /**
     * Convert Arrow C Data Interface pointers back to Flink Rows.
     *
     * @param schemaPtr pointer to FFI_ArrowSchema struct
     * @param arrayPtr  pointer to FFI_ArrowArray struct
     * @return list of Row objects
     */
    public List<Row> arrowFfiToRows(long schemaPtr, long arrayPtr) {
        ArrowSchema arrowSchema = ArrowSchema.wrap(schemaPtr);
        ArrowArray arrowArray = ArrowArray.wrap(arrayPtr);

        CDataDictionaryProvider dictProvider = new CDataDictionaryProvider();

        try (VectorSchemaRoot root =
                Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, dictProvider)) {

            int rowCount = root.getRowCount();
            List<Row> rows = new ArrayList<>(rowCount);
            List<FieldVector> fieldVectors = root.getFieldVectors();
            int fieldCount = fieldVectors.size();

            for (int r = 0; r < rowCount; r++) {
                Row row = new Row(fieldCount);
                for (int c = 0; c < fieldCount; c++) {
                    row.setField(c, readVectorValue(fieldVectors.get(c), r));
                }
                rows.add(row);
            }
            return rows;
        }
    }

    /**
     * Read schema_ptr and array_ptr from an ArrowResult struct at the given pointer.
     * The struct layout is: { i64 schema_ptr, i64 array_ptr }.
     */
    public long[] readArrowResultStruct(long resultPtr) {
        long schemaPtr = UNSAFE.getLong(resultPtr);
        long arrayPtr = UNSAFE.getLong(resultPtr + 8);
        return new long[]{schemaPtr, arrayPtr};
    }

    @Override
    public void close() {
        allocator.close();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private Schema inferSchema(Row sample) {
        int arity = sample.getArity();
        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>(arity);
        for (int i = 0; i < arity; i++) {
            Object val = sample.getField(i);
            fields.add(new org.apache.arrow.vector.types.pojo.Field(
                    "f" + i, FieldType.nullable(arrowTypeFor(val)), null));
        }
        return new Schema(fields);
    }

    private ArrowType arrowTypeFor(Object val) {
        if (val instanceof Long)    return new ArrowType.Int(64, true);
        if (val instanceof Integer) return new ArrowType.Int(32, true);
        if (val instanceof Double)  return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (val instanceof Float)   return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        if (val instanceof Boolean) return ArrowType.Bool.INSTANCE;
        // Default: UTF-8 string
        return ArrowType.Utf8.INSTANCE;
    }

    private void fillVectors(VectorSchemaRoot root, List<Row> rows) {
        List<FieldVector> vectors = root.getFieldVectors();
        int fieldCount = vectors.size();

        for (int r = 0; r < rows.size(); r++) {
            Row row = rows.get(r);
            for (int c = 0; c < fieldCount; c++) {
                Object val = row.getField(c);
                setVectorValue(vectors.get(c), r, val);
            }
        }
    }

    private void setVectorValue(FieldVector vector, int index, Object val) {
        if (val == null) {
            // Mark null in the validity bitmap; each vector type handles this differently
            if (vector instanceof BigIntVector) {
                ((BigIntVector) vector).setNull(index);
            } else if (vector instanceof IntVector) {
                ((IntVector) vector).setNull(index);
            } else if (vector instanceof Float8Vector) {
                ((Float8Vector) vector).setNull(index);
            } else if (vector instanceof Float4Vector) {
                ((Float4Vector) vector).setNull(index);
            } else if (vector instanceof BitVector) {
                ((BitVector) vector).setNull(index);
            } else if (vector instanceof VarCharVector) {
                ((VarCharVector) vector).setNull(index);
            }
            return;
        }

        if (vector instanceof BigIntVector) {
            long v = (val instanceof Integer) ? ((Integer) val).longValue() : (Long) val;
            ((BigIntVector) vector).setSafe(index, v);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, (Integer) val);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, (Double) val);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(index, (Float) val);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, (Boolean) val ? 1 : 0);
        } else if (vector instanceof VarCharVector) {
            byte[] bytes = val.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((VarCharVector) vector).setSafe(index, bytes);
        }
    }

    private Object readVectorValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }
        if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(index);
        } else if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(index);
        } else if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(index);
        } else if (vector instanceof BitVector) {
            return ((BitVector) vector).get(index) != 0;
        } else if (vector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        // Fallback: return Java object representation
        return vector.getObject(index);
    }
}
