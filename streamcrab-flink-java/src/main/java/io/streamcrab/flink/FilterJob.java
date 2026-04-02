package io.streamcrab.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.google.gson.JsonObject;

/**
 * Simple Flink job that uses the native StreamCrab filter operator.
 * Generates integers 1..100, filters id > 50 via native operator.
 */
public class FilterJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        // Generate source data: rows with (id: i64)
        DataStream<Row> source = env.fromSequence(1, 100)
                .map(i -> Row.of(i))
                .returns(Types.ROW(Types.LONG));

        // Configure native filter operator: id > 50
        // Rust expects: {"type": "filter", "predicate": {"op": "gt", "col": 0, "val": 50}}
        JsonObject config = new JsonObject();
        config.addProperty("type", "filter");
        JsonObject predicate = new JsonObject();
        predicate.addProperty("op", "gt");
        predicate.addProperty("col", 0);
        predicate.addProperty("val", 50);
        config.add("predicate", predicate);
        byte[] configBytes = config.toString().getBytes();

        // Apply native operator
        DataStream<Row> filtered = source
                .transform(
                        "NativeFilter",
                        Types.ROW(Types.LONG),
                        new NativeOneInputStreamOperator(configBytes)
                );

        // Print results
        filtered.print();

        env.execute("StreamCrab Native Filter Job");
    }
}
