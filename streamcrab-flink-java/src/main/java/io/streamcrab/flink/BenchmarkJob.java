package io.streamcrab.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.types.Row;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Benchmark comparing Native Rust HashAggregate vs Pure Java aggregate.
 *
 * Usage:
 *   flink run benchmark.jar [native|java] [numRecords]
 *
 * Default: native mode, 1M records
 */
public class BenchmarkJob {

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "native";
        int numRecords = args.length > 1 ? Integer.parseInt(args[1]) : 1_000_000;

        System.out.printf("=== Benchmark: %s mode, %d records ===%n", mode, numRecords);

        int parallelism = args.length > 2 ? Integer.parseInt(args[2]) : 1;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Generate source: (department_id: i64, salary: i64)
        // 10 departments, random salaries
        DataStream<Row> source = env.fromSequence(1, numRecords)
                .map(new RichMapFunction<Long, Row>() {
                    @Override
                    public Row map(Long i) {
                        long deptId = i % 10;       // 10 groups
                        long salary = 1000 + (i % 500);  // deterministic "salary"
                        return Row.of(deptId, salary);
                    }
                })
                .returns(Types.ROW(Types.LONG, Types.LONG));

        long startTime = System.nanoTime();

        if ("native".equals(mode)) {
            // Native Rust HashAggregate
            JsonObject config = new JsonObject();
            config.addProperty("type", "hash_aggregate");

            JsonArray groupBy = new JsonArray();
            groupBy.add(0);
            config.add("group_by_cols", groupBy);

            JsonArray aggs = new JsonArray();

            JsonObject sumAgg = new JsonObject();
            sumAgg.addProperty("function", "sum");
            sumAgg.addProperty("input_col", 1);
            sumAgg.addProperty("output_name", "total_salary");
            aggs.add(sumAgg);

            JsonObject countAgg = new JsonObject();
            countAgg.addProperty("function", "count");
            countAgg.addProperty("input_col", 1);
            countAgg.addProperty("output_name", "emp_count");
            aggs.add(countAgg);

            config.add("aggregates", aggs);

            byte[] configBytes = config.toString().getBytes();

            // Output schema: (group_key: LONG, total_salary: DOUBLE, emp_count: DOUBLE)
            // Rust HashAggregate outputs all aggregates as f64
            // Use default batch size (256) for mini-batch processing
            source.transform(
                    "NativeHashAggregate",
                    Types.ROW(Types.LONG, Types.DOUBLE, Types.DOUBLE),
                    new NativeOneInputStreamOperator(configBytes)
            ).addSink(new DiscardingSink<>());

        } else {
            // Pure Java: group by dept, sum salary, count
            source.keyBy(row -> (Long) row.getField(0))
                    .reduce((r1, r2) -> {
                        long salary1 = (Long) r1.getField(1);
                        long salary2 = (Long) r2.getField(1);
                        return Row.of(r1.getField(0), salary1 + salary2);
                    })
                    .addSink(new DiscardingSink<>());
        }

        env.execute(String.format("Benchmark-%s-%dM", mode, numRecords / 1_000_000));
    }
}
