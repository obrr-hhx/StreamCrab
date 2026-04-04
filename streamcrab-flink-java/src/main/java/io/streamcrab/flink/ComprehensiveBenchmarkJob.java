package io.streamcrab.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/**
 * Comprehensive benchmark job covering throughput, key cardinality, and scalability tests.
 *
 * Usage:
 *   flink run -c io.streamcrab.flink.ComprehensiveBenchmarkJob jar \
 *       <test_name> <num_records> <parallelism> [num_keys]
 *
 * Test names:
 *   t1_filter      - Filter only (even numbers) → keyBy → reduce
 *   t2_map         - Map only (x → x*2) → keyBy → reduce
 *   t3_filter_map  - Filter → Map → keyed reduce
 *   t4_keyed_reduce- Keyed reduce (sum) with configurable key cardinality
 *   k_cardinality  - Key cardinality sweep (uses numKeys param)
 *   s_stateless    - Scalability test (stateless, same as t3)
 *   s_stateful     - Scalability test (stateful, same as t4)
 *
 * Output format:
 *   BENCH|<test>|records=N|p=P|keys=K|time_ms=T|throughput=R|mrec_sec=M
 */
public class ComprehensiveBenchmarkJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ComprehensiveBenchmarkJob <test_name> <num_records> <parallelism> [num_keys]");
            System.err.println("Tests: t1_filter, t2_map, t3_filter_map, t4_keyed_reduce, k_cardinality, s_stateless, s_stateful");
            System.exit(1);
        }

        String testName = args[0];
        long numRecords = Long.parseLong(args[1]);
        int parallelism = Integer.parseInt(args[2]);
        long numKeys = args.length > 3 ? Long.parseLong(args[3]) : 10L;

        System.out.printf("=== ComprehensiveBenchmark: test=%s records=%d p=%d keys=%d ===%n",
                testName, numRecords, parallelism, numKeys);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        // No checkpointing — benchmark pure throughput

        final long finalNumKeys = numKeys;

        switch (testName) {
            case "t1_filter":
                // Filter(even) → map(x → (x%10, x)) → keyBy → reduce
                env.fromSequence(1, numRecords)
                        .filter((FilterFunction<Long>) x -> x % 2 == 0)
                        .map((MapFunction<Long, Tuple2<Long, Long>>) x -> Tuple2.of(x % 10, x))
                        .returns(Types.TUPLE(Types.LONG, Types.LONG))
                        .keyBy(t -> t.f0)
                        .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                        .addSink(new DiscardingSink<>());
                break;

            case "t2_map":
                // Map(x → (x%10, x*2)) → keyBy → reduce
                env.fromSequence(1, numRecords)
                        .map((MapFunction<Long, Tuple2<Long, Long>>) x -> Tuple2.of(x % 10, x * 2))
                        .returns(Types.TUPLE(Types.LONG, Types.LONG))
                        .keyBy(t -> t.f0)
                        .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                        .addSink(new DiscardingSink<>());
                break;

            case "t3_filter_map":
            case "s_stateless":
                // Filter(even) → map(x → (x%10, x*x)) → keyBy → reduce
                env.fromSequence(1, numRecords)
                        .filter((FilterFunction<Long>) x -> x % 2 == 0)
                        .map((MapFunction<Long, Tuple2<Long, Long>>) x -> Tuple2.of(x % 10, x * x))
                        .returns(Types.TUPLE(Types.LONG, Types.LONG))
                        .keyBy(t -> t.f0)
                        .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                        .addSink(new DiscardingSink<>());
                break;

            case "t4_keyed_reduce":
            case "k_cardinality":
            case "s_stateful":
                // map(i → (i%numKeys, 1000+(i%500))) → keyBy → reduce(sum)
                env.fromSequence(1, numRecords)
                        .map((MapFunction<Long, Tuple2<Long, Long>>) i ->
                                Tuple2.of(i % finalNumKeys, 1000L + (i % 500L)))
                        .returns(Types.TUPLE(Types.LONG, Types.LONG))
                        .keyBy(t -> t.f0)
                        .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                        .addSink(new DiscardingSink<>());
                break;

            default:
                System.err.println("Unknown test: " + testName);
                System.err.println("Valid tests: t1_filter, t2_map, t3_filter_map, t4_keyed_reduce, k_cardinality, s_stateless, s_stateful");
                System.exit(1);
                return;
        }

        org.apache.flink.api.common.JobExecutionResult result =
                env.execute(String.format("ComprehensiveBench-%s-r%d-p%d-k%d",
                        testName, numRecords, parallelism, numKeys));

        long runtime = result.getNetRuntime();
        double throughput = numRecords * 1000.0 / runtime;
        double mrecSec = numRecords / 1000.0 / runtime;

        System.out.printf("BENCH|%s|records=%d|p=%d|keys=%d|time_ms=%d|throughput=%.0f|mrec_sec=%.2f%n",
                testName, numRecords, parallelism, numKeys, runtime, throughput, mrecSec);
    }
}
