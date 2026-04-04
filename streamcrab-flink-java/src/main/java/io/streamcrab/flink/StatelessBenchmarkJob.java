package io.streamcrab.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/**
 * Stateless benchmark: filter + map + keyed reduce.
 * Usage: flink run -c ...StatelessBenchmarkJob jar [numRecords] [parallelism]
 */
public class StatelessBenchmarkJob {

    public static void main(String[] args) throws Exception {
        int numRecords = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        int parallelism = args.length > 1 ? Integer.parseInt(args[1]) : 1;

        System.out.printf("=== Flink Stateless Benchmark: %dM records, p=%d ===%n",
                numRecords / 1_000_000, parallelism);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // filter(even) → map(x → (x%10, x*x)) → keyBy → reduce(sum)
        env.fromSequence(1, numRecords)
                .filter((FilterFunction<Long>) x -> x % 2 == 0)
                .map((MapFunction<Long, Tuple2<Long, Long>>) x -> Tuple2.of(x % 10, x * x))
                .returns(Types.TUPLE(Types.LONG, Types.LONG))
                .keyBy(t -> t.f0)
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .addSink(new DiscardingSink<>());

        env.execute(String.format("StatelessBench-%dM-p%d", numRecords / 1_000_000, parallelism));
    }
}
