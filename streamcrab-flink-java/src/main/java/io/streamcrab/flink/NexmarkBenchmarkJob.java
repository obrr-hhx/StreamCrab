package io.streamcrab.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import io.streamcrab.flink.NexmarkGenerator.NexmarkEvent;
import io.streamcrab.flink.NexmarkGenerator.EventType;
import io.streamcrab.flink.NexmarkGenerator.Bid;
import io.streamcrab.flink.NexmarkGenerator.Config;

import java.util.Arrays;
import java.util.List;

/**
 * Flink Nexmark benchmark job (Q1, Q2, Q5, Q8).
 *
 * Usage:
 *   flink run -c io.streamcrab.flink.NexmarkBenchmarkJob jar <query> <num_events> <parallelism>
 *
 * Structured output line:
 *   BENCH|nexmark_q<N>|records=R|p=P|time_ms=T|throughput=X
 */
public class NexmarkBenchmarkJob {

    // -----------------------------------------------------------------------
    // Discard sink (no-op, avoids I/O bottleneck)
    // -----------------------------------------------------------------------

    private static final SinkFunction<Object> DISCARD = new SinkFunction<Object>() {
        @Override
        public void invoke(Object value, Context context) { /* discard */ }
    };

    // -----------------------------------------------------------------------
    // Source function: generates Nexmark events in-process
    // -----------------------------------------------------------------------

    public static class NexmarkSource implements SourceFunction<NexmarkEvent> {
        private final long numEvents;
        private final long seed;
        private volatile boolean running = true;

        public NexmarkSource(long numEvents, long seed) {
            this.numEvents = numEvents;
            this.seed = seed;
        }

        @Override
        public void run(SourceContext<NexmarkEvent> ctx) {
            Config cfg = new Config();
            cfg.totalEvents = numEvents;
            cfg.seed = seed;
            NexmarkGenerator gen = new NexmarkGenerator(cfg);
            NexmarkEvent evt;
            while (running && (evt = gen.next()) != null) {
                ctx.collect(evt);
            }
        }

        @Override
        public void cancel() { running = false; }
    }

    // -----------------------------------------------------------------------
    // Q1: Currency Conversion — map bid price * 0.908
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void runQ1(StreamExecutionEnvironment env, long numEvents, int parallelism)
            throws Exception {
        long startMs = System.currentTimeMillis();

        DataStream<NexmarkEvent> source = env
                .addSource(new NexmarkSource(numEvents, 12345L))
                .setParallelism(1)
                .name("nexmark-source");

        source
                .filter((FilterFunction<NexmarkEvent>) e -> e.type == EventType.BID)
                .setParallelism(parallelism)
                .map((MapFunction<NexmarkEvent, Tuple2<Long, Double>>)
                        e -> Tuple2.of(e.bid.auction, e.bid.price * 0.908))
                .setParallelism(parallelism)
                .addSink((SinkFunction<Tuple2<Long, Double>>) (SinkFunction<?>) DISCARD)
                .setParallelism(parallelism)
                .name("discard");

        env.execute("nexmark_q1");
        long elapsed = System.currentTimeMillis() - startMs;
        printResult("nexmark_q1", numEvents, parallelism, elapsed);
    }

    // -----------------------------------------------------------------------
    // Q2: Selection — filter bids on auction IDs 1,3,5,7,9
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void runQ2(StreamExecutionEnvironment env, long numEvents, int parallelism)
            throws Exception {
        long startMs = System.currentTimeMillis();

        final List<Long> hotAuctions = Arrays.asList(1L, 3L, 5L, 7L, 9L);

        DataStream<NexmarkEvent> source = env
                .addSource(new NexmarkSource(numEvents, 12345L))
                .setParallelism(1)
                .name("nexmark-source");

        source
                .filter((FilterFunction<NexmarkEvent>) e ->
                        e.type == EventType.BID && hotAuctions.contains(e.bid.auction))
                .setParallelism(parallelism)
                .map((MapFunction<NexmarkEvent, Tuple2<Long, Long>>)
                        e -> Tuple2.of(e.bid.auction, e.bid.price))
                .setParallelism(parallelism)
                .addSink((SinkFunction<Tuple2<Long, Long>>) (SinkFunction<?>) DISCARD)
                .setParallelism(parallelism)
                .name("discard");

        env.execute("nexmark_q2");
        long elapsed = System.currentTimeMillis() - startMs;
        printResult("nexmark_q2", numEvents, parallelism, elapsed);
    }

    // -----------------------------------------------------------------------
    // Q5: Hot Items — count bids per auction via keyBy + reduce
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void runQ5(StreamExecutionEnvironment env, long numEvents, int parallelism)
            throws Exception {
        long startMs = System.currentTimeMillis();

        DataStream<NexmarkEvent> source = env
                .addSource(new NexmarkSource(numEvents, 12345L))
                .setParallelism(1)
                .name("nexmark-source");

        source
                .filter((FilterFunction<NexmarkEvent>) e -> e.type == EventType.BID)
                .setParallelism(parallelism)
                .map((MapFunction<NexmarkEvent, Tuple2<Long, Long>>)
                        e -> Tuple2.of(e.bid.auction, 1L))
                .setParallelism(parallelism)
                .keyBy(t -> t.f0)
                .reduce((ReduceFunction<Tuple2<Long, Long>>)
                        (a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .setParallelism(parallelism)
                .addSink((SinkFunction<Tuple2<Long, Long>>) (SinkFunction<?>) DISCARD)
                .setParallelism(parallelism)
                .name("discard");

        env.execute("nexmark_q5");
        long elapsed = System.currentTimeMillis() - startMs;
        printResult("nexmark_q5", numEvents, parallelism, elapsed);
    }

    // -----------------------------------------------------------------------
    // Q8: Local Item Suggestion — filter to persons + auctions
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void runQ8(StreamExecutionEnvironment env, long numEvents, int parallelism)
            throws Exception {
        long startMs = System.currentTimeMillis();

        DataStream<NexmarkEvent> source = env
                .addSource(new NexmarkSource(numEvents, 12345L))
                .setParallelism(1)
                .name("nexmark-source");

        source
                .filter((FilterFunction<NexmarkEvent>) e -> e.type != EventType.BID)
                .setParallelism(parallelism)
                .addSink((SinkFunction<NexmarkEvent>) (SinkFunction<?>) DISCARD)
                .setParallelism(parallelism)
                .name("discard");

        env.execute("nexmark_q8");
        long elapsed = System.currentTimeMillis() - startMs;
        printResult("nexmark_q8", numEvents, parallelism, elapsed);
    }

    // -----------------------------------------------------------------------
    // Output formatter
    // -----------------------------------------------------------------------

    private static void printResult(String query, long records, int parallelism, long timeMs) {
        double throughput = timeMs > 0 ? (records * 1000.0 / timeMs) : Double.POSITIVE_INFINITY;
        System.out.printf("BENCH|%s|records=%d|p=%d|time_ms=%d|throughput=%.0f%n",
                query, records, parallelism, timeMs, throughput);
    }

    // -----------------------------------------------------------------------
    // Main
    // -----------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: NexmarkBenchmarkJob <query> <num_events> <parallelism>");
            System.err.println("  query: q1 | q2 | q5 | q8 | all");
            System.exit(1);
        }

        String query      = args[0].toLowerCase();
        long   numEvents  = Long.parseLong(args[1]);
        int    parallelism = Integer.parseInt(args[2]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        // Disable operator chaining so per-operator metrics are meaningful.
        env.disableOperatorChaining();

        switch (query) {
            case "q1":  runQ1(env, numEvents, parallelism); break;
            case "q2":  runQ2(env, numEvents, parallelism); break;
            case "q5":  runQ5(env, numEvents, parallelism); break;
            case "q8":  runQ8(env, numEvents, parallelism); break;
            case "all":
                // Re-create env for each query so they don't share state.
                runQ1(StreamExecutionEnvironment.getExecutionEnvironment(), numEvents, parallelism);
                runQ2(StreamExecutionEnvironment.getExecutionEnvironment(), numEvents, parallelism);
                runQ5(StreamExecutionEnvironment.getExecutionEnvironment(), numEvents, parallelism);
                runQ8(StreamExecutionEnvironment.getExecutionEnvironment(), numEvents, parallelism);
                break;
            default:
                System.err.println("Unknown query: " + query + ". Use q1, q2, q5, q8, or all.");
                System.exit(1);
        }
    }
}
