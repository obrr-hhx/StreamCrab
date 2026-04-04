#!/bin/bash
# StreamCrab vs Flink Comprehensive Benchmark Suite
# Usage: ./benchmarks/run_all.sh [iterations]

set -euo pipefail

ITERATIONS=${1:-3}
RESULTS_DIR="benchmarks/results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

FLINK_HOME="/tmp/flink-1.20.0"
JAVA_HOME="$(/usr/libexec/java_home -v 17)"
export JAVA_HOME
JAR="streamcrab-flink-java/target/streamcrab-flink-java-0.1.0.jar"

echo "=== StreamCrab vs Flink Benchmark Suite ==="
echo "Iterations: $ITERATIONS"
echo "Results: $RESULTS_DIR"
echo ""

# Build
echo "--- Building ---"
cargo build --release -p streamcrab-api 2>&1 | tail -1
(cd streamcrab-flink-java && mvn package -DskipTests -q)

# Phase 1: StreamCrab benchmarks
echo ""
echo "--- StreamCrab Throughput ---"
for i in $(seq 1 $ITERATIONS); do
    echo "  Iteration $i/$ITERATIONS"
    cargo test -p streamcrab-api --test benchmark_comprehensive --release -- --nocapture 2>&1 \
        | grep "^BENCH|" >> "$RESULTS_DIR/streamcrab_throughput.txt"
done

echo "--- StreamCrab Latency ---"
for i in $(seq 1 $ITERATIONS); do
    cargo test -p streamcrab-core --test benchmark_latency --release -- --nocapture 2>&1 \
        | grep "^LATENCY|" >> "$RESULTS_DIR/streamcrab_latency.txt"
done

echo "--- StreamCrab Memory ---"
cargo test -p streamcrab-core --test benchmark_memory --release -- --nocapture 2>&1 \
    | grep "^MEMORY|" >> "$RESULTS_DIR/streamcrab_memory.txt"

# Phase 2: Flink benchmarks
echo ""
echo "--- Starting Flink Cluster ---"
# Configure 8 task slots
sed -i '' 's/numberOfTaskSlots: [0-9]*/numberOfTaskSlots: 8/' "$FLINK_HOME/conf/config.yaml"
$FLINK_HOME/bin/start-cluster.sh
sleep 8

echo "--- Flink Throughput ---"
for test in t1_filter t2_map t3_filter_map t4_keyed_reduce; do
    for p in 1 4; do
        for i in $(seq 1 $ITERATIONS); do
            echo "  $test p=$p iter=$i"
            $FLINK_HOME/bin/flink run -c io.streamcrab.flink.ComprehensiveBenchmarkJob \
                "$JAR" "$test" 10000000 "$p" 10 2>&1 | grep "^BENCH|" >> "$RESULTS_DIR/flink_throughput.txt"
            # Restart between runs for clean state
            $FLINK_HOME/bin/stop-cluster.sh 2>/dev/null; sleep 2; $FLINK_HOME/bin/start-cluster.sh; sleep 5
        done
    done
done

# Key cardinality
for keys in 10 1000 100000 1000000; do
    for i in $(seq 1 $ITERATIONS); do
        echo "  k_cardinality keys=$keys iter=$i"
        $FLINK_HOME/bin/flink run -c io.streamcrab.flink.ComprehensiveBenchmarkJob \
            "$JAR" k_cardinality 10000000 1 "$keys" 2>&1 | grep "^BENCH|" >> "$RESULTS_DIR/flink_throughput.txt"
        $FLINK_HOME/bin/stop-cluster.sh 2>/dev/null; sleep 2; $FLINK_HOME/bin/start-cluster.sh; sleep 5
    done
done

# Scalability
for p in 1 2 4 8; do
    for i in $(seq 1 $ITERATIONS); do
        echo "  s_stateful p=$p iter=$i"
        $FLINK_HOME/bin/flink run -c io.streamcrab.flink.ComprehensiveBenchmarkJob \
            "$JAR" s_stateful 10000000 "$p" 10 2>&1 | grep "^BENCH|" >> "$RESULTS_DIR/flink_throughput.txt"
        $FLINK_HOME/bin/stop-cluster.sh 2>/dev/null; sleep 2; $FLINK_HOME/bin/start-cluster.sh; sleep 5
    done
done

echo "--- Stopping Flink ---"
$FLINK_HOME/bin/stop-cluster.sh 2>/dev/null

echo ""
echo "=== Results saved to $RESULTS_DIR ==="
echo "Run: python3 benchmarks/compare.py $RESULTS_DIR"
