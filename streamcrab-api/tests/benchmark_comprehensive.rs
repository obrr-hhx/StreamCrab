//! Comprehensive benchmark suite for StreamCrab.
//!
//! Covers throughput (T1-T4), key cardinality (K1-K4), and scalability (S1-S2).
//! Run with: cargo test -p streamcrab-api --test benchmark_comprehensive --release -- --nocapture

use std::time::Instant;
use streamcrab_api::environment::StreamExecutionEnvironment;

fn print_result(name: &str, records: usize, elapsed_ms: u128, parallelism: usize) {
    let throughput = records as f64 / (elapsed_ms as f64 / 1000.0);
    println!(
        "BENCH|{}|records={}|p={}|time_ms={}|throughput={:.0}|mrec_sec={:.2}",
        name,
        records,
        parallelism,
        elapsed_ms,
        throughput,
        throughput / 1_000_000.0
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Throughput Benchmarks (T1-T6)
// ─────────────────────────────────────────────────────────────────────────────

/// T1: Filter only — 10M records, p=1
#[test]
fn bench_t1_filter_10m_p1() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .key_by(|x: &i64| *x % 10)
        .reduce(|a: i64, b: i64| a + b)
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("T1_filter_10m_p1", 10_000_000, elapsed.as_millis(), 1);
    // even numbers mod 10: 0,2,4,6,8
    assert_eq!(results.len(), 5);
}

/// T2: Map only — 10M records, p=1
#[test]
fn bench_t2_map_10m_p1() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .map(|x: &i64| (*x % 10, *x * 2))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("T2_map_10m_p1", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), 10);
}

/// T3: Filter→Map→Reduce — 10M records, p=1
#[test]
fn bench_t3_filter_map_reduce_10m_p1() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x % 10, *x % 1000))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("T3_filter_map_reduce_10m_p1", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), 5);
}

/// T3: Filter→Map→Reduce — 10M records, p=4
#[test]
fn bench_t3_filter_map_reduce_10m_p4() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x % 10, *x % 1000))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("T3_filter_map_reduce_10m_p4", 10_000_000, elapsed.as_millis(), 4);
    assert_eq!(results.len(), 5);
}

/// T4: Keyed Reduce — 10M records, p=1
/// Data formula matches Flink: key = i % 10, value = 1000 + (i % 500)
#[test]
fn bench_t4_keyed_reduce_10m_p1() {
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("T4_keyed_reduce_10m_p1", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), 10);
}

/// T4: Keyed Reduce — 10M records, p=4
#[test]
fn bench_t4_keyed_reduce_10m_p4() {
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("T4_keyed_reduce_10m_p4", 10_000_000, elapsed.as_millis(), 4);
    assert_eq!(results.len(), 10);
}

// T5 and T6: Window benchmarks
// TODO: Implement once the window API is stable (P2 milestone). Window benchmarks
// require event-time watermarks and tumbling/sliding window assigners not yet
// exposed via the DataStream DSL in streamcrab-api.

// ─────────────────────────────────────────────────────────────────────────────
// Key Cardinality Benchmarks (K1-K4)
// Pipeline: from_iter → key_by(i % N) → reduce(sum), 10M records, p=1
// ─────────────────────────────────────────────────────────────────────────────

/// K1: 10 keys
#[test]
fn bench_k1_cardinality_10_keys() {
    const NUM_KEYS: i64 = 10;
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % NUM_KEYS, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("K1_cardinality_10_keys", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), NUM_KEYS as usize);
}

/// K2: 1,000 keys
#[test]
fn bench_k2_cardinality_1k_keys() {
    const NUM_KEYS: i64 = 1_000;
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % NUM_KEYS, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("K2_cardinality_1k_keys", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), NUM_KEYS as usize);
}

/// K3: 100,000 keys
#[test]
fn bench_k3_cardinality_100k_keys() {
    const NUM_KEYS: i64 = 100_000;
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % NUM_KEYS, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("K3_cardinality_100k_keys", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), NUM_KEYS as usize);
}

/// K4: 1,000,000 keys
#[test]
fn bench_k4_cardinality_1m_keys() {
    const NUM_KEYS: i64 = 1_000_000;
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % NUM_KEYS, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("K4_cardinality_1m_keys", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), NUM_KEYS as usize);
}

// ─────────────────────────────────────────────────────────────────────────────
// Scalability Benchmarks (S1-S2)
// ─────────────────────────────────────────────────────────────────────────────

/// S1: Stateless scaling — Filter→Map→Reduce, 10M, p=1,2,4,8
#[test]
fn bench_s1_stateless_scale_p1() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x % 10, *x % 1000))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S1_stateless_10m_p1", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), 5);
}

#[test]
fn bench_s1_stateless_scale_p2() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x % 10, *x % 1000))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(2)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S1_stateless_10m_p2", 10_000_000, elapsed.as_millis(), 2);
    assert_eq!(results.len(), 5);
}

#[test]
fn bench_s1_stateless_scale_p4() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x % 10, *x % 1000))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S1_stateless_10m_p4", 10_000_000, elapsed.as_millis(), 4);
    assert_eq!(results.len(), 5);
}

#[test]
fn bench_s1_stateless_scale_p8() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x % 10, *x % 1000))
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(8)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S1_stateless_10m_p8", 10_000_000, elapsed.as_millis(), 8);
    assert_eq!(results.len(), 5);
}

/// S2: Stateful scaling — Keyed Reduce, 10M, p=1,2,4,8
/// Data formula matches Flink: key = i % 10, value = 1000 + (i % 500)
#[test]
fn bench_s2_stateful_scale_p1() {
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S2_stateful_10m_p1", 10_000_000, elapsed.as_millis(), 1);
    assert_eq!(results.len(), 10);
}

#[test]
fn bench_s2_stateful_scale_p2() {
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(2)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S2_stateful_10m_p2", 10_000_000, elapsed.as_millis(), 2);
    assert_eq!(results.len(), 10);
}

#[test]
fn bench_s2_stateful_scale_p4() {
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S2_stateful_10m_p4", 10_000_000, elapsed.as_millis(), 4);
    assert_eq!(results.len(), 10);
}

#[test]
fn bench_s2_stateful_scale_p8() {
    let data: Vec<(i64, i64)> = (1..=10_000_000_i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(8)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    print_result("S2_stateful_10m_p8", 10_000_000, elapsed.as_millis(), 8);
    assert_eq!(results.len(), 10);
}
