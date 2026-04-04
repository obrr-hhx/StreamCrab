//! Benchmark: StreamCrab native engine on stateful workloads.
//!
//! Compares against Flink Java results obtained from BenchmarkJob.java.
//! Run with: cargo test -p streamcrab-api --test benchmark_stateful --release -- --nocapture

use std::time::Instant;
use streamcrab_api::environment::StreamExecutionEnvironment;

/// Generate (dept_id, salary) tuples matching the Flink benchmark.
fn generate_data(n: usize) -> Vec<(i64, i64)> {
    (1..=n as i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect()
}

#[test]
fn bench_keyed_reduce_1m() {
    let data = generate_data(1_000_000);
    let env = StreamExecutionEnvironment::new("benchmark");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== StreamCrab Keyed Reduce (1M records, p=1) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.1} M records/sec", throughput / 1_000_000.0);
    println!("  Groups:     {}", results.len());

    // Verify correctness: 10 groups
    assert_eq!(results.len(), 10);
}

#[test]
fn bench_keyed_reduce_1m_p4() {
    let data = generate_data(1_000_000);
    let env = StreamExecutionEnvironment::new("benchmark");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== StreamCrab Keyed Reduce (1M records, p=4) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.1} M records/sec", throughput / 1_000_000.0);
    println!("  Groups:     {}", results.len());

    assert_eq!(results.len(), 10);
}

#[test]
fn bench_keyed_reduce_10m() {
    let data = generate_data(10_000_000);
    let env = StreamExecutionEnvironment::new("benchmark");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 10_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== StreamCrab Keyed Reduce (10M records, p=1) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.1} M records/sec", throughput / 1_000_000.0);
    println!("  Groups:     {}", results.len());

    assert_eq!(results.len(), 10);
}

#[test]
fn bench_keyed_reduce_10m_p4() {
    let data = generate_data(10_000_000);
    let env = StreamExecutionEnvironment::new("benchmark");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 10_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== StreamCrab Keyed Reduce (10M records, p=4) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.1} M records/sec", throughput / 1_000_000.0);
    println!("  Groups:     {}", results.len());

    assert_eq!(results.len(), 10);
}

#[test]
fn bench_filter_map_reduce_10m() {
    // More complex pipeline: filter → map → key_by → reduce
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("benchmark");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0) // 5M even numbers
        .map(|x: &i64| (*x % 10, *x))  // (digit, value)
        .key_by(|pair: &(i64, i64)| pair.0)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 10_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== StreamCrab Filter→Map→Reduce (10M input, p=4) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.1} M records/sec", throughput / 1_000_000.0);
    println!("  Groups:     {}", results.len());

    // Only even numbers grouped by last digit: 0, 2, 4, 6, 8
    assert_eq!(results.len(), 5);
}
