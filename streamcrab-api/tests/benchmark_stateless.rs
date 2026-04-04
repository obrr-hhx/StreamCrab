//! Benchmark: StreamCrab stateless operators (filter, map, filter+map pipeline)
//! Run: cargo test -p streamcrab-api --test benchmark_stateless --release -- --nocapture

use std::time::Instant;
use streamcrab_api::environment::StreamExecutionEnvironment;

#[test]
fn bench_filter_1m_p1() {
    let data: Vec<i64> = (1..=1_000_000).collect();
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
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();
    println!("\n=== StreamCrab Filter→Reduce (1M, p=1) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M rec/sec", throughput / 1_000_000.0);
    println!("  Groups:     {}", results.len());
    assert_eq!(results.len(), 5); // even numbers mod 10: 0,2,4,6,8
}

#[test]
fn bench_filter_1m_p4() {
    let data: Vec<i64> = (1..=1_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .key_by(|x: &i64| *x % 10)
        .reduce(|a: i64, b: i64| a + b)
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();
    println!("\n=== StreamCrab Filter→Reduce (1M, p=4) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M rec/sec", throughput / 1_000_000.0);
    assert_eq!(results.len(), 5);
}

#[test]
fn bench_map_1m_p1() {
    let data: Vec<i64> = (1..=1_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .map(|x: &i64| (*x, *x * 2))
        .key_by(|pair: &(i64, i64)| pair.0 % 10)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();
    println!("\n=== StreamCrab Map→Reduce (1M, p=1) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M rec/sec", throughput / 1_000_000.0);
    assert_eq!(results.len(), 10);
}

#[test]
fn bench_filter_map_1m_p1() {
    let data: Vec<i64> = (1..=1_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x, *x * *x))
        .key_by(|pair: &(i64, i64)| pair.0 % 10)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();
    println!("\n=== StreamCrab Filter→Map→Reduce (1M, p=1) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M rec/sec", throughput / 1_000_000.0);
    assert_eq!(results.len(), 5);
}

#[test]
fn bench_filter_map_10m_p1() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x, *x * *x))
        .key_by(|pair: &(i64, i64)| pair.0 % 10)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(1)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 10_000_000.0 / elapsed.as_secs_f64();
    println!("\n=== StreamCrab Filter→Map→Reduce (10M, p=1) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M rec/sec", throughput / 1_000_000.0);
    assert_eq!(results.len(), 5);
}

#[test]
fn bench_filter_map_10m_p4() {
    let data: Vec<i64> = (1..=10_000_000).collect();
    let env = StreamExecutionEnvironment::new("bench");

    let start = Instant::now();
    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| (*x, *x * *x))
        .key_by(|pair: &(i64, i64)| pair.0 % 10)
        .reduce(|a: (i64, i64), b: (i64, i64)| (a.0, a.1 + b.1))
        .execute_with_parallelism(4)
        .unwrap();
    let elapsed = start.elapsed();

    let results = results.lock().unwrap();
    let throughput = 10_000_000.0 / elapsed.as_secs_f64();
    println!("\n=== StreamCrab Filter→Map→Reduce (10M, p=4) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M rec/sec", throughput / 1_000_000.0);
    assert_eq!(results.len(), 5);
}
