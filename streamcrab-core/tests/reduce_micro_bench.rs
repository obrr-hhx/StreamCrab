//! Micro-benchmark: isolate ReduceOperator performance from the execution framework.
//! Run: cargo test -p streamcrab-core --test reduce_micro_bench --release -- --nocapture

use std::time::Instant;
use streamcrab_core::runtime::operator_chain::Operator;
use streamcrab_core::runtime::process::{ReduceFunction, ReduceOperator};
use streamcrab_core::state::HashMapStateBackend;

struct SumReducer;

impl ReduceFunction<(i64, i64)> for SumReducer {
    fn reduce(&mut self, a: (i64, i64), b: (i64, i64)) -> anyhow::Result<(i64, i64)> {
        Ok((a.0, a.1 + b.1))
    }
}

#[test]
fn micro_reduce_operator_1m() {
    let backend = HashMapStateBackend::new();
    let mut operator = ReduceOperator::new(SumReducer, backend);

    // Prepare 1M (key, value) pairs — 10 groups
    let data: Vec<(i64, (i64, i64))> = (1..=1_000_000i64)
        .map(|i| {
            let key = i % 10;
            (key, (key, 1000 + (i % 500)))
        })
        .collect();

    let mut input_buf = Vec::with_capacity(1);
    let mut output_buf = Vec::with_capacity(1);

    let start = Instant::now();
    for (key, item) in &data {
        // Simulate what the framework does: set key, process one record
        input_buf.clear();
        input_buf.push((key.clone(), item.clone()));
        output_buf.clear();
        operator.process_batch(&input_buf, &mut output_buf).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== Micro: ReduceOperator alone (1M records) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M records/sec", throughput / 1_000_000.0);
}

#[test]
fn micro_channel_overhead_1m() {
    use crossbeam_channel::{bounded, Receiver, Sender};

    let (tx, rx): (Sender<(i64, i64)>, Receiver<(i64, i64)>) = bounded(1024);

    let sender_handle = std::thread::spawn(move || {
        for i in 1..=1_000_000i64 {
            tx.send((i % 10, 1000 + (i % 500))).unwrap();
        }
    });

    let start = Instant::now();
    let mut count = 0u64;
    loop {
        match rx.recv() {
            Ok(_) => count += 1,
            Err(_) => break,
        }
        if count >= 1_000_000 {
            break;
        }
    }
    let elapsed = start.elapsed();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();

    sender_handle.join().unwrap();

    println!("\n=== Micro: Channel pass-through (1M records) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M records/sec", throughput / 1_000_000.0);
}

#[test]
fn micro_hashmap_state_1m() {
    use streamcrab_core::state::KeyedStateBackend;

    let mut backend = HashMapStateBackend::new();

    let start = Instant::now();
    for i in 1..=1_000_000i64 {
        let key = i % 10;
        let key_bytes = bincode::serialize(&key).unwrap();
        backend.set_current_key(key_bytes);
        let prev: Option<i64> = backend.get_value("sum").unwrap();
        let new_val = prev.unwrap_or(0) + (1000 + (i % 500));
        backend.put_value("sum", new_val).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = 1_000_000.0 / elapsed.as_secs_f64();

    println!("\n=== Micro: HashMapStateBackend get+put (1M records) ===");
    println!("  Time:       {:.1} ms", elapsed.as_millis());
    println!("  Throughput:  {:.2} M records/sec", throughput / 1_000_000.0);
}
