//! Profile: simulate the exact reduce pipeline to find the bottleneck.
//! Run: cargo test -p streamcrab-core --test reduce_pipeline_profile --release -- --nocapture

use std::time::Instant;
use crossbeam_channel::{bounded, Sender, Receiver};
use streamcrab_core::runtime::operator_chain::Operator;
use streamcrab_core::runtime::process::{ReduceFunction, ReduceOperator};
use streamcrab_core::state::HashMapStateBackend;
use streamcrab_core::types::StreamElement;
// Partitioner unused in this test

struct SumReducer;

impl ReduceFunction<(i64, i64)> for SumReducer {
    fn reduce(&mut self, a: (i64, i64), b: (i64, i64)) -> anyhow::Result<(i64, i64)> {
        Ok((a.0, a.1 + b.1))
    }
}

#[test]
fn profile_full_pipeline_1m() {
    let n = 1_000_000usize;
    let parallelism = 1;

    // Generate data
    let data: Vec<(i64, i64)> = (1..=n as i64)
        .map(|i| (i % 10, 1000 + (i % 500)))
        .collect();

    // Create channels: source → reduce, reduce → collector
    // ReduceOperator<K, T> takes (K, T) and outputs (K, T)
    type KV = (i64, (i64, i64));
    let (src_tx, src_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    let (red_tx, red_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);

    let total_start = Instant::now();

    // Source thread: send (key, (key, salary)) tuples
    let src_handle = std::thread::spawn(move || {
        let start = Instant::now();
        for item in data {
            let key = item.0;
            src_tx.send(StreamElement::Record(
                streamcrab_core::types::StreamRecord::new((key, item))
            )).unwrap();
        }
        src_tx.send(StreamElement::End).unwrap();
        println!("  Source:     {:.1} ms", start.elapsed().as_millis());
    });

    // Reduce thread
    let red_handle = std::thread::spawn(move || {
        let mut operator = ReduceOperator::new(SumReducer, HashMapStateBackend::new());
        let mut input_buf: Vec<(i64, (i64, i64))> = Vec::with_capacity(1);
        let mut output_buf: Vec<(i64, (i64, i64))> = Vec::with_capacity(1);
        let mut count = 0u64;
        let start = Instant::now();

        loop {
            match src_rx.recv().unwrap() {
                StreamElement::Record(record) => {
                    input_buf.clear();
                    input_buf.push(record.value);
                    output_buf.clear();
                    operator.process_batch(&input_buf, &mut output_buf).unwrap();
                    for result in output_buf.drain(..) {
                        red_tx.send(StreamElement::Record(
                            streamcrab_core::types::StreamRecord::new(result)
                        )).unwrap();
                    }
                    count += 1;
                }
                StreamElement::End => {
                    red_tx.send(StreamElement::End).unwrap();
                    break;
                }
                _ => {}
            }
        }
        println!("  Reduce:    {:.1} ms ({count} records)", start.elapsed().as_millis());
    });

    // Collector thread
    let col_handle = std::thread::spawn(move || {
        let mut count = 0u64;
        let start = Instant::now();
        loop {
            match red_rx.recv().unwrap() {
                StreamElement::Record(_) => count += 1,
                StreamElement::End => break,
                _ => {}
            }
        }
        println!("  Collector: {:.1} ms ({count} records)", start.elapsed().as_millis());
    });

    src_handle.join().unwrap();
    red_handle.join().unwrap();
    col_handle.join().unwrap();

    let total = total_start.elapsed();
    let throughput = n as f64 / total.as_secs_f64();

    println!("\n=== Pipeline Profile (1M records, 3 threads) ===");
    println!("  Total:      {:.1} ms", total.as_millis());
    println!("  Throughput:  {:.2} M records/sec", throughput / 1_000_000.0);
}
