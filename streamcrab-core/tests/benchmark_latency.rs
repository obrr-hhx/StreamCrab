//! Latency benchmarks: measure per-record P50/P99/P999 through crossbeam pipelines.
//! Run: cargo test -p streamcrab-core --test benchmark_latency --release -- --nocapture

use crossbeam_channel::{Receiver, Sender, bounded};
use std::time::{Duration, Instant};
use streamcrab_core::runtime::operator_chain::Operator;
use streamcrab_core::runtime::process::{ReduceFunction, ReduceOperator};
use streamcrab_core::state::HashMapStateBackend;
use streamcrab_core::types::{StreamElement, StreamRecord};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Shared reducer
// ---------------------------------------------------------------------------

struct SumReducer;

impl ReduceFunction<(i64, i64)> for SumReducer {
    fn reduce(&mut self, a: (i64, i64), b: (i64, i64)) -> anyhow::Result<(i64, i64)> {
        Ok((a.0, a.1 + b.1))
    }
}

// ---------------------------------------------------------------------------
// Test 1: keyed reduce latency
// ---------------------------------------------------------------------------

/// Measures end-to-end per-record latency through a keyed reduce pipeline.
///
/// Pipeline: source thread → reduce thread → collector thread.
/// Each record carries its creation `Instant`; the collector computes
/// `Instant::now() - created_at` after the record exits the reduce operator.
///
/// Configuration: 1 M records, 10 keys, first 10% discarded as warm-up.
#[test]
fn bench_latency_keyed_reduce() {
    const N: usize = 1_000_000;
    const WARMUP: usize = N / 10;

    // (key, value, created_at_nanos_since_epoch_approx) — we ship the Instant
    // as a raw pointer inside the record to avoid Clone restrictions.
    // Simpler: use a separate latency channel.
    //
    // We use a side channel: source pushes Instants, collector reads them in
    // lock-step with results.

    type KV = (i64, (i64, i64));
    let (src_tx, src_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    let (red_tx, red_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    // Side channel: timestamp per record in same order
    let (ts_tx, ts_rx): (Sender<Instant>, Receiver<Instant>) = bounded(1024);

    // Source thread
    let src_handle = std::thread::spawn(move || {
        for i in 0..N as i64 {
            let key = i % 10;
            let ts = Instant::now();
            src_tx
                .send(StreamElement::Record(StreamRecord::new((
                    key,
                    (key, 1000 + (i % 500)),
                ))))
                .unwrap();
            ts_tx.send(ts).unwrap();
        }
        src_tx.send(StreamElement::End).unwrap();
    });

    // Reduce thread
    let red_handle = std::thread::spawn(move || {
        let mut operator = ReduceOperator::new(SumReducer, HashMapStateBackend::new());
        let mut input_buf: Vec<KV> = Vec::with_capacity(1);
        let mut output_buf: Vec<KV> = Vec::with_capacity(1);
        loop {
            match src_rx.recv().unwrap() {
                StreamElement::Record(rec) => {
                    input_buf.clear();
                    input_buf.push(rec.value);
                    output_buf.clear();
                    operator.process_batch(&input_buf, &mut output_buf).unwrap();
                    for result in output_buf.drain(..) {
                        red_tx
                            .send(StreamElement::Record(StreamRecord::new(result)))
                            .unwrap();
                    }
                }
                StreamElement::End => {
                    red_tx.send(StreamElement::End).unwrap();
                    break;
                }
                _ => {}
            }
        }
    });

    // Collector thread
    let col_handle = std::thread::spawn(move || {
        let mut latencies_ns: Vec<u64> = Vec::with_capacity(N - WARMUP);
        let mut idx = 0usize;
        loop {
            match red_rx.recv().unwrap() {
                StreamElement::Record(_) => {
                    let ts = ts_rx.recv().unwrap();
                    let lat = ts.elapsed().as_nanos() as u64;
                    if idx >= WARMUP {
                        latencies_ns.push(lat);
                    }
                    idx += 1;
                }
                StreamElement::End => break,
                _ => {}
            }
        }
        latencies_ns
    });

    src_handle.join().unwrap();
    red_handle.join().unwrap();
    let mut latencies_ns = col_handle.join().unwrap();
    latencies_ns.sort_unstable();

    let to_us = |ns: u64| ns / 1_000;
    let p50 = to_us(percentile(&latencies_ns, 50.0));
    let p99 = to_us(percentile(&latencies_ns, 99.0));
    let p999 = to_us(percentile(&latencies_ns, 99.9));
    let max = to_us(*latencies_ns.last().unwrap_or(&0));
    let samples = latencies_ns.len();

    println!(
        "LATENCY|keyed_reduce|p50_us={}|p99_us={}|p999_us={}|max_us={}|samples={}",
        p50, p99, p999, max, samples
    );
}

// ---------------------------------------------------------------------------
// Test 2: filter → map latency (no state)
// ---------------------------------------------------------------------------

/// Measures per-record latency for a stateless filter→map pipeline.
///
/// Filter: keep records where value % 2 == 0.
/// Map: multiply value by 2.
/// Same warm-up and output format as `bench_latency_keyed_reduce`.
#[test]
fn bench_latency_filter_map() {
    const N: usize = 1_000_000;
    const WARMUP: usize = N / 10;

    type KV = (i64, i64); // (key, value)
    let (src_tx, src_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    let (out_tx, out_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    let (ts_tx, ts_rx): (Sender<Instant>, Receiver<Instant>) = bounded(1024);

    // Source thread
    let src_handle = std::thread::spawn(move || {
        for i in 0..N as i64 {
            let ts = Instant::now();
            src_tx
                .send(StreamElement::Record(StreamRecord::new((i % 10, i))))
                .unwrap();
            ts_tx.send(ts).unwrap();
        }
        src_tx.send(StreamElement::End).unwrap();
    });

    // Filter → map thread (stateless)
    let worker_handle = std::thread::spawn(move || {
        loop {
            match src_rx.recv().unwrap() {
                StreamElement::Record(rec) => {
                    let (key, val) = rec.value;
                    // filter: keep even values
                    if val % 2 == 0 {
                        // map: double the value
                        out_tx
                            .send(StreamElement::Record(StreamRecord::new((key, val * 2))))
                            .unwrap();
                    } else {
                        // dropped: consume but don't forward; still drain timestamp
                        // We signal with a sentinel so collector stays in sync.
                        out_tx
                            .send(StreamElement::Record(StreamRecord::new((key, i64::MIN))))
                            .unwrap();
                    }
                }
                StreamElement::End => {
                    out_tx.send(StreamElement::End).unwrap();
                    break;
                }
                _ => {}
            }
        }
    });

    // Collector
    let col_handle = std::thread::spawn(move || {
        let mut latencies_ns: Vec<u64> = Vec::with_capacity(N - WARMUP);
        let mut idx = 0usize;
        loop {
            match out_rx.recv().unwrap() {
                StreamElement::Record(_) => {
                    let ts = ts_rx.recv().unwrap();
                    let lat = ts.elapsed().as_nanos() as u64;
                    if idx >= WARMUP {
                        latencies_ns.push(lat);
                    }
                    idx += 1;
                }
                StreamElement::End => break,
                _ => {}
            }
        }
        latencies_ns
    });

    src_handle.join().unwrap();
    worker_handle.join().unwrap();
    let mut latencies_ns = col_handle.join().unwrap();
    latencies_ns.sort_unstable();

    let to_us = |ns: u64| ns / 1_000;
    let p50 = to_us(percentile(&latencies_ns, 50.0));
    let p99 = to_us(percentile(&latencies_ns, 99.0));
    let p999 = to_us(percentile(&latencies_ns, 99.9));
    let max = to_us(*latencies_ns.last().unwrap_or(&0));
    let samples = latencies_ns.len();

    println!(
        "LATENCY|filter_map|p50_us={}|p99_us={}|p999_us={}|max_us={}|samples={}",
        p50, p99, p999, max, samples
    );
}

// ---------------------------------------------------------------------------
// Test 3: backpressure latency
// ---------------------------------------------------------------------------

/// Shows how latency degrades when the reduce thread is artificially slowed.
///
/// A 500 ns sleep per record simulates a slow downstream operator.
/// The bounded channel (capacity 1024) fills quickly, so the source thread
/// stalls, and per-record latency climbs into the microsecond range.
#[test]
fn bench_latency_backpressure() {
    // Use fewer records so the test finishes in reasonable time
    const N: usize = 100_000;
    const WARMUP: usize = N / 10;

    type KV = (i64, (i64, i64));
    let (src_tx, src_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    let (red_tx, red_rx): (Sender<StreamElement<KV>>, Receiver<StreamElement<KV>>) = bounded(1024);
    let (ts_tx, ts_rx): (Sender<Instant>, Receiver<Instant>) = bounded(1024);

    // Source thread
    let src_handle = std::thread::spawn(move || {
        for i in 0..N as i64 {
            let key = i % 10;
            let ts = Instant::now();
            src_tx
                .send(StreamElement::Record(StreamRecord::new((
                    key,
                    (key, 1000 + (i % 500)),
                ))))
                .unwrap();
            ts_tx.send(ts).unwrap();
        }
        src_tx.send(StreamElement::End).unwrap();
    });

    // Slow reduce thread: 500 ns artificial delay per record
    let red_handle = std::thread::spawn(move || {
        let mut operator = ReduceOperator::new(SumReducer, HashMapStateBackend::new());
        let mut input_buf: Vec<KV> = Vec::with_capacity(1);
        let mut output_buf: Vec<KV> = Vec::with_capacity(1);
        loop {
            match src_rx.recv().unwrap() {
                StreamElement::Record(rec) => {
                    // Simulate slow processing
                    std::thread::sleep(Duration::from_nanos(500));
                    input_buf.clear();
                    input_buf.push(rec.value);
                    output_buf.clear();
                    operator.process_batch(&input_buf, &mut output_buf).unwrap();
                    for result in output_buf.drain(..) {
                        red_tx
                            .send(StreamElement::Record(StreamRecord::new(result)))
                            .unwrap();
                    }
                }
                StreamElement::End => {
                    red_tx.send(StreamElement::End).unwrap();
                    break;
                }
                _ => {}
            }
        }
    });

    // Collector
    let col_handle = std::thread::spawn(move || {
        let mut latencies_ns: Vec<u64> = Vec::with_capacity(N - WARMUP);
        let mut idx = 0usize;
        loop {
            match red_rx.recv().unwrap() {
                StreamElement::Record(_) => {
                    let ts = ts_rx.recv().unwrap();
                    let lat = ts.elapsed().as_nanos() as u64;
                    if idx >= WARMUP {
                        latencies_ns.push(lat);
                    }
                    idx += 1;
                }
                StreamElement::End => break,
                _ => {}
            }
        }
        latencies_ns
    });

    src_handle.join().unwrap();
    red_handle.join().unwrap();
    let mut latencies_ns = col_handle.join().unwrap();
    latencies_ns.sort_unstable();

    let to_us = |ns: u64| ns / 1_000;
    let p50 = to_us(percentile(&latencies_ns, 50.0));
    let p99 = to_us(percentile(&latencies_ns, 99.0));
    let p999 = to_us(percentile(&latencies_ns, 99.9));
    let max = to_us(*latencies_ns.last().unwrap_or(&0));
    let samples = latencies_ns.len();

    println!(
        "LATENCY|backpressure|p50_us={}|p99_us={}|p999_us={}|max_us={}|samples={}",
        p50, p99, p999, max, samples
    );
}
