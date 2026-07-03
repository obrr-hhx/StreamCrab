# StreamCrab

A Rust-based stream processing engine implementing Apache Flink semantics with three core promises:

1. **Minimal** -- ~28K LOC Rust + ~400 LOC Java, 1/100th of Flink
2. **High Performance** -- Zero GC pauses, 50-100x Flink on stateless operators, 2-6x on stateful
3. **Operator-Level Elasticity** -- Single operator rescale <5s, zero state migration

Plus a lakehouse-native data plane: Apache Paimon source & exactly-once 2PC
sink, DataFusion-powered vectorized operators, and a SQL layer that compiles
queries onto them.

StreamCrab is an educational-grade implementation prioritizing code readability and architectural clarity over production features.

## Architecture

```
CONTROL PLANE     JobManager (Scheduler, Checkpoint Coordinator, Elastic Manager)
                         |  gRPC
COMPUTE PLANE     TaskManagers (Tasks, WASM/Native UDFs, Local/Tiered State)
                         |  (Tiered mode only)
STATE PLANE       State Service (Epoch-based Commit, Checkpoint to FS/S3)

VECTORIZED        Flink JVM ──JNI (Arrow zero-copy)── Rust cdylib
                  streamcrab-flink-bridge → streamcrab-vectorized
                  (Filter, Project, HashAgg, HashJoin, WindowAgg)

SQL & LAKEHOUSE   SQL ──datafusion-sql──> LogicalPlan ──> Df operators
                  Paimon table ──scan──> RecordBatch ──> vectorized pipeline
                  pipeline ──2PC PaimonSink──> Paimon snapshot commit
```

**Key design decisions:**

| Decision | Choice | Rationale |
|----------|--------|-----------|
| State mode | Dual-mode Local / Tiered | Local for speed; Tiered for elasticity |
| JM HA | Single-point (v1) | K8s restart + checkpoint recovery |
| Elastic granularity | Per-Operator | Core differentiator vs Flink/Arroyo |
| UDF isolation | WASM + Native | WASM for safety/multi-lang; Native for zero overhead |
| JNI data exchange | Arrow C Data Interface | Zero-copy, 1M rows ~0ms overhead |

## Crate Structure

| Crate | Purpose | LOC |
|-------|---------|-----|
| `streamcrab-core` | Runtime, state, checkpoint, cluster, elastic scaling | ~15K |
| `streamcrab-api` | DataStream DSL | ~1K |
| `streamcrab-cli` | CLI (jobmanager/taskmanager/submit) | ~200 |
| `streamcrab-examples` | Examples + Nexmark benchmark | ~500 |
| `streamcrab-vectorized` | Velox-inspired vectorized execution engine | ~5K |
| `streamcrab-flink-bridge` | JNI cdylib (Arrow C Data Interface) | ~1K |
| `streamcrab-flink-java` | Flink 1.20 Operator wrapper (Maven) | ~400 |
| `streamcrab-wasm` | WASM Host runtime (wasmtime) + WasmOperator | ~600 |
| `streamcrab-wasm-guest` | Guest SDK for WASM UDFs | ~200 |
| `streamcrab-paimon` | Apache Paimon source + exactly-once 2PC sink | ~400 |
| `streamcrab-sql` | SQL layer: DataFusion planner → vectorized pipelines | ~300 |

## Quick Start

```rust
use streamcrab_api::StreamExecutionEnvironment;

let env = StreamExecutionEnvironment::new();
let results = env
    .from_iter(vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
        "world hello world".to_string(),
    ])
    .flat_map(|line: &String| {
        line.split_whitespace()
            .map(|w| (w.to_string(), 1i64))
            .collect::<Vec<_>>()
    })
    .key_by(|pair: &(String, i64)| pair.0.clone())
    .reduce(|a: (String, i64), b: (String, i64)| (a.0, a.1 + b.1))
    .execute_with_parallelism(2)
    .unwrap();

let results = results.lock().unwrap();
assert_eq!(results["hello"].1, 3);
assert_eq!(results["world"].1, 3);
assert_eq!(results["streamcrab"].1, 1);
```

### SQL over a Paimon Table

```rust
use streamcrab_paimon::{PaimonSink, PaimonSource};
use streamcrab_sql::SqlContext;

// Exactly-once write: prepare per epoch, commit after checkpoint seal.
let mut sink = PaimonSink::new("/warehouse", "db", "t");
sink.open()?;
sink.write_batch(&batch)?;         // Arrow RecordBatch
sink.pre_commit(epoch)?;           // phase 1: paimon prepare_commit
sink.commit_transaction(epoch)?;   // phase 2: paimon snapshot commit

// Arrow-native scan straight into the SQL layer — no row conversion.
let batches = PaimonSource::new("/warehouse", "db", "t").scan_blocking()?;
let mut ctx = SqlContext::new();
ctx.register_batches("t", batches)?;
let result = ctx.sql("SELECT k, SUM(v) AS total FROM t WHERE v > 50 GROUP BY k")?;
```

### Vectorized Operators in a DataStream Pipeline

```rust
use streamcrab_vectorized::bridge::{Batcher, Unbatcher, VectorOp};

// RecordBatch flows as a plain element type: barriers, watermarks and the
// rescale protocol are untouched.
let results = env
    .from_iter(rows)
    .transform(Batcher::<(i64, i64)>::new(256))
    .transform(VectorOp::new(FilterOperator::new(gt(col(1), lit_i64(500)))))
    .transform(Unbatcher::<(i64, i64)>::new())
    .key_by(|r: &(i64, i64)| r.0)
    .reduce(|a, b| (a.0, a.1 + b.1))
    .execute_with_parallelism(2)?;
```

### WASM UDF Example

```rust
// Guest side (compile with --target wasm32-unknown-unknown)
use streamcrab_wasm_guest::*;

#[derive(Default)]
struct Counter;

impl ProcessFunction for Counter {
    fn process(&mut self, record: WasmRecord, state: Option<Vec<u8>>) -> WasmOutput {
        let count = state.as_ref()
            .map(|s| u64::from_le_bytes(s[..8].try_into().unwrap()))
            .unwrap_or(0) + 1;
        WasmOutput {
            records: vec![WasmRecord {
                key: record.key,
                value: count.to_le_bytes().to_vec(),
                timestamp: record.timestamp,
            }],
            state: Some(count.to_le_bytes().to_vec()),
        }
    }
}

export_process_fn!(Counter);
```

## Building

```bash
cargo build                    # Debug build
cargo build --release          # Release with SIMD (target-cpu=native)
cargo test --workspace         # Run all 500+ tests
cargo clippy --workspace       # Lint (should be clean)

# WASM guest compilation
cd examples/wasm-counter
cargo build --target wasm32-unknown-unknown --release

# Vectorized engine benchmarks (standalone + in-runtime bridge)
cargo test -p streamcrab-vectorized --test benchmark_tests --release -- --nocapture
cargo test -p streamcrab-vectorized --test bridge_benchmark --release -- --nocapture

# Paimon roundtrip + 2PC sink tests (self-contained, local warehouse)
cargo test -p streamcrab-paimon

# SQL layer tests (includes SQL over a real Paimon table)
cargo test -p streamcrab-sql

# Kafka integration tests (requires Docker Kafka on localhost:9092)
cargo test -p streamcrab-core --test kafka_e2e -- --ignored

# S3 checkpoint E2E (requires MinIO; see tests/s3_checkpoint_e2e.rs docs)
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  cargo test -p streamcrab-core --test s3_checkpoint_e2e -- --ignored
```

## Performance

### Vectorized Engine (Release build, 1M rows)
```
Filter:              155.9 M rows/sec
Project:             326.7 M rows/sec
HashAggregate:         6.1 M rows/sec
HashJoin:              4.5 M rows/sec (probe)
WindowAggregate:       3.3 M rows/sec
Arrow FFI roundtrip:   ~0ms per 1M rows (zero-copy)
JNI Filter E2E:      187.1 M rows/sec
```

### In-Runtime Bridge (Release build, 10M rows, Apple M2)
```
filter+project   row chain (LLVM-inlined)   115.8 M rows/sec
                 DataFusion operators       133.8 M rows/sec  (1.16x)
keyed sum        DataFusion GroupsAccumulator 42.3 M rows/sec
```
DataFusion physical expressions beat the fully inlined row chain in-process;
the gap widens further at parallelism > 1 where batching amortizes channel
sends by the batch size.

### vs Flink
- Stateless operators: **50-100x** faster
- Stateful operators: **2-6x** faster
- Zero GC pauses

## Features

### Core Streaming
- Event time processing with watermarks and out-of-order handling
- 4 window assigners: Tumbling, Sliding, Session, Global
- Exactly-Once via Chandy-Lamport checkpoint (barrier alignment + 2PC)
- Keyed state: Value, List, Map backends
- Operator chaining with compile-time monomorphization

### Distributed
- gRPC-based JobManager / TaskManager
- TCP network layer with frame multiplexing
- Distributed checkpoint coordination
- Graceful shutdown with final checkpoint

### Elastic Scaling
- Per-operator rescale via Barrier-Based protocol
- Dual state mode: Local (fast) / Tiered (elastic)
- Epoch-based commit model for State Service
- Autoscaler with configurable policies

### WASM Sandbox
- wasmtime 27 Host runtime with security invariants
- Guest SDK with `ProcessFunction` trait and `export_process_fn!` macro
- Hot update: swap WASM module at checkpoint barrier
- I_WASM_1 (No IO), I_WASM_2 (Stateless Instance), I_WASM_3 (Determinism)

### Connectors & Monitoring
- Kafka Source + 2PC Sink (rdkafka)
- Apache Paimon Source (bounded snapshot scan, Arrow-native)
- Apache Paimon 2PC Sink (prepare_commit per epoch, snapshot commit after seal)
- S3/object-store checkpoint storage (OpenDAL: S3/OSS/GCS/MinIO), CLI-configurable
- AsyncExternalCall operator (At-Least-Once, retry policy)
- TaskMetrics: throughput, latency, queue usage, checkpoint duration
- Nexmark benchmark: events, generator, queries Q1/Q2/Q5/Q8

### Vectorized Execution
- Velox-inspired Arrow-based engine
- Filter, Project, HashAggregate, HashJoin, WindowAggregate
- Flink JNI bridge via Arrow C Data Interface (zero-copy)
- In-runtime bridge: `RecordBatch` as a plain element type — Batcher/Unbatcher
  boundary operators, `DataStream::transform()`, batch-level keyed repartition
- DataFusion operators (physical expressions + GroupsAccumulator fast path)
  with checkpoint support

### SQL (v1 subset)
- `SELECT` / `WHERE` / expression projections / `GROUP BY` + `SUM` over
  registered tables (in-memory batches or Paimon scans)
- datafusion-sql parser & planner; logical plans compile to the DataFusion
  operator pipeline; unsupported constructs fail explicitly, never silently
- Full lakehouse loop tested: 2PC sink write → Paimon scan → SQL aggregation

## Flink Semantics Preserved

StreamCrab preserves Flink's 8 core philosophies:

- **F1** Everything is a Stream (`StreamElement::End` for bounded)
- **F2** Event Time Semantics (watermark generation/propagation/alignment)
- **F3** Exactly-Once via Chandy-Lamport (barrier alignment + 2PC sink)
- **F4** Stateful Processing (keyed state: Value/List/Map)
- **F5** Windowing (Tumbling/Sliding/Session/Global)
- **F6** Distributed Fault Tolerance (JM/TM + checkpoint + recovery)
- **F7** Operator DAG + Chaining (compile-time merge, LLVM inlining)
- **F8** Backpressure (bounded channels)

Plus three extensions:

- **S1** Per-Operator Elastic Scaling
- **S2** Dual State Mode (Local for performance, Tiered for elasticity)
- **S3** WASM Sandbox (safe isolation + multi-language)

## License

MIT License. See [LICENSE](LICENSE) for details.
