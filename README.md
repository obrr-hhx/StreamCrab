# StreamCrab

A Rust-based stream processing engine implementing Apache Flink semantics with three core promises:

1. **Minimal** -- ~13,000 core LOC, 1/150th of Flink
2. **High Performance** -- Zero GC pauses, 2-5x Flink throughput in Local mode
3. **Operator-Level Elasticity** -- Single operator rescale <5s, zero state migration

StreamCrab is an educational-grade implementation prioritizing code readability and architectural clarity over production features.

## Architecture

```
CONTROL PLANE     JobManager (Scheduler, Checkpoint Coordinator, Elastic Manager)
                         |  gRPC
COMPUTE PLANE     TaskManagers (Tasks, WASM/Native UDFs, Local/Tiered State)
                         |  (Tiered mode only)
STATE PLANE       State Service (Epoch-based Commit, Checkpoint to FS/S3)
```

**Key design decisions:**

| Decision | Choice | Rationale |
|----------|--------|-----------|
| State mode | Dual-mode Local / Tiered | Local for speed; Tiered for elasticity |
| JM HA | Single-point (v1) | K8s restart + checkpoint recovery |
| Elastic granularity | Per-Operator | Core differentiator vs Flink/Arroyo |
| UDF isolation | WASM + Native | WASM for safety/multi-lang; Native for zero overhead |

## Quick Start

```rust
use streamcrab_api::environment::StreamExecutionEnvironment;

let env = StreamExecutionEnvironment::new("wordcount");
env.from_iter(vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
    ])
    .flat_map(|line: String| {
        line.split_whitespace().map(String::from).collect::<Vec<_>>()
    })
    .map(|word: String| (word, 1i32))
    .key_by::<String, _>(|(w, _)| w.clone())
    .reduce(|(w, c1), (_, c2)| (w, c1 + c2))
    .print();
env.execute().unwrap();
// Output: ("hello", 1) ("world", 1) ("hello", 2) ("streamcrab", 1)
```

## Building

```bash
cargo build            # Debug build
cargo build --release  # Release build with SIMD optimizations
cargo test             # Run all tests
cargo clippy -- -D warnings
cargo doc --no-deps --open   # View API documentation
```

## Roadmap

| Phase | Weeks | Description | Milestone |
|-------|:-----:|-------------|-----------|
| **P0** | 1-2 | Skeleton: DataStream API, StreamGraph, single-thread runtime | **WordCount runs** |
| P1 | 3-4 | Parallel execution, operator chaining, keyed state | Parallel keyed aggregation |
| P2 | 5-6 | Event time, watermarks, timers, window assigners | Event time window aggregation |
| P3 | 7-8 | Checkpoint: barrier alignment, snapshots, 2PC sink | Exactly-once recovery |
| P4 | 9-11 | Distributed: gRPC, TCP, JobManager/TaskManager | 3-node cluster with fault tolerance |
| P5 | 12-13 | WASM: wasmtime host, guest SDK, hot update | WASM UDF dynamic deployment |
| P6 | 14-16 | Elastic scaling: State Service, Tiered state, Autoscaler | Per-operator rescale <5s |
| P7 | 17-20 | Polish: AsyncExternalCall, Kafka, Nexmark benchmark | Nexmark + 5 acceptance tests |

**Current status: P0 complete.**

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
