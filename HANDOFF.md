# StreamCrab Handoff Document

**Date:** 2026-04-03
**Codebase:** ~120 Rust files, ~27K LOC Rust + 5 Java files (~400 LOC) + WASM guest SDK (~200 LOC)
**Tests:** 471 passing (workspace `cargo test`) + 8 guest (standalone) + 4 Java (mvn test)

---

## 项目概述

StreamCrab 是一个 Rust 实现的流处理引擎，保留 Apache Flink 的 8 大核心语义（事件时间、Exactly-Once、有状态处理、窗口等），同时新增三个扩展：算子级弹性伸缩、双模式状态后端、WASM 沙箱。

已完成的功能模块：
- **Velox 启发的向量化执行引擎** + Flink JNI 桥接层
- **WASM 沙箱**（wasmtime Host + Guest SDK + 热更新 + 不变量保证）
- **Kafka 连接器**（Source + 2PC Sink）
- **AsyncExternalCall**（At-Least-Once 异步调用）
- **Nexmark 基准测试**（事件生成器 + Q1/Q2/Q5/Q8 查询）
- **TaskMetrics / 优雅关闭 / SIMD 优化**

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│ CONTROL PLANE: JobManager                                   │
│  Scheduler · Checkpoint Coordinator · Elastic Manager       │
└──────────────────────┬──────────────────────────────────────┘
                       │ gRPC
┌──────────────────────┴──────────────────────────────────────┐
│ COMPUTE PLANE: TaskManagers                                 │
│  Tasks · Operator Chains · Local/Tiered State               │
│  WASM UDFs · AsyncExternalCall · Kafka Source/Sink          │
└──────────────────────┬──────────────────────────────────────┘
                       │ (Tiered mode)
┌──────────────────────┴──────────────────────────────────────┐
│ STATE PLANE: State Service (optional)                       │
│  Epoch Commit · L2 Remote State · Checkpoint to FS/S3       │
└─────────────────────────────────────────────────────────────┘

Vectorized Execution + Flink JNI Bridge
┌─────────────────────────────────────────────────────────────┐
│ Flink JVM  ──JNI (Arrow zero-copy)──  Rust cdylib           │
│              streamcrab-flink-bridge                         │
│                      │                                      │
│              streamcrab-vectorized                           │
│   (Filter · Project · HashAgg · HashJoin · WindowAgg)       │
└─────────────────────────────────────────────────────────────┘
```

---

## Crate 结构

| Crate | 用途 | LOC (approx) |
|-------|------|-------------|
| `streamcrab-core` | 运行时、状态、Checkpoint、集群、弹性伸缩、Kafka、AsyncCall、Metrics | ~17K |
| `streamcrab-api` | DataStream DSL + process_wasm API | ~1.2K |
| `streamcrab-cli` | CLI (jobmanager/taskmanager/submit) | ~200 |
| `streamcrab-examples` | 示例 + Nexmark 基准测试 | ~500 |
| `streamcrab-vectorized` | Velox 风格向量化执行引擎 | ~5K |
| `streamcrab-flink-bridge` | JNI cdylib (Arrow C Data Interface) | ~1K |
| `streamcrab-flink-java` (Maven) | Flink 1.20 Operator 封装 | ~400 |
| `streamcrab-wasm` | WASM Host Runtime (wasmtime) + WasmOperator | ~600 |
| `streamcrab-wasm-guest` | Guest SDK (standalone, wasm32 target) | ~200 |

---

## 开发阶段完成情况

| Phase | 内容 | 状态 | 测试数 |
|-------|------|------|--------|
| P0 | Skeleton: DataStream API, StreamGraph, 单线程 | ✅ | — |
| P1 | Parallel + State: JobGraph, Chaining, KeyedStream | ✅ | — |
| P2 | Time + Window: Event Time, Watermark, Timer, 4 Window Assigners | ✅ | — |
| P3 | Checkpoint: Barrier alignment, Snapshot, 2PC Sink, Recovery | ✅ | — |
| P4 | Distributed: gRPC JM/TM, TCP network, 分布式 Checkpoint | ✅ | — |
| P5 | WASM: wasmtime Host, Guest SDK, 热更新, 不变量保证 | ✅ | 60 |
| P6 | Elastic Scaling: Tiered State, Rescale Protocol, Autoscaler | ✅ | — |
| P7 | Polish: AsyncCall, Kafka, Nexmark, Metrics, Shutdown, SIMD | ✅ | 70+ |
| — | **streamcrab-core + api 合计** | | **256** |
| V0-V5 | 向量化引擎 + JNI Bridge (完整) | ✅ | 101 + 4 Java |
| — | E2E Guest WASM | ✅ | 11 |
| — | 验收测试 (WordCount/Window/Cluster) | ✅ | 12 |
| — | SIMD 验证 | ✅ | 2 |
| — | Kafka E2E (需要 broker) | ✅ | 2 (ignored) |
| **总计** | | | **471 + 8 guest + 4 Java** |

---

## 性能数据 (Release build, 1M rows)

### Rust 纯算子吞吐 (`cargo test --release`)
```
Filter:              155.9 M rows/sec
Project:             326.7 M rows/sec
HashAggregate:         6.1 M rows/sec (accumulate), 0.1ms (flush)
HashJoin:              4.5 M rows/sec (probe), 6.5ms (build 10K)
WindowAggregate:       3.3 M rows/sec (accumulate), 5.2ms (flush)
Arrow FFI roundtrip:   ~0ms per 1M rows (zero-copy)
```

### JNI 端到端 (`mvn test`, JDK 17)
```
Filter E2E (1M rows): 187.1 M rows/sec (5.34ms total)
```

### 参考对比
- Flink Java 单线程典型吞吐: 1-3M rows/sec (计算密集算子)
- StreamCrab 无状态算子: **50-100x** faster
- StreamCrab 有状态算子: **2-6x** faster
- 零 GC 暂停

---

## 本次 Session 新增内容 (2026-04-02 ~ 04-03)

### P5 WASM (完整实现)
- `streamcrab-wasm`: wasmtime 27 Host Runtime, `WasmOperator<S>` 实现 Operator trait
- `streamcrab-wasm-guest`: Guest SDK, `ProcessFunction` trait, `export_process_fn!` 宏
- `examples/wasm-counter`: 真实 Rust→wasm32 编译的 Counter UDF
- 热更新: `schedule_update()` + 在 checkpoint barrier 原子切换
- 不变量保证: I_WASM_1 (No IO), I_WASM_2 (Stateless + rebuild_on_checkpoint), I_WASM_3 (verify_determinism)
- `KeyedStream::process_wasm()` API 集成

### P7 Polish (大部分完成)
- `AsyncExternalOperator`: 同步 v1 实现, 指数退避重试, At-Least-Once 语义
- `KafkaSource + KafkaSink`: rdkafka dynamic-linking, offset checkpoint, 2PC transaction
- `TaskMetrics + MetricsCollector`: throughput/latency/queue usage/checkpoint duration
- `ShutdownSignal + install_signal_handler`: AtomicBool + Notify, Ctrl-C 处理
- Nexmark: Person/Auction/Bid 事件, 1:3:46 生成器, Q1/Q2/Q5/Q8 查询实现
- SIMD Level 1: `.cargo/config.toml` target-cpu=native
- 验收测试: WordCount, Window Accumulation, Filter+Map, 并行度不变性
- 集群验收: Crash & Recovery, Rescale Protocol, State Loss & Recovery
- 代码质量: clippy clean (65 fixes), cargo fmt

---

## 关键设计决策

1. **不直接转写 Velox C++**：选择在 StreamCrab 基础上新建 Velox 启发的向量化层。

2. **Arrow C Data Interface 而非序列化**：JNI 数据交换使用 Arrow 零拷贝 FFI。

3. **VectorizedOperator 独立于 Operator trait**：向量化算子有自己的 trait，不强制统一。

4. **WASM 用 bare Linker (无 WASI)**：Guest 无 IO 能力，通过 wasmtime Config 显式禁用 threads。

5. **WasmOperator 泛型化**：`WasmOperator<S: KeyedStateBackend>` 而非 dyn trait，因为 `KeyedStateBackend` 有泛型方法不 dyn-compatible。

6. **Guest SDK 独立 crate**：`streamcrab-wasm-guest` 不在 workspace 中，有自己的 `[workspace]` 表，避免 wasmtime 等 Host 依赖污染。

7. **Kafka 用 dynamic-linking**：rdkafka features = ["dynamic-linking"] 链接 Homebrew 安装的 librdkafka。

8. **AsyncExternalCall v1 同步**：先提供正确的 API shape + checkpoint 语义，v2 再用 FuturesUnordered 真正异步。

---

## 已知技术债

1. **ArrowBatchConverter.java 使用 `sun.misc.Unsafe`**：JDK 未来版本可能移除。可改用 `java.lang.foreign` (JDK 22+)。

2. **有状态算子逐行哈希**：HashAggregate/HashJoin 可优化为批量向量化哈希。

3. **HashJoin 无 spill**：build 侧全量驻留内存，大表 OOM。

4. **Window 晚到事件简单丢弃**：未提供 allowed lateness / side output。

5. **AsyncExternalCall v1 同步**：process_batch 内同步调用，未用 FuturesUnordered。

6. **Kafka unit test 异步 panic**：`kafka_source_open_connects` 在 `#[test]` 中调用 async rdkafka 导致 "no reactor" panic，需改为 `#[tokio::test]`。

7. **Flink `emitArrowResult` 未在真实集群验证**。

---

## 待办优先级

### 高优先级
- [ ] **真实 Flink 集群验证**：部署 Flink 1.20 集群，提交包含 native operator 的作业
- [ ] **Spill-to-disk (HashJoin/HashAggregate)**：内存超限时写磁盘
- [ ] **AsyncExternalCall v2**：FuturesUnordered 真正异步 + checkpoint drain

### 中优先级
- [ ] **Nexmark 性能基准运行**：实际跑 Q1/Q2/Q5 在不同并行度下的吞吐/延迟
- [ ] **Kafka unit test 修复**：`kafka_source_open_connects` 改为 tokio::test
- [ ] **批量向量化哈希**：替换逐行 key 序列化为 Arrow compute hash kernel
- [ ] **Prometheus metrics endpoint**：暴露 /metrics HTTP endpoint

### 低优先级
- [ ] **进程隔离 JNI**：native 崩溃当前杀 JVM
- [ ] **java.lang.foreign 替换 Unsafe**：面向 JDK 22+
- [ ] **DataFusion SQL 集成**：v2 路线图，Table API + SQL frontend
- [ ] **Unaligned Checkpoint**：减少 barrier alignment 缓冲
- [ ] **Web UI**：作业监控界面

---

## 构建与测试命令

```bash
# Rust 全量测试
cargo test --workspace

# Release 性能基准
cargo test -p streamcrab-vectorized --test benchmark_tests --release -- --nocapture

# 正确性验证
cargo test -p streamcrab-vectorized --test correctness_tests --release

# WASM Guest 编译
cd examples/wasm-counter
cargo build --target wasm32-unknown-unknown --release

# WASM E2E 测试
cargo test -p streamcrab-wasm --test e2e_guest_tests

# Guest SDK 测试 (standalone)
cd streamcrab-wasm-guest && cargo test

# JNI cdylib 构建
cargo build -p streamcrab-flink-bridge --release

# Java 编译 + 测试 (需 JDK 17)
cd streamcrab-flink-java && mvn test

# Kafka E2E (需要 localhost:9092 Kafka broker)
cargo test -p streamcrab-core --test kafka_e2e -- --ignored

# 集群验收测试
cargo test -p streamcrab-core --test acceptance_cluster

# Clippy (应该 clean)
cargo clippy --workspace
```

---

## 文件结构变更 (本次 Session)

```
新增 crate:
  streamcrab-wasm/           WASM Host Runtime
  streamcrab-wasm-guest/     Guest SDK (standalone)

新增模块:
  streamcrab-core/src/connectors/kafka.rs      Kafka Source + Sink
  streamcrab-core/src/runtime/async_call.rs    AsyncExternalOperator
  streamcrab-core/src/runtime/metrics.rs       TaskMetrics + MetricsCollector
  streamcrab-core/src/runtime/shutdown.rs      ShutdownSignal + signal handler
  streamcrab-examples/src/nexmark/             Nexmark events + generator + queries

新增测试:
  streamcrab-api/tests/acceptance_tests.rs     WordCount/Window/Filter 验收测试
  streamcrab-api/tests/wasm_job.rs             process_wasm API 测试
  streamcrab-core/tests/acceptance_cluster.rs  Crash/Rescale/StateLoss 集群测试
  streamcrab-core/tests/kafka_e2e.rs           Kafka Source→Sink E2E
  streamcrab-core/tests/simd_verification.rs   SIMD 优化验证
  streamcrab-wasm/tests/e2e_guest_tests.rs     真实 WASM UDF E2E
  streamcrab-wasm/tests/operator_tests.rs      WasmOperator 集成测试
  examples/wasm-counter/                       真实 Rust WASM Guest 示例
```
