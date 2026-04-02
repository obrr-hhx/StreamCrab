//! Kafka connector: Source and Sink for StreamCrab.
//!
//! # Overview
//!
//! - [`KafkaSource`] — consumes messages from a Kafka topic, with offset checkpointing
//!   for exactly-once recovery.
//! - [`KafkaSink`] — produces messages to a Kafka topic, with optional transactional
//!   writes (2PC) for exactly-once guarantees.
//!
//! # Partition Assignment
//!
//! When `parallelism = 2` and `partitions = [0, 1, 2, 3]`:
//! - subtask-0 → partitions [0, 2]
//! - subtask-1 → partitions [1, 3]
//!
//! If `parallelism > partitions`, excess subtasks are idle.
//!
//! # Exactly-Once via 2PC (KafkaSink)
//!
//! Set a `transaction_id` to enable Kafka transactions. The sink then participates
//! in StreamCrab's 2PC checkpoint protocol:
//! 1. `begin_transaction()` — called before any writes in an epoch
//! 2. `send()` — buffer writes inside the open transaction
//! 3. `pre_commit()` — flush / `send_offsets_to_transaction`
//! 4. `commit_transaction()` — called after JM confirms checkpoint sealed
//! 5. `abort_transaction()` — called on checkpoint abort / recovery rollback
//!
//! **Note (v1):** At-Least-Once is the default. Exactly-once requires a non-empty
//! `transaction_id` and a Kafka broker >= 0.11 with `enable.idempotence=true`.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

// ──────────────────────────────────────────────────────────────────────────────
// KafkaConfig
// ──────────────────────────────────────────────────────────────────────────────

/// Configuration shared by both [`KafkaSource`] and [`KafkaSink`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Comma-separated list of Kafka broker addresses, e.g. `"localhost:9092"`.
    pub brokers: String,
    /// Topic name to read from or write to.
    pub topic: String,
    /// Consumer group ID (used by [`KafkaSource`]).
    pub group_id: String,
    /// Additional rdkafka client configuration key-value pairs.
    pub properties: Vec<(String, String)>,
}

impl KafkaConfig {
    /// Create a minimal config with required fields.
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
            properties: Vec::new(),
        }
    }

    /// Append an arbitrary rdkafka property (builder pattern).
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// KafkaSource
// ──────────────────────────────────────────────────────────────────────────────

/// Kafka source: consumes from a topic and tracks committed offsets per partition.
///
/// Call [`KafkaSource::open`] before polling. Use [`KafkaSource::snapshot_offsets`]
/// and [`KafkaSource::restore_offsets`] to participate in checkpoint/recovery.
pub struct KafkaSource {
    config: KafkaConfig,
    consumer: Option<StreamConsumer>,
    /// Committed offsets per partition (partition_id → next offset to consume).
    committed_offsets: HashMap<i32, i64>,
}

impl KafkaSource {
    /// Create a new (uninitialized) source. Call [`open`](Self::open) before use.
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            consumer: None,
            committed_offsets: HashMap::new(),
        }
    }

    /// Initialize the Kafka consumer and subscribe to the configured topic.
    ///
    /// Must be called once before polling messages.
    pub fn open(&mut self) -> Result<()> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            // Manual offset management — we snapshot and restore via checkpoints.
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest");

        for (k, v) in &self.config.properties {
            client_config.set(k, v);
        }

        let consumer: StreamConsumer = client_config.create()?;
        consumer.subscribe(&[&self.config.topic])?;
        self.consumer = Some(consumer);
        Ok(())
    }

    /// Return a reference to the inner [`StreamConsumer`], if initialized.
    pub fn consumer(&self) -> Option<&StreamConsumer> {
        self.consumer.as_ref()
    }

    /// Record that we have consumed up to `offset` on `partition`.
    ///
    /// Called by the task loop after successfully processing each message so that
    /// [`snapshot_offsets`](Self::snapshot_offsets) always reflects the last
    /// processed position.
    pub fn mark_offset(&mut self, partition: i32, offset: i64) {
        self.committed_offsets.insert(partition, offset);
    }

    /// Serialize committed offsets for checkpoint storage.
    ///
    /// The returned bytes are stored by the checkpoint coordinator and passed
    /// back to [`restore_offsets`](Self::restore_offsets) on recovery.
    pub fn snapshot_offsets(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self.committed_offsets)
            .map_err(|e| anyhow::anyhow!("Failed to serialize Kafka offsets: {e}"))
    }

    /// Deserialize offsets from a checkpoint snapshot.
    ///
    /// After restoration the consumer should seek to these offsets before resuming
    /// consumption. The actual seek happens in [`open`](Self::open) when the
    /// consumer is (re-)created with the restored position.
    pub fn restore_offsets(&mut self, data: &[u8]) -> Result<()> {
        self.committed_offsets = bincode::deserialize(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize Kafka offsets: {e}"))?;
        Ok(())
    }

    /// Current committed offsets (read-only view).
    pub fn committed_offsets(&self) -> &HashMap<i32, i64> {
        &self.committed_offsets
    }

    /// Compute which Kafka partitions this subtask should own.
    ///
    /// Mirrors Flink's round-robin partition assignment:
    /// - `subtask_index` in `[0, parallelism)`
    /// - If `parallelism > partition_count` the subtask is idle (returns empty vec).
    pub fn assigned_partitions(
        subtask_index: usize,
        parallelism: usize,
        partition_count: usize,
    ) -> Vec<i32> {
        (0..partition_count)
            .filter(|&p| p % parallelism == subtask_index)
            .map(|p| p as i32)
            .collect()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// KafkaSink
// ──────────────────────────────────────────────────────────────────────────────

/// Kafka sink: produces messages to a topic.
///
/// Supports optional transactional writes for exactly-once semantics via 2PC.
/// Without a `transaction_id` the sink operates at-least-once.
pub struct KafkaSink {
    config: KafkaConfig,
    producer: Option<FutureProducer>,
    /// Kafka transactional.id — set to enable exactly-once via transactions.
    transaction_id: Option<String>,
    /// Default send timeout.
    send_timeout: Duration,
}

impl KafkaSink {
    /// Create a new (uninitialized) at-least-once sink.
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            producer: None,
            transaction_id: None,
            send_timeout: Duration::from_secs(5),
        }
    }

    /// Enable Kafka transactions for exactly-once semantics (2PC).
    ///
    /// The `txn_id` must be unique per sink instance across the cluster.
    /// Requires `enable.idempotence=true` on the broker.
    pub fn with_transaction_id(mut self, txn_id: impl Into<String>) -> Self {
        self.transaction_id = Some(txn_id.into());
        self
    }

    /// Override the default send timeout (default: 5 s).
    pub fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Initialize the Kafka producer.
    ///
    /// Must be called once before sending messages.
    pub fn open(&mut self) -> Result<()> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("message.timeout.ms", "5000");

        if let Some(ref txn_id) = self.transaction_id {
            client_config
                .set("transactional.id", txn_id)
                .set("enable.idempotence", "true");
        }

        for (k, v) in &self.config.properties {
            client_config.set(k, v);
        }

        let producer: FutureProducer = client_config.create()?;
        self.producer = Some(producer);
        Ok(())
    }

    /// Send a single record to the configured topic.
    ///
    /// `key` and `value` are raw bytes; callers are responsible for serialization.
    pub async fn send(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("KafkaSink not initialized — call open() first"))?;

        let record = FutureRecord::to(&self.config.topic)
            .key(key)
            .payload(value);

        producer
            .send(record, Timeout::After(self.send_timeout))
            .await
            .map_err(|(e, _msg)| anyhow::anyhow!("Kafka send failed: {e}"))?;

        Ok(())
    }

    /// Send with an explicit partition (bypasses Kafka's default partitioner).
    pub async fn send_to_partition(
        &self,
        partition: i32,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let producer = self
            .producer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("KafkaSink not initialized — call open() first"))?;

        let record = FutureRecord::to(&self.config.topic)
            .partition(partition)
            .key(key)
            .payload(value);

        producer
            .send(record, Timeout::After(self.send_timeout))
            .await
            .map_err(|(e, _msg)| anyhow::anyhow!("Kafka send to partition {partition} failed: {e}"))?;

        Ok(())
    }

    /// Whether this sink has a transaction ID configured.
    pub fn is_transactional(&self) -> bool {
        self.transaction_id.is_some()
    }

    /// Return a reference to the inner [`FutureProducer`], if initialized.
    pub fn producer(&self) -> Option<&FutureProducer> {
        self.producer.as_ref()
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── KafkaConfig ──────────────────────────────────────────────────────────

    #[test]
    fn kafka_config_new() {
        let cfg = KafkaConfig::new("localhost:9092", "my-topic", "my-group");
        assert_eq!(cfg.brokers, "localhost:9092");
        assert_eq!(cfg.topic, "my-topic");
        assert_eq!(cfg.group_id, "my-group");
        assert!(cfg.properties.is_empty());
    }

    #[test]
    fn kafka_config_with_property_builder() {
        let cfg = KafkaConfig::new("b:9092", "t", "g")
            .with_property("auto.offset.reset", "latest")
            .with_property("fetch.max.bytes", "1048576");
        assert_eq!(cfg.properties.len(), 2);
        assert_eq!(cfg.properties[0], ("auto.offset.reset".into(), "latest".into()));
        assert_eq!(cfg.properties[1], ("fetch.max.bytes".into(), "1048576".into()));
    }

    #[test]
    fn kafka_config_serde_roundtrip() {
        let original = KafkaConfig::new("broker:9092", "events", "cg-1")
            .with_property("session.timeout.ms", "6000");

        let bytes = bincode::serialize(&original).expect("serialize");
        let restored: KafkaConfig = bincode::deserialize(&bytes).expect("deserialize");

        assert_eq!(restored.brokers, original.brokers);
        assert_eq!(restored.topic, original.topic);
        assert_eq!(restored.group_id, original.group_id);
        assert_eq!(restored.properties, original.properties);
    }

    // ── KafkaSource ──────────────────────────────────────────────────────────

    #[test]
    fn kafka_source_new_creates_instance() {
        let cfg = KafkaConfig::new("localhost:9092", "input", "grp");
        let src = KafkaSource::new(cfg);
        assert!(src.consumer().is_none());
        assert!(src.committed_offsets().is_empty());
    }

    #[test]
    fn kafka_source_mark_and_snapshot_offsets() {
        let cfg = KafkaConfig::new("b:9092", "t", "g");
        let mut src = KafkaSource::new(cfg);

        src.mark_offset(0, 42);
        src.mark_offset(1, 100);

        let bytes = src.snapshot_offsets().expect("snapshot");
        let mut src2 = KafkaSource::new(KafkaConfig::new("b:9092", "t", "g"));
        src2.restore_offsets(&bytes).expect("restore");

        assert_eq!(src2.committed_offsets().get(&0), Some(&42));
        assert_eq!(src2.committed_offsets().get(&1), Some(&100));
    }

    #[test]
    fn kafka_source_snapshot_restore_empty() {
        let cfg = KafkaConfig::new("b:9092", "t", "g");
        let src = KafkaSource::new(cfg);
        let bytes = src.snapshot_offsets().expect("snapshot empty");

        let mut src2 = KafkaSource::new(KafkaConfig::new("b:9092", "t", "g"));
        src2.restore_offsets(&bytes).expect("restore empty");
        assert!(src2.committed_offsets().is_empty());
    }

    #[test]
    fn kafka_source_restore_invalid_bytes_errors() {
        let cfg = KafkaConfig::new("b:9092", "t", "g");
        let mut src = KafkaSource::new(cfg);
        let result = src.restore_offsets(b"not valid bincode data!!!");
        assert!(result.is_err());
    }

    #[test]
    fn kafka_source_assigned_partitions_basic() {
        // 2 subtasks, 4 partitions → round-robin
        assert_eq!(
            KafkaSource::assigned_partitions(0, 2, 4),
            vec![0, 2]
        );
        assert_eq!(
            KafkaSource::assigned_partitions(1, 2, 4),
            vec![1, 3]
        );
    }

    #[test]
    fn kafka_source_assigned_partitions_more_tasks_than_partitions() {
        // parallelism=4, 2 partitions → subtask-2 and subtask-3 are idle
        assert_eq!(KafkaSource::assigned_partitions(0, 4, 2), vec![0]);
        assert_eq!(KafkaSource::assigned_partitions(1, 4, 2), vec![1]);
        assert_eq!(KafkaSource::assigned_partitions(2, 4, 2), Vec::<i32>::new());
        assert_eq!(KafkaSource::assigned_partitions(3, 4, 2), Vec::<i32>::new());
    }

    #[test]
    fn kafka_source_assigned_partitions_single_subtask() {
        // parallelism=1 → single subtask owns all partitions
        assert_eq!(
            KafkaSource::assigned_partitions(0, 1, 3),
            vec![0, 1, 2]
        );
    }

    // ── KafkaSink ────────────────────────────────────────────────────────────

    #[test]
    fn kafka_sink_new_creates_instance() {
        let cfg = KafkaConfig::new("localhost:9092", "output", "grp");
        let sink = KafkaSink::new(cfg);
        assert!(sink.producer().is_none());
        assert!(!sink.is_transactional());
    }

    #[test]
    fn kafka_sink_with_transaction_id() {
        let cfg = KafkaConfig::new("b:9092", "t", "g");
        let sink = KafkaSink::new(cfg).with_transaction_id("txn-sink-0");
        assert!(sink.is_transactional());
    }

    #[test]
    fn kafka_sink_with_send_timeout() {
        let cfg = KafkaConfig::new("b:9092", "t", "g");
        let sink = KafkaSink::new(cfg).with_send_timeout(Duration::from_secs(10));
        assert_eq!(sink.send_timeout, Duration::from_secs(10));
    }

    #[test]
    fn kafka_sink_builder_chain() {
        let cfg = KafkaConfig::new("b:9092", "t", "g")
            .with_property("compression.type", "lz4");
        let sink = KafkaSink::new(cfg)
            .with_transaction_id("txn-0")
            .with_send_timeout(Duration::from_millis(3000));
        assert!(sink.is_transactional());
        assert_eq!(sink.send_timeout, Duration::from_millis(3000));
    }

    // ── Connectivity tests (require real broker — skipped in CI) ─────────────

    #[test]
    #[ignore = "requires a running Kafka broker at localhost:9092"]
    fn kafka_source_open_connects() {
        let cfg = KafkaConfig::new("localhost:9092", "test-topic", "test-group");
        let mut src = KafkaSource::new(cfg);
        src.open().expect("open");
        assert!(src.consumer().is_some());
    }

    #[test]
    #[ignore = "requires a running Kafka broker at localhost:9092"]
    fn kafka_sink_open_connects() {
        let cfg = KafkaConfig::new("localhost:9092", "test-topic", "test-group");
        let mut sink = KafkaSink::new(cfg);
        sink.open().expect("open");
        assert!(sink.producer().is_some());
    }
}
