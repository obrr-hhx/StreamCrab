//! End-to-end integration tests for the Kafka connector.
//!
//! These tests require a running Kafka broker at `localhost:9092`.
//! Run with:
//!   cargo test -p streamcrab-core --test kafka_e2e -- --ignored --nocapture

use std::time::Duration;

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use streamcrab_core::connectors::kafka::{KafkaConfig, KafkaSink, KafkaSource};

const BROKER: &str = "localhost:9092";
const TOPIC: &str = "test-streamcrab";

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a raw StreamConsumer for reading back messages in tests.
fn make_consumer(group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", BROKER)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        // Low timeouts keep tests fast.
        .set("session.timeout.ms", "6000")
        .set("fetch.wait.max.ms", "100")
        .create()
        .expect("create consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    consumer
}

// ── kafka_source_sink_roundtrip ───────────────────────────────────────────────

/// Send messages via KafkaSink, then consume them via KafkaSource (raw
/// StreamConsumer) and verify they match byte-for-byte.
#[tokio::test]
#[ignore = "requires Docker Kafka"]
async fn kafka_source_sink_roundtrip() {
    // Use a unique topic suffix to avoid interference between test runs.
    let unique_topic = format!("{}-roundtrip", TOPIC);

    // Create topic (ignore error if it already exists).
    let _ = tokio::process::Command::new("/opt/homebrew/opt/kafka/bin/kafka-topics")
        .args([
            "--create",
            "--topic",
            &unique_topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--if-not-exists",
            "--bootstrap-server",
            BROKER,
        ])
        .output()
        .await;

    // --- Sink: produce messages ---
    let cfg = KafkaConfig::new(BROKER, &unique_topic, "roundtrip-group");
    let mut sink = KafkaSink::new(cfg);
    sink.open().expect("sink open");

    let messages: Vec<(&[u8], &[u8])> = vec![
        (b"key-0", b"hello"),
        (b"key-1", b"world"),
        (b"key-2", b"streamcrab"),
    ];

    for (key, value) in &messages {
        sink.send(key, value).await.expect("send");
    }

    // Give the broker a moment to commit the messages.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Source: consume messages via raw consumer ---
    let consumer = make_consumer("roundtrip-verify-group", &unique_topic);

    let mut received: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while received.len() < messages.len() {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "Timeout: only received {}/{} messages",
                received.len(),
                messages.len()
            );
        }
        match tokio::time::timeout(
            Duration::from_secs(2),
            consumer.recv(),
        )
        .await
        {
            Ok(Ok(msg)) => {
                let key = msg.key().unwrap_or(&[]).to_vec();
                let payload = msg.payload().unwrap_or(&[]).to_vec();
                received.push((key, payload));
            }
            Ok(Err(e)) => panic!("consumer error: {e}"),
            Err(_) => continue, // timeout, retry
        }
    }

    // Verify all sent messages were received (order may differ across partitions).
    assert_eq!(received.len(), messages.len(), "message count mismatch");

    let sent_set: std::collections::HashSet<(&[u8], &[u8])> = messages.iter().cloned().collect();
    for (key, payload) in &received {
        assert!(
            sent_set.contains(&(key.as_slice(), payload.as_slice())),
            "received unexpected message: key={:?} payload={:?}",
            key,
            payload
        );
    }

    println!(
        "kafka_source_sink_roundtrip: {} messages sent and verified",
        messages.len()
    );
}

// ── kafka_source_offset_checkpoint ───────────────────────────────────────────

/// Consume some messages, snapshot offsets, restore to a new KafkaSource,
/// and verify that the restored offsets match what was consumed.
#[tokio::test]
#[ignore = "requires Docker Kafka"]
async fn kafka_source_offset_checkpoint() {
    // Produce a few seed messages to the main test topic so there is something
    // to consume.
    let unique_topic = format!("{}-offsets", TOPIC);

    // Create topic.
    let _ = tokio::process::Command::new("/opt/homebrew/opt/kafka/bin/kafka-topics")
        .args([
            "--create",
            "--topic",
            &unique_topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--if-not-exists",
            "--bootstrap-server",
            BROKER,
        ])
        .output()
        .await;

    // Produce 5 messages.
    let cfg = KafkaConfig::new(BROKER, &unique_topic, "offset-check-group");
    let mut sink = KafkaSink::new(cfg);
    sink.open().expect("sink open");

    for i in 0..5u32 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        sink.send(key.as_bytes(), val.as_bytes())
            .await
            .expect("send");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Open KafkaSource and consume 3 messages, tracking offsets ---
    let src_cfg = KafkaConfig::new(BROKER, &unique_topic, "offset-source-group");
    let mut source = KafkaSource::new(src_cfg);
    source.open().expect("source open");

    // Collect (partition, offset) pairs separately to avoid simultaneous
    // mutable + immutable borrows of `source`.
    let mut offset_records: Vec<(i32, i64)> = Vec::new();
    {
        let consumer = source.consumer().expect("consumer initialized");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

        while offset_records.len() < 3 {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "Timeout waiting for messages (consumed {})",
                    offset_records.len()
                );
            }
            match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let partition = msg.partition();
                    let offset = msg.offset();
                    println!("Consumed message: partition={partition} offset={offset}");
                    offset_records.push((partition, offset));
                }
                Ok(Err(e)) => panic!("consumer error: {e}"),
                Err(_) => continue,
            }
        }
    }

    // Now apply the tracked offsets to the source.
    for (partition, offset) in &offset_records {
        source.mark_offset(*partition, *offset);
    }
    let consumed = offset_records.len();
    assert_eq!(consumed, 3);

    assert_eq!(consumed, 3);

    // --- Snapshot offsets ---
    let snapshot_bytes = source.snapshot_offsets().expect("snapshot");
    assert!(!snapshot_bytes.is_empty(), "snapshot should not be empty");

    // --- Restore into a fresh KafkaSource ---
    let src_cfg2 = KafkaConfig::new(BROKER, &unique_topic, "offset-source-group-2");
    let mut source2 = KafkaSource::new(src_cfg2);
    source2.restore_offsets(&snapshot_bytes).expect("restore");

    // Verify that the restored offsets match those tracked during consumption.
    let original = source.committed_offsets();
    let restored = source2.committed_offsets();

    assert_eq!(
        original, restored,
        "restored offsets must match snapshotted offsets"
    );

    // Sanity: partition 0 offset should be >= 0 (we consumed messages from it).
    let partition_0_offset = restored.get(&0).copied().unwrap_or(-1);
    assert!(
        partition_0_offset >= 0,
        "partition 0 offset should be non-negative after consuming, got {partition_0_offset}"
    );

    println!(
        "kafka_source_offset_checkpoint: offsets snapshot/restore verified: {:?}",
        restored
    );
}
