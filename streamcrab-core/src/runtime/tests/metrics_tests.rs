use super::*;
use std::time::Duration;

#[test]
fn test_new_collector_defaults() {
    let collector = MetricsCollector::new();
    // Flush immediately to get the current snapshot
    let mut collector = collector;
    // Check that a freshly created collector has zero total records
    assert_eq!(collector.current.total_records_processed, 0);
    assert_eq!(collector.current.input_queue_usage, 0.0);
    assert_eq!(collector.current.checkpoint_duration_ms, 0);
    assert_eq!(collector.current.state_size_bytes, 0);
    assert_eq!(collector.window_records, 0);
    assert_eq!(collector.window_processing_nanos, 0);
}

#[test]
fn test_record_batch_updates_total() {
    let mut collector = MetricsCollector::new();
    collector.record_batch(100, Duration::from_millis(10));
    collector.record_batch(50, Duration::from_millis(5));
    assert_eq!(collector.current.total_records_processed, 150);
}

#[test]
fn test_flush_computes_throughput() {
    let mut collector = MetricsCollector::new();
    collector.record_batch(1000, Duration::from_millis(100));
    let snapshot = collector.flush();
    // throughput should be non-zero
    assert!(snapshot.throughput_records_per_sec > 0.0);
}

#[test]
fn test_flush_computes_latency() {
    let mut collector = MetricsCollector::new();
    // 10 records, each taking 2ms = 2_000_000 nanos total
    // avg = 2_000_000 / 10 = 200_000 nanos = 200 us
    collector.record_batch(10, Duration::from_nanos(2_000_000));
    let snapshot = collector.flush();
    assert_eq!(snapshot.processing_latency_us, 200);
}

#[test]
fn test_queue_usage_calculation() {
    let mut collector = MetricsCollector::new();
    collector.update_queue_usage(50, 100);
    assert!((collector.current.input_queue_usage - 0.5).abs() < f64::EPSILON);

    collector.update_queue_usage(0, 100);
    assert_eq!(collector.current.input_queue_usage, 0.0);
}

#[test]
fn test_queue_usage_zero_capacity() {
    let mut collector = MetricsCollector::new();
    collector.update_queue_usage(0, 0);
    assert_eq!(collector.current.input_queue_usage, 0.0);
}

#[test]
fn test_checkpoint_duration_recorded() {
    let mut collector = MetricsCollector::new();
    collector.record_checkpoint(Duration::from_millis(42));
    assert_eq!(collector.current.checkpoint_duration_ms, 42);
}

#[test]
fn test_state_size_recorded() {
    let mut collector = MetricsCollector::new();
    collector.update_state_size(1024 * 1024);
    assert_eq!(collector.current.state_size_bytes, 1024 * 1024);
}

#[test]
fn test_flush_resets_window() {
    let mut collector = MetricsCollector::new();
    collector.record_batch(500, Duration::from_millis(50));
    let _first = collector.flush();

    // After flush, window counters are reset
    assert_eq!(collector.window_records, 0);
    assert_eq!(collector.window_processing_nanos, 0);

    // Second flush with no new records should yield zero throughput (capped by min elapsed)
    // but total_records_processed is cumulative and unchanged
    assert_eq!(collector.current.total_records_processed, 500);
    let second = collector.flush();
    assert_eq!(second.processing_latency_us, 0);
}

#[test]
fn test_metrics_serialization_roundtrip() {
    let metrics = TaskMetrics {
        input_queue_usage: 0.75,
        throughput_records_per_sec: 12345.6,
        processing_latency_us: 88,
        checkpoint_duration_ms: 250,
        state_size_bytes: 4096,
        total_records_processed: 999_999,
    };

    let encoded = bincode::serialize(&metrics).expect("serialization failed");
    let decoded: TaskMetrics = bincode::deserialize(&encoded).expect("deserialization failed");

    assert!((decoded.input_queue_usage - metrics.input_queue_usage).abs() < f64::EPSILON);
    assert!(
        (decoded.throughput_records_per_sec - metrics.throughput_records_per_sec).abs() < 0.001
    );
    assert_eq!(decoded.processing_latency_us, metrics.processing_latency_us);
    assert_eq!(
        decoded.checkpoint_duration_ms,
        metrics.checkpoint_duration_ms
    );
    assert_eq!(decoded.state_size_bytes, metrics.state_size_bytes);
    assert_eq!(
        decoded.total_records_processed,
        metrics.total_records_processed
    );
}
