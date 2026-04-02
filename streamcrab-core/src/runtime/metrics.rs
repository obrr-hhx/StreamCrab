//! Per-task metrics collection for stream processing tasks.
//!
//! Provides `TaskMetrics` (a snapshot of current metrics) and `MetricsCollector`
//! (accumulates measurements over a sampling window and produces snapshots on flush).

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Per-task metrics collected during execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMetrics {
    /// Input queue usage ratio (0.0 = empty, 1.0 = full). Backpressure indicator.
    pub input_queue_usage: f64,
    /// Records processed per second.
    pub throughput_records_per_sec: f64,
    /// Average processing latency in microseconds per record.
    pub processing_latency_us: u64,
    /// Duration of last checkpoint in milliseconds.
    pub checkpoint_duration_ms: u64,
    /// Estimated state size in bytes.
    pub state_size_bytes: u64,
    /// Total records processed since task start.
    pub total_records_processed: u64,
}

/// Collector that tracks metrics over a sampling window.
pub struct MetricsCollector {
    /// Current accumulated metrics.
    current: TaskMetrics,
    /// Start of current measurement window.
    window_start: Instant,
    /// Records in current window (for throughput calculation).
    window_records: u64,
    /// Total processing time in current window (for latency calculation).
    window_processing_nanos: u64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            current: TaskMetrics::default(),
            window_start: Instant::now(),
            window_records: 0,
            window_processing_nanos: 0,
        }
    }

    /// Record that `count` records were processed in `duration`.
    pub fn record_batch(&mut self, count: u64, duration: Duration) {
        self.window_records += count;
        self.window_processing_nanos += duration.as_nanos() as u64;
        self.current.total_records_processed += count;
    }

    /// Update input queue usage ratio.
    pub fn update_queue_usage(&mut self, used: usize, capacity: usize) {
        self.current.input_queue_usage = if capacity > 0 {
            used as f64 / capacity as f64
        } else {
            0.0
        };
    }

    /// Record a checkpoint duration.
    pub fn record_checkpoint(&mut self, duration: Duration) {
        self.current.checkpoint_duration_ms = duration.as_millis() as u64;
    }

    /// Update state size estimate.
    pub fn update_state_size(&mut self, bytes: u64) {
        self.current.state_size_bytes = bytes;
    }

    /// Flush the current window and compute derived metrics (throughput, latency).
    /// Returns a snapshot of the metrics.
    pub fn flush(&mut self) -> TaskMetrics {
        let elapsed = self.window_start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64().max(0.001); // avoid div by 0

        self.current.throughput_records_per_sec = self.window_records as f64 / elapsed_secs;
        self.current.processing_latency_us = if self.window_records > 0 {
            (self.window_processing_nanos / self.window_records) / 1000
        } else {
            0
        };

        let snapshot = self.current.clone();

        // Reset window
        self.window_start = Instant::now();
        self.window_records = 0;
        self.window_processing_nanos = 0;

        snapshot
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "tests/metrics_tests.rs"]
mod tests;
