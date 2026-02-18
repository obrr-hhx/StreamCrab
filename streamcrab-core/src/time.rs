use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::types::{EventTime, Watermark};

/// Minimum possible event time. Used as the initial "no watermark" sentinel.
pub const EVENT_TIME_MIN: EventTime = i64::MIN;

/// Maximum possible event time. Used to represent no upper bound.
pub const EVENT_TIME_MAX: EventTime = i64::MAX;

/// Extracts timestamps from stream elements and creates watermark generators.
///
/// Implement this trait to define event time semantics for your data type.
/// The strategy is split into two responsibilities:
/// - `extract_timestamp`: pure function, called per element
/// - `create_watermark_generator`: factory, called once per task at startup
pub trait WatermarkStrategy<T>: Send + Sync {
    /// Extract the event time timestamp (milliseconds) from an element.
    fn extract_timestamp(&self, element: &T) -> EventTime;

    /// Create a fresh [`WatermarkGenerator`] for this strategy.
    fn create_watermark_generator(&self) -> Box<dyn WatermarkGenerator>;
}

/// Observes events and decides when to advance the watermark.
///
/// Called by the runtime after each element is processed.
pub trait WatermarkGenerator: Send {
    /// Notify the generator that an event with the given timestamp was observed.
    fn on_event(&mut self, timestamp: EventTime);

    /// Return the current watermark, or `None` if no watermark has been emitted yet.
    fn current_watermark(&self) -> Option<Watermark>;
}

/// Watermark strategy for streams where events can arrive out of order by at most `max_delay`.
///
/// The watermark is `max_seen_timestamp - max_delay`, which means the system
/// waits `max_delay` before closing any window.
///
/// # Example
/// ```
/// use std::time::Duration;
/// use streamcrab_core::time::BoundedOutOfOrderness;
///
/// // Allow up to 5 seconds of out-of-order arrival.
/// let strategy = BoundedOutOfOrderness::new(Duration::from_secs(5), |ts: &i64| *ts);
/// ```
pub struct BoundedOutOfOrderness<T, F> {
    max_delay_ms: i64,
    timestamp_extractor: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F: Fn(&T) -> EventTime + Send + Sync> BoundedOutOfOrderness<T, F> {
    /// Create a new strategy with the given maximum out-of-order delay and timestamp extractor.
    pub fn new(max_delay: Duration, timestamp_extractor: F) -> Self {
        Self {
            max_delay_ms: max_delay.as_millis() as i64,
            timestamp_extractor,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, F> WatermarkStrategy<T> for BoundedOutOfOrderness<T, F>
where
    T: Send + Sync,
    F: Fn(&T) -> EventTime + Send + Sync,
{
    fn extract_timestamp(&self, element: &T) -> EventTime {
        (self.timestamp_extractor)(element)
    }

    fn create_watermark_generator(&self) -> Box<dyn WatermarkGenerator> {
        Box::new(BoundedOutOfOrdernessGenerator {
            max_delay_ms: self.max_delay_ms,
            max_seen_timestamp: EVENT_TIME_MIN,
        })
    }
}

// Internal generator — not part of the public API.
struct BoundedOutOfOrdernessGenerator {
    max_delay_ms: i64,
    max_seen_timestamp: EventTime,
}

impl WatermarkGenerator for BoundedOutOfOrdernessGenerator {
    fn on_event(&mut self, timestamp: EventTime) {
        if timestamp > self.max_seen_timestamp {
            self.max_seen_timestamp = timestamp;
        }
    }

    fn current_watermark(&self) -> Option<Watermark> {
        if self.max_seen_timestamp == EVENT_TIME_MIN {
            // No event seen yet; don't emit a watermark.
            return None;
        }
        Some(Watermark::new(self.max_seen_timestamp - self.max_delay_ms))
    }
}

/// Tracks per-channel watermarks inside a Task and computes the global minimum.
///
/// In a multi-input operator each upstream channel delivers watermarks independently.
/// The operator can only advance its event-time clock to the minimum of all channel
/// watermarks, because a lower-watermark channel might still deliver events with
/// earlier timestamps.
///
/// # Idle channels
/// A channel that stops emitting events is considered *idle* (signalled via
/// [`mark_idle`](Self::mark_idle)). Idle channels are excluded from the minimum
/// calculation so that a single quiet upstream cannot stall the entire pipeline.
pub struct WatermarkTracker {
    /// Per-channel last-seen watermark timestamp. Starts at EVENT_TIME_MIN.
    channel_watermarks: Vec<EventTime>,
    /// Whether each channel is currently considered idle.
    is_idle: Vec<bool>,
    /// Last activity time per channel.
    last_active: Vec<Instant>,
    /// Optional idle timeout for automatic idle detection.
    idle_timeout: Option<Duration>,
    /// The global min watermark timestamp last emitted downstream.
    current_min: EventTime,
}

impl WatermarkTracker {
    /// Create a tracker for `num_channels` upstream input channels.
    pub fn new(num_channels: usize) -> Self {
        let now = Instant::now();
        Self {
            channel_watermarks: vec![EVENT_TIME_MIN; num_channels],
            is_idle: vec![false; num_channels],
            last_active: vec![now; num_channels],
            idle_timeout: None,
            current_min: EVENT_TIME_MIN,
        }
    }

    /// Create a tracker with automatic idle detection enabled.
    pub fn new_with_idle_timeout(num_channels: usize, idle_timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            channel_watermarks: vec![EVENT_TIME_MIN; num_channels],
            is_idle: vec![false; num_channels],
            last_active: vec![now; num_channels],
            idle_timeout: Some(idle_timeout),
            current_min: EVENT_TIME_MIN,
        }
    }

    /// Notify the tracker that `channel_id` received a new watermark.
    ///
    /// Returns `Some(watermark)` if the global min has advanced and the
    /// downstream should receive a new watermark.  Returns `None` if unchanged.
    ///
    /// Watermarks are monotonically non-decreasing: if a waking idle channel
    /// sends a watermark below the current global minimum, it is clamped to
    /// `current_min` so the global watermark never regresses.
    pub fn advance(&mut self, channel_id: usize, watermark: Watermark) -> Option<Watermark> {
        // Clamp to current_min: an idle channel waking up with a stale low
        // watermark must not pull the global minimum backward.
        self.channel_watermarks[channel_id] = watermark.timestamp.max(self.current_min);
        self.is_idle[channel_id] = false; // receiving a watermark means the channel is active
        self.last_active[channel_id] = Instant::now();
        let new_min = self.compute_min();
        if new_min > self.current_min {
            self.current_min = new_min;
            Some(Watermark::new(new_min))
        } else {
            None
        }
    }

    /// Mark `channel_id` as idle, excluding it from the minimum computation.
    ///
    /// Call this when a source has not emitted events for its idle timeout.
    /// The channel is automatically un-idled the next time [`advance`](Self::advance)
    /// is called for it.
    pub fn mark_idle(&mut self, channel_id: usize) {
        self.is_idle[channel_id] = true;
        // Excluding this channel may allow the global min to advance.
        self.current_min = self.compute_min();
    }

    /// Return whether a channel is currently marked idle.
    pub fn is_idle(&self, channel_id: usize) -> bool {
        self.is_idle[channel_id]
    }

    /// Detect channels that exceeded idle timeout and mark them idle.
    ///
    /// Returns a watermark when the global min advances.
    pub fn detect_idle_channels(&mut self) -> Option<Watermark> {
        let Some(timeout) = self.idle_timeout else {
            return None;
        };

        let now = Instant::now();
        let mut changed = false;
        for i in 0..self.is_idle.len() {
            if !self.is_idle[i] && now.duration_since(self.last_active[i]) >= timeout {
                self.is_idle[i] = true;
                changed = true;
            }
        }

        if !changed {
            return None;
        }

        let new_min = self.compute_min();
        if new_min > self.current_min {
            self.current_min = new_min;
            Some(Watermark::new(new_min))
        } else {
            self.current_min = new_min;
            None
        }
    }

    /// Return the current global minimum watermark timestamp.
    pub fn current_min_timestamp(&self) -> EventTime {
        self.current_min
    }

    /// Compute the minimum across all non-idle channels.
    /// Returns `EVENT_TIME_MIN` when all channels are idle or none has reported.
    fn compute_min(&self) -> EventTime {
        self.channel_watermarks
            .iter()
            .zip(self.is_idle.iter())
            .filter(|(_, idle)| !*idle)
            .map(|(ts, _)| *ts)
            .min()
            .unwrap_or(EVENT_TIME_MIN)
    }
}

/// Manages event-time timers for a single operator subtask.
///
/// Timers are sorted by fire time in a `BTreeMap`, enabling O(log n) range scans.
///
/// # P6 upgrade path
/// In Tiered mode (P6), this `BTreeMap` becomes an L1 cache backed by
/// `KeyedStateBackend`.  Timers are additionally written to state so they
/// survive rescale and recovery.  For P2 (Local mode) the BTreeMap is
/// authoritative.
///
/// # Invariant
/// A `(key_bytes, fire_at)` pair is registered at most once; re-registering
/// the same pair is idempotent.
pub struct TimerService {
    /// Sorted map: fire_at -> set of serialized key bytes registered at that time.
    timers: BTreeMap<EventTime, BTreeSet<Vec<u8>>>,
}

impl TimerService {
    /// Create an empty `TimerService`.
    pub fn new() -> Self {
        Self {
            timers: BTreeMap::new(),
        }
    }

    /// Register an event-time timer for `key_bytes` to fire at `fire_at`.
    ///
    /// Re-registering the same `(key_bytes, fire_at)` pair is idempotent.
    pub fn register(&mut self, key_bytes: Vec<u8>, fire_at: EventTime) {
        self.timers.entry(fire_at).or_default().insert(key_bytes);
    }

    /// Cancel an event-time timer.
    ///
    /// No-op if the `(key_bytes, fire_at)` pair was not registered.
    pub fn delete(&mut self, key_bytes: &[u8], fire_at: EventTime) {
        if let Some(keys) = self.timers.get_mut(&fire_at) {
            keys.remove(key_bytes);
            if keys.is_empty() {
                self.timers.remove(&fire_at);
            }
        }
    }

    /// Drain and return all timers with `fire_at <= watermark_ts`.
    ///
    /// The returned vector contains `(key_bytes, fire_at)` pairs in ascending
    /// `fire_at` order.
    pub fn drain_due(&mut self, watermark_ts: EventTime) -> Vec<(Vec<u8>, EventTime)> {
        let fire_times: Vec<EventTime> = self
            .timers
            .range(..=watermark_ts)
            .map(|(ts, _)| *ts)
            .collect();

        let mut fired = Vec::new();
        for fire_at in fire_times {
            if let Some(keys) = self.timers.remove(&fire_at) {
                for key in keys {
                    fired.push((key, fire_at));
                }
            }
        }
        fired
    }

    /// Fire all timers with `fire_at <= watermark_ts`.
    ///
    /// Calls `callback(key_bytes, fire_at)` for each fired timer in ascending
    /// `fire_at` order.  Fired timers are removed from the service.
    pub fn fire_timers(
        &mut self,
        watermark_ts: EventTime,
        mut callback: impl FnMut(&[u8], EventTime) -> Result<()>,
    ) -> Result<()> {
        for (key, fire_at) in self.drain_due(watermark_ts) {
            callback(&key, fire_at)?;
        }
        Ok(())
    }

    /// Return the timestamp of the earliest pending timer, or `None`.
    pub fn next_timer(&self) -> Option<EventTime> {
        self.timers.keys().next().copied()
    }

    /// Return the total count of registered `(key, fire_at)` pairs.
    pub fn len(&self) -> usize {
        self.timers.values().map(|keys| keys.len()).sum()
    }

    /// Return `true` if no timers are registered.
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }
}

impl Default for TimerService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_watermark_before_first_event() {
        let strategy = BoundedOutOfOrderness::new(Duration::from_secs(5), |ts: &i64| *ts);
        let wm_gen = strategy.create_watermark_generator();
        assert_eq!(wm_gen.current_watermark(), None);
    }

    #[test]
    fn test_watermark_advances_with_max_seen() {
        let strategy = BoundedOutOfOrderness::new(Duration::from_secs(5), |ts: &i64| *ts);
        let mut wm_gen = strategy.create_watermark_generator();

        wm_gen.on_event(10_000); // 10s
        assert_eq!(wm_gen.current_watermark(), Some(Watermark::new(5_000))); // 10s - 5s

        wm_gen.on_event(20_000); // 20s
        assert_eq!(wm_gen.current_watermark(), Some(Watermark::new(15_000))); // 20s - 5s
    }

    #[test]
    fn test_out_of_order_event_does_not_regress_watermark() {
        let strategy = BoundedOutOfOrderness::new(Duration::from_secs(5), |ts: &i64| *ts);
        let mut wm_gen = strategy.create_watermark_generator();

        wm_gen.on_event(20_000);
        wm_gen.on_event(5_000); // late event, older than max_seen
        assert_eq!(wm_gen.current_watermark(), Some(Watermark::new(15_000)));
    }

    #[test]
    fn test_zero_delay_watermark() {
        let strategy = BoundedOutOfOrderness::new(Duration::ZERO, |ts: &i64| *ts);
        let mut wm_gen = strategy.create_watermark_generator();

        wm_gen.on_event(1_000);
        assert_eq!(wm_gen.current_watermark(), Some(Watermark::new(1_000)));
    }

    #[test]
    fn test_watermark_display() {
        let wm = Watermark::new(42_000);
        assert_eq!(wm.to_string(), "Watermark(42000ms)");
    }

    // --- WatermarkTracker tests ---

    #[test]
    fn test_tracker_single_channel_advances() {
        let mut tracker = WatermarkTracker::new(1);
        // First advance: min goes from MIN to 1000.
        assert_eq!(
            tracker.advance(0, Watermark::new(1_000)),
            Some(Watermark::new(1_000))
        );
        // Older watermark: no advancement.
        assert_eq!(tracker.advance(0, Watermark::new(500)), None);
        // Newer watermark: advances again.
        assert_eq!(
            tracker.advance(0, Watermark::new(2_000)),
            Some(Watermark::new(2_000))
        );
    }

    #[test]
    fn test_tracker_two_channels_min() {
        let mut tracker = WatermarkTracker::new(2);
        // Channel 0 at 1000; channel 1 still at MIN -> no global advance.
        assert_eq!(tracker.advance(0, Watermark::new(1_000)), None);
        // Channel 1 at 500 -> global min = 500 (advances from MIN).
        assert_eq!(
            tracker.advance(1, Watermark::new(500)),
            Some(Watermark::new(500))
        );
        // Channel 1 at 2000 -> min is still 1000 (limited by channel 0).
        assert_eq!(
            tracker.advance(1, Watermark::new(2_000)),
            Some(Watermark::new(1_000))
        );
        // Channel 0 at 3000 -> min is now 2000 (channel 1).
        assert_eq!(
            tracker.advance(0, Watermark::new(3_000)),
            Some(Watermark::new(2_000))
        );
    }

    #[test]
    fn test_tracker_idle_channel_excluded() {
        let mut tracker = WatermarkTracker::new(2);
        // Channel 0 advances; channel 1 is still at MIN, so global min stays at MIN.
        tracker.advance(0, Watermark::new(1_000));
        // Mark channel 1 idle: excluded from min, global min advances to 1000.
        tracker.mark_idle(1);
        assert_eq!(tracker.current_min_timestamp(), 1_000);
        // Channel 1 wakes up with a stale low watermark (500 < current_min 1000).
        // The incoming watermark is clamped to current_min, so no regression.
        assert_eq!(tracker.advance(1, Watermark::new(500)), None);
        assert_eq!(tracker.current_min_timestamp(), 1_000); // clamped, not regressed
        // Channel 1 advances past current_min: global min does not move (ch0 is still at 1000).
        assert_eq!(tracker.advance(1, Watermark::new(1_500)), None);
        // Channel 0 advances to 2000: global min moves to 1500 (limited by ch1).
        assert_eq!(
            tracker.advance(0, Watermark::new(2_000)),
            Some(Watermark::new(1_500))
        );
    }

    #[test]
    fn test_tracker_all_channels_idle_returns_min() {
        let mut tracker = WatermarkTracker::new(2);
        tracker.mark_idle(0);
        tracker.mark_idle(1);
        // All channels idle: compute_min returns EVENT_TIME_MIN.
        assert_eq!(tracker.current_min_timestamp(), EVENT_TIME_MIN);
    }

    #[test]
    fn test_tracker_auto_idle_detection() {
        let mut tracker = WatermarkTracker::new_with_idle_timeout(1, Duration::from_millis(1));
        tracker.advance(0, Watermark::new(1_000));
        assert!(!tracker.is_idle(0));

        std::thread::sleep(Duration::from_millis(2));
        tracker.detect_idle_channels();
        assert!(tracker.is_idle(0));
    }

    // --- TimerService tests ---

    #[test]
    fn test_timer_register_and_fire() {
        let mut svc = TimerService::new();
        svc.register(b"key-a".to_vec(), 1_000);

        let mut fired: Vec<(Vec<u8>, EventTime)> = Vec::new();
        svc.fire_timers(1_000, |k, t| {
            fired.push((k.to_vec(), t));
            Ok(())
        })
        .unwrap();

        assert_eq!(fired, vec![(b"key-a".to_vec(), 1_000)]);
        assert!(svc.is_empty());
    }

    #[test]
    fn test_timer_does_not_fire_before_watermark() {
        let mut svc = TimerService::new();
        svc.register(b"key-a".to_vec(), 2_000);

        let mut fired = Vec::new();
        svc.fire_timers(1_000, |k, t| {
            fired.push((k.to_vec(), t));
            Ok(())
        })
        .unwrap();

        assert!(fired.is_empty(), "timer should not fire before its time");
        assert_eq!(svc.len(), 1);
    }

    #[test]
    fn test_timer_fires_in_ascending_order() {
        let mut svc = TimerService::new();
        svc.register(b"k".to_vec(), 3_000);
        svc.register(b"k".to_vec(), 1_000);
        svc.register(b"k".to_vec(), 2_000);

        let mut fire_times = Vec::new();
        svc.fire_timers(3_000, |_, t| {
            fire_times.push(t);
            Ok(())
        })
        .unwrap();

        assert_eq!(fire_times, vec![1_000, 2_000, 3_000]);
        assert!(svc.is_empty());
    }

    #[test]
    fn test_timer_delete_cancels_timer() {
        let mut svc = TimerService::new();
        svc.register(b"key-a".to_vec(), 1_000);
        svc.delete(b"key-a", 1_000);

        let mut fired = Vec::new();
        svc.fire_timers(2_000, |k, t| {
            fired.push((k.to_vec(), t));
            Ok(())
        })
        .unwrap();

        assert!(fired.is_empty(), "deleted timer must not fire");
        assert!(svc.is_empty());
    }

    #[test]
    fn test_timer_register_idempotent() {
        let mut svc = TimerService::new();
        svc.register(b"key-a".to_vec(), 1_000);
        svc.register(b"key-a".to_vec(), 1_000); // duplicate
        assert_eq!(svc.len(), 1, "duplicate registration must be idempotent");

        let mut fired = Vec::new();
        svc.fire_timers(1_000, |k, t| {
            fired.push((k.to_vec(), t));
            Ok(())
        })
        .unwrap();
        // Must fire exactly once.
        assert_eq!(fired.len(), 1);
    }
}
