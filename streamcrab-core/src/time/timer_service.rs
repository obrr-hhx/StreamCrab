use super::*;

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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
