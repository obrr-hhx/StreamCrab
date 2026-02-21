use super::*;

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
