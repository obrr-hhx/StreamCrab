use super::*;

// ── Tumbling ──────────────────────────────────────────────────────────────────

/// Fixed-size, non-overlapping event-time windows aligned to multiples of `size`.
#[derive(Clone)]
pub struct TumblingEventTimeWindows {
    size_ms: i64,
    offset_ms: i64,
}

impl TumblingEventTimeWindows {
    /// Create tumbling windows of the given `size`.
    pub fn of(size: Duration) -> Self {
        Self {
            size_ms: size.as_millis() as i64,
            offset_ms: 0,
        }
    }

    /// Create tumbling windows with a non-zero alignment `offset`.
    pub fn of_with_offset(size: Duration, offset: Duration) -> Self {
        Self {
            size_ms: size.as_millis() as i64,
            offset_ms: offset.as_millis() as i64,
        }
    }
}

impl<T: Send + Sync> WindowAssigner<T> for TumblingEventTimeWindows {
    type DefaultTrigger = EventTimeTrigger;

    fn assign_windows(&self, _element: &T, timestamp: EventTime) -> Vec<TimeWindow> {
        let start = timestamp - (timestamp - self.offset_ms).rem_euclid(self.size_ms);
        vec![TimeWindow::new(start, start + self.size_ms)]
    }

    fn default_trigger(&self) -> Self::DefaultTrigger {
        EventTimeTrigger
    }
}

// ── Sliding ───────────────────────────────────────────────────────────────────

/// Fixed-size, possibly overlapping event-time windows.
/// An element belongs to `ceil(size / slide)` windows.
#[derive(Clone)]
pub struct SlidingEventTimeWindows {
    size_ms: i64,
    slide_ms: i64,
    offset_ms: i64,
}

impl SlidingEventTimeWindows {
    /// Create sliding windows of the given `size` advancing every `slide`.
    pub fn of(size: Duration, slide: Duration) -> Self {
        Self {
            size_ms: size.as_millis() as i64,
            slide_ms: slide.as_millis() as i64,
            offset_ms: 0,
        }
    }
}

impl<T: Send + Sync> WindowAssigner<T> for SlidingEventTimeWindows {
    type DefaultTrigger = EventTimeTrigger;

    fn assign_windows(&self, _element: &T, timestamp: EventTime) -> Vec<TimeWindow> {
        // Mirrors Flink: walk back from last_start by slide until no window covers ts.
        let last_start = timestamp - (timestamp - self.offset_ms).rem_euclid(self.slide_ms);
        let mut windows = Vec::new();
        let mut start = last_start;
        while start > timestamp - self.size_ms {
            windows.push(TimeWindow::new(start, start + self.size_ms));
            start -= self.slide_ms;
        }
        windows
    }

    fn default_trigger(&self) -> Self::DefaultTrigger {
        EventTimeTrigger
    }
}

// ── Session ───────────────────────────────────────────────────────────────────

/// Gap-based windows: a new session starts whenever the gap between consecutive
/// events exceeds `gap`.  Each element initially gets a window
/// `[timestamp, timestamp + gap)`.  The `WindowOperator` merges overlapping
/// session windows as new elements arrive.
///
/// Note: Session window rescale is not supported in v1 (marked unsupported in P6).
#[derive(Clone)]
pub struct SessionWindows {
    gap_ms: i64,
}

impl SessionWindows {
    /// Create session windows with the given minimum `gap` between sessions.
    pub fn with_gap(gap: Duration) -> Self {
        Self {
            gap_ms: gap.as_millis() as i64,
        }
    }
}

impl<T: Send + Sync> WindowAssigner<T> for SessionWindows {
    type DefaultTrigger = EventTimeTrigger;

    fn assign_windows(&self, _element: &T, timestamp: EventTime) -> Vec<TimeWindow> {
        vec![TimeWindow::new(timestamp, timestamp + self.gap_ms)]
    }

    fn default_trigger(&self) -> Self::DefaultTrigger {
        EventTimeTrigger
    }

    fn is_merging(&self) -> bool {
        true
    }
}

// ── Global ────────────────────────────────────────────────────────────────────

/// A single window that spans all time.  Useful with custom triggers.
#[derive(Clone, Copy)]
pub struct GlobalWindows;

impl GlobalWindows {
    pub fn new() -> Self {
        Self
    }
}

impl Default for GlobalWindows {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync> WindowAssigner<T> for GlobalWindows {
    type DefaultTrigger = EventTimeTrigger;

    fn assign_windows(&self, _element: &T, _timestamp: EventTime) -> Vec<TimeWindow> {
        vec![TimeWindow::new(EVENT_TIME_MIN, EVENT_TIME_MAX)]
    }

    fn default_trigger(&self) -> Self::DefaultTrigger {
        EventTimeTrigger
    }
}
