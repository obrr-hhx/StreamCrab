use super::*;

/// A half-open event-time window `[start, end)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TimeWindow {
    pub start: EventTime,
    pub end: EventTime,
}

impl TimeWindow {
    pub fn new(start: EventTime, end: EventTime) -> Self {
        Self { start, end }
    }

    /// The maximum timestamp that belongs to this window.
    /// Used by triggers: a window fires when watermark >= max_timestamp().
    pub fn max_timestamp(&self) -> EventTime {
        self.end - 1
    }

    /// Return true if `timestamp` falls inside this window.
    pub fn contains(&self, timestamp: EventTime) -> bool {
        timestamp >= self.start && timestamp < self.end
    }
}

impl std::fmt::Display for TimeWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TimeWindow([{}, {}))", self.start, self.end)
    }
}

/// Assigns one or more [`TimeWindow`]s to each stream element based on its
/// event-time timestamp.
///
/// The four standard assigners mirror Apache Flink:
/// - [`TumblingEventTimeWindows`] — fixed-size, non-overlapping
/// - [`SlidingEventTimeWindows`]  — fixed-size, possibly overlapping
/// - [`SessionWindows`]           — gap-based, merging
/// - [`GlobalWindows`]            — single window for all elements
pub trait WindowAssigner<T>: Send + Sync {
    /// Default trigger type for this assigner.
    type DefaultTrigger: Trigger<T, TimeWindow> + Clone + Send + 'static;

    /// Return the windows that contain the element with the given timestamp.
    fn assign_windows(&self, element: &T, timestamp: EventTime) -> Vec<TimeWindow>;

    /// Create the default trigger for this assigner.
    fn default_trigger(&self) -> Self::DefaultTrigger;

    /// Whether this assigner produces windows that must be merged (Session only).
    fn is_merging(&self) -> bool {
        false
    }
}
