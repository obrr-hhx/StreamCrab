use super::*;

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
