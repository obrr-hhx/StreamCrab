use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use anyhow::Result;

use crate::time::{EVENT_TIME_MAX, EVENT_TIME_MIN, TimerService};
use crate::types::{EventTime, StreamData, StreamElement};

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

// ── Trigger ───────────────────────────────────────────────────────────────────

/// The result returned by a [`Trigger`] to control window evaluation.
///
/// Mirrors Flink's `TriggerResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerResult {
    /// Keep accumulating elements; do not fire the window yet.
    Continue,
    /// Evaluate the window function and emit results; keep the window state.
    Fire,
    /// Discard window state without emitting any result.
    Purge,
    /// Evaluate the window function, emit results, then discard state.
    FireAndPurge,
}

impl TriggerResult {
    /// Return true if the window function should be evaluated.
    pub fn is_fire(self) -> bool {
        matches!(self, TriggerResult::Fire | TriggerResult::FireAndPurge)
    }

    /// Return true if the window state should be discarded after this result.
    pub fn is_purge(self) -> bool {
        matches!(self, TriggerResult::Purge | TriggerResult::FireAndPurge)
    }
}

/// Determines when a window should be evaluated (fired) and when its state
/// should be discarded (purged).
///
/// Called by `WindowOperator` on two event-time paths:
/// - When an element arrives (`on_element`)
/// - When event time advances (`on_event_time`)
pub trait Trigger<T, W>: Send {
    /// Called for every element assigned to a window.
    fn on_element(&mut self, element: &T, timestamp: EventTime, window: &W) -> TriggerResult;

    /// Called when event time (watermark) advances.
    fn on_event_time(&mut self, event_time: EventTime, window: &W) -> TriggerResult;

    /// Called when processing time advances.
    ///
    /// Default implementation does nothing.
    fn on_processing_time(&mut self, _processing_time: EventTime, _window: &W) -> TriggerResult {
        TriggerResult::Continue
    }

    /// Backward-compatible alias for old watermark naming.
    fn on_watermark(&mut self, watermark: EventTime, window: &W) -> TriggerResult {
        self.on_event_time(watermark, window)
    }
}

impl<T, W> Trigger<T, W> for Box<dyn Trigger<T, W>> {
    fn on_element(&mut self, element: &T, timestamp: EventTime, window: &W) -> TriggerResult {
        self.as_mut().on_element(element, timestamp, window)
    }

    fn on_event_time(&mut self, event_time: EventTime, window: &W) -> TriggerResult {
        self.as_mut().on_event_time(event_time, window)
    }

    fn on_processing_time(&mut self, processing_time: EventTime, window: &W) -> TriggerResult {
        self.as_mut().on_processing_time(processing_time, window)
    }
}

// ── EventTimeTrigger ──────────────────────────────────────────────────────────

/// The default trigger for event-time windowing.
///
/// Fires when the watermark passes the window's maximum timestamp
/// (`window.end - 1`).  After firing it also purges the window, because
/// late elements are not supported in v1.
#[derive(Clone, Default)]
pub struct EventTimeTrigger;

impl<T: Send, W: Send + Sync> Trigger<T, W> for EventTimeTrigger
where
    W: AsRef<TimeWindow>,
{
    fn on_element(&mut self, _element: &T, _timestamp: EventTime, _window: &W) -> TriggerResult {
        // No per-element action needed; the watermark timer drives firing.
        TriggerResult::Continue
    }

    fn on_event_time(&mut self, event_time: EventTime, window: &W) -> TriggerResult {
        // Fire (and purge) as soon as watermark covers the entire window.
        if event_time >= window.as_ref().max_timestamp() {
            TriggerResult::FireAndPurge
        } else {
            TriggerResult::Continue
        }
    }
}

/// Processing-time trigger.
///
/// Fires when processing time reaches the window max timestamp.
#[derive(Clone, Default)]
pub struct ProcessingTimeTrigger;

impl<T: Send, W: Send + Sync> Trigger<T, W> for ProcessingTimeTrigger
where
    W: AsRef<TimeWindow>,
{
    fn on_element(&mut self, _element: &T, _timestamp: EventTime, _window: &W) -> TriggerResult {
        TriggerResult::Continue
    }

    fn on_event_time(&mut self, _event_time: EventTime, _window: &W) -> TriggerResult {
        TriggerResult::Continue
    }

    fn on_processing_time(&mut self, processing_time: EventTime, window: &W) -> TriggerResult {
        if processing_time >= window.as_ref().max_timestamp() {
            TriggerResult::FireAndPurge
        } else {
            TriggerResult::Continue
        }
    }
}

/// Allow `TimeWindow` to act as its own `AsRef<TimeWindow>`.
impl AsRef<TimeWindow> for TimeWindow {
    fn as_ref(&self) -> &TimeWindow {
        self
    }
}

// ── WindowFunction ────────────────────────────────────────────────────────────

/// Evaluates a window by processing all buffered elements at once.
///
/// Used when the computation requires the full element list (e.g. sort, percentile).
/// For incremental aggregation prefer [`AggregateFunction`].
pub trait WindowFunction<K, IN, OUT>: Send {
    /// Called when a window fires.
    ///
    /// - `key`: the group key
    /// - `window`: the time window that fired
    /// - `elements`: all records buffered for this (key, window) pair
    /// - `output`: push computed results here
    fn apply(&mut self, key: &K, window: &TimeWindow, elements: &[IN], output: &mut Vec<OUT>);
}

// ── AggregateFunction ─────────────────────────────────────────────────────────

/// Incremental aggregation function.
///
/// Unlike [`WindowFunction`], the accumulator is updated on each incoming
/// element, so only `O(1)` state is kept per window instead of `O(n)`.
///
/// Mirrors Flink's `AggregateFunction<IN, ACC, OUT>`.
pub trait AggregateFunction<IN, ACC, OUT>: Send {
    /// Create a fresh accumulator for a new window.
    fn create_accumulator(&self) -> ACC;
    /// Fold one element into the accumulator.
    fn add(&self, acc: &mut ACC, element: &IN);
    /// Convert the final accumulator into the window result.
    fn get_result(&self, acc: ACC) -> OUT;
    /// Merge two accumulators (used in session window merging, v2).
    fn merge(&self, acc: &mut ACC, other: ACC);
}

// ── WindowOperator ────────────────────────────────────────────────────────────

/// Core windowing operator.
///
/// Accepts [`StreamElement<T>`] items (records + watermarks) and emits
/// [`StreamElement<OUT>`] items when windows fire.
///
/// # Processing model
///
/// - **Records**: assigned to one or more windows by the `WindowAssigner`,
///   then buffered in memory per `(key, window)`.
/// - **Watermarks**: trigger all windows whose `max_timestamp < watermark`;
///   fired windows are evaluated by `WindowFunction` and then purged.
///   The watermark is re-emitted downstream unchanged.
pub struct WindowOperator<K, T, OUT, KF, TF, WA, TR, WFN>
where
    K: StreamData,
    T: StreamData,
    OUT: StreamData,
    KF: Fn(&T) -> K + Send,
    TF: Fn(&T) -> EventTime + Send,
    WA: WindowAssigner<T>,
    TR: Trigger<T, TimeWindow>,
    WFN: WindowFunction<K, T, OUT>,
{
    key_fn: KF,
    timestamp_fn: TF,
    assigner: WA,
    trigger: TR,
    window_fn: WFN,
    /// Buffered elements: (key_bytes, window) -> (original_key, elements).
    /// key_bytes is used as the HashMap key to allow O(1) lookup.
    /// The original key is kept alongside to avoid deserialization.
    buffers: HashMap<(Vec<u8>, TimeWindow), (K, Vec<T>)>,
    /// Event-time timers used to drive trigger callbacks.
    timer_service: TimerService,
    current_watermark: EventTime,
    _phantom: PhantomData<OUT>,
}

impl<K, T, OUT, KF, TF, WA, TR, WFN> WindowOperator<K, T, OUT, KF, TF, WA, TR, WFN>
where
    K: StreamData,
    T: StreamData,
    OUT: StreamData,
    KF: Fn(&T) -> K + Send,
    TF: Fn(&T) -> EventTime + Send,
    WA: WindowAssigner<T>,
    TR: Trigger<T, TimeWindow>,
    WFN: WindowFunction<K, T, OUT>,
{
    /// Create a new `WindowOperator`.
    ///
    /// - `key_fn`: extracts the grouping key from each element
    /// - `timestamp_fn`: extracts the event-time timestamp from each element
    /// - `assigner`: assigns windows (Tumbling / Sliding / Session / Global)
    /// - `trigger`: controls when windows fire/purge
    /// - `window_fn`: computes the result when a window fires
    pub fn new(key_fn: KF, timestamp_fn: TF, assigner: WA, trigger: TR, window_fn: WFN) -> Self {
        Self {
            key_fn,
            timestamp_fn,
            assigner,
            trigger,
            window_fn,
            buffers: HashMap::new(),
            timer_service: TimerService::new(),
            current_watermark: EVENT_TIME_MIN,
            _phantom: PhantomData,
        }
    }

    fn register_event_time_timer(&mut self, map_key: &(Vec<u8>, TimeWindow)) -> Result<()> {
        let timer_key = bincode::serialize(map_key)?;
        self.timer_service
            .register(timer_key, map_key.1.max_timestamp());
        Ok(())
    }

    fn delete_event_time_timer(&mut self, map_key: &(Vec<u8>, TimeWindow)) -> Result<()> {
        let timer_key = bincode::serialize(map_key)?;
        self.timer_service
            .delete(&timer_key, map_key.1.max_timestamp());
        Ok(())
    }

    fn apply_trigger_result(
        &mut self,
        map_key: (Vec<u8>, TimeWindow),
        trigger_result: TriggerResult,
        output: &mut Vec<StreamElement<OUT>>,
    ) -> Result<()> {
        if trigger_result.is_fire() {
            if let Some((orig_key, elements)) = self.buffers.remove(&map_key) {
                let mut out_values: Vec<OUT> = Vec::new();
                self.window_fn
                    .apply(&orig_key, &map_key.1, &elements, &mut out_values);
                for val in out_values {
                    output.push(StreamElement::timestamped_record(
                        val,
                        map_key.1.max_timestamp(),
                    ));
                }

                if !trigger_result.is_purge() {
                    self.buffers.insert(map_key, (orig_key, elements));
                } else {
                    self.delete_event_time_timer(&map_key)?;
                }
            }
            return Ok(());
        }

        if trigger_result.is_purge() {
            self.buffers.remove(&map_key);
            self.delete_event_time_timer(&map_key)?;
        }
        Ok(())
    }

    /// Fire due event-time timers at `event_time`.
    ///
    /// This is the explicit timer callback path:
    /// - drains due timers
    /// - calls `trigger.on_event_time`
    /// - applies fire/purge and emits window results
    pub fn on_timer(&mut self, event_time: EventTime) -> Result<Vec<StreamElement<OUT>>> {
        self.current_watermark = self.current_watermark.max(event_time);

        let mut trigger_results: Vec<((Vec<u8>, TimeWindow), TriggerResult)> = Vec::new();
        for (timer_key, fire_at) in self.timer_service.drain_due(event_time) {
            let map_key: (Vec<u8>, TimeWindow) = bincode::deserialize(&timer_key)?;
            let result = self.trigger.on_event_time(fire_at, &map_key.1);
            trigger_results.push((map_key, result));
        }

        let mut output: Vec<StreamElement<OUT>> = Vec::new();
        for (map_key, trigger_result) in trigger_results {
            self.apply_trigger_result(map_key, trigger_result, &mut output)?;
        }
        Ok(output)
    }

    /// Process one stream element and return any window results produced.
    ///
    /// - Records are buffered; output is empty unless a trigger fires immediately.
    /// - Watermarks advance event time, firing all expired windows and then
    ///   re-emitting the watermark so downstream operators stay in sync.
    pub fn process(&mut self, elem: StreamElement<T>) -> Result<Vec<StreamElement<OUT>>> {
        match elem {
            StreamElement::Record(rec) => {
                let key = (self.key_fn)(&rec.value);
                let key_bytes = bincode::serialize(&key)?;
                // Use timestamp from record, fall back to timestamp_fn.
                let ts = rec
                    .timestamp
                    .unwrap_or_else(|| (self.timestamp_fn)(&rec.value));
                let windows = self.assigner.assign_windows(&rec.value, ts);
                let mut output = Vec::new();
                for window in windows {
                    let map_key = (key_bytes.clone(), window.clone());
                    let entry = self
                        .buffers
                        .entry(map_key.clone())
                        .or_insert_with(|| (key.clone(), Vec::new()));
                    entry.1.push(rec.value.clone());

                    // Default event-time semantics: register timer at window.max_timestamp().
                    self.register_event_time_timer(&map_key)?;

                    let trigger_result = self.trigger.on_element(&rec.value, ts, &window);
                    self.apply_trigger_result(map_key, trigger_result, &mut output)?;
                }
                Ok(output)
            }

            StreamElement::Watermark(wm) => {
                self.current_watermark = wm.timestamp;
                let mut output = self.on_timer(wm.timestamp)?;

                // Re-emit the watermark downstream so the pipeline keeps advancing.
                output.push(StreamElement::Watermark(wm));
                Ok(output)
            }

            StreamElement::CheckpointBarrier(b) => Ok(vec![StreamElement::CheckpointBarrier(b)]),

            StreamElement::End => Ok(vec![StreamElement::End]),
        }
    }

    /// Trigger processing-time callbacks for all active windows.
    pub fn on_processing_time(
        &mut self,
        processing_time: EventTime,
    ) -> Result<Vec<StreamElement<OUT>>> {
        let trigger_results: Vec<((Vec<u8>, TimeWindow), TriggerResult)> = self
            .buffers
            .keys()
            .map(|map_key| {
                (
                    map_key.clone(),
                    self.trigger.on_processing_time(processing_time, &map_key.1),
                )
            })
            .collect();

        let mut output: Vec<StreamElement<OUT>> = Vec::new();
        for (map_key, trigger_result) in trigger_results {
            self.apply_trigger_result(map_key, trigger_result, &mut output)?;
        }
        Ok(output)
    }

    /// Return the number of currently buffered (key, window) pairs.
    pub fn buffered_window_count(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── TimeWindow ────────────────────────────────────────────────────────────

    #[test]
    fn test_time_window_contains() {
        let w = TimeWindow::new(0, 10_000);
        assert!(w.contains(0));
        assert!(w.contains(5_000));
        assert!(!w.contains(10_000)); // end is exclusive
    }

    #[test]
    fn test_time_window_max_timestamp() {
        let w = TimeWindow::new(0, 10_000);
        assert_eq!(w.max_timestamp(), 9_999);
    }

    // ── Tumbling ──────────────────────────────────────────────────────────────

    #[test]
    fn test_tumbling_assigns_correct_window() {
        let assigner = TumblingEventTimeWindows::of(Duration::from_secs(10));
        // timestamp 3s falls in [0, 10s)
        let wins = assigner.assign_windows(&(), 3_000);
        assert_eq!(wins, vec![TimeWindow::new(0, 10_000)]);
        // timestamp 10s falls in [10s, 20s)
        let wins = assigner.assign_windows(&(), 10_000);
        assert_eq!(wins, vec![TimeWindow::new(10_000, 20_000)]);
    }

    #[test]
    fn test_tumbling_single_window_per_element() {
        let assigner = TumblingEventTimeWindows::of(Duration::from_secs(5));
        let wins = assigner.assign_windows(&(), 7_000);
        assert_eq!(wins.len(), 1);
        assert!(wins[0].contains(7_000));
    }

    // ── Sliding ───────────────────────────────────────────────────────────────

    #[test]
    fn test_sliding_element_in_multiple_windows() {
        // size=10s, slide=5s -> each element belongs to 2 windows
        let assigner = SlidingEventTimeWindows::of(Duration::from_secs(10), Duration::from_secs(5));
        let wins = assigner.assign_windows(&(), 7_000);
        assert_eq!(wins.len(), 2);
        for w in &wins {
            assert!(w.contains(7_000), "{w} should contain 7000ms");
        }
    }

    #[test]
    fn test_sliding_windows_cover_timestamp() {
        // size=15s, slide=5s -> each element belongs to 3 windows
        let assigner = SlidingEventTimeWindows::of(Duration::from_secs(15), Duration::from_secs(5));
        let wins = assigner.assign_windows(&(), 12_000);
        assert_eq!(wins.len(), 3);
    }

    // ── Session ───────────────────────────────────────────────────────────────

    #[test]
    fn test_session_assigns_gap_window() {
        let assigner = SessionWindows::with_gap(Duration::from_secs(5));
        let wins = assigner.assign_windows(&(), 10_000);
        assert_eq!(wins, vec![TimeWindow::new(10_000, 15_000)]);
    }

    #[test]
    fn test_session_is_merging() {
        let assigner = SessionWindows::with_gap(Duration::from_secs(5));
        // Use UFCS to supply the type parameter T explicitly.
        assert!(<SessionWindows as WindowAssigner<()>>::is_merging(
            &assigner
        ));
        // Tumbling must NOT be merging.
        let tumbling = TumblingEventTimeWindows::of(Duration::from_secs(10));
        assert!(!<TumblingEventTimeWindows as WindowAssigner<()>>::is_merging(&tumbling));
    }

    // ── Global ────────────────────────────────────────────────────────────────

    #[test]
    fn test_global_single_all_time_window() {
        let assigner = GlobalWindows::new();
        let wins = assigner.assign_windows(&(), 999_999_999);
        assert_eq!(wins.len(), 1);
        assert_eq!(wins[0], TimeWindow::new(EVENT_TIME_MIN, EVENT_TIME_MAX));
    }

    // ── TriggerResult ─────────────────────────────────────────────────────────

    #[test]
    fn test_trigger_result_is_fire_and_purge() {
        assert!(!TriggerResult::Continue.is_fire());
        assert!(!TriggerResult::Continue.is_purge());
        assert!(TriggerResult::Fire.is_fire());
        assert!(!TriggerResult::Fire.is_purge());
        assert!(!TriggerResult::Purge.is_fire());
        assert!(TriggerResult::Purge.is_purge());
        assert!(TriggerResult::FireAndPurge.is_fire());
        assert!(TriggerResult::FireAndPurge.is_purge());
    }

    // ── EventTimeTrigger ──────────────────────────────────────────────────────

    #[test]
    fn test_event_time_trigger_continues_before_window_end() {
        let mut trigger = EventTimeTrigger;
        let window = TimeWindow::new(0, 10_000);

        // Watermark has not yet covered the window's max timestamp (9999ms).
        // Use UFCS to supply T explicitly.
        let result = <EventTimeTrigger as Trigger<(), TimeWindow>>::on_event_time(
            &mut trigger,
            9_998,
            &window,
        );
        assert_eq!(result, TriggerResult::Continue);
    }

    #[test]
    fn test_event_time_trigger_fires_at_window_end() {
        let mut trigger = EventTimeTrigger;
        let window = TimeWindow::new(0, 10_000);

        // Watermark equals max_timestamp (9999ms) -> should fire and purge.
        let result = <EventTimeTrigger as Trigger<(), TimeWindow>>::on_event_time(
            &mut trigger,
            9_999,
            &window,
        );
        assert_eq!(result, TriggerResult::FireAndPurge);
    }

    #[test]
    fn test_event_time_trigger_fires_past_window_end() {
        let mut trigger = EventTimeTrigger;
        let window = TimeWindow::new(0, 10_000);

        // Watermark is beyond max_timestamp.
        let result = <EventTimeTrigger as Trigger<(), TimeWindow>>::on_event_time(
            &mut trigger,
            20_000,
            &window,
        );
        assert_eq!(result, TriggerResult::FireAndPurge);
    }

    #[test]
    fn test_event_time_trigger_on_element_always_continues() {
        let mut trigger = EventTimeTrigger;
        let window = TimeWindow::new(0, 10_000);

        // on_element should never trigger a fire; firing is driven by watermarks.
        let result = <EventTimeTrigger as Trigger<(), TimeWindow>>::on_element(
            &mut trigger,
            &(),
            5_000,
            &window,
        );
        assert_eq!(result, TriggerResult::Continue);
    }

    #[test]
    fn test_processing_time_trigger_fires_at_window_end() {
        let mut trigger = ProcessingTimeTrigger;
        let window = TimeWindow::new(0, 10_000);

        let before = <ProcessingTimeTrigger as Trigger<(), TimeWindow>>::on_processing_time(
            &mut trigger,
            9_998,
            &window,
        );
        assert_eq!(before, TriggerResult::Continue);

        let at_end = <ProcessingTimeTrigger as Trigger<(), TimeWindow>>::on_processing_time(
            &mut trigger,
            9_999,
            &window,
        );
        assert_eq!(at_end, TriggerResult::FireAndPurge);
    }

    // ── WindowOperator ────────────────────────────────────────────────────────

    /// Helper: count-per-window WindowFunction used in tests.
    struct CountWindowFn;

    impl WindowFunction<String, (String, i32), (String, i32)> for CountWindowFn {
        fn apply(
            &mut self,
            key: &String,
            _window: &TimeWindow,
            elements: &[(String, i32)],
            output: &mut Vec<(String, i32)>,
        ) {
            let sum: i32 = elements.iter().map(|(_, v)| v).sum();
            output.push((key.clone(), sum));
        }
    }

    fn make_operator() -> WindowOperator<
        String,
        (String, i32),
        (String, i32),
        impl Fn(&(String, i32)) -> String + Send,
        impl Fn(&(String, i32)) -> EventTime + Send,
        TumblingEventTimeWindows,
        EventTimeTrigger,
        CountWindowFn,
    > {
        WindowOperator::new(
            |(k, _): &(String, i32)| k.clone(),
            |(_, _): &(String, i32)| 0i64, // timestamp extracted from record, not used directly
            TumblingEventTimeWindows::of(Duration::from_secs(10)),
            EventTimeTrigger,
            CountWindowFn,
        )
    }

    #[test]
    fn test_window_operator_buffers_records_no_output() {
        let mut op = make_operator();

        // Send a record with timestamp 5s (inside [0, 10s) window).
        let elem = StreamElement::timestamped_record(("hello".to_string(), 1i32), 5_000);
        let out = op.process(elem).unwrap();

        // No watermark yet, so no window fires.
        assert!(out.is_empty());
        assert_eq!(op.buffered_window_count(), 1);
    }

    #[test]
    fn test_window_operator_fires_on_watermark() {
        let mut op = make_operator();

        // Three records in the [0, 10s) tumbling window.
        for v in [1i32, 2, 3] {
            let e = StreamElement::timestamped_record(("key".to_string(), v), 3_000);
            op.process(e).unwrap();
        }
        assert_eq!(op.buffered_window_count(), 1);

        // Watermark at 9999ms (== max_timestamp of [0, 10s)) triggers the window.
        let wm = StreamElement::Watermark(crate::types::Watermark::new(9_999));
        let out = op.process(wm).unwrap();

        // Expect: one result record + one watermark forwarded.
        let records: Vec<_> = out
            .iter()
            .filter_map(|e| {
                if let StreamElement::Record(r) = e {
                    Some(r.value.clone())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(records, vec![("key".to_string(), 6i32)]);

        // Window is purged after firing.
        assert_eq!(op.buffered_window_count(), 0);

        // Watermark must be forwarded downstream.
        let has_wm = out.iter().any(|e| matches!(e, StreamElement::Watermark(_)));
        assert!(has_wm, "watermark must be re-emitted downstream");
    }

    #[test]
    fn test_window_operator_on_timer_fires_without_forwarding_watermark() {
        let mut op = make_operator();

        // Three records in [0, 10s) window.
        for v in [1i32, 2, 3] {
            op.process(StreamElement::timestamped_record(
                ("key".to_string(), v),
                3_000,
            ))
            .unwrap();
        }
        assert_eq!(op.buffered_window_count(), 1);

        let out = op.on_timer(9_999).unwrap();
        let records: Vec<_> = out
            .iter()
            .filter_map(|e| match e {
                StreamElement::Record(r) => Some(r.value.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(records, vec![("key".to_string(), 6i32)]);
        assert!(
            out.iter()
                .all(|e| !matches!(e, StreamElement::Watermark(_))),
            "on_timer should only emit timer-driven outputs"
        );
        assert_eq!(op.buffered_window_count(), 0);
    }

    #[test]
    fn test_window_operator_multiple_keys_separate_windows() {
        let mut op = make_operator();

        // Two different keys in the same tumbling window.
        op.process(StreamElement::timestamped_record(
            ("a".to_string(), 10i32),
            1_000,
        ))
        .unwrap();
        op.process(StreamElement::timestamped_record(
            ("b".to_string(), 20i32),
            2_000,
        ))
        .unwrap();
        assert_eq!(op.buffered_window_count(), 2);

        let wm = StreamElement::Watermark(crate::types::Watermark::new(9_999));
        let out = op.process(wm).unwrap();

        let mut sums: Vec<(String, i32)> = out
            .iter()
            .filter_map(|e| {
                if let StreamElement::Record(r) = e {
                    Some(r.value.clone())
                } else {
                    None
                }
            })
            .collect();
        sums.sort();
        assert_eq!(sums, vec![("a".to_string(), 10), ("b".to_string(), 20)]);
        assert_eq!(op.buffered_window_count(), 0);
    }

    #[test]
    fn test_window_operator_watermark_forwarded_on_no_fire() {
        let mut op = make_operator();

        // No records; watermark should still be forwarded.
        let wm = StreamElement::Watermark(crate::types::Watermark::new(5_000));
        let out = op.process(wm).unwrap();

        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], StreamElement::Watermark(_)));
    }

    #[test]
    fn test_window_operator_out_of_order_t1_t5_t3_with_watermark6() {
        // Use 5ms windows so [1, 3] fall into [0, 5) and 5 falls into [5, 10).
        let mut op = WindowOperator::new(
            |(k, _): &(String, i32)| k.clone(),
            |_: &(String, i32)| 0i64,
            TumblingEventTimeWindows::of(Duration::from_millis(5)),
            EventTimeTrigger,
            CountWindowFn,
        );

        op.process(StreamElement::timestamped_record(("k".to_string(), 1), 1))
            .unwrap();
        op.process(StreamElement::timestamped_record(("k".to_string(), 5), 5))
            .unwrap();
        op.process(StreamElement::timestamped_record(("k".to_string(), 3), 3))
            .unwrap();

        let out = op
            .process(StreamElement::Watermark(crate::types::Watermark::new(6)))
            .unwrap();

        let records: Vec<_> = out
            .iter()
            .filter_map(|e| match e {
                StreamElement::Record(r) => Some(r.value.clone()),
                _ => None,
            })
            .collect();

        // Only [0, 5) should fire at watermark=6 => 1 + 3 = 4.
        assert_eq!(records, vec![("k".to_string(), 4)]);
        assert_eq!(
            op.buffered_window_count(),
            1,
            "window [5, 10) should still be buffered"
        );
        assert!(out.iter().any(|e| matches!(e, StreamElement::Watermark(_))));
    }
}
