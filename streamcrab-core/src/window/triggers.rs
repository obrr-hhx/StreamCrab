use super::*;

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
