use super::*;

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
