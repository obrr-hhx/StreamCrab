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
    let result =
        <EventTimeTrigger as Trigger<(), TimeWindow>>::on_event_time(&mut trigger, 9_998, &window);
    assert_eq!(result, TriggerResult::Continue);
}

#[test]
fn test_event_time_trigger_fires_at_window_end() {
    let mut trigger = EventTimeTrigger;
    let window = TimeWindow::new(0, 10_000);

    // Watermark equals max_timestamp (9999ms) -> should fire and purge.
    let result =
        <EventTimeTrigger as Trigger<(), TimeWindow>>::on_event_time(&mut trigger, 9_999, &window);
    assert_eq!(result, TriggerResult::FireAndPurge);
}

#[test]
fn test_event_time_trigger_fires_past_window_end() {
    let mut trigger = EventTimeTrigger;
    let window = TimeWindow::new(0, 10_000);

    // Watermark is beyond max_timestamp.
    let result =
        <EventTimeTrigger as Trigger<(), TimeWindow>>::on_event_time(&mut trigger, 20_000, &window);
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

#[test]
fn test_window_operator_snapshot_restore() {
    let mut op = make_operator();

    op.process(StreamElement::timestamped_record(
        ("key".to_string(), 1i32),
        1_000,
    ))
    .unwrap();
    op.process(StreamElement::timestamped_record(
        ("key".to_string(), 2i32),
        2_000,
    ))
    .unwrap();
    assert_eq!(op.buffered_window_count(), 1);

    let snapshot = op.snapshot_state().unwrap();

    let mut restored = make_operator();
    restored.restore_state(&snapshot).unwrap();
    assert_eq!(restored.buffered_window_count(), 1);

    let out = restored
        .process(StreamElement::Watermark(crate::types::Watermark::new(
            9_999,
        )))
        .unwrap();
    let records: Vec<_> = out
        .iter()
        .filter_map(|e| match e {
            StreamElement::Record(r) => Some(r.value.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(records, vec![("key".to_string(), 3)]);
}
