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
