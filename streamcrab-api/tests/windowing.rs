use std::time::Duration;

use streamcrab_api::environment::StreamExecutionEnvironment;
use streamcrab_core::time::BoundedOutOfOrderness;
use streamcrab_core::window::{
    AggregateFunction, SlidingEventTimeWindows, TimeWindow, Trigger, TriggerResult,
    TumblingEventTimeWindows,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Event {
    user: String,
    ts: i64,
    value: i32,
}

#[test]
fn test_event_time_tumbling_window_reduce_out_of_order_and_late_drop() {
    let env = StreamExecutionEnvironment::new("tumbling-window-reduce");

    // Window size: 10s. Out-of-orderness: 2s.
    // Watermark = max_seen_ts - 2s.
    let strategy = BoundedOutOfOrderness::new(Duration::from_secs(2), |e: &Event| e.ts);

    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 1_000,
            value: 1,
        },
        Event {
            user: "u1".to_string(),
            ts: 9_000,
            value: 2,
        },
        // Out of order but within the 2s delay (9_000 -> 8_000 is 1s).
        Event {
            user: "u1".to_string(),
            ts: 8_000,
            value: 3,
        },
        // This advances watermark to 10_000, closing the first window [0, 10_000).
        Event {
            user: "u1".to_string(),
            ts: 12_000,
            value: 10,
        },
        // Late element: watermark already >= 10_000, so ts=5_000 must be dropped.
        Event {
            user: "u1".to_string(),
            ts: 5_000,
            value: 1000,
        },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .reduce(|a, b| Event {
            user: a.user.clone(),
            ts: a.ts.max(b.ts),
            value: a.value + b.value,
        })
        .execute()
        .unwrap();

    // Expect two window results for u1:
    // - window [0, 10_000) => max_timestamp=9_999, sum=1+2+3=6
    // - window [10_000, 20_000) => max_timestamp=19_999, sum=10
    assert_eq!(out.len(), 2);

    let mut by_ts: Vec<_> = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value))
        .collect();
    by_ts.sort_by_key(|(ts, _)| *ts);

    assert_eq!(by_ts[0].0, 9_999);
    assert_eq!(by_ts[0].1.0, "u1".to_string());
    assert_eq!(by_ts[0].1.1.value, 6);

    assert_eq!(by_ts[1].0, 19_999);
    assert_eq!(by_ts[1].1.0, "u1".to_string());
    assert_eq!(by_ts[1].1.1.value, 10);
}

#[test]
fn test_out_of_order_sequence_t1_t5_t3_and_watermark6_behavior() {
    let env = StreamExecutionEnvironment::new("out-of-order-t1-t5-t3-wm6");

    // Sequence target:
    // (T=1), (T=5), (T=3), Watermark(6)
    // Here Watermark(6) is produced after T=9 with max_delay=3.
    let strategy = BoundedOutOfOrderness::new(Duration::from_millis(3), |e: &Event| e.ts);
    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 1,
            value: 1,
        },
        Event {
            user: "u1".to_string(),
            ts: 5,
            value: 5,
        },
        Event {
            user: "u1".to_string(),
            ts: 3,
            value: 3,
        },
        Event {
            user: "u1".to_string(),
            ts: 9,
            value: 9,
        },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_millis(5)))
        .reduce(|a, b| Event {
            user: a.user.clone(),
            ts: a.ts.max(b.ts),
            value: a.value + b.value,
        })
        .execute()
        .unwrap();

    let mut by_ts: Vec<_> = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value.1.value))
        .collect();
    by_ts.sort_by_key(|(ts, _)| *ts);

    // [0,5) fires when watermark reaches 6 => 1 + 3 = 4 (ts=4)
    // [5,10) flushed at final watermark => 5 + 9 = 14 (ts=9)
    assert_eq!(by_ts, vec![(4, 4), (9, 14)]);
}

// ── aggregate() test ──────────────────────────────────────────────────────────

/// Sum aggregator: accumulates value sum (ACC = i32, OUT = i32).
struct SumAgg;

impl AggregateFunction<Event, i32, i32> for SumAgg {
    fn create_accumulator(&self) -> i32 {
        0
    }
    fn add(&self, acc: &mut i32, element: &Event) {
        *acc += element.value;
    }
    fn get_result(&self, acc: i32) -> i32 {
        acc
    }
    fn merge(&self, acc: &mut i32, other: i32) {
        *acc += other;
    }
}

/// Average aggregator: ACC = (sum, count), OUT = f64.
struct AvgAgg;

impl AggregateFunction<Event, (i64, u32), f64> for AvgAgg {
    fn create_accumulator(&self) -> (i64, u32) {
        (0, 0)
    }
    fn add(&self, acc: &mut (i64, u32), element: &Event) {
        acc.0 += element.value as i64;
        acc.1 += 1;
    }
    fn get_result(&self, acc: (i64, u32)) -> f64 {
        if acc.1 == 0 {
            0.0
        } else {
            acc.0 as f64 / acc.1 as f64
        }
    }
    fn merge(&self, acc: &mut (i64, u32), other: (i64, u32)) {
        acc.0 += other.0;
        acc.1 += other.1;
    }
}

#[test]
fn test_event_time_tumbling_window_aggregate_sum() {
    let env = StreamExecutionEnvironment::new("tumbling-window-aggregate-sum");
    let strategy = BoundedOutOfOrderness::new(Duration::from_secs(2), |e: &Event| e.ts);

    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 1_000,
            value: 10,
        },
        Event {
            user: "u1".to_string(),
            ts: 5_000,
            value: 20,
        },
        // Advance watermark past 10_000 to close the first window [0, 10_000).
        Event {
            user: "u1".to_string(),
            ts: 15_000,
            value: 5,
        },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .aggregate(SumAgg)
        .execute()
        .unwrap();

    // Expect two windows:
    // - [0, 10_000) fired at watermark=13_000: sum = 10+20 = 30, ts = 9_999
    // - [10_000, 20_000) flushed at final watermark: sum = 5, ts = 19_999
    assert_eq!(out.len(), 2);

    let mut by_ts: Vec<_> = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value))
        .collect();
    by_ts.sort_by_key(|(ts, _)| *ts);

    assert_eq!(by_ts[0].0, 9_999);
    assert_eq!(by_ts[0].1.0, "u1");
    assert_eq!(by_ts[0].1.1, 30);

    assert_eq!(by_ts[1].0, 19_999);
    assert_eq!(by_ts[1].1.0, "u1");
    assert_eq!(by_ts[1].1.1, 5);
}

#[test]
fn test_event_time_tumbling_window_aggregate_avg() {
    let env = StreamExecutionEnvironment::new("tumbling-window-aggregate-avg");
    let strategy = BoundedOutOfOrderness::new(Duration::from_secs(1), |e: &Event| e.ts);

    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 1_000,
            value: 10,
        },
        Event {
            user: "u1".to_string(),
            ts: 3_000,
            value: 20,
        },
        Event {
            user: "u1".to_string(),
            ts: 7_000,
            value: 30,
        },
        // Advance watermark past 10_000.
        Event {
            user: "u1".to_string(),
            ts: 12_000,
            value: 0,
        },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .aggregate(AvgAgg)
        .execute()
        .unwrap();

    // Window [0, 10_000): avg(10, 20, 30) = 20.0, ts = 9_999
    assert_eq!(out.len(), 2);

    let mut by_ts: Vec<_> = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value))
        .collect();
    by_ts.sort_by_key(|(ts, _)| *ts);

    assert_eq!(by_ts[0].0, 9_999);
    assert_eq!(by_ts[0].1.0, "u1");
    assert!((by_ts[0].1.1 - 20.0_f64).abs() < 1e-9);
}

#[derive(Clone)]
struct FireOnElementTrigger;

impl Trigger<Event, TimeWindow> for FireOnElementTrigger {
    fn on_element(
        &mut self,
        _element: &Event,
        _timestamp: i64,
        _window: &TimeWindow,
    ) -> TriggerResult {
        TriggerResult::FireAndPurge
    }

    fn on_event_time(&mut self, _event_time: i64, _window: &TimeWindow) -> TriggerResult {
        TriggerResult::Continue
    }
}

#[test]
fn test_custom_trigger_fire_on_each_element() {
    let env = StreamExecutionEnvironment::new("window-custom-trigger");
    let strategy = BoundedOutOfOrderness::new(Duration::from_secs(1), |e: &Event| e.ts);

    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 1_000,
            value: 10,
        },
        Event {
            user: "u1".to_string(),
            ts: 2_000,
            value: 20,
        },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .trigger(FireOnElementTrigger)
        .reduce(|a, b| Event {
            user: a.user.clone(),
            ts: a.ts.max(b.ts),
            value: a.value + b.value,
        })
        .execute()
        .unwrap();

    assert_eq!(out.len(), 2);

    let mut values: Vec<i32> = out.into_iter().map(|r| r.value.1.value).collect();
    values.sort();
    assert_eq!(values, vec![10, 20]);
}

#[test]
fn test_window_execute_with_parallelism_matches_single_thread() {
    let env = StreamExecutionEnvironment::new("window-parallel-consistency");
    let strategy = BoundedOutOfOrderness::new(Duration::from_secs(2), |e: &Event| e.ts);

    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 1_000,
            value: 1,
        },
        Event {
            user: "u2".to_string(),
            ts: 2_000,
            value: 2,
        },
        Event {
            user: "u1".to_string(),
            ts: 9_000,
            value: 3,
        },
        Event {
            user: "u2".to_string(),
            ts: 11_000,
            value: 4,
        },
        Event {
            user: "u1".to_string(),
            ts: 12_000,
            value: 5,
        },
        Event {
            user: "u2".to_string(),
            ts: 18_000,
            value: 6,
        },
    ];

    let single = env
        .from_iter(events.clone())
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .reduce(|a, b| Event {
            user: a.user.clone(),
            ts: a.ts.max(b.ts),
            value: a.value + b.value,
        })
        .execute()
        .unwrap();

    let strategy = BoundedOutOfOrderness::new(Duration::from_secs(2), |e: &Event| e.ts);
    let parallel = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .reduce(|a, b| Event {
            user: a.user.clone(),
            ts: a.ts.max(b.ts),
            value: a.value + b.value,
        })
        .execute_with_parallelism(2)
        .unwrap();

    let mut single_norm: Vec<(i64, String, i32)> = single
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value.0, r.value.1.value))
        .collect();
    single_norm.sort();

    let mut parallel_norm: Vec<(i64, String, i32)> = parallel
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value.0, r.value.1.value))
        .collect();
    parallel_norm.sort();

    assert_eq!(single_norm, parallel_norm);
}

#[test]
fn test_event_time_sliding_window_multi_membership() {
    let env = StreamExecutionEnvironment::new("sliding-window-multi-membership");
    let strategy = BoundedOutOfOrderness::new(Duration::ZERO, |e: &Event| e.ts);

    let events = vec![
        Event {
            user: "u1".to_string(),
            ts: 7_000,
            value: 1,
        },
        Event {
            user: "u1".to_string(),
            ts: 12_000,
            value: 2,
        },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &Event| e.user.clone())
        .window(SlidingEventTimeWindows::of(
            Duration::from_secs(10),
            Duration::from_secs(5),
        ))
        .reduce(|a, b| Event {
            user: a.user.clone(),
            ts: a.ts.max(b.ts),
            value: a.value + b.value,
        })
        .execute()
        .unwrap();

    // For ts=7000 and ts=12000 with size=10s, slide=5s:
    // [0,10s) -> 1
    // [5s,15s) -> 1+2 = 3
    // [10s,20s) -> 2
    let mut by_ts: Vec<(i64, i32)> = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value.1.value))
        .collect();
    by_ts.sort();
    assert_eq!(by_ts, vec![(9_999, 1), (14_999, 3), (19_999, 2)]);
}
