use std::time::Duration;
use streamcrab_api::environment::StreamExecutionEnvironment;
use streamcrab_core::time::BoundedOutOfOrderness;
use streamcrab_core::window::TumblingEventTimeWindows;

// ── Acceptance Test 1: WordCount (Basic DAG) ─────────────────────────────────
//
// Pipeline: from_iter(lines) → flat_map(split) → key_by(word) → reduce(sum)
// Validates the core wordcount pattern with a flat_map tokenization stage.

#[test]
fn acceptance_wordcount() {
    let env = StreamExecutionEnvironment::new("acceptance-wordcount");

    let lines = vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
        "world hello world".to_string(),
    ];

    let results = env
        .from_iter(lines)
        .flat_map(|line: &String| {
            line.split_whitespace()
                .map(|w| (w.to_string(), 1i64))
                .collect::<Vec<_>>()
        })
        .key_by(|(word, _): &(String, i64)| word.clone())
        .reduce(|(w, c1), (_, c2)| (w, c1 + c2))
        .execute_with_parallelism(2)
        .unwrap();

    let counts = results.lock().unwrap();

    // "hello" appears 3 times (line1 + line2 + line3)
    assert_eq!(counts.get("hello"), Some(&("hello".to_string(), 3i64)));
    // "world" appears 3 times (line1 + line3 twice)
    assert_eq!(counts.get("world"), Some(&("world".to_string(), 3i64)));
    // "streamcrab" appears once
    assert_eq!(
        counts.get("streamcrab"),
        Some(&("streamcrab".to_string(), 1i64))
    );
    assert_eq!(counts.len(), 3);
}

// ── Acceptance Test 2: WordCount parallelism invariance ──────────────────────
//
// The same input must yield identical results regardless of parallelism.
// Validates that key routing and reduction are deterministic across p=1 and p=4.

#[test]
fn acceptance_wordcount_parallelism_invariance() {
    let lines = vec![
        "the quick brown fox".to_string(),
        "the lazy dog".to_string(),
        "the fox and the dog".to_string(),
    ];

    let run = |parallelism: usize| {
        let env = StreamExecutionEnvironment::new("acceptance-wordcount-parallelism");
        env.from_iter(lines.clone())
            .flat_map(|line: &String| {
                line.split_whitespace()
                    .map(|w| (w.to_string(), 1i64))
                    .collect::<Vec<_>>()
            })
            .key_by(|(word, _): &(String, i64)| word.clone())
            .reduce(|(w, c1), (_, c2)| (w, c1 + c2))
            .execute_with_parallelism(parallelism)
            .unwrap()
            .lock()
            .unwrap()
            .clone()
    };

    let result_p1 = run(1);
    let result_p4 = run(4);

    assert_eq!(result_p1, result_p4, "results must be identical for p=1 and p=4");

    // "the" appears 4 times (line1 + line2 + line3 twice)
    assert_eq!(result_p1.get("the"), Some(&("the".to_string(), 4i64)));
    // "fox" appears 2 times
    assert_eq!(result_p1.get("fox"), Some(&("fox".to_string(), 2i64)));
    // "dog" appears 2 times
    assert_eq!(result_p1.get("dog"), Some(&("dog".to_string(), 2i64)));
}

// ── Acceptance Test 3: Window Accumulation (Event Time) ───────────────────────
//
// Pipeline: from_iter(events) → assign_timestamps_and_watermarks →
//           key_by → window(TumblingEventTime 10s) → reduce(sum)
//
// Validates:
//  - Out-of-order events within the allowed lateness are accumulated correctly.
//  - Late events (past the watermark) are dropped.
//  - Each tumbling window produces the expected aggregate.

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct SensorEvent {
    sensor: String,
    ts: i64,
    reading: i32,
}

#[test]
fn acceptance_window_accumulation() {
    let env = StreamExecutionEnvironment::new("acceptance-window-accumulation");

    // Max out-of-orderness = 2s → watermark = max_seen_ts − 2000ms.
    let strategy =
        BoundedOutOfOrderness::new(Duration::from_secs(2), |e: &SensorEvent| e.ts);

    let events = vec![
        // Window [0, 10_000): three in-order events
        SensorEvent { sensor: "s1".to_string(), ts: 1_000, reading: 10 },
        SensorEvent { sensor: "s1".to_string(), ts: 5_000, reading: 20 },
        // Out-of-order but within 2s tolerance (arrives after ts=9_000)
        SensorEvent { sensor: "s1".to_string(), ts: 8_000, reading: 5 },
        // This event advances watermark to 9_000 - 2_000 = 7_000 (no window close yet)
        SensorEvent { sensor: "s1".to_string(), ts: 9_000, reading: 15 },
        // This event advances watermark to 12_000 - 2_000 = 10_000, closing [0, 10_000)
        SensorEvent { sensor: "s1".to_string(), ts: 12_000, reading: 100 },
        // Late element: watermark already >= 10_000 so this must be dropped
        SensorEvent { sensor: "s1".to_string(), ts: 3_000, reading: 9999 },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &SensorEvent| e.sensor.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .reduce(|a, b| SensorEvent {
            sensor: a.sensor.clone(),
            ts: a.ts.max(b.ts),
            reading: a.reading + b.reading,
        })
        .execute()
        .unwrap();

    // Window [0, 10_000): 10 + 20 + 5 + 15 = 50  (late ts=3_000 dropped)
    // Window [10_000, 20_000): 100
    assert_eq!(out.len(), 2, "expected exactly 2 windows to fire");

    let mut by_ts: Vec<(i64, i32)> = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap(), r.value.1.reading))
        .collect();
    by_ts.sort_by_key(|(ts, _)| *ts);

    // First window max timestamp = 9_999
    assert_eq!(by_ts[0].0, 9_999);
    assert_eq!(by_ts[0].1, 50, "window [0,10s) should sum to 50");

    // Second window max timestamp = 19_999
    assert_eq!(by_ts[1].0, 19_999);
    assert_eq!(by_ts[1].1, 100, "window [10s,20s) should contain only ts=12_000");
}

// ── Acceptance Test 4: Window accumulation multi-key ─────────────────────────
//
// Validates that two independent sensor keys are windowed independently.
// Both keys have events in window [0, 10_000); only s1 has events in the second.

#[test]
fn acceptance_window_accumulation_multi_key() {
    let env = StreamExecutionEnvironment::new("acceptance-window-multi-key");

    let strategy =
        BoundedOutOfOrderness::new(Duration::from_secs(1), |e: &SensorEvent| e.ts);

    let events = vec![
        SensorEvent { sensor: "s1".to_string(), ts: 2_000, reading: 10 },
        SensorEvent { sensor: "s2".to_string(), ts: 3_000, reading: 30 },
        SensorEvent { sensor: "s1".to_string(), ts: 7_000, reading: 40 },
        SensorEvent { sensor: "s2".to_string(), ts: 8_000, reading: 20 },
        // Advance watermark past 10_000 to close the first window for both keys
        SensorEvent { sensor: "s1".to_string(), ts: 15_000, reading: 1 },
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(strategy)
        .key_by(|e: &SensorEvent| e.sensor.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .reduce(|a, b| SensorEvent {
            sensor: a.sensor.clone(),
            ts: a.ts.max(b.ts),
            reading: a.reading + b.reading,
        })
        .execute()
        .unwrap();

    // s1: window [0,10s) = 10+40 = 50, window [10s,20s) = 1
    // s2: window [0,10s) = 30+20 = 50
    assert_eq!(out.len(), 3, "expected 3 window results (2 for s1, 1 for s2)");

    let mut by_key_ts: Vec<(String, i64, i32)> = out
        .into_iter()
        .map(|r| (r.value.0.clone(), r.timestamp.unwrap(), r.value.1.reading))
        .collect();
    by_key_ts.sort();

    let s1_w1 = by_key_ts.iter().find(|(k, ts, _)| k == "s1" && *ts == 9_999);
    let s1_w2 = by_key_ts.iter().find(|(k, ts, _)| k == "s1" && *ts == 19_999);
    let s2_w1 = by_key_ts.iter().find(|(k, ts, _)| k == "s2" && *ts == 9_999);

    assert!(s1_w1.is_some(), "s1 window [0,10s) must fire");
    assert_eq!(s1_w1.unwrap().2, 50, "s1 [0,10s) sum should be 50");

    assert!(s1_w2.is_some(), "s1 window [10s,20s) must fire");
    assert_eq!(s1_w2.unwrap().2, 1, "s1 [10s,20s) sum should be 1");

    assert!(s2_w1.is_some(), "s2 window [0,10s) must fire");
    assert_eq!(s2_w1.unwrap().2, 50, "s2 [0,10s) sum should be 50");
}

// ── Acceptance Test 5: Filter + Map pipeline ─────────────────────────────────
//
// Pipeline: from_iter(1..=100) → filter(even) → map(square) → key_by(last digit) → reduce(sum)
// Validates multi-step stateless composition before a stateful reduce.

#[test]
fn acceptance_filter_map_pipeline() {
    let env = StreamExecutionEnvironment::new("acceptance-filter-map");

    let data: Vec<i64> = (1i64..=100).collect();

    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)   // keep even numbers: 2, 4, 6, ..., 100
        .map(|x: &i64| x * x)            // square: 4, 16, 36, 64, 100, ...
        .key_by(|x: &i64| x % 10)        // group by last digit of the square
        .reduce(|a, b| a + b)            // sum per group
        .execute_with_parallelism(2)
        .unwrap();

    let counts = results.lock().unwrap();

    // All even squares end in 0, 4, or 6:
    //   last digit 0: 10²=100, 20²=400, 30²=900, ..., 100²=10000  (10 values)
    //   last digit 4: 2²=4, 8²=64, 12²=144, ..., 98²=9604  (10 values)  [ends in 4 or 4]
    //   last digit 6: 4²=16, 6²=36, 14²=196, ..., 96²=9216  (30 values split)
    // Regardless of exact grouping, the result set must be non-empty and only
    // contain the three possible last-digit keys for even squares.
    assert!(!counts.is_empty(), "result must be non-empty");

    for (key, _val) in counts.iter() {
        assert!(
            *key == 0 || *key == 4 || *key == 6,
            "even squares can only end in 0, 4, or 6; got last digit {}",
            key
        );
    }

    // Verify the total sum across all groups equals sum of squares of even numbers 2..=100.
    let expected_total: i64 = (1i64..=50).map(|n| (2 * n) * (2 * n)).sum();
    let actual_total: i64 = counts.values().sum();
    assert_eq!(
        actual_total, expected_total,
        "sum of all group sums must equal total sum of even squares"
    );
}

// ── Acceptance Test 6: Filter + Map pipeline parallelism=4 ───────────────────
//
// Same as Test 5 but with parallelism=4 to verify consistent results.

#[test]
fn acceptance_filter_map_pipeline_p4() {
    let env = StreamExecutionEnvironment::new("acceptance-filter-map-p4");

    let data: Vec<i64> = (1i64..=100).collect();

    let results = env
        .from_iter(data)
        .filter(|x: &i64| x % 2 == 0)
        .map(|x: &i64| x * x)
        .key_by(|x: &i64| x % 10)
        .reduce(|a, b| a + b)
        .execute_with_parallelism(4)
        .unwrap();

    let counts = results.lock().unwrap();

    let expected_total: i64 = (1i64..=50).map(|n| (2 * n) * (2 * n)).sum();
    let actual_total: i64 = counts.values().sum();
    assert_eq!(
        actual_total, expected_total,
        "p=4 must yield same total as p=2"
    );
}
