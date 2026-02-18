use std::time::Duration;

use streamcrab_api::environment::StreamExecutionEnvironment;
use streamcrab_core::time::BoundedOutOfOrderness;
use streamcrab_core::window::TumblingEventTimeWindows;

fn main() -> anyhow::Result<()> {
    let env = StreamExecutionEnvironment::new("event-time-windowing");

    // (user, ts_ms, value)
    let events: Vec<(String, i64, i32)> = vec![
        ("u1".to_string(), 1_000, 1),
        ("u1".to_string(), 9_000, 2),
        // Out of order but within the 2s delay.
        ("u1".to_string(), 8_000, 3),
        // Advances watermark to 10_000, closing the first window [0, 10_000).
        ("u1".to_string(), 12_000, 10),
    ];

    let out = env
        .from_iter(events)
        .assign_timestamps_and_watermarks(BoundedOutOfOrderness::new(
            Duration::from_secs(2),
            |e: &(String, i64, i32)| e.1,
        ))
        .key_by(|e: &(String, i64, i32)| e.0.clone())
        .window(TumblingEventTimeWindows::of(Duration::from_secs(10)))
        .reduce(|a, b| {
            // Keep max timestamp and sum values.
            (a.0.clone(), a.1.max(b.1), a.2 + b.2)
        })
        .execute()?;

    let mut out = out
        .into_iter()
        .map(|r| (r.timestamp.unwrap_or(-1), r.value))
        .collect::<Vec<_>>();
    out.sort_by_key(|(ts, _)| *ts);

    for (ts, (user, event)) in out {
        println!(
            "ts={} key={} value={} (event_ts={})",
            ts, user, event.2, event.1
        );
    }

    Ok(())
}
