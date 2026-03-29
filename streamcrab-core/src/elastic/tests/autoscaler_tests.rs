use crate::elastic::{Autoscaler, ElasticConfig, ScaleDirection, ScalePolicy, TaskMetrics};
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[test]
fn test_autoscaler_backpressure_scale_up() {
    let mut autoscaler = Autoscaler::new(Duration::from_secs(60));
    let config = ElasticConfig {
        min_parallelism: 2,
        max_parallelism: 16,
        scale_policy: ScalePolicy::Backpressure {
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.2,
        },
        cooldown: Duration::from_secs(60),
    };
    let mut metrics = HashMap::new();
    metrics.insert(
        "t1".to_string(),
        TaskMetrics {
            input_queue_usage: 0.95,
            throughput: 100.0,
        },
    );
    metrics.insert(
        "t2".to_string(),
        TaskMetrics {
            input_queue_usage: 0.90,
            throughput: 120.0,
        },
    );

    let decision = autoscaler
        .evaluate_scale_decision(1, &config, 4, &metrics, Instant::now())
        .expect("should scale up");
    assert_eq!(decision.direction, ScaleDirection::Up);
    assert_eq!(decision.target_parallelism, 8);
}

#[test]
fn test_autoscaler_respects_cooldown() {
    let mut autoscaler = Autoscaler::new(Duration::from_secs(60));
    let config = ElasticConfig {
        min_parallelism: 1,
        max_parallelism: 8,
        scale_policy: ScalePolicy::Backpressure {
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.2,
        },
        cooldown: Duration::from_secs(60),
    };
    let mut metrics = HashMap::new();
    metrics.insert(
        "t1".to_string(),
        TaskMetrics {
            input_queue_usage: 0.99,
            throughput: 50.0,
        },
    );
    let now = Instant::now();
    let first = autoscaler.evaluate_scale_decision(7, &config, 2, &metrics, now);
    assert!(first.is_some());

    let second = autoscaler.evaluate_scale_decision(
        7,
        &config,
        first.unwrap().target_parallelism,
        &metrics,
        now + Duration::from_secs(5),
    );
    assert!(second.is_none());
}
