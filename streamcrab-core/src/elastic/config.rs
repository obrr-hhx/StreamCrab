use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Autoscaling policy for elastic operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalePolicy {
    Backpressure {
        scale_up_threshold: f64,
        scale_down_threshold: f64,
    },
    TargetThroughput {
        target_per_task: f64,
    },
    Manual,
}

/// Elastic behavior configuration for a keyed operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticConfig {
    pub min_parallelism: usize,
    pub max_parallelism: usize,
    pub scale_policy: ScalePolicy,
    pub cooldown: Duration,
}

impl Default for ElasticConfig {
    fn default() -> Self {
        Self {
            min_parallelism: 1,
            max_parallelism: 64,
            scale_policy: ScalePolicy::Manual,
            cooldown: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
#[path = "tests/config_tests.rs"]
mod tests;
