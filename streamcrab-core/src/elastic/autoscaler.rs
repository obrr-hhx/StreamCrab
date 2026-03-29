use super::{ElasticConfig, ScalePolicy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub type OperatorId = u32;
pub type TaskId = String;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub input_queue_usage: f64,
    pub throughput: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDirection {
    Up,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaleDecision {
    pub direction: ScaleDirection,
    pub target_parallelism: usize,
}

/// Stateless evaluation helper used by JM autoscaler loop.
pub struct Autoscaler {
    cooldown: Duration,
    last_scale_time: HashMap<OperatorId, Instant>,
}

impl Autoscaler {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            last_scale_time: HashMap::new(),
        }
    }

    pub fn evaluate_scale_decision(
        &mut self,
        operator_id: OperatorId,
        config: &ElasticConfig,
        current_parallelism: usize,
        metrics: &HashMap<TaskId, TaskMetrics>,
        now: Instant,
    ) -> Option<ScaleDecision> {
        if current_parallelism == 0 || metrics.is_empty() {
            return None;
        }
        if let Some(last) = self.last_scale_time.get(&operator_id)
            && now.duration_since(*last) < self.cooldown
        {
            return None;
        }

        let decision = match config.scale_policy {
            ScalePolicy::Manual => None,
            ScalePolicy::Backpressure {
                scale_up_threshold,
                scale_down_threshold,
            } => {
                let avg_queue = metrics.values().map(|m| m.input_queue_usage).sum::<f64>()
                    / metrics.len() as f64;
                if avg_queue >= scale_up_threshold && current_parallelism < config.max_parallelism {
                    Some(ScaleDecision {
                        direction: ScaleDirection::Up,
                        target_parallelism: (current_parallelism * 2).min(config.max_parallelism),
                    })
                } else if avg_queue <= scale_down_threshold
                    && current_parallelism > config.min_parallelism
                {
                    Some(ScaleDecision {
                        direction: ScaleDirection::Down,
                        target_parallelism: (current_parallelism / 2).max(config.min_parallelism),
                    })
                } else {
                    None
                }
            }
            ScalePolicy::TargetThroughput { target_per_task } => {
                let avg_throughput =
                    metrics.values().map(|m| m.throughput).sum::<f64>() / metrics.len() as f64;
                if avg_throughput > target_per_task * 1.25
                    && current_parallelism > config.min_parallelism
                {
                    Some(ScaleDecision {
                        direction: ScaleDirection::Down,
                        target_parallelism: (current_parallelism / 2).max(config.min_parallelism),
                    })
                } else if avg_throughput < target_per_task * 0.75
                    && current_parallelism < config.max_parallelism
                {
                    Some(ScaleDecision {
                        direction: ScaleDirection::Up,
                        target_parallelism: (current_parallelism * 2).min(config.max_parallelism),
                    })
                } else {
                    None
                }
            }
        };

        if decision.is_some() {
            self.last_scale_time.insert(operator_id, now);
        }
        decision
    }
}

#[cfg(test)]
#[path = "tests/autoscaler_tests.rs"]
mod tests;
