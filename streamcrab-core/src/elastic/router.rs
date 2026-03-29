use ahash::AHasher;
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::task::TaskId;

/// Minimal rescale plan used by JM coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RescalePlan {
    pub job_id: String,
    pub operator_id: u32,
    pub new_parallelism: usize,
}

/// A consistent-hash ring over task IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsistentHashRing {
    vnodes_per_task: usize,
    virtual_nodes: BTreeMap<u64, TaskId>,
}

impl ConsistentHashRing {
    pub fn new(vnodes_per_task: usize) -> Self {
        Self {
            vnodes_per_task: vnodes_per_task.max(1),
            virtual_nodes: BTreeMap::new(),
        }
    }

    pub fn add_task(&mut self, task_id: TaskId) {
        for vnode in 0..self.vnodes_per_task {
            let hash = hash_u64(&(task_id, vnode as u64));
            self.virtual_nodes.insert(hash, task_id);
        }
    }

    pub fn remove_task(&mut self, task_id: TaskId) {
        self.virtual_nodes.retain(|_, v| *v != task_id);
    }

    pub fn route(&self, key: &[u8]) -> Option<TaskId> {
        if self.virtual_nodes.is_empty() {
            return None;
        }
        let hash = hash_u64(&key);
        if let Some((_, task_id)) = self.virtual_nodes.range(hash..).next() {
            return Some(*task_id);
        }
        self.virtual_nodes
            .first_key_value()
            .map(|(_, task_id)| *task_id)
    }
}

/// Router snapshot carried in rescale barriers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRouter {
    pub version: u64,
    pub ring: ConsistentHashRing,
}

impl KeyRouter {
    pub fn new(version: u64, tasks: &[TaskId], vnodes_per_task: usize) -> Self {
        let mut ring = ConsistentHashRing::new(vnodes_per_task);
        for task_id in tasks {
            ring.add_task(*task_id);
        }
        Self { version, ring }
    }

    pub fn route(&self, key: &[u8]) -> Option<TaskId> {
        self.ring.route(key)
    }
}

/// Thread-safe task-side router pointer with atomic swap for cutover.
pub struct PartitionRouter {
    current: ArcSwap<KeyRouter>,
}

impl PartitionRouter {
    pub fn new(initial: KeyRouter) -> Self {
        Self {
            current: ArcSwap::new(Arc::new(initial)),
        }
    }

    pub fn route(&self, key: &[u8]) -> Option<TaskId> {
        self.current.load().route(key)
    }

    pub fn current_version(&self) -> u64 {
        self.current.load().version
    }

    pub fn swap(&self, next_router: KeyRouter) {
        self.current.store(Arc::new(next_router));
    }
}

fn hash_u64<T: Hash>(value: &T) -> u64 {
    let mut hasher = AHasher::default();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
#[path = "tests/router_tests.rs"]
mod tests;
