//! # Partitioner
//!
//! Data partitioning strategies for routing records between parallel operator instances.

use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use ahash::AHasher;

/// Trait for partitioning data across parallel instances.
pub trait Partitioner<T>: Send + Sync {
    /// Determine which partition (0..num_partitions) this value should go to.
    fn partition(&self, value: &T, num_partitions: usize) -> usize;
}

/// Hash-based partitioner using a key selector function.
///
/// Uses ahash for SIMD-accelerated hashing.
pub struct HashPartitioner<K, F> {
    key_selector: F,
    _phantom: PhantomData<K>,
}

impl<K, F> HashPartitioner<K, F> {
    /// Create a new hash partitioner with the given key selector.
    pub fn new(key_selector: F) -> Self {
        Self {
            key_selector,
            _phantom: PhantomData,
        }
    }
}

impl<K, T, F> Partitioner<T> for HashPartitioner<K, F>
where
    K: Hash + Send + Sync,
    F: Fn(&T) -> K + Send + Sync,
{
    fn partition(&self, value: &T, num_partitions: usize) -> usize {
        let key = (self.key_selector)(value);
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize) % num_partitions
    }
}

/// Round-robin partitioner (stateful, requires Arc<Mutex<>>).
pub struct RoundRobinPartitioner {
    counter: std::sync::atomic::AtomicUsize,
}

impl RoundRobinPartitioner {
    pub fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Partitioner<T> for RoundRobinPartitioner {
    fn partition(&self, _value: &T, num_partitions: usize) -> usize {
        let count = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        count % num_partitions
    }
}

#[cfg(test)]
#[path = "tests/partitioner_tests.rs"]
mod tests;
