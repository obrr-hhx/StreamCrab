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
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct Record {
        user_id: String,
        value: i32,
    }

    #[test]
    fn test_hash_partitioner_same_key_same_partition() {
        let partitioner = HashPartitioner::new(|r: &Record| r.user_id.clone());

        let rec1 = Record {
            user_id: "user_1".to_string(),
            value: 100,
        };
        let rec2 = Record {
            user_id: "user_1".to_string(),
            value: 200,
        };

        let p1 = partitioner.partition(&rec1, 4);
        let p2 = partitioner.partition(&rec2, 4);

        // Same key should go to same partition
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_hash_partitioner_distribution() {
        let partitioner = HashPartitioner::new(|r: &Record| r.user_id.clone());

        let mut counts = vec![0; 4];
        for i in 0..1000 {
            let rec = Record {
                user_id: format!("user_{}", i),
                value: i,
            };
            let partition = partitioner.partition(&rec, 4);
            counts[partition] += 1;
        }

        // Check that distribution is reasonably balanced
        // Each partition should get roughly 250 records (1000 / 4)
        for count in counts {
            assert!(count > 200 && count < 300, "Unbalanced distribution: {}", count);
        }
    }

    #[test]
    fn test_hash_partitioner_within_bounds() {
        let partitioner = HashPartitioner::new(|r: &Record| r.user_id.clone());

        for i in 0..100 {
            let rec = Record {
                user_id: format!("user_{}", i),
                value: i,
            };
            let partition = partitioner.partition(&rec, 8);
            assert!(partition < 8);
        }
    }

    #[test]
    fn test_round_robin_partitioner() {
        let partitioner = RoundRobinPartitioner::new();

        let rec = Record {
            user_id: "user_1".to_string(),
            value: 100,
        };

        // Should cycle through partitions
        assert_eq!(partitioner.partition(&rec, 4), 0);
        assert_eq!(partitioner.partition(&rec, 4), 1);
        assert_eq!(partitioner.partition(&rec, 4), 2);
        assert_eq!(partitioner.partition(&rec, 4), 3);
        assert_eq!(partitioner.partition(&rec, 4), 0);
    }
}

