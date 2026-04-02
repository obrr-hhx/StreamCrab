//! Memory tracking for vectorized operators.
//!
//! Provides a lightweight memory accounting layer so the JNI bridge can report
//! native memory usage back to Flink's memory manager.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Tracks memory allocated by native vectorized operators.
#[derive(Debug, Clone)]
pub struct MemoryTracker {
    allocated: Arc<AtomicUsize>,
    limit: usize,
}

impl MemoryTracker {
    /// Create a tracker with the given byte limit (0 = unlimited).
    pub fn new(limit: usize) -> Self {
        Self {
            allocated: Arc::new(AtomicUsize::new(0)),
            limit,
        }
    }

    /// Create an unlimited tracker.
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Current bytes allocated.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Try to reserve `bytes` additional memory. Returns false if over limit.
    pub fn try_reserve(&self, bytes: usize) -> bool {
        if self.limit == 0 {
            self.allocated.fetch_add(bytes, Ordering::Relaxed);
            return true;
        }
        let mut current = self.allocated.load(Ordering::Relaxed);
        loop {
            if current + bytes > self.limit {
                return false;
            }
            match self.allocated.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Release previously reserved memory.
    pub fn release(&self, bytes: usize) {
        self.allocated.fetch_sub(bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unlimited_tracker() {
        let tracker = MemoryTracker::unlimited();
        assert!(tracker.try_reserve(1_000_000));
        assert_eq!(tracker.allocated(), 1_000_000);
        tracker.release(500_000);
        assert_eq!(tracker.allocated(), 500_000);
    }

    #[test]
    fn test_limited_tracker() {
        let tracker = MemoryTracker::new(1024);
        assert!(tracker.try_reserve(512));
        assert!(tracker.try_reserve(512));
        assert!(!tracker.try_reserve(1)); // over limit
        tracker.release(100);
        assert!(tracker.try_reserve(100));
    }
}
