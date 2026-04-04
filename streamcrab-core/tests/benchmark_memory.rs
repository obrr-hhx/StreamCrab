//! Memory benchmarks: measure state backend memory usage with different key counts.
//! Run: cargo test -p streamcrab-core --test benchmark_memory --release -- --nocapture

use streamcrab_core::state::{HashMapStateBackend, KeyedStateBackend};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Insert `num_keys` keys into a fresh backend and return the serialized
/// snapshot size (bytes) as a proxy for in-memory state footprint.
fn measure_state_size(num_keys: usize) -> usize {
    let mut backend = HashMapStateBackend::new();
    for i in 0..num_keys as i64 {
        let key_bytes = bincode::serialize(&i).unwrap();
        backend.set_current_key(key_bytes);
        backend.put_value("sum", 1000i64 + i).unwrap();
    }
    let snapshot = backend.snapshot().unwrap();
    snapshot.len()
}

// ---------------------------------------------------------------------------
// Test: memory growth per key
// ---------------------------------------------------------------------------

/// Measures snapshot size (bytes) as a proxy for per-key state overhead.
///
/// Runs for num_keys ∈ {1K, 10K, 100K, 1M} and prints:
///   MEMORY|num_keys=X|snapshot_bytes=X|bytes_per_key=X
///
/// The snapshot is a `bincode`-serialized representation of all state maps,
/// so it closely approximates the in-process memory consumed by the backend.
#[test]
fn bench_memory_per_key() {
    let key_counts: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];

    for &num_keys in key_counts {
        let snapshot_bytes = measure_state_size(num_keys);
        let bytes_per_key = if num_keys > 0 {
            snapshot_bytes / num_keys
        } else {
            0
        };
        println!(
            "MEMORY|num_keys={}|snapshot_bytes={}|bytes_per_key={}",
            num_keys, snapshot_bytes, bytes_per_key
        );
    }
}

// ---------------------------------------------------------------------------
// Test: incremental growth (insert keys in stages, snapshot at each stage)
// ---------------------------------------------------------------------------

/// Shows how snapshot size grows incrementally as keys are added.
///
/// Useful for verifying that growth is linear (O(keys)) and detecting
/// unexpected overhead (e.g. hash-map over-allocation rounding).
#[test]
fn bench_memory_incremental_growth() {
    let stages: &[usize] = &[1_000, 5_000, 10_000, 50_000, 100_000];
    let mut backend = HashMapStateBackend::new();
    let mut inserted = 0usize;

    for &target in stages {
        // Insert keys up to target
        while inserted < target {
            let key_bytes = bincode::serialize(&(inserted as i64)).unwrap();
            backend.set_current_key(key_bytes);
            backend.put_value("sum", 1000i64 + inserted as i64).unwrap();
            inserted += 1;
        }

        let snapshot_bytes = backend.snapshot().unwrap().len();
        let bytes_per_key = if inserted > 0 {
            snapshot_bytes / inserted
        } else {
            0
        };

        println!(
            "MEMORY_INCREMENTAL|num_keys={}|snapshot_bytes={}|bytes_per_key={}",
            inserted, snapshot_bytes, bytes_per_key
        );
    }
}

// ---------------------------------------------------------------------------
// Test: multi-state-name overhead
// ---------------------------------------------------------------------------

/// Measures memory when each key has multiple named state fields.
///
/// Each key gets three state entries: "sum", "count", "max".
/// Compares per-key bytes to the single-field baseline.
#[test]
fn bench_memory_multi_field() {
    let key_counts: &[usize] = &[1_000, 10_000, 100_000];

    for &num_keys in key_counts {
        let mut backend = HashMapStateBackend::new();
        for i in 0..num_keys as i64 {
            let key_bytes = bincode::serialize(&i).unwrap();
            backend.set_current_key(key_bytes);
            backend.put_value("sum", 1000i64 + i).unwrap();
            backend.put_value("count", i).unwrap();
            backend.put_value("max", 2000i64 + i).unwrap();
        }

        let snapshot_bytes = backend.snapshot().unwrap().len();
        let bytes_per_key = snapshot_bytes / num_keys;

        println!(
            "MEMORY_MULTIFIELD|num_keys={}|snapshot_bytes={}|bytes_per_key={}",
            num_keys, snapshot_bytes, bytes_per_key
        );
    }
}
