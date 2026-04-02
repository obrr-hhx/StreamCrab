//! Thread-safe registry mapping u64 handles to boxed VectorizedOperator trait objects.
//!
//! Flink's Java side holds integer handles returned from `createNative` and
//! passes them back on every subsequent call. The handle map translates them
//! back to the concrete Rust operator.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use anyhow::{Result, anyhow};
use streamcrab_vectorized::operators::VectorizedOperator;

// ── Global state ──────────────────────────────────────────────────────────────

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// Global operator registry.  `Option` allows lazy initialisation without
/// `OnceLock` — the `None` state is replaced with `Some(HashMap::new())` on
/// first access.
static HANDLE_MAP: Mutex<Option<HashMap<u64, Box<dyn VectorizedOperator>>>> = Mutex::new(None);

// ── Internal helpers ──────────────────────────────────────────────────────────

fn get_map() -> MutexGuard<'static, Option<HashMap<u64, Box<dyn VectorizedOperator>>>> {
    let mut guard = HANDLE_MAP.lock().expect("handle_map mutex poisoned");
    if guard.is_none() {
        *guard = Some(HashMap::new());
    }
    guard
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Insert an operator into the registry and return its opaque handle.
pub fn insert(op: Box<dyn VectorizedOperator>) -> u64 {
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    let mut guard = get_map();
    guard.as_mut().unwrap().insert(handle, op);
    handle
}

/// Remove an operator from the registry, returning it (or `None` if not found).
pub fn remove(handle: u64) -> Option<Box<dyn VectorizedOperator>> {
    let mut guard = get_map();
    guard.as_mut().unwrap().remove(&handle)
}

/// Execute a closure with mutable access to the operator identified by `handle`.
/// Returns an error if the handle is unknown.
pub fn with_operator<F, R>(handle: u64, f: F) -> Result<R>
where
    F: FnOnce(&mut dyn VectorizedOperator) -> Result<R>,
{
    let mut guard = get_map();
    let map = guard.as_mut().unwrap();
    let op = map
        .get_mut(&handle)
        .ok_or_else(|| anyhow!("unknown operator handle: {handle}"))?;
    f(op.as_mut())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use streamcrab_vectorized::{batch::VeloxBatch, operators::VectorizedOperator};

    /// A trivial no-op operator used for handle map tests.
    struct NoopOperator;

    impl VectorizedOperator for NoopOperator {
        fn add_input(&mut self, _batch: VeloxBatch) -> Result<()> {
            Ok(())
        }
        fn get_output(&mut self) -> Result<Option<VeloxBatch>> {
            Ok(None)
        }
        fn is_finished(&self) -> bool {
            false
        }
    }

    fn make_noop() -> Box<dyn VectorizedOperator> {
        Box::new(NoopOperator)
    }

    #[test]
    fn test_insert_returns_unique_handles() {
        let h1 = insert(make_noop());
        let h2 = insert(make_noop());
        assert_ne!(h1, h2, "each insert must produce a unique handle");
        // Cleanup.
        let _ = remove(h1);
        let _ = remove(h2);
    }

    #[test]
    fn test_remove_returns_operator() {
        let h = insert(make_noop());
        let op = remove(h);
        assert!(op.is_some(), "remove should return the boxed operator");
        // Second remove must return None.
        let op2 = remove(h);
        assert!(op2.is_none(), "double-remove should yield None");
    }

    #[test]
    fn test_with_operator_unknown_handle_errors() {
        let result = with_operator(u64::MAX, |op| {
            let _ = op.is_finished();
            Ok(())
        });
        assert!(result.is_err(), "unknown handle must produce an error");
    }

    #[test]
    fn test_with_operator_calls_closure() {
        let h = insert(make_noop());
        let finished = with_operator(h, |op| Ok(op.is_finished())).unwrap();
        assert!(!finished);
        let _ = remove(h);
    }

    #[test]
    fn test_with_operator_snapshot_state() {
        // Use a real operator that implements snapshot_state.
        use streamcrab_vectorized::operators::HashAggregateOperator;
        use streamcrab_vectorized::operators::hash_aggregate::{
            AggregateDescriptor, AggregateFunction,
        };

        let op = Box::new(HashAggregateOperator::new(
            vec![0],
            vec![AggregateDescriptor {
                function: AggregateFunction::Sum,
                input_col: 1,
                output_name: "total".into(),
            }],
        ));
        let h = insert(op);
        let state = with_operator(h, |op| op.snapshot_state()).unwrap();
        // HashAggregateOperator always serializes its struct fields, so the
        // snapshot is non-empty even with zero groups.
        assert!(
            !state.is_empty(),
            "snapshot_state should produce serialized bytes"
        );
        let _ = remove(h);
    }
}
