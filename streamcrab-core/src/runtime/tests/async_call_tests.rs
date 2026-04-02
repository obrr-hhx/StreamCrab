use super::*;
use crate::runtime::operator_chain::Operator;

// ============================================================================
// Tests: AsyncExternalOperator
// ============================================================================

/// Helper: build an operator that doubles integers (simulates an external call).
fn double_op() -> AsyncExternalOperator<i32, i32, impl Fn(&i32) -> anyhow::Result<i32> + Send> {
    AsyncExternalOperator::new(|x: &i32| Ok(x * 2), AsyncCallConfig::default())
}

#[test]
fn test_basic_call_through_operator() {
    let mut op = double_op();
    let input = vec![1, 2, 3];
    let mut output = Vec::new();

    op.process_batch(&input, &mut output).unwrap();

    assert_eq!(output, vec![2, 4, 6]);
}

#[test]
fn test_process_batch_multiple_records() {
    let mut op = double_op();
    let input: Vec<i32> = (0..10).collect();
    let mut output = Vec::new();

    op.process_batch(&input, &mut output).unwrap();

    let expected: Vec<i32> = input.iter().map(|x| x * 2).collect();
    assert_eq!(output, expected);
}

#[test]
fn test_process_batch_empty_input() {
    let mut op = double_op();
    let input: Vec<i32> = vec![];
    let mut output = Vec::new();

    op.process_batch(&input, &mut output).unwrap();

    assert!(output.is_empty());
}

#[test]
fn test_processed_count_increments() {
    let mut op = double_op();

    let input = vec![10, 20, 30];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();

    // After processing 3 records, snapshot/restore into a second operator and
    // confirm the count is preserved (indirect check via snapshot roundtrip).
    let snap_bytes = op.snapshot_state().unwrap();
    let mut op2 = double_op();
    op2.restore_state(&snap_bytes).unwrap();

    // op2 snapshot must equal op snapshot (same processed_count).
    let snap2 = op2.snapshot_state().unwrap();
    assert_eq!(snap_bytes, snap2);
}

#[test]
fn test_snapshot_restore_roundtrip() {
    let mut op = double_op();

    // Process some records to advance the counter.
    let input = vec![1, 2, 3, 4, 5];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();

    // Take a snapshot.
    let snap = op.snapshot_state().unwrap();

    // Create a fresh operator and restore into it.
    let mut op2 = double_op();
    op2.restore_state(&snap).unwrap();

    // The restored operator should have the same processed_count.
    let snap2 = op2.snapshot_state().unwrap();
    assert_eq!(snap, snap2);
}

#[test]
fn test_restore_from_empty_snapshot() {
    // Restoring from empty bytes must be a no-op (processed_count stays 0).
    let mut op = double_op();
    op.restore_state(&[]).unwrap();

    // snapshot of a fresh (count=0) operator and a restored-from-empty operator must be equal.
    let fresh = double_op();
    assert_eq!(
        op.snapshot_state().unwrap(),
        fresh.snapshot_state().unwrap()
    );
}

#[test]
fn test_checkpoint_barrier_is_noop() {
    let mut op = double_op();
    // Should succeed without error.
    op.on_checkpoint_barrier(42).unwrap();

    // Processing still works after barrier.
    let mut output = Vec::new();
    op.process_batch(&[7], &mut output).unwrap();
    assert_eq!(output, vec![14]);
}

#[test]
fn test_call_fn_error_propagates() {
    // A call function that always fails should propagate the error.
    let mut op = AsyncExternalOperator::new(
        |_x: &i32| anyhow::bail!("service unavailable"),
        AsyncCallConfig {
            retry: RetryPolicy {
                max_attempts: 1, // no retries
                ..RetryPolicy::default()
            },
            ..AsyncCallConfig::default()
        },
    );

    let mut output: Vec<i32> = Vec::new();
    let result = op.process_batch(&[1], &mut output);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("service unavailable")
    );
    assert!(output.is_empty());
}

#[test]
fn test_retry_policy_config() {
    // Verify custom retry config is stored (structural check).
    let policy = RetryPolicy {
        max_attempts: 5,
        initial_backoff: std::time::Duration::from_millis(50),
        max_backoff: std::time::Duration::from_secs(30),
    };

    let config = AsyncCallConfig {
        retry: policy,
        max_concurrent: 200,
        ..AsyncCallConfig::default()
    };

    assert_eq!(config.retry.max_attempts, 5);
    assert_eq!(config.max_concurrent, 200);
}

#[test]
fn test_retry_succeeds_after_transient_failure() {
    use std::sync::{Arc, Mutex};

    // Fails on the first call, succeeds on the second.
    let attempt_count = Arc::new(Mutex::new(0u32));
    let counter = attempt_count.clone();

    let mut op = AsyncExternalOperator::new(
        move |x: &i32| {
            let mut count = counter.lock().unwrap();
            *count += 1;
            if *count == 1 {
                anyhow::bail!("transient error")
            } else {
                Ok(*x * 3)
            }
        },
        AsyncCallConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                initial_backoff: std::time::Duration::from_millis(1), // fast for tests
                max_backoff: std::time::Duration::from_millis(10),
            },
            ..AsyncCallConfig::default()
        },
    );

    let mut output = Vec::new();
    op.process_batch(&[5], &mut output).unwrap();

    assert_eq!(output, vec![15]);
    assert_eq!(*attempt_count.lock().unwrap(), 2);
}

#[test]
fn test_semantics_at_least_once() {
    // Verify the enum variant and bincode serde roundtrip.
    let sem = AsyncSemantics::AtLeastOnce;
    let bytes = bincode::serialize(&sem).unwrap();
    let decoded: AsyncSemantics = bincode::deserialize(&bytes).unwrap();
    assert_eq!(sem, decoded);
}

#[test]
fn test_string_in_string_out() {
    // Verify the operator works with non-integer types.
    let mut op = AsyncExternalOperator::new(
        |s: &String| Ok(format!("hello, {s}")),
        AsyncCallConfig::default(),
    );

    let input = vec!["world".to_string(), "stream".to_string()];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();

    assert_eq!(
        output,
        vec!["hello, world".to_string(), "hello, stream".to_string()]
    );
}
