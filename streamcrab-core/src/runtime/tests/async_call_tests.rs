use super::*;
use crate::runtime::operator_chain::Operator;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

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

// ============================================================================
// Tests: AsyncExternalOperatorV2
// ============================================================================

/// Helper: build a v2 operator that doubles integers inside a tokio runtime.
/// Must be called from within a `#[tokio::test]` so `Handle::current()` works.
fn double_op_v2(
) -> AsyncExternalOperatorV2<i32, i32, impl Fn(i32) -> std::future::Ready<anyhow::Result<i32>> + Send + Sync + 'static, std::future::Ready<anyhow::Result<i32>>>
{
    AsyncExternalOperatorV2::new(
        |x: i32| std::future::ready(Ok(x * 2)),
        AsyncCallConfig::default(),
        tokio::runtime::Handle::current(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_basic_async_call() {
    let mut op = double_op_v2();
    let input = vec![1, 2, 3];
    let mut output = Vec::new();

    op.process_batch(&input, &mut output).unwrap();

    // Order may differ due to concurrent execution; sort before comparing.
    output.sort_unstable();
    assert_eq!(output, vec![2, 4, 6]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_concurrent_limit() {
    // max_concurrent = 2, but we submit 10 tasks — all must still complete.
    let mut op = AsyncExternalOperatorV2::new(
        |x: i32| async move { Ok(x * 3) },
        AsyncCallConfig {
            max_concurrent: 2,
            ..AsyncCallConfig::default()
        },
        tokio::runtime::Handle::current(),
    );

    let input: Vec<i32> = (0..10).collect();
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();

    output.sort_unstable();
    let expected: Vec<i32> = (0..10).map(|x| x * 3).collect();
    assert_eq!(output, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_retry_on_failure() {
    // Async fn that fails on the first attempt and succeeds on the second.
    let attempt_count = Arc::new(AtomicU32::new(0));
    let counter = attempt_count.clone();

    let mut op = AsyncExternalOperatorV2::new(
        move |x: i32| {
            let c = counter.clone();
            async move {
                let prev = c.fetch_add(1, Ordering::SeqCst);
                if prev == 0 {
                    anyhow::bail!("transient error")
                } else {
                    Ok(x * 5)
                }
            }
        },
        AsyncCallConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                initial_backoff: std::time::Duration::from_millis(1),
                max_backoff: std::time::Duration::from_millis(10),
            },
            ..AsyncCallConfig::default()
        },
        tokio::runtime::Handle::current(),
    );

    let mut output = Vec::new();
    op.process_batch(&[4], &mut output).unwrap();

    assert_eq!(output, vec![20]);
    assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_checkpoint_drain() {
    // on_checkpoint_barrier is a no-op for v2 (block_on drains all futures).
    let mut op = double_op_v2();

    // Process some records first.
    let mut output = Vec::new();
    op.process_batch(&[1, 2], &mut output).unwrap();

    // Barrier must succeed without error.
    op.on_checkpoint_barrier(99).unwrap();

    // Processing still works after barrier.
    let mut output2 = Vec::new();
    op.process_batch(&[10], &mut output2).unwrap();
    assert_eq!(output2, vec![20]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_snapshot_restore() {
    let mut op = double_op_v2();

    let input = vec![1, 2, 3, 4, 5];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();

    // Snapshot the state.
    let snap = op.snapshot_state().unwrap();

    // Restore into a fresh operator and verify processed_count matches.
    let mut op2 = double_op_v2();
    op2.restore_state(&snap).unwrap();
    let snap2 = op2.snapshot_state().unwrap();

    assert_eq!(snap, snap2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_empty_input() {
    let mut op = double_op_v2();
    let mut output: Vec<i32> = Vec::new();

    op.process_batch(&[], &mut output).unwrap();

    assert!(output.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_v2_error_propagates() {
    let mut op = AsyncExternalOperatorV2::new(
        |_x: i32| async move { anyhow::bail!("service down") },
        AsyncCallConfig {
            retry: RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            },
            ..AsyncCallConfig::default()
        },
        tokio::runtime::Handle::current(),
    );

    let mut output: Vec<i32> = Vec::new();
    let result = op.process_batch(&[1], &mut output);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("service down"));
}
