//! AsyncExternalCall operator for enriching stream records via external service calls.
//!
//! # v1 Semantics: At-Least-Once Only
//!
//! This v1 implementation is **synchronous**: calls happen inline in `process_batch`.
//! On failure recovery, the source replays from the last sealed checkpoint, which
//! means external calls may be re-issued. The external service **must be idempotent**.
//!
//! **Do NOT use for non-idempotent operations** (e.g., financial payments, one-time emails).
//!
//! # Design Notes
//!
//! - v1: Calls happen synchronously, one-by-one, inside `process_batch`.
//! - v2: Will use `FuturesUnordered` + tokio for true async concurrency.
//! - The `AsyncCallConfig` and `RetryPolicy` types are forward-compatible with v2.
//!
//! # Example
//!
//! ```ignore
//! let enrichment_fn = |record: &UserId| -> Result<UserProfile> {
//!     http_client.get_user(record.id)
//! };
//!
//! let op = AsyncExternalOperator::new(
//!     enrichment_fn,
//!     AsyncCallConfig::default(),
//! );
//!
//! // Chain into pipeline
//! let chain = chain(op);
//! ```

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::runtime::operator_chain::Operator;

// ============================================================================
// Configuration Types
// ============================================================================

/// Configuration for async external calls.
///
/// Controls concurrency, batching, timeouts, and retry behavior.
/// All fields have sensible defaults via [`AsyncCallConfig::default`].
#[derive(Debug, Clone)]
pub struct AsyncCallConfig {
    /// Maximum concurrent in-flight requests (v2 will enforce this via semaphore).
    pub max_concurrent: usize,
    /// Batch size before sending to external service (0 or 1 = no batching, send per-record).
    pub batch_size: usize,
    /// Maximum time to wait for a batch to fill before flushing.
    pub batch_timeout: Duration,
    /// Timeout for each individual request.
    pub request_timeout: Duration,
    /// Retry policy for transient failures.
    pub retry: RetryPolicy,
}

impl Default for AsyncCallConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            batch_size: 1,
            batch_timeout: Duration::from_millis(10),
            request_timeout: Duration::from_secs(5),
            retry: RetryPolicy::default(),
        }
    }
}

/// Retry policy for failed external calls.
///
/// Uses exponential backoff: each retry doubles the wait time up to `max_backoff`.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (1 = no retry, try once and fail).
    pub max_attempts: u32,
    /// Initial backoff duration before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
        }
    }
}

/// Semantics guarantee for async external calls.
///
/// # v1 Limitation
///
/// Only [`AtLeastOnce`][AsyncSemantics::AtLeastOnce] is supported in v1.
/// Exactly-Once requires transactional external services and is deferred to v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AsyncSemantics {
    /// Recovery replays requests from the last checkpoint.
    /// The external service **must be idempotent** (safe to call multiple times).
    AtLeastOnce,
}

// ============================================================================
// Operator State (for snapshot/restore)
// ============================================================================

/// Snapshot of `AsyncExternalOperator` state, persisted during checkpoints.
///
/// In v1 (synchronous), there are no truly in-flight requests at checkpoint time,
/// so only the processed record count is stored for observability and future use.
#[derive(Debug, Serialize, Deserialize)]
struct AsyncCallSnapshot {
    /// Total records successfully processed since operator start (or last restore).
    processed_count: u64,
}

// ============================================================================
// AsyncExternalOperator
// ============================================================================

/// Operator that calls an external service for each input record.
///
/// ## Type Parameters
///
/// - `IN`: Input record type (must satisfy [`Send`] + `'static`).
/// - `OUT`: Output record type produced by the external call.
/// - `F`: Synchronous call function `Fn(&IN) -> Result<OUT>`.
///
/// ## At-Least-Once Semantics (v1)
///
/// Recovery replays from the last checkpoint. The call function `F` will be
/// re-invoked for all records processed since the last checkpoint. The external
/// service must handle duplicate calls correctly (idempotency).
///
/// ## Retry Behavior (v1)
///
/// When `retry.max_attempts > 1`, failed calls are retried with exponential backoff
/// **inline** (blocking). This is acceptable for v1; v2 will use async retry.
///
/// ## Upgrade Path to v2 (Async)
///
/// v2 will replace the synchronous `call_fn: F` with an async function and use
/// `FuturesUnordered` to issue up to `config.max_concurrent` requests in parallel.
/// The `AsyncCallConfig` and `AsyncSemantics` types are forward-compatible.
pub struct AsyncExternalOperator<IN, OUT, F>
where
    F: Fn(&IN) -> Result<OUT> + Send,
    IN: Send,
    OUT: Send,
{
    /// The synchronous call function. In v2 this becomes `async Fn`.
    call_fn: F,
    /// Configuration for concurrency, timeout, and retry.
    config: AsyncCallConfig,
    /// Total records processed (for snapshot/restore and observability).
    processed_count: u64,
    _phantom: std::marker::PhantomData<(IN, OUT)>,
}

impl<IN, OUT, F> AsyncExternalOperator<IN, OUT, F>
where
    F: Fn(&IN) -> Result<OUT> + Send,
    IN: Send,
    OUT: Send,
{
    /// Create a new operator with the given call function and configuration.
    ///
    /// # Arguments
    ///
    /// - `call_fn`: Synchronous function invoked for each record. Must be idempotent.
    /// - `config`: Controls concurrency, batching, timeouts, and retry behavior.
    pub fn new(call_fn: F, config: AsyncCallConfig) -> Self {
        Self {
            call_fn,
            config,
            processed_count: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Call the external function for a single record, retrying on failure.
    ///
    /// Uses exponential backoff up to `config.retry.max_backoff`.
    /// In v1, retries block the calling thread (synchronous sleep).
    fn call_with_retry(&self, input: &IN) -> Result<OUT> {
        let retry = &self.config.retry;
        let mut backoff = retry.initial_backoff;
        let mut last_err = None;

        for attempt in 0..retry.max_attempts {
            match (self.call_fn)(input) {
                Ok(out) => return Ok(out),
                Err(e) => {
                    last_err = Some(e);
                    // Don't sleep after the final attempt.
                    if attempt + 1 < retry.max_attempts {
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(retry.max_backoff);
                    }
                }
            }
        }

        Err(last_err.expect("max_attempts must be >= 1"))
    }
}

impl<IN, OUT, F> Operator<IN> for AsyncExternalOperator<IN, OUT, F>
where
    F: Fn(&IN) -> Result<OUT> + Send,
    IN: Send,
    OUT: Send,
{
    type OUT = OUT;

    /// Process a batch of records by calling the external function for each.
    ///
    /// In v1 all calls are synchronous and inline. v2 will dispatch them
    /// concurrently via `FuturesUnordered`.
    ///
    /// **At-Least-Once**: if the task crashes after processing some records but
    /// before the next checkpoint, these records will be replayed on recovery.
    fn process_batch(&mut self, input: &[IN], output: &mut Vec<OUT>) -> Result<()> {
        for record in input {
            let result = self.call_with_retry(record)?;
            output.push(result);
            self.processed_count += 1;
        }
        Ok(())
    }

    /// Snapshot operator state for checkpoint.
    ///
    /// Stores `processed_count` for observability. In v1 there are no pending
    /// in-flight requests at checkpoint time (all calls are synchronous).
    fn snapshot_state(&self) -> Result<Vec<u8>> {
        let snap = AsyncCallSnapshot {
            processed_count: self.processed_count,
        };
        let bytes = bincode::serialize(&snap)
            .map_err(|e| anyhow::anyhow!("snapshot serialize error: {e}"))?;
        Ok(bytes)
    }

    /// Restore operator state from a checkpoint snapshot.
    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let snap: AsyncCallSnapshot = bincode::deserialize(data)
            .map_err(|e| anyhow::anyhow!("snapshot deserialize error: {e}"))?;
        self.processed_count = snap.processed_count;
        Ok(())
    }

    /// Hook called when a checkpoint barrier is aligned.
    ///
    /// In v1 (synchronous), all records are processed before this is called,
    /// so there is nothing to flush. v2 will drain the `FuturesUnordered` here.
    fn on_checkpoint_barrier(&mut self, _checkpoint_id: u64) -> Result<()> {
        // v1: no-op — all records processed synchronously before barrier arrives.
        // v2: drain pending futures and wait for all in-flight calls to complete.
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[path = "tests/async_call_tests.rs"]
mod tests;
