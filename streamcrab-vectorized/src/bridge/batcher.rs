//! Row ↔ column boundary operators.

use std::marker::PhantomData;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde::de::DeserializeOwned;
use streamcrab_core::operator_chain::{Operator, TimerDomain};
use streamcrab_core::types::EventTime;

use crate::convert::ArrowConvertible;

/// Buffers rows and emits Arrow RecordBatches of `batch_size` rows.
///
/// Flush triggers:
/// - the buffer reaches `batch_size` (inside `process_batch`)
/// - any timer fires (`on_timer`) — the runtime fires event-time timers on
///   watermark alignment and drives a final `EVENT_TIME_MAX` timer before a
///   bounded stream ends, so the tail partial batch is never lost
/// - checkpoint barriers need no emit hook: pending rows are part of
///   `snapshot_state`, and recovery re-emits them from the restored buffer
pub struct Batcher<T> {
    buf: Vec<T>,
    batch_size: usize,
}

impl<T> Batcher<T> {
    pub fn new(batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be positive");
        Self {
            buf: Vec::with_capacity(batch_size),
            batch_size,
        }
    }
}

impl<T> Operator<T> for Batcher<T>
where
    T: ArrowConvertible + Clone + Send + Serialize + DeserializeOwned + 'static,
{
    type OUT = RecordBatch;

    fn process_batch(&mut self, input: &[T], output: &mut Vec<RecordBatch>) -> Result<()> {
        let mut rest = input;

        // Top up a partially filled buffer first.
        if !self.buf.is_empty() {
            let need = self.batch_size - self.buf.len();
            let take = need.min(rest.len());
            self.buf.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.buf.len() == self.batch_size {
                let rows =
                    std::mem::replace(&mut self.buf, Vec::with_capacity(self.batch_size));
                output.push(T::to_batch(&rows)?);
            }
        }

        // Whole batches convert straight from the input slice — no copy into
        // the buffer, and never drain from a Vec front (that shifts every
        // remaining element and turns a large input into O(n²) memmove).
        let mut chunks = rest.chunks_exact(self.batch_size);
        for chunk in &mut chunks {
            output.push(T::to_batch(chunk)?);
        }
        self.buf.extend_from_slice(chunks.remainder());
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timestamp: EventTime,
        _domain: TimerDomain,
        output: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if !self.buf.is_empty() {
            let rows = std::mem::take(&mut self.buf);
            output.push(T::to_batch(&rows)?);
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        bincode::serialize(&self.buf).context("Batcher: snapshot pending rows")
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.buf = bincode::deserialize(data).context("Batcher: restore pending rows")?;
        Ok(())
    }
}

/// Splits RecordBatches back into rows for row-oriented operators and sinks.
pub struct Unbatcher<T> {
    _marker: PhantomData<T>,
}

impl<T> Unbatcher<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for Unbatcher<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Operator<RecordBatch> for Unbatcher<T>
where
    T: ArrowConvertible + Send + 'static,
{
    type OUT = T;

    fn process_batch(&mut self, input: &[RecordBatch], output: &mut Vec<T>) -> Result<()> {
        for batch in input {
            output.extend(T::from_batch(batch)?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamcrab_core::time::EVENT_TIME_MAX;

    #[test]
    fn batcher_emits_full_batches_and_keeps_remainder() {
        let mut batcher = Batcher::<i64>::new(4);
        let mut out = Vec::new();
        batcher.process_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9], &mut out).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|b| b.num_rows() == 4));

        // Remainder flushes on timer.
        out.clear();
        batcher
            .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);

        // Nothing pending afterwards.
        out.clear();
        batcher
            .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
            .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn batcher_pending_rows_survive_snapshot_restore() {
        let mut batcher = Batcher::<i64>::new(100);
        let mut out = Vec::new();
        batcher.process_batch(&[1, 2, 3], &mut out).unwrap();
        assert!(out.is_empty());

        let snapshot = batcher.snapshot_state().unwrap();
        let mut restored = Batcher::<i64>::new(100);
        restored.restore_state(&snapshot).unwrap();

        restored
            .on_timer(EVENT_TIME_MAX, TimerDomain::EventTime, &mut out)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(i64::from_batch(&out[0]).unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn unbatcher_roundtrip() {
        let rows = vec![(1i64, 10i64), (2, 20), (3, 30)];
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();
        let mut unbatcher = Unbatcher::<(i64, i64)>::new();
        let mut out = Vec::new();
        unbatcher.process_batch(&[batch], &mut out).unwrap();
        assert_eq!(out, rows);
    }
}
