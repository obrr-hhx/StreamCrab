//! Adapter: any [`VectorizedOperator`] as a core runtime [`Operator`].

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use streamcrab_core::operator_chain::{Operator, TimerDomain};
use streamcrab_core::types::EventTime;

use crate::batch::VeloxBatch;
use crate::operators::VectorizedOperator;

/// Wraps a vectorized operator (Filter/Project/HashAggregate/...) so it can
/// be chained into StreamCrab's operator chain over `RecordBatch` elements.
///
/// Semantics:
/// - each incoming batch goes through `add_input` / `get_output`
/// - event-time timers (watermarks, end-of-stream) call `on_watermark`, which
///   is where accumulating operators like HashAggregate emit their state —
///   for a bounded stream that means exactly one final result set
/// - processing-time ticks are ignored (some execution paths tick every
///   element, which would make accumulators spew intermediate results)
/// - snapshot/restore delegate to the wrapped operator's own state
pub struct VectorOp<V> {
    inner: V,
}

impl<V> VectorOp<V> {
    pub fn new(inner: V) -> Self {
        Self { inner }
    }
}

impl<V> Operator<RecordBatch> for VectorOp<V>
where
    V: VectorizedOperator + 'static,
{
    type OUT = RecordBatch;

    fn process_batch(&mut self, input: &[RecordBatch], output: &mut Vec<RecordBatch>) -> Result<()> {
        for batch in input {
            self.inner.add_input(VeloxBatch::new(batch.clone()))?;
            while let Some(out) = self.inner.get_output()? {
                let rb = out.into_record_batch()?;
                if rb.num_rows() > 0 {
                    output.push(rb);
                }
            }
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        timestamp: EventTime,
        domain: TimerDomain,
        output: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if domain == TimerDomain::EventTime {
            for vb in self.inner.on_watermark(timestamp)? {
                let rb = vb.into_record_batch()?;
                if rb.num_rows() > 0 {
                    output.push(rb);
                }
            }
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        self.inner.snapshot_state()
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        self.inner.restore_state(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::ArrowConvertible;
    use crate::expression::{col, gt, lit_i64};
    use crate::operators::FilterOperator;

    #[test]
    fn vector_op_filters_batches() {
        let rows: Vec<(i64, i64)> = (1..=10).map(|i| (i, i * 10)).collect();
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();

        let mut op = VectorOp::new(FilterOperator::new(gt(col(1), lit_i64(50))));
        let mut out = Vec::new();
        op.process_batch(&[batch], &mut out).unwrap();

        assert_eq!(out.len(), 1);
        let survivors = <(i64, i64)>::from_batch(&out[0]).unwrap();
        assert_eq!(survivors, vec![(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)]);
    }

    #[test]
    fn vector_op_drops_empty_result_batches() {
        let rows = vec![(1i64, 10i64)];
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();

        let mut op = VectorOp::new(FilterOperator::new(gt(col(1), lit_i64(999))));
        let mut out = Vec::new();
        op.process_batch(&[batch], &mut out).unwrap();
        assert!(out.is_empty());
    }
}
