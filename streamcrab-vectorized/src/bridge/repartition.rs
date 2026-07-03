//! Batch-level key repartitioning: split a RecordBatch into per-partition
//! sub-batches so a whole sub-batch is one channel send instead of N rows.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use streamcrab_core::operator_chain::Operator;
use streamcrab_core::partitioner::Partitioner;

/// Splits each RecordBatch by hashing the key columns (any Arrow type, via
/// the arrow row format) into `num_partitions` sub-batches.
///
/// Emits `(partition, sub-batch)` pairs; route them with
/// [`SubBatchPartitioner`]. Hashing uses fixed seeds so every upstream
/// subtask maps the same key to the same partition.
pub struct BatchRepartition {
    key_cols: Vec<usize>,
    num_partitions: usize,
    hash_state: ahash::RandomState,
}

impl BatchRepartition {
    pub fn new(key_cols: Vec<usize>, num_partitions: usize) -> Self {
        assert!(!key_cols.is_empty(), "key_cols must not be empty");
        assert!(num_partitions > 0, "num_partitions must be positive");
        Self {
            key_cols,
            num_partitions,
            hash_state: ahash::RandomState::with_seeds(0x51EA, 0xC3A8, 0x2F4D, 0x9E37),
        }
    }

    fn split(&self, batch: &RecordBatch, output: &mut Vec<(usize, RecordBatch)>) -> Result<()> {
        let key_arrays: Vec<ArrayRef> = self
            .key_cols
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        let fields: Vec<SortField> = key_arrays
            .iter()
            .map(|a| SortField::new(a.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields).context("BatchRepartition: row converter")?;
        let rows = converter
            .convert_columns(&key_arrays)
            .context("BatchRepartition: convert key columns")?;

        let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); self.num_partitions];
        for i in 0..batch.num_rows() {
            let hash = self.hash_state.hash_one(rows.row(i).as_ref());
            buckets[(hash as usize) % self.num_partitions].push(i as u32);
        }

        for (p, indices) in buckets.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let idx = UInt32Array::from(indices);
            let cols = batch
                .columns()
                .iter()
                .map(|c| take(c, &idx, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("BatchRepartition: take rows")?;
            let sub = RecordBatch::try_new(batch.schema(), cols)
                .context("BatchRepartition: build sub-batch")?;
            output.push((p, sub));
        }
        Ok(())
    }
}

impl Operator<RecordBatch> for BatchRepartition {
    type OUT = (usize, RecordBatch);

    fn process_batch(
        &mut self,
        input: &[RecordBatch],
        output: &mut Vec<(usize, RecordBatch)>,
    ) -> Result<()> {
        for batch in input {
            self.split(batch, output)?;
        }
        Ok(())
    }
}

/// Routes the `(partition, sub-batch)` pairs produced by [`BatchRepartition`].
pub struct SubBatchPartitioner;

impl<B: Send + Sync> Partitioner<(usize, B)> for SubBatchPartitioner {
    fn partition(&self, value: &(usize, B), num_partitions: usize) -> usize {
        value.0 % num_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::ArrowConvertible;
    use std::collections::HashMap;

    #[test]
    fn repartition_preserves_rows_and_key_locality() {
        let rows: Vec<(i64, i64)> = (0..100).map(|i| (i % 7, i)).collect();
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();

        let mut op = BatchRepartition::new(vec![0], 4);
        let mut out = Vec::new();
        op.process_batch(&[batch], &mut out).unwrap();

        // Row count preserved and every key lands in exactly one partition.
        let mut total = 0usize;
        let mut key_to_partition: HashMap<i64, usize> = HashMap::new();
        for (p, sub) in &out {
            assert!(*p < 4);
            let sub_rows = <(i64, i64)>::from_batch(sub).unwrap();
            total += sub_rows.len();
            for (key, _) in sub_rows {
                let prev = key_to_partition.insert(key, *p);
                assert!(prev.is_none_or(|old| old == *p), "key {key} split across partitions");
            }
        }
        assert_eq!(total, 100);
    }

    #[test]
    fn repartition_is_deterministic_across_instances() {
        let rows: Vec<(i64, i64)> = (0..50).map(|i| (i, i)).collect();
        let batch = <(i64, i64)>::to_batch(&rows).unwrap();

        let run = |op: &mut BatchRepartition| {
            let mut out = Vec::new();
            op.process_batch(std::slice::from_ref(&batch), &mut out).unwrap();
            let mut map: Vec<(usize, Vec<(i64, i64)>)> = out
                .iter()
                .map(|(p, b)| (*p, <(i64, i64)>::from_batch(b).unwrap()))
                .collect();
            map.sort_by_key(|(p, _)| *p);
            map
        };

        let a = run(&mut BatchRepartition::new(vec![0], 8));
        let b = run(&mut BatchRepartition::new(vec![0], 8));
        assert_eq!(a, b);
    }

    #[test]
    fn sub_batch_partitioner_routes_by_index() {
        let p = SubBatchPartitioner;
        assert_eq!(p.partition(&(5usize, ()), 4), 1);
        assert_eq!(p.partition(&(3usize, ()), 4), 3);
    }
}
