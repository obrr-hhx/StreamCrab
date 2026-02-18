//! OutputGate for sending to multiple output channels.
//!
//! Implements Flink's OutputGate semantics:
//! - Routes data to downstream tasks based on partitioning strategy
//! - Supports broadcast (send to all downstream)
//! - Supports forward (send to single downstream)

use crate::channel::LocalChannelSender;
use crate::partitioner::Partitioner;
use crate::types::StreamElement;
use anyhow::Result;

/// OutputGate sends to multiple output channels.
///
/// Provides different routing strategies:
/// - Partitioned: route based on key (using Partitioner)
/// - Broadcast: send to all downstream tasks
/// - Forward: send to single downstream (parallelism=1)
pub struct OutputGate<T> {
    /// Output channels to downstream tasks
    channels: Vec<LocalChannelSender<T>>,
}

impl<T> OutputGate<T> {
    /// Create a new OutputGate with the given output channels.
    pub fn new(channels: Vec<LocalChannelSender<T>>) -> Self {
        Self { channels }
    }

    /// Emit a record to a specific channel (by index).
    ///
    /// Used for partitioned output where the partitioner determines the target.
    pub fn emit_to(&self, channel_idx: usize, element: StreamElement<T>) -> Result<()> {
        self.channels[channel_idx].send(element)
    }

    /// Emit a record using a partitioner to determine the target channel.
    ///
    /// The partitioner computes: `target = hash(key) % num_channels`
    pub fn emit_partitioned<K, P>(
        &self,
        element: StreamElement<T>,
        key: &K,
        partitioner: &P,
    ) -> Result<()>
    where
        P: Partitioner<K>,
    {
        let target = partitioner.partition(key, self.channels.len());
        self.emit_to(target, element)
    }

    /// Broadcast an element to all output channels.
    ///
    /// Used for:
    /// - Watermarks (must propagate to all downstream)
    /// - Checkpoint barriers (must propagate to all downstream)
    /// - End markers (signal all downstream to finish)
    pub fn broadcast(&self, element: StreamElement<T>) -> Result<()>
    where
        T: Clone,
    {
        if self.channels.is_empty() {
            return Ok(());
        }

        // Clone for all but the last channel
        for channel in &self.channels[..self.channels.len() - 1] {
            channel.send(element.clone())?;
        }

        // Move for the last channel (no clone needed)
        self.channels.last().unwrap().send(element)?;

        Ok(())
    }

    /// Send to the single output channel (forward partitioning).
    ///
    /// Panics if there is not exactly one output channel.
    pub fn forward(&self, element: StreamElement<T>) -> Result<()> {
        assert_eq!(
            self.channels.len(),
            1,
            "Forward requires exactly 1 output channel"
        );
        self.channels[0].send(element)
    }

    /// Get the number of output channels.
    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::local_channel;
    use crate::partitioner::HashPartitioner;

    #[test]
    fn test_output_gate_emit_to() {
        let (sender1, receiver1) = local_channel::<i32>(10);
        let (sender2, receiver2) = local_channel::<i32>(10);
        let gate = OutputGate::new(vec![sender1, sender2]);

        gate.emit_to(0, StreamElement::record(1)).unwrap();
        gate.emit_to(1, StreamElement::record(2)).unwrap();

        let elem1 = receiver1.recv().unwrap();
        match elem1 {
            StreamElement::Record(rec) => assert_eq!(rec.value, 1),
            _ => panic!("Expected Record"),
        }

        let elem2 = receiver2.recv().unwrap();
        match elem2 {
            StreamElement::Record(rec) => assert_eq!(rec.value, 2),
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_output_gate_broadcast() {
        let (sender1, receiver1) = local_channel::<i32>(10);
        let (sender2, receiver2) = local_channel::<i32>(10);
        let gate = OutputGate::new(vec![sender1, sender2]);

        gate.broadcast(StreamElement::watermark(1000)).unwrap();

        let elem1 = receiver1.recv().unwrap();
        assert_eq!(elem1, StreamElement::watermark(1000));

        let elem2 = receiver2.recv().unwrap();
        assert_eq!(elem2, StreamElement::watermark(1000));
    }

    #[test]
    fn test_output_gate_forward() {
        let (sender, receiver) = local_channel::<i32>(10);
        let gate = OutputGate::new(vec![sender]);

        gate.forward(StreamElement::record(42)).unwrap();

        let elem = receiver.recv().unwrap();
        match elem {
            StreamElement::Record(rec) => assert_eq!(rec.value, 42),
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_output_gate_emit_partitioned() {
        let (sender1, receiver1) = local_channel::<i32>(10);
        let (sender2, receiver2) = local_channel::<i32>(10);
        let gate = OutputGate::new(vec![sender1, sender2]);

        let partitioner = HashPartitioner::new(|x: &i32| *x);

        // Emit multiple records with partitioning
        for i in 0..10 {
            gate.emit_partitioned(StreamElement::record(i), &i, &partitioner)
                .unwrap();
        }

        // Verify that records are distributed across channels
        let mut count1 = 0;
        let mut count2 = 0;

        while let Ok(Some(_)) = receiver1.try_recv() {
            count1 += 1;
        }
        while let Ok(Some(_)) = receiver2.try_recv() {
            count2 += 1;
        }

        // Should have distributed all 10 records
        assert_eq!(count1 + count2, 10);
        // Both channels should have received some records (with high probability)
        assert!(count1 > 0);
        assert!(count2 > 0);
    }
}
