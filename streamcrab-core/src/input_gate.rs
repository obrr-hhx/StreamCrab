//! InputGate for reading from multiple input channels.
//!
//! Implements Flink's InputGate semantics:
//! - Reads from multiple upstream tasks
//! - Fair selection across channels
//! - Tracks channel end markers

use crate::channel::LocalChannelReceiver;
use crate::types::StreamElement;
use anyhow::{anyhow, Result};
use crossbeam_channel::Select;

/// Channel identifier (index in the input gate).
pub type ChannelIndex = usize;

/// InputGate reads from multiple input channels.
///
/// Provides fair selection across channels using crossbeam's Select.
/// Tracks which channels have ended to detect when all inputs are exhausted.
pub struct InputGate<T> {
    /// Input channels from upstream tasks
    channels: Vec<LocalChannelReceiver<T>>,
    
    /// Track which channels have ended (received StreamElement::End)
    ended_channels: Vec<bool>,
    
    /// Number of channels that have ended
    ended_count: usize,
}

impl<T> InputGate<T> {
    /// Create a new InputGate with the given input channels.
    pub fn new(channels: Vec<LocalChannelReceiver<T>>) -> Self {
        let num_channels = channels.len();
        Self {
            channels,
            ended_channels: vec![false; num_channels],
            ended_count: 0,
        }
    }

    /// Get the next stream element from any input channel.
    ///
    /// Returns (channel_index, element).
    ///
    /// Uses fair selection: all channels have equal priority.
    /// Skips channels that have already ended.
    ///
    /// Returns error when all channels have ended.
    pub fn next(&mut self) -> Result<(ChannelIndex, StreamElement<T>)> {
        if self.ended_count == self.channels.len() {
            return Err(anyhow!("All input channels have ended"));
        }

        loop {
            // Build a Select operation for non-ended channels
            let mut select = Select::new();
            let mut active_indices = Vec::new();

            for (idx, receiver) in self.channels.iter().enumerate() {
                if !self.ended_channels[idx] {
                    select.recv(&receiver.receiver);
                    active_indices.push(idx);
                }
            }

            // Wait for any channel to have data
            let oper = select.select();
            let selected_idx = oper.index();
            let channel_idx = active_indices[selected_idx];

            // Receive from the selected channel
            let element = oper
                .recv(&self.channels[channel_idx].receiver)
                .map_err(|_| anyhow!("Channel {} closed unexpectedly", channel_idx))?;

            // Check if this is an End marker
            if matches!(element, StreamElement::End) {
                self.mark_ended(channel_idx);
                
                // If all channels ended, return the End marker
                if self.ended_count == self.channels.len() {
                    return Ok((channel_idx, element));
                }
                
                // Otherwise, continue to next iteration to read from other channels
                continue;
            }

            return Ok((channel_idx, element));
        }
    }

    /// Mark a channel as ended.
    ///
    /// Called when StreamElement::End is received from a channel.
    pub fn mark_ended(&mut self, channel_idx: ChannelIndex) {
        if !self.ended_channels[channel_idx] {
            self.ended_channels[channel_idx] = true;
            self.ended_count += 1;
        }
    }

    /// Check if all input channels have ended.
    pub fn all_ended(&self) -> bool {
        self.ended_count == self.channels.len()
    }

    /// Get the number of input channels.
    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }

    /// Get the number of channels that have ended.
    pub fn num_ended(&self) -> usize {
        self.ended_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::local_channel;

    #[test]
    fn test_input_gate_single_channel() {
        let (sender, receiver) = local_channel::<i32>(10);
        let mut gate = InputGate::new(vec![receiver]);

        sender.send(StreamElement::record(42)).unwrap();
        sender.send(StreamElement::End).unwrap();

        let (ch, elem) = gate.next().unwrap();
        assert_eq!(ch, 0);
        match elem {
            StreamElement::Record(rec) => assert_eq!(rec.value, 42),
            _ => panic!("Expected Record"),
        }

        let (ch, elem) = gate.next().unwrap();
        assert_eq!(ch, 0);
        assert_eq!(elem, StreamElement::End);
        assert!(gate.all_ended());
    }

    #[test]
    fn test_input_gate_multiple_channels() {
        let (sender1, receiver1) = local_channel::<i32>(10);
        let (sender2, receiver2) = local_channel::<i32>(10);
        let mut gate = InputGate::new(vec![receiver1, receiver2]);

        assert_eq!(gate.num_channels(), 2);
        assert_eq!(gate.num_ended(), 0);

        // Send from both channels
        sender1.send(StreamElement::record(1)).unwrap();
        sender2.send(StreamElement::record(2)).unwrap();

        // Should receive from both (order may vary)
        let (_, elem1) = gate.next().unwrap();
        let (_, elem2) = gate.next().unwrap();

        let mut values = vec![];
        if let StreamElement::Record(rec) = elem1 {
            values.push(rec.value);
        }
        if let StreamElement::Record(rec) = elem2 {
            values.push(rec.value);
        }
        values.sort();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn test_input_gate_partial_end() {
        let (sender1, receiver1) = local_channel::<i32>(10);
        let (sender2, receiver2) = local_channel::<i32>(10);
        let mut gate = InputGate::new(vec![receiver1, receiver2]);

        // Send data from channel 1
        sender2.send(StreamElement::record(1)).unwrap();
        sender2.send(StreamElement::record(2)).unwrap();

        // Channel 0 ends
        sender1.send(StreamElement::End).unwrap();
        drop(sender1);

        // Read all elements (should get 2 records and skip the End)
        let mut received = vec![];
        for _ in 0..2 {
            let (_, elem) = gate.next().unwrap();
            if let StreamElement::Record(rec) = elem {
                received.push(rec.value);
            }
        }
        received.sort();
        assert_eq!(received, vec![1, 2]);

        // After reading, channel 0 should be marked as ended
        assert_eq!(gate.num_ended(), 1);
        assert!(!gate.all_ended());

        // Now channel 1 ends
        sender2.send(StreamElement::End).unwrap();
        let (ch, elem) = gate.next().unwrap();
        assert_eq!(ch, 1);
        assert_eq!(elem, StreamElement::End);
        assert!(gate.all_ended());
    }

    #[test]
    fn test_input_gate_all_ended_error() {
        let (sender, receiver) = local_channel::<i32>(10);
        let mut gate = InputGate::new(vec![receiver]);

        sender.send(StreamElement::End).unwrap();
        gate.next().unwrap(); // Consume the End

        // Next call should error
        let result = gate.next();
        assert!(result.is_err());
    }
}

