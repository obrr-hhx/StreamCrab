//! InputGate for reading from multiple input channels.
//!
//! Implements Flink's InputGate semantics:
//! - Reads from multiple upstream tasks
//! - Fair selection across channels
//! - Tracks channel end markers

use crate::channel::LocalChannelReceiver;
use crate::types::StreamElement;
use anyhow::{Result, anyhow};
use crossbeam_channel::Select;
use std::time::Duration;

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

    /// Channels that newly transitioned to ended state since last drain.
    newly_ended_channels: Vec<ChannelIndex>,
}

impl<T> InputGate<T> {
    /// Create a new InputGate with the given input channels.
    pub fn new(channels: Vec<LocalChannelReceiver<T>>) -> Self {
        let num_channels = channels.len();
        Self {
            channels,
            ended_channels: vec![false; num_channels],
            ended_count: 0,
            newly_ended_channels: Vec::new(),
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

    /// Get the next stream element with timeout.
    ///
    /// Returns:
    /// - `Ok(Some((channel, element)))` when an element is received
    /// - `Ok(None)` on timeout
    /// - `Err(...)` when all channels ended or channel closed unexpectedly
    pub fn next_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<(ChannelIndex, StreamElement<T>)>> {
        if self.ended_count == self.channels.len() {
            return Err(anyhow!("All input channels have ended"));
        }

        loop {
            let mut select = Select::new();
            let mut active_indices = Vec::new();

            for (idx, receiver) in self.channels.iter().enumerate() {
                if !self.ended_channels[idx] {
                    select.recv(&receiver.receiver);
                    active_indices.push(idx);
                }
            }

            let oper = match select.select_timeout(timeout) {
                Ok(oper) => oper,
                Err(_) => return Ok(None),
            };

            let selected_idx = oper.index();
            let channel_idx = active_indices[selected_idx];
            let element = oper
                .recv(&self.channels[channel_idx].receiver)
                .map_err(|_| anyhow!("Channel {} closed unexpectedly", channel_idx))?;

            if matches!(element, StreamElement::End) {
                self.mark_ended(channel_idx);
                if self.ended_count == self.channels.len() {
                    return Ok(Some((channel_idx, element)));
                }
                continue;
            }

            return Ok(Some((channel_idx, element)));
        }
    }

    /// Mark a channel as ended.
    ///
    /// Called when StreamElement::End is received from a channel.
    pub fn mark_ended(&mut self, channel_idx: ChannelIndex) {
        if !self.ended_channels[channel_idx] {
            self.ended_channels[channel_idx] = true;
            self.ended_count += 1;
            self.newly_ended_channels.push(channel_idx);
        }
    }

    /// Drain channels that became ended since last call.
    ///
    /// This allows the task loop to observe non-terminal End markers that are
    /// otherwise consumed internally by [`next`](Self::next) / [`next_timeout`](Self::next_timeout).
    pub fn drain_newly_ended_channels(&mut self) -> Vec<ChannelIndex> {
        std::mem::take(&mut self.newly_ended_channels)
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
#[path = "tests/input_gate_tests.rs"]
mod tests;
