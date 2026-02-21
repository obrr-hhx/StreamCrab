//! Local channels for inter-task communication.
//!
//! Uses crossbeam-channel for bounded, backpressure-aware communication
//! between Task instances running in different threads.

use crate::types::StreamElement;
use anyhow::{Result, anyhow};
use crossbeam_channel::{Receiver, Sender, bounded};

/// Default channel buffer size (bounded for backpressure).
///
/// Flink uses credit-based flow control; we use simpler bounded channels.
/// Buffer size affects:
/// - Memory usage: larger = more buffering
/// - Latency: smaller = lower latency
/// - Throughput: larger = better throughput under bursty load
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Sender side of a local channel.
///
/// Wraps crossbeam-channel Sender with StreamElement-specific API.
#[derive(Clone)]
pub struct LocalChannelSender<T> {
    sender: Sender<StreamElement<T>>,
}

impl<T> LocalChannelSender<T> {
    /// Send a stream element to the channel.
    ///
    /// Blocks if the channel is full (backpressure).
    pub fn send(&self, element: StreamElement<T>) -> Result<()> {
        self.sender
            .send(element)
            .map_err(|_| anyhow!("Channel closed: receiver dropped"))
    }

    /// Try to send without blocking.
    ///
    /// Returns error if channel is full or closed.
    pub fn try_send(&self, element: StreamElement<T>) -> Result<()> {
        self.sender
            .try_send(element)
            .map_err(|e| anyhow!("Failed to send: {:?}", e))
    }
}

/// Receiver side of a local channel.
pub struct LocalChannelReceiver<T> {
    pub(crate) receiver: Receiver<StreamElement<T>>,
}

impl<T> LocalChannelReceiver<T> {
    /// Receive the next stream element from the channel.
    ///
    /// Blocks until an element is available.
    pub fn recv(&self) -> Result<StreamElement<T>> {
        self.receiver
            .recv()
            .map_err(|_| anyhow!("Channel closed: sender dropped"))
    }

    /// Try to receive without blocking.
    ///
    /// Returns None if no element is available.
    pub fn try_recv(&self) -> Result<Option<StreamElement<T>>> {
        match self.receiver.try_recv() {
            Ok(elem) => Ok(Some(elem)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Err(anyhow!("Channel closed: sender dropped"))
            }
        }
    }

    /// Check if the channel is closed (all senders dropped).
    pub fn is_closed(&self) -> bool {
        self.receiver.is_empty() && self.receiver.len() == 0
    }
}

/// Create a bounded local channel pair.
///
/// Returns (sender, receiver) with the specified capacity.
///
/// # Backpressure
/// When the channel is full, `send()` will block until space is available.
/// This provides natural backpressure propagation through the pipeline.
pub fn local_channel<T>(capacity: usize) -> (LocalChannelSender<T>, LocalChannelReceiver<T>) {
    let (sender, receiver) = bounded(capacity);
    (
        LocalChannelSender { sender },
        LocalChannelReceiver { receiver },
    )
}

/// Create a local channel with default capacity.
pub fn local_channel_default<T>() -> (LocalChannelSender<T>, LocalChannelReceiver<T>) {
    local_channel(DEFAULT_CHANNEL_CAPACITY)
}

#[cfg(test)]
#[path = "tests/channel_tests.rs"]
mod tests;
