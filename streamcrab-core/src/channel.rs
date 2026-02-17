//! Local channels for inter-task communication.
//!
//! Uses crossbeam-channel for bounded, backpressure-aware communication
//! between Task instances running in different threads.

use crate::types::StreamElement;
use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, Receiver, Sender};

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
mod tests {
    use super::*;
    use crate::types::StreamRecord;

    #[test]
    fn test_local_channel_send_recv() {
        let (sender, receiver) = local_channel::<i32>(10);

        // Send a record
        sender.send(StreamElement::record(42)).unwrap();

        // Receive it
        let elem = receiver.recv().unwrap();
        match elem {
            StreamElement::Record(rec) => assert_eq!(rec.value, 42),
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_local_channel_watermark() {
        let (sender, receiver) = local_channel::<i32>(10);

        sender.send(StreamElement::watermark(1000)).unwrap();

        let elem = receiver.recv().unwrap();
        match elem {
            StreamElement::Watermark(wm) => assert_eq!(wm.timestamp, 1000),
            _ => panic!("Expected Watermark"),
        }
    }

    #[test]
    fn test_local_channel_end() {
        let (sender, receiver) = local_channel::<i32>(10);

        sender.send(StreamElement::End).unwrap();

        let elem = receiver.recv().unwrap();
        assert_eq!(elem, StreamElement::End);
    }

    #[test]
    fn test_local_channel_backpressure() {
        let (sender, receiver) = local_channel::<i32>(2);

        // Fill the channel
        sender.send(StreamElement::record(1)).unwrap();
        sender.send(StreamElement::record(2)).unwrap();

        // try_send should fail (channel full)
        let result = sender.try_send(StreamElement::record(3));
        assert!(result.is_err());

        // Consume one element
        receiver.recv().unwrap();

        // Now try_send should succeed
        sender.try_send(StreamElement::record(3)).unwrap();
    }

    #[test]
    fn test_local_channel_closed() {
        let (sender, receiver) = local_channel::<i32>(10);

        sender.send(StreamElement::record(42)).unwrap();

        // Drop sender
        drop(sender);

        // Can still receive buffered element
        let elem = receiver.recv().unwrap();
        match elem {
            StreamElement::Record(rec) => assert_eq!(rec.value, 42),
            _ => panic!("Expected Record"),
        }

        // Next recv should fail (channel closed)
        let result = receiver.recv();
        assert!(result.is_err());
    }

    #[test]
    fn test_local_channel_clone_sender() {
        let (sender, receiver) = local_channel::<i32>(10);

        // Clone sender
        let sender2 = sender.clone();

        sender.send(StreamElement::record(1)).unwrap();
        sender2.send(StreamElement::record(2)).unwrap();

        assert_eq!(
            receiver.recv().unwrap(),
            StreamElement::Record(StreamRecord::new(1))
        );
        assert_eq!(
            receiver.recv().unwrap(),
            StreamElement::Record(StreamRecord::new(2))
        );
    }
}

