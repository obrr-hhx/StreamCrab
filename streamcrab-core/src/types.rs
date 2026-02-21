use serde::{Deserialize, Serialize};

/// Event time in milliseconds since epoch.
pub type EventTime = i64;

/// Unique identifier for checkpoint barriers.
pub type CheckpointId = u64;

/// Unique identifier for graph nodes.
pub type NodeId = u32;

/// A record in the stream, carrying user data and optional event time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamRecord<T> {
    pub value: T,
    pub timestamp: Option<EventTime>,
}

impl<T> StreamRecord<T> {
    /// Create a record with no event time.
    pub fn new(value: T) -> Self {
        Self {
            value,
            timestamp: None,
        }
    }

    /// Create a record with an explicit event time.
    pub fn with_timestamp(value: T, timestamp: EventTime) -> Self {
        Self {
            value,
            timestamp: Some(timestamp),
        }
    }
}

/// Watermark indicates that no elements with timestamp <= this value will arrive.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Watermark {
    pub timestamp: EventTime,
}

impl Watermark {
    /// Create a new watermark at the given timestamp.
    pub fn new(timestamp: EventTime) -> Self {
        Self { timestamp }
    }
}

impl std::fmt::Display for Watermark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Watermark({}ms)", self.timestamp)
    }
}

/// Checkpoint barrier for Chandy-Lamport snapshots.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Barrier {
    pub checkpoint_id: CheckpointId,
    pub timestamp: EventTime,
}

impl Barrier {
    /// Create a new checkpoint barrier with the given ID.
    pub fn new(checkpoint_id: CheckpointId) -> Self {
        Self {
            checkpoint_id,
            timestamp: 0,
        }
    }

    /// Create a new checkpoint barrier with explicit timestamp.
    pub fn with_timestamp(checkpoint_id: CheckpointId, timestamp: EventTime) -> Self {
        Self {
            checkpoint_id,
            timestamp,
        }
    }
}

/// The fundamental unit flowing through the stream processing pipeline.
/// Everything is a stream element: data records, watermarks, barriers, and end markers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StreamElement<T> {
    /// User data record.
    Record(StreamRecord<T>),
    /// Watermark for event time progress tracking.
    Watermark(Watermark),
    /// Checkpoint barrier for exactly-once snapshots.
    CheckpointBarrier(Barrier),
    /// End of bounded stream.
    End,
}

impl<T> StreamElement<T> {
    /// Create a record element with no timestamp.
    pub fn record(value: T) -> Self {
        Self::Record(StreamRecord::new(value))
    }

    /// Create a record element with a timestamp.
    pub fn timestamped_record(value: T, timestamp: EventTime) -> Self {
        Self::Record(StreamRecord::with_timestamp(value, timestamp))
    }

    /// Create a watermark element.
    pub fn watermark(timestamp: EventTime) -> Self {
        Self::Watermark(Watermark::new(timestamp))
    }

    /// Create a checkpoint barrier element.
    pub fn barrier(checkpoint_id: CheckpointId) -> Self {
        Self::CheckpointBarrier(Barrier::new(checkpoint_id))
    }

    /// Create a checkpoint barrier element with explicit timestamp.
    pub fn barrier_with_timestamp(checkpoint_id: CheckpointId, timestamp: EventTime) -> Self {
        Self::CheckpointBarrier(Barrier::with_timestamp(checkpoint_id, timestamp))
    }
}

/// Trait bound for types that can flow through the stream.
/// All user data types must satisfy this.
pub trait StreamData: Send + Clone + Serialize + for<'de> Deserialize<'de> + 'static {}

// Blanket implementation: any type satisfying the bounds is StreamData.
impl<T> StreamData for T where T: Send + Clone + Serialize + for<'de> Deserialize<'de> + 'static {}

// --- Type-erased cloneable value for the runtime ---

/// Trait object that supports Any + Clone.
pub trait CloneableAny: std::any::Any {
    fn clone_box(&self) -> Box<dyn CloneableAny>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any>;
}

impl<T: Clone + 'static> CloneableAny for T {
    fn clone_box(&self) -> Box<dyn CloneableAny> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

/// A cloneable, type-erased value. Used in the runtime to pass data between operators.
pub struct BoxedValue(Box<dyn CloneableAny>);

impl BoxedValue {
    /// Wrap a concrete value into a type-erased box.
    pub fn new<T: Clone + 'static>(val: T) -> Self {
        Self(Box::new(val))
    }

    /// Try to get a reference to the inner value as type `T`. Returns `None` on mismatch.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.as_any().downcast_ref()
    }

    /// Unwrap the inner value. Panics on type mismatch.
    pub fn downcast<T: 'static>(self) -> T {
        *self
            .0
            .into_any()
            .downcast::<T>()
            .expect("BoxedValue::downcast type mismatch")
    }
}

impl Clone for BoxedValue {
    fn clone(&self) -> Self {
        Self(self.0.clone_box())
    }
}

impl std::fmt::Debug for BoxedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoxedValue(<type-erased>)")
    }
}

#[cfg(test)]
#[path = "tests/types_tests.rs"]
mod tests;
