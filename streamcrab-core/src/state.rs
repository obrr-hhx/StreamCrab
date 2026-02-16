//! # State Management
//!
//! Keyed state backends for stateful stream processing.
//!
//! ## State Types
//!
//! - [`ValueState`] — Single value per key
//! - [`ListState`] — List of values per key
//! - [`MapState`] — Map of key-value pairs per key
//!
//! ## Backends
//!
//! - [`HashMapStateBackend`] — In-memory HashMap (Local mode, extreme performance)
//! - Future: RocksDBStateBackend (for larger-than-memory state)

use crate::types::StreamData;
use anyhow::Result;

pub mod hashmap;

pub use hashmap::HashMapStateBackend;

/// Value state: stores a single value per key.
pub trait ValueState<V>: Send
where
    V: StreamData,
{
    /// Get the current value.
    fn get(&self) -> Result<Option<V>>;

    /// Update the value.
    fn put(&mut self, value: V) -> Result<()>;

    /// Clear the value.
    fn clear(&mut self) -> Result<()>;
}

/// List state: stores a list of values per key.
pub trait ListState<V>: Send
where
    V: StreamData,
{
    /// Get all values in the list.
    fn get(&self) -> Result<Vec<V>>;

    /// Add a value to the list.
    fn add(&mut self, value: V) -> Result<()>;

    /// Clear the list.
    fn clear(&mut self) -> Result<()>;
}

/// Map state: stores a map of key-value pairs per key.
pub trait MapState<K, V>: Send
where
    K: StreamData + std::hash::Hash + Eq,
    V: StreamData,
{
    /// Get a value by map key.
    fn get(&self, key: &K) -> Result<Option<V>>;

    /// Put a key-value pair.
    fn put(&mut self, key: K, value: V) -> Result<()>;

    /// Remove a key.
    fn remove(&mut self, key: &K) -> Result<Option<V>>;

    /// Clear the entire map.
    fn clear(&mut self) -> Result<()>;
}

/// Keyed state backend: manages state for the current processing key.
///
/// The backend maintains a "current key" context. All state operations
/// are scoped to this key.
pub trait KeyedStateBackend: Send {
    /// Set the current processing key.
    ///
    /// All subsequent state operations will be scoped to this key.
    fn set_current_key(&mut self, key: Vec<u8>);

    /// Get a value state handle.
    fn get_value_state<V>(&mut self, name: &str) -> Box<dyn ValueState<V> + '_>
    where
        V: StreamData;

    /// Get a list state handle.
    fn get_list_state<V>(&mut self, name: &str) -> Box<dyn ListState<V> + '_>
    where
        V: StreamData;

    /// Get a map state handle.
    fn get_map_state<K, V>(&mut self, name: &str) -> Box<dyn MapState<K, V> + '_>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData;

    /// Snapshot all state for checkpointing.
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Restore state from a checkpoint.
    fn restore(&mut self, data: &[u8]) -> Result<()>;
}

