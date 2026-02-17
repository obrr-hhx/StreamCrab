//! # State Management
//!
//! Keyed state backends for stateful stream processing.
//!
//! ## Descriptor Pattern
//!
//! This module uses a **descriptor pattern** where state handles are lightweight
//! metadata objects that don't hold references to the backend. This design:
//!
//! - Avoids borrow-checker conflicts (can hold multiple handles simultaneously)
//! - Zero runtime overhead (no Arc/Mutex/RefCell needed)
//! - Compile-time safety (explicit borrows at call sites)
//! - Fit Rust ownership model (ownership model friendly)
//!
//! ## State Types
//!
//! - [`ValueStateHandle`] — Single value per key
//! - [`ListStateHandle`] — List of values per key
//! - [`MapStateHandle`] — Map of key-value pairs per key
//!
//! ## Backends
//!
//! - [`HashMapStateBackend`] — In-memory HashMap (Local mode, extreme performance)
//! - Future: RocksDBStateBackend (for larger-than-memory state)
//!
//! ## Example
//!
//! ```ignore
//! // Create handles (no borrowing)
//! let count = ValueStateHandle::<i32>::new("count");
//! let sum = ValueStateHandle::<i64>::new("sum");
//!
//! // Use handles (temporary borrows)
//! let c = count.get(&backend)?.unwrap_or(0);
//! count.put(&mut backend, c + 1)?;
//! ```

use crate::types::StreamData;
use anyhow::Result;
use std::marker::PhantomData;

pub mod hashmap;
pub use hashmap::HashMapStateBackend;

// ============================================================================
// State Handles (Descriptors)
// ============================================================================

/// Value state handle: a lightweight descriptor for accessing single-value state.
///
/// This handle doesn't hold any reference to the backend, allowing you to create
/// multiple handles without borrow-checker conflicts.
#[derive(Debug, Clone)]
pub struct ValueStateHandle<V> {
    name: String,
    _phantom: PhantomData<V>,
}

impl<V> ValueStateHandle<V> {
    /// Create a new value state handle.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _phantom: PhantomData,
        }
    }

    /// Get the state name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<V: StreamData> ValueStateHandle<V> {
    /// Get the current value.
    pub fn get(&self, backend: &impl KeyedStateBackend) -> Result<Option<V>> {
        backend.get_value(&self.name)
    }

    /// Update the value.
    pub fn put(&self, backend: &mut impl KeyedStateBackend, value: V) -> Result<()> {
        backend.put_value(&self.name, value)
    }

    /// Clear the value.
    pub fn clear(&self, backend: &mut impl KeyedStateBackend) -> Result<()> {
        backend.clear_value(&self.name)
    }
}

/// List state handle: a lightweight descriptor for accessing list state.
#[derive(Debug, Clone)]
pub struct ListStateHandle<V> {
    name: String,
    _phantom: PhantomData<V>,
}

impl<V> ListStateHandle<V> {
    /// Create a new list state handle.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _phantom: PhantomData,
        }
    }

    /// Get the state name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<V: StreamData> ListStateHandle<V> {
    /// Get all values.
    pub fn get(&self, backend: &impl KeyedStateBackend) -> Result<Vec<V>> {
        backend.get_list(&self.name)
    }

    /// Add a value to the list.
    pub fn add(&self, backend: &mut impl KeyedStateBackend, value: V) -> Result<()> {
        backend.add_to_list(&self.name, value)
    }

    /// Clear all values.
    pub fn clear(&self, backend: &mut impl KeyedStateBackend) -> Result<()> {
        backend.clear_list(&self.name)
    }
}

/// Map state handle: a lightweight descriptor for accessing map state.
#[derive(Debug, Clone)]
pub struct MapStateHandle<K, V> {
    name: String,
    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

impl<K, V> MapStateHandle<K, V> {
    /// Create a new map state handle.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _phantom_k: PhantomData,
            _phantom_v: PhantomData,
        }
    }

    /// Get the state name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V> MapStateHandle<K, V>
where
    K: StreamData + std::hash::Hash + Eq,
    V: StreamData,
{
    /// Get a value by key.
    pub fn get(&self, backend: &impl KeyedStateBackend, key: &K) -> Result<Option<V>> {
        backend.get_from_map(&self.name, key)
    }

    /// Put a key-value pair.
    pub fn put(&self, backend: &mut impl KeyedStateBackend, key: K, value: V) -> Result<()> {
        backend.put_in_map(&self.name, key, value)
    }

    /// Remove a key.
    pub fn remove(&self, backend: &mut impl KeyedStateBackend, key: &K) -> Result<Option<V>> {
        backend.remove_from_map(&self.name, key)
    }

    /// Clear all entries.
    pub fn clear(&self, backend: &mut impl KeyedStateBackend) -> Result<()> {
        backend.clear_map(&self.name)
    }
}

// ============================================================================
// Keyed State Backend Trait
// ============================================================================

/// Keyed state backend: manages state for the current processing key.
///
/// The backend maintains a "current key" context. All state operations
/// are scoped to this key.
///
/// This trait uses a descriptor pattern: handles are lightweight and don't borrow
/// the backend, allowing multiple handles to coexist without borrow-checker conflicts.
pub trait KeyedStateBackend: Send {
    /// Set the current processing key.
    ///
    /// All subsequent state operations will be scoped to this key.
    fn set_current_key(&mut self, key: Vec<u8>);

    // ValueState operations
    fn get_value<V: StreamData>(&self, name: &str) -> Result<Option<V>>;
    fn put_value<V: StreamData>(&mut self, name: &str, value: V) -> Result<()>;
    fn clear_value(&mut self, name: &str) -> Result<()>;

    // ListState operations
    fn get_list<V: StreamData>(&self, name: &str) -> Result<Vec<V>>;
    fn add_to_list<V: StreamData>(&mut self, name: &str, value: V) -> Result<()>;
    fn clear_list(&mut self, name: &str) -> Result<()>;

    // MapState operations
    fn get_from_map<K, V>(&self, name: &str, key: &K) -> Result<Option<V>>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData;
    fn put_in_map<K, V>(&mut self, name: &str, key: K, value: V) -> Result<()>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData;
    fn remove_from_map<K, V>(&mut self, name: &str, key: &K) -> Result<Option<V>>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData;
    fn clear_map(&mut self, name: &str) -> Result<()>;

    /// Snapshot all state for checkpointing.
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Restore state from a checkpoint.
    fn restore(&mut self, data: &[u8]) -> Result<()>;
}

