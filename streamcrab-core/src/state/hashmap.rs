//! HashMap-based state backend for Local mode.
//!
//! Uses the descriptor pattern: zero-overhead state access with no Arc/Mutex/RefCell.

use super::KeyedStateBackend;
use crate::types::StreamData;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// In-memory state backend using HashMap.
///
/// This is the "Local mode" backend:
/// - Zero overhead (~100ns DRAM access, no locks)
/// - Does NOT support dynamic rescale (must stop→checkpoint→restart)
/// - Best for: stable load, extreme performance requirements
///
/// Uses descriptor pattern: state handles don't borrow the backend,
/// avoiding borrow-checker conflicts.
#[derive(Debug, Default)]
pub struct HashMapStateBackend {
    /// Value states: (key, state_name) -> value_bytes
    value_states: HashMap<(Vec<u8>, String), Vec<u8>>,
    /// List states: (key, state_name) -> Vec<elem_bytes>
    ///
    /// Performance: Each element is serialized individually, making add() O(1)
    /// instead of O(n). Critical for high-frequency state updates.
    list_states: HashMap<(Vec<u8>, String), Vec<Vec<u8>>>,
    /// Map states: (key, state_name) -> HashMap<k_bytes, v_bytes>
    ///
    /// Performance: Nested HashMap allows O(1) put/get/remove operations.
    map_states: HashMap<(Vec<u8>, String), HashMap<Vec<u8>, Vec<u8>>>,
    /// Current processing key
    current_key: Option<Vec<u8>>,
}

impl HashMapStateBackend {
    /// Create a new empty HashMap state backend.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Snapshot container for checkpointing.
#[derive(Serialize, Deserialize)]
struct SnapshotData {
    value_states: HashMap<(Vec<u8>, String), Vec<u8>>,
    list_states: HashMap<(Vec<u8>, String), Vec<Vec<u8>>>,
    map_states: HashMap<(Vec<u8>, String), HashMap<Vec<u8>, Vec<u8>>>,
}

impl KeyedStateBackend for HashMapStateBackend {
    fn set_current_key(&mut self, key: Vec<u8>) {
        self.current_key = Some(key);
    }

    // ========== ValueState operations ==========

    fn get_value<V: StreamData>(&self, name: &str) -> Result<Option<V>> {
        let key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?;
        let state_key = (key.clone(), name.to_string());

        match self.value_states.get(&state_key) {
            Some(bytes) => Ok(Some(bincode::deserialize(bytes)?)),
            None => Ok(None),
        }
    }

    fn put_value<V: StreamData>(&mut self, name: &str, value: V) -> Result<()> {
        let key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        let bytes = bincode::serialize(&value)?;
        self.value_states.insert(state_key, bytes);
        Ok(())
    }

    fn clear_value(&mut self, name: &str) -> Result<()> {
        let key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        self.value_states.remove(&state_key);
        Ok(())
    }

    // ========== ListState operations ==========

    fn get_list<V: StreamData>(&self, name: &str) -> Result<Vec<V>> {
        let key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?;
        let state_key = (key.clone(), name.to_string());

        match self.list_states.get(&state_key) {
            Some(elems) => {
                let mut out = Vec::with_capacity(elems.len());
                for b in elems {
                    out.push(bincode::deserialize(b)?);
                }
                Ok(out)
            }
            None => Ok(Vec::new()),
        }
    }

    fn add_to_list<V: StreamData>(&mut self, name: &str, value: V) -> Result<()> {
        let key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        let bytes = bincode::serialize(&value)?;

        self.list_states.entry(state_key).or_insert_with(Vec::new).push(bytes);
        Ok(())
    }

    fn clear_list(&mut self, name: &str) -> Result<()> {
        let key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        self.list_states.remove(&state_key);
        Ok(())
    }

    // ========== MapState operations ==========

    fn get_from_map<K, V>(&self, name: &str, key: &K) -> Result<Option<V>>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        let current_key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?;
        let state_key = (current_key.clone(), name.to_string());
        let kbytes = bincode::serialize(key)?;

        match self.map_states.get(&state_key) {
            Some(map) => match map.get(&kbytes) {
                Some(vbytes) => Ok(Some(bincode::deserialize(vbytes)?)),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn put_in_map<K, V>(&mut self, name: &str, key: K, value: V) -> Result<()>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        let current_key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (current_key, name.to_string());
        let kbytes = bincode::serialize(&key)?;
        let vbytes = bincode::serialize(&value)?;

        self.map_states
            .entry(state_key)
            .or_insert_with(HashMap::new)
            .insert(kbytes, vbytes);
        Ok(())
    }

    fn remove_from_map<K, V>(&mut self, name: &str, key: &K) -> Result<Option<V>>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        let current_key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (current_key, name.to_string());
        let kbytes = bincode::serialize(key)?;

        match self.map_states.get_mut(&state_key) {
            Some(map) => match map.remove(&kbytes) {
                Some(vbytes) => Ok(Some(bincode::deserialize(&vbytes)?)),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn clear_map(&mut self, name: &str) -> Result<()> {
        let current_key = self.current_key.as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (current_key, name.to_string());
        self.map_states.remove(&state_key);
        Ok(())
    }

    // ========== Snapshot/Restore ==========

    fn snapshot(&self) -> Result<Vec<u8>> {
        let data = SnapshotData {
            value_states: self.value_states.clone(),
            list_states: self.list_states.clone(),
            map_states: self.map_states.clone(),
        };
        Ok(bincode::serialize(&data).map_err(|e| anyhow!("Snapshot failed: {}", e))?)
    }

    fn restore(&mut self, data: &[u8]) -> Result<()> {
        let snap: SnapshotData =
            bincode::deserialize(data).map_err(|e| anyhow!("Restore failed: {}", e))?;
        self.value_states = snap.value_states;
        self.list_states = snap.list_states;
        self.map_states = snap.map_states;
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ValueStateHandle, ListStateHandle, MapStateHandle};

    #[test]
    fn test_value_state() {
        let mut backend = HashMapStateBackend::new();
        backend.set_current_key(b"user_1".to_vec());

        // Create handle (no borrowing)
        let count = ValueStateHandle::<i32>::new("count");

        assert_eq!(count.get(&backend).unwrap(), None);

        count.put(&mut backend, 42).unwrap();
        assert_eq!(count.get(&backend).unwrap(), Some(42));

        count.put(&mut backend, 100).unwrap();
        assert_eq!(count.get(&backend).unwrap(), Some(100));

        count.clear(&mut backend).unwrap();
        assert_eq!(count.get(&backend).unwrap(), None);
    }

    #[test]
    fn test_value_state_different_keys() {
        let mut backend = HashMapStateBackend::new();

        // Create handle once
        let count = ValueStateHandle::<i32>::new("count");

        // Set key and put value for user_1
        backend.set_current_key(b"user_1".to_vec());
        count.put(&mut backend, 10).unwrap();

        // Set key and put value for user_2
        backend.set_current_key(b"user_2".to_vec());
        count.put(&mut backend, 20).unwrap();

        // Verify user_1's value
        backend.set_current_key(b"user_1".to_vec());
        assert_eq!(count.get(&backend).unwrap(), Some(10));

        // Verify user_2's value
        backend.set_current_key(b"user_2".to_vec());
        assert_eq!(count.get(&backend).unwrap(), Some(20));
    }

    #[test]
    fn test_list_state() {
        let mut backend = HashMapStateBackend::new();
        backend.set_current_key(b"key1".to_vec());

        // Create handle (no borrowing)
        let events = ListStateHandle::<String>::new("events");

        assert_eq!(events.get(&backend).unwrap(), Vec::<String>::new());

        events.add(&mut backend, "event1".to_string()).unwrap();
        events.add(&mut backend, "event2".to_string()).unwrap();
        assert_eq!(
            events.get(&backend).unwrap(),
            vec!["event1".to_string(), "event2".to_string()]
        );

        events.clear(&mut backend).unwrap();
        assert_eq!(events.get(&backend).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_map_state() {
        let mut backend = HashMapStateBackend::new();
        backend.set_current_key(b"user_1".to_vec());

        // Create handle (no borrowing)
        let metrics = MapStateHandle::<String, i32>::new("metrics");

        assert_eq!(metrics.get(&backend, &"clicks".to_string()).unwrap(), None);

        metrics.put(&mut backend, "clicks".to_string(), 10).unwrap();
        metrics.put(&mut backend, "views".to_string(), 100).unwrap();
        assert_eq!(metrics.get(&backend, &"clicks".to_string()).unwrap(), Some(10));
        assert_eq!(metrics.get(&backend, &"views".to_string()).unwrap(), Some(100));

        let removed = metrics.remove(&mut backend, &"clicks".to_string()).unwrap();
        assert_eq!(removed, Some(10));
        assert_eq!(metrics.get(&backend, &"clicks".to_string()).unwrap(), None);

        metrics.clear(&mut backend).unwrap();
        assert_eq!(metrics.get(&backend, &"views".to_string()).unwrap(), None);
    }

    #[test]
    fn test_snapshot_restore() {
        let mut backend = HashMapStateBackend::new();

        // Create handles
        let count = ValueStateHandle::<i32>::new("count");
        let events = ListStateHandle::<String>::new("events");
        let metrics = MapStateHandle::<String, i32>::new("metrics");

        // Put values for two keys
        backend.set_current_key(b"user_1".to_vec());
        count.put(&mut backend, 42).unwrap();

        backend.set_current_key(b"user_2".to_vec());
        count.put(&mut backend, 100).unwrap();

        backend.set_current_key(b"user_1".to_vec());
        events.add(&mut backend, "e1".to_string()).unwrap();
        events.add(&mut backend, "e2".to_string()).unwrap();
        metrics.put(&mut backend, "clicks".to_string(), 7).unwrap();

        // Snapshot
        let snapshot = backend.snapshot().unwrap();

        // Create new backend and restore
        let mut new_backend = HashMapStateBackend::new();
        new_backend.restore(&snapshot).unwrap();

        // Verify restored values
        new_backend.set_current_key(b"user_1".to_vec());
        assert_eq!(count.get(&new_backend).unwrap(), Some(42));
        assert_eq!(events.get(&new_backend).unwrap(), vec!["e1".to_string(), "e2".to_string()]);
        assert_eq!(metrics.get(&new_backend, &"clicks".to_string()).unwrap(), Some(7));

        new_backend.set_current_key(b"user_2".to_vec());
        assert_eq!(count.get(&new_backend).unwrap(), Some(100));
    }
}

