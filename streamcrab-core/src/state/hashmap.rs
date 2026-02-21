//! HashMap-based state backend for Local mode.
//!
//! Uses the descriptor pattern: zero-overhead state access with no Arc/Mutex/RefCell.

use super::KeyedStateBackend;
use crate::types::StreamData;
use anyhow::{Result, anyhow};
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
        let key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?;
        let state_key = (key.clone(), name.to_string());

        match self.value_states.get(&state_key) {
            Some(bytes) => Ok(Some(bincode::deserialize(bytes)?)),
            None => Ok(None),
        }
    }

    fn put_value<V: StreamData>(&mut self, name: &str, value: V) -> Result<()> {
        let key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        let bytes = bincode::serialize(&value)?;
        self.value_states.insert(state_key, bytes);
        Ok(())
    }

    fn clear_value(&mut self, name: &str) -> Result<()> {
        let key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        self.value_states.remove(&state_key);
        Ok(())
    }

    // ========== ListState operations ==========

    fn get_list<V: StreamData>(&self, name: &str) -> Result<Vec<V>> {
        let key = self
            .current_key
            .as_ref()
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
        let key = self
            .current_key
            .as_ref()
            .ok_or_else(|| anyhow!("No current key set"))?
            .clone();
        let state_key = (key, name.to_string());
        let bytes = bincode::serialize(&value)?;

        self.list_states
            .entry(state_key)
            .or_insert_with(Vec::new)
            .push(bytes);
        Ok(())
    }

    fn clear_list(&mut self, name: &str) -> Result<()> {
        let key = self
            .current_key
            .as_ref()
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
        let current_key = self
            .current_key
            .as_ref()
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
        let current_key = self
            .current_key
            .as_ref()
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
        let current_key = self
            .current_key
            .as_ref()
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
        let current_key = self
            .current_key
            .as_ref()
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
#[path = "tests/hashmap_tests.rs"]
mod tests;
