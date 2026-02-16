//! HashMap-based state backend for Local mode.

use super::{KeyedStateBackend, ListState, MapState, ValueState};
use crate::types::StreamData;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;

/// In-memory state backend using HashMap.
///
/// Storage structure: `HashMap<(current_key, state_name), serialized_value>`
///
/// This is the "Local mode" backend:
/// - Zero network overhead (~100ns access)
/// - Does NOT support dynamic rescale (must stop→checkpoint→restart)
/// - Best for: stable load, extreme performance requirements
#[derive(Debug, Default)]
pub struct HashMapStateBackend {
    /// Value states: (key, state_name) -> value_bytes
    value_states: HashMap<(Vec<u8>, String), Vec<u8>>,
    /// List states: (key, state_name) -> Vec<elem_bytes>
    list_states: HashMap<(Vec<u8>, String), Vec<Vec<u8>>>,
    /// Map states: (key, state_name) -> HashMap<k_bytes, v_bytes>
    map_states: HashMap<(Vec<u8>, String), HashMap<Vec<u8>, Vec<u8>>>,
    /// Current processing key
    current_key: Option<Vec<u8>>,
}

impl HashMapStateBackend {
    /// Create a new empty HashMap state backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the current key (for internal use).
    fn current_key(&self) -> Result<&[u8]> {
        self.current_key
            .as_ref()
            .map(|k| k.as_slice())
            .ok_or_else(|| anyhow!("No current key set"))
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

    fn get_value_state<V>(&mut self, name: &str) -> Box<dyn ValueState<V> + '_>
    where
        V: StreamData,
    {
        Box::new(ValueStateImpl {
            backend: self,
            name: name.to_string(),
            _phantom: PhantomData,
        })
    }

    fn get_list_state<V>(&mut self, name: &str) -> Box<dyn ListState<V> + '_>
    where
        V: StreamData,
    {
        Box::new(ListStateImpl {
            backend: self,
            name: name.to_string(),
            _phantom: PhantomData,
        })
    }

    fn get_map_state<K, V>(&mut self, name: &str) -> Box<dyn MapState<K, V> + '_>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        Box::new(MapStateImpl {
            backend: self,
            name: name.to_string(),
            _phantom_k: PhantomData,
            _phantom_v: PhantomData,
        })
    }

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

/// ValueState implementation.
struct ValueStateImpl<'a, V> {
    backend: &'a mut HashMapStateBackend,
    name: String,
    _phantom: PhantomData<V>,
}

impl<'a, V> ValueState<V> for ValueStateImpl<'a, V>
where
    V: StreamData,
{
    fn get(&self) -> Result<Option<V>> {
        let key = self.backend.current_key()?;
        let state_key = (key.to_vec(), self.name.clone());

        match self.backend.value_states.get(&state_key) {
            Some(bytes) => Ok(Some(bincode::deserialize::<V>(bytes)?)),
            None => Ok(None),
        }
    }

    fn put(&mut self, value: V) -> Result<()> {
        let key = self.backend.current_key()?.to_vec();
        let state_key = (key, self.name.clone());
        let bytes = bincode::serialize(&value)?;
        self.backend.value_states.insert(state_key, bytes);
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        let key = self.backend.current_key()?.to_vec();
        let state_key = (key, self.name.clone());
        self.backend.value_states.remove(&state_key);
        Ok(())
    }
}

/// ListState implementation.
struct ListStateImpl<'a, V> {
    backend: &'a mut HashMapStateBackend,
    name: String,
    _phantom: PhantomData<V>,
}

impl<'a, V> ListState<V> for ListStateImpl<'a, V>
where
    V: StreamData,
{
    fn get(&self) -> Result<Vec<V>> {
        let key = self.backend.current_key()?;
        let state_key = (key.to_vec(), self.name.clone());

        match self.backend.list_states.get(&state_key) {
            Some(elems) => {
                let mut out = Vec::with_capacity(elems.len());
                for b in elems {
                    out.push(bincode::deserialize::<V>(b)?);
                }
                Ok(out)
            }
            None => Ok(Vec::new()),
        }
    }

    fn add(&mut self, value: V) -> Result<()> {
        let key = self.backend.current_key()?.to_vec();
        let state_key = (key, self.name.clone());

        let bytes = bincode::serialize(&value)?;

        self.backend.list_states.entry(state_key).or_insert_with(Vec::new).push(bytes);
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        let key = self.backend.current_key()?.to_vec();
        let state_key = (key, self.name.clone());
        self.backend.list_states.remove(&state_key);
        Ok(())
    }
}

/// MapState implementation.
struct MapStateImpl<'a, K, V> {
    backend: &'a mut HashMapStateBackend,
    name: String,
    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

impl<'a, K, V> MapState<K, V> for MapStateImpl<'a, K, V>
where
    K: StreamData + std::hash::Hash + Eq,
    V: StreamData,
{
    fn get(&self, key: &K) -> Result<Option<V>> {
        let current_key = self.backend.current_key()?;
        let state_key = (current_key.to_vec(), self.name.clone());

        let kbytes = bincode::serialize(key)?;
        match self.backend.map_states.get(&state_key) {
            Some(map) => match map.get(&kbytes) {
                Some(vbytes) => Ok(Some(bincode::deserialize::<V>(vbytes)?)),
                None => Ok(None),
            },
            None => Ok(None)
        }
    }

    fn put(&mut self, key: K, value: V) -> Result<()> {
        let current_key = self.backend.current_key()?.to_vec();
        let state_key = (current_key, self.name.clone());
        let kbytes = bincode::serialize(&key)?;
        let vbytes = bincode::serialize(&value)?;

        self.backend
            .map_states
            .entry(state_key)
            .or_insert_with(HashMap::new)
            .insert(kbytes, vbytes);
        Ok(())
    }

    fn remove(&mut self, key: &K) -> Result<Option<V>> {
        let current_key = self.backend.current_key()?.to_vec();
        let state_key = (current_key, self.name.clone());

        let kbytes = bincode::serialize(key)?;
        let map = match self.backend.map_states.get_mut(&state_key) {
            Some(m) => m,
            None =>  return Ok(None),
        };

        match map.remove(&kbytes) {
            Some(vbytes) => Ok(Some(bincode::deserialize::<V>(&vbytes)?)),
            None => Ok(None),
        }
    }

    fn clear(&mut self) -> Result<()> {
        let current_key = self.backend.current_key()?.to_vec();
        let state_key = (current_key, self.name.clone());
        self.backend.map_states.remove(&state_key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_state() {
        let mut backend = HashMapStateBackend::new();
        backend.set_current_key(b"user_1".to_vec());

        let mut state = backend.get_value_state::<i32>("count");
        assert_eq!(state.get().unwrap(), None);

        state.put(42).unwrap();
        assert_eq!(state.get().unwrap(), Some(42));

        state.put(100).unwrap();
        assert_eq!(state.get().unwrap(), Some(100));

        state.clear().unwrap();
        assert_eq!(state.get().unwrap(), None);
    }

    #[test]
    fn test_value_state_different_keys() {
        let mut backend = HashMapStateBackend::new();

        // Set key and put value for user_1
        backend.set_current_key(b"user_1".to_vec());
        {
            let mut state = backend.get_value_state::<i32>("count");
            state.put(10).unwrap();
        } // Drop state before next borrow

        // Set key and put value for user_2
        backend.set_current_key(b"user_2".to_vec());
        {
            let mut state = backend.get_value_state::<i32>("count");
            state.put(20).unwrap();
        }

        // Verify user_1's value
        backend.set_current_key(b"user_1".to_vec());
        {
            let state = backend.get_value_state::<i32>("count");
            assert_eq!(state.get().unwrap(), Some(10));
        }

        // Verify user_2's value
        backend.set_current_key(b"user_2".to_vec());
        {
            let state = backend.get_value_state::<i32>("count");
            assert_eq!(state.get().unwrap(), Some(20));
        }
    }

    #[test]
    fn test_list_state() {
        let mut backend = HashMapStateBackend::new();
        backend.set_current_key(b"key1".to_vec());

        let mut state = backend.get_list_state::<String>("events");
        assert_eq!(state.get().unwrap(), Vec::<String>::new());

        state.add("event1".to_string()).unwrap();
        state.add("event2".to_string()).unwrap();
        assert_eq!(
            state.get().unwrap(),
            vec!["event1".to_string(), "event2".to_string()]
        );

        state.clear().unwrap();
        assert_eq!(state.get().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_map_state() {
        let mut backend = HashMapStateBackend::new();
        backend.set_current_key(b"user_1".to_vec());

        let mut state = backend.get_map_state::<String, i32>("metrics");
        assert_eq!(state.get(&"clicks".to_string()).unwrap(), None);

        state.put("clicks".to_string(), 10).unwrap();
        state.put("views".to_string(), 100).unwrap();
        assert_eq!(state.get(&"clicks".to_string()).unwrap(), Some(10));
        assert_eq!(state.get(&"views".to_string()).unwrap(), Some(100));

        let removed = state.remove(&"clicks".to_string()).unwrap();
        assert_eq!(removed, Some(10));
        assert_eq!(state.get(&"clicks".to_string()).unwrap(), None);

        state.clear().unwrap();
        assert_eq!(state.get(&"views".to_string()).unwrap(), None);
    }

    #[test]
    fn test_snapshot_restore() {
        let mut backend = HashMapStateBackend::new();

        // Put values for two keys
        backend.set_current_key(b"user_1".to_vec());
        {
            let mut state = backend.get_value_state::<i32>("count");
            state.put(42).unwrap();
        }

        backend.set_current_key(b"user_2".to_vec());
        {
            let mut state = backend.get_value_state::<i32>("count");
            state.put(100).unwrap();
        }

        backend.set_current_key(b"user_1".to_vec());
        {
            let mut ls = backend.get_list_state::<String>("events");
            ls.add("e1".to_string()).unwrap();
            ls.add("e2".to_string()).unwrap();
        }
        {
            let mut ms = backend.get_map_state::<String, i32>("metrics");
            ms.put("clicks".to_string(), 7).unwrap();
        }

        // Snapshot
        let snapshot = backend.snapshot().unwrap();

        // Create new backend and restore
        let mut new_backend = HashMapStateBackend::new();
        new_backend.restore(&snapshot).unwrap();

        // Verify restored values
        new_backend.set_current_key(b"user_1".to_vec());
        {
            let state = new_backend.get_value_state::<i32>("count");
            assert_eq!(state.get().unwrap(), Some(42));
        }
        {

            let ls = new_backend.get_list_state::<String>("events");
            assert_eq!(ls.get().unwrap(), vec!["e1".to_string(), "e2".to_string()]);
        }
        {
            let ms = new_backend.get_map_state::<String, i32>("metrics");
            assert_eq!(ms.get(&"clicks".to_string()).unwrap(), Some(7));
        }

        new_backend.set_current_key(b"user_2".to_vec());
        {
            let state = new_backend.get_value_state::<i32>("count");
            assert_eq!(state.get().unwrap(), Some(100));
        }
    }
}

