use std::collections::HashMap;

/// Local in-memory state backend using HashMap.
/// Keys are (serialized_key, state_name) pairs, values are serialized bytes.
///
/// This is the "Local mode" backend — zero network overhead, no dynamic rescale support.
#[derive(Debug, Default)]
pub struct LocalStateBackend {
    states: HashMap<(Vec<u8>, String), Vec<u8>>,
}

impl LocalStateBackend {
    /// Create an empty local state backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a value for the given key and state name.
    pub fn put(&mut self, key: &[u8], state_name: &str, value: Vec<u8>) {
        self.states
            .insert((key.to_vec(), state_name.to_string()), value);
    }

    /// Retrieve a value for the given key and state name.
    pub fn get(&self, key: &[u8], state_name: &str) -> Option<&Vec<u8>> {
        self.states.get(&(key.to_vec(), state_name.to_string()))
    }

    /// Remove a value for the given key and state name.
    pub fn remove(&mut self, key: &[u8], state_name: &str) -> Option<Vec<u8>> {
        self.states.remove(&(key.to_vec(), state_name.to_string()))
    }

    /// Clear all state.
    pub fn clear(&mut self) {
        self.states.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get() {
        let mut backend = LocalStateBackend::new();
        backend.put(b"key1", "count", vec![1, 0, 0, 0]);
        assert_eq!(backend.get(b"key1", "count"), Some(&vec![1, 0, 0, 0]));
        assert_eq!(backend.get(b"key1", "other"), None);
        assert_eq!(backend.get(b"key2", "count"), None);
    }

    #[test]
    fn test_remove() {
        let mut backend = LocalStateBackend::new();
        backend.put(b"k", "s", vec![42]);
        assert_eq!(backend.remove(b"k", "s"), Some(vec![42]));
        assert_eq!(backend.get(b"k", "s"), None);
    }

    #[test]
    fn test_clear() {
        let mut backend = LocalStateBackend::new();
        backend.put(b"a", "s", vec![1]);
        backend.put(b"b", "s", vec![2]);
        backend.clear();
        assert_eq!(backend.get(b"a", "s"), None);
        assert_eq!(backend.get(b"b", "s"), None);
    }
}
