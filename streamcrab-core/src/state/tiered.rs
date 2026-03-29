use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

use super::{KeyedStateBackend, StateServiceClient};
use crate::types::StreamData;

const VALUE_NS: u8 = 1;
const LIST_NS: u8 = 2;
const MAP_NS: u8 = 3;
const TOMBSTONE: &[u8] = b"__streamcrab_tombstone__";

#[derive(Debug, Clone)]
pub struct TieredStateBackendConfig {
    pub cache_capacity: usize,
    pub flush_batch_size: usize,
    pub auto_flush_interval: Duration,
    pub warmup_top_keys: usize,
    pub require_remote_flush: bool,
}

impl Default for TieredStateBackendConfig {
    fn default() -> Self {
        Self {
            cache_capacity: 10_000,
            flush_batch_size: 64,
            auto_flush_interval: Duration::from_millis(10),
            warmup_top_keys: 256,
            require_remote_flush: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StateAddress {
    namespace: u8,
    key: Vec<u8>,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TieredSnapshot {
    current_key: Option<Vec<u8>>,
    write_buffer: HashMap<Vec<u8>, Vec<u8>>,
    current_epoch: u64,
}

#[derive(Debug)]
struct LocalCache {
    capacity: usize,
    map: HashMap<Vec<u8>, Vec<u8>>,
    order: VecDeque<Vec<u8>>,
}

impl LocalCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).cloned()
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if !self.map.contains_key(&key) {
            self.order.push_back(key.clone());
        }
        self.map.insert(key, value);
        while self.map.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }
}

/// Tiered backend: L1 cache + buffered writes + remote state service.
pub struct TieredStateBackend<C: StateServiceClient> {
    current_key: Option<Vec<u8>>,
    local_cache: LocalCache,
    write_buffer: HashMap<Vec<u8>, Vec<u8>>,
    remote: Arc<C>,
    current_epoch: u64,
    config: TieredStateBackendConfig,
    last_flush_at: Instant,
}

impl<C: StateServiceClient> TieredStateBackend<C> {
    pub fn new(remote: Arc<C>, config: TieredStateBackendConfig) -> Self {
        Self {
            current_key: None,
            local_cache: LocalCache::new(config.cache_capacity),
            write_buffer: HashMap::new(),
            remote,
            current_epoch: 1,
            config,
            last_flush_at: Instant::now(),
        }
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch.max(1);
    }

    pub fn write_buffer_len(&self) -> usize {
        self.write_buffer.len()
    }

    pub fn prefetch_hot_keys(&mut self, limit: usize) -> Result<usize> {
        if limit == 0 {
            return Ok(0);
        }
        let keys = self.remote.top_keys(limit, self.current_epoch)?;
        if keys.is_empty() {
            return Ok(0);
        }
        let values = self.remote.batch_get(&keys, self.current_epoch)?;
        let mut warmed = 0usize;
        for (key, maybe_value) in keys.into_iter().zip(values) {
            if let Some(value) = maybe_value {
                self.local_cache.put(key, value);
                warmed += 1;
            }
        }
        Ok(warmed)
    }

    fn get_current_key(&self) -> Result<&[u8]> {
        self.current_key
            .as_deref()
            .ok_or_else(|| anyhow!("No current key set"))
    }

    fn state_key(&self, namespace: u8, name: &str) -> Result<Vec<u8>> {
        let current_key = self.get_current_key()?;
        Ok(bincode::serialize(&StateAddress {
            namespace,
            key: current_key.to_vec(),
            name: name.to_string(),
        })?)
    }

    fn read_raw(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.write_buffer.get(key) {
            return Ok(if is_tombstone(value) {
                None
            } else {
                Some(value.clone())
            });
        }
        if let Some(value) = self.local_cache.get(key) {
            return Ok(if is_tombstone(&value) {
                None
            } else {
                Some(value)
            });
        }
        if let Some(value) = self.remote.get(key, self.current_epoch)? {
            self.local_cache.put(key.to_vec(), value.clone());
            return Ok(if is_tombstone(&value) {
                None
            } else {
                Some(value)
            });
        }
        Ok(None)
    }

    fn put_raw(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.write_buffer.insert(key, value);
        if self.write_buffer.len() >= self.config.flush_batch_size
            || self.last_flush_at.elapsed() >= self.config.auto_flush_interval
        {
            self.flush()?;
        }
        Ok(())
    }

    fn clear_raw(&mut self, key: Vec<u8>) -> Result<()> {
        self.write_buffer.insert(key, TOMBSTONE.to_vec());
        if self.write_buffer.len() >= self.config.flush_batch_size
            || self.last_flush_at.elapsed() >= self.config.auto_flush_interval
        {
            self.flush()?;
        }
        Ok(())
    }
}

impl<C: StateServiceClient> KeyedStateBackend for TieredStateBackend<C> {
    fn set_current_key(&mut self, key: Vec<u8>) {
        self.current_key = Some(key);
    }

    fn set_epoch(&mut self, epoch: u64) {
        TieredStateBackend::set_epoch(self, epoch);
    }

    fn get_value<V: StreamData>(&self, name: &str) -> Result<Option<V>> {
        let mut cloned = self.clone_for_read();
        let key = cloned.state_key(VALUE_NS, name)?;
        cloned
            .read_raw(&key)?
            .map(|bytes| bincode::deserialize::<V>(&bytes))
            .transpose()
            .map_err(Into::into)
    }

    fn put_value<V: StreamData>(&mut self, name: &str, value: V) -> Result<()> {
        let key = self.state_key(VALUE_NS, name)?;
        self.put_raw(key, bincode::serialize(&value)?)
    }

    fn clear_value(&mut self, name: &str) -> Result<()> {
        let key = self.state_key(VALUE_NS, name)?;
        self.clear_raw(key)
    }

    fn get_list<V: StreamData>(&self, name: &str) -> Result<Vec<V>> {
        let mut cloned = self.clone_for_read();
        let key = cloned.state_key(LIST_NS, name)?;
        match cloned.read_raw(&key)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(Vec::new()),
        }
    }

    fn add_to_list<V: StreamData>(&mut self, name: &str, value: V) -> Result<()> {
        let key = self.state_key(LIST_NS, name)?;
        let mut values: Vec<V> = match self.read_raw(&key)? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => Vec::new(),
        };
        values.push(value);
        self.put_raw(key, bincode::serialize(&values)?)
    }

    fn clear_list(&mut self, name: &str) -> Result<()> {
        let key = self.state_key(LIST_NS, name)?;
        self.clear_raw(key)
    }

    fn get_from_map<K, V>(&self, name: &str, key: &K) -> Result<Option<V>>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        let mut cloned = self.clone_for_read();
        let state_key = cloned.state_key(MAP_NS, name)?;
        let key_bytes = bincode::serialize(key)?;
        let map: HashMap<Vec<u8>, Vec<u8>> = match cloned.read_raw(&state_key)? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => HashMap::new(),
        };
        map.get(&key_bytes)
            .map(|value_bytes| bincode::deserialize::<V>(value_bytes))
            .transpose()
            .map_err(Into::into)
    }

    fn put_in_map<K, V>(&mut self, name: &str, key: K, value: V) -> Result<()>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        let state_key = self.state_key(MAP_NS, name)?;
        let mut map: HashMap<Vec<u8>, Vec<u8>> = match self.read_raw(&state_key)? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => HashMap::new(),
        };
        map.insert(bincode::serialize(&key)?, bincode::serialize(&value)?);
        self.put_raw(state_key, bincode::serialize(&map)?)
    }

    fn remove_from_map<K, V>(&mut self, name: &str, key: &K) -> Result<Option<V>>
    where
        K: StreamData + std::hash::Hash + Eq,
        V: StreamData,
    {
        let state_key = self.state_key(MAP_NS, name)?;
        let mut map: HashMap<Vec<u8>, Vec<u8>> = match self.read_raw(&state_key)? {
            Some(bytes) => bincode::deserialize(&bytes)?,
            None => HashMap::new(),
        };
        let removed = map
            .remove(&bincode::serialize(key)?)
            .map(|value_bytes| bincode::deserialize::<V>(&value_bytes))
            .transpose()?;
        self.put_raw(state_key, bincode::serialize(&map)?)?;
        Ok(removed)
    }

    fn clear_map(&mut self, name: &str) -> Result<()> {
        let key = self.state_key(MAP_NS, name)?;
        self.clear_raw(key)
    }

    fn flush(&mut self) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self.write_buffer.drain().collect();
        self.remote.batch_put(entries.clone(), self.current_epoch)?;
        if self.config.require_remote_flush {
            self.remote.flush(self.current_epoch)?;
        }
        for (key, value) in entries {
            self.local_cache.put(key, value);
        }
        self.last_flush_at = Instant::now();
        Ok(())
    }

    fn trigger_remote_snapshot(&mut self, epoch: u64) -> Result<()> {
        self.remote.trigger_snapshot(epoch)
    }

    fn on_rescale_activate(&mut self, _generation: u64) -> Result<()> {
        self.flush()?;
        self.local_cache.clear();
        let _ = self.prefetch_hot_keys(self.config.warmup_top_keys)?;
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&TieredSnapshot {
            current_key: self.current_key.clone(),
            write_buffer: self.write_buffer.clone(),
            current_epoch: self.current_epoch,
        })?)
    }

    fn restore(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            self.current_key = None;
            self.write_buffer.clear();
            self.current_epoch = 1;
            return Ok(());
        }
        let snapshot: TieredSnapshot = bincode::deserialize(data)?;
        self.current_key = snapshot.current_key;
        self.write_buffer = snapshot.write_buffer;
        self.current_epoch = snapshot.current_epoch.max(1);
        Ok(())
    }
}

impl<C: StateServiceClient> TieredStateBackend<C> {
    fn clone_for_read(&self) -> Self {
        Self {
            current_key: self.current_key.clone(),
            local_cache: LocalCache {
                capacity: self.local_cache.capacity,
                map: self.local_cache.map.clone(),
                order: self.local_cache.order.clone(),
            },
            write_buffer: self.write_buffer.clone(),
            remote: Arc::clone(&self.remote),
            current_epoch: self.current_epoch,
            config: self.config.clone(),
            last_flush_at: self.last_flush_at,
        }
    }
}

fn is_tombstone(bytes: &[u8]) -> bool {
    bytes == TOMBSTONE
}

#[cfg(test)]
#[path = "tests/tiered_tests.rs"]
mod tests;
