//! Spill-to-disk support for memory-bounded operators.
//!
//! [`SpillManager`] tracks estimated memory usage for a single operator
//! instance and serializes overflow data to temporary files on disk. Callers
//! serialize their in-memory state to bytes, hand them to [`SpillManager::spill`],
//! and later read them back with [`SpillManager::read_spill`].

use anyhow::Result;
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

// ── SpillConfig ───────────────────────────────────────────────────────────────

/// Configuration for spill-to-disk behavior.
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// Maximum memory budget in bytes before spilling.
    pub memory_limit_bytes: usize,
    /// Directory for spill files. Defaults to `<system-tmp>/streamcrab-spill`.
    pub spill_dir: PathBuf,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            memory_limit_bytes: 256 * 1024 * 1024, // 256 MB
            spill_dir: std::env::temp_dir().join("streamcrab-spill"),
        }
    }
}

// ── SpillManager ──────────────────────────────────────────────────────────────

/// Manages spill files for a single operator instance.
///
/// # Lifecycle
/// 1. Create with [`SpillManager::new`] (creates the spill directory).
/// 2. Call [`update_memory_usage`] after each batch to track estimated bytes.
/// 3. When [`should_spill`] returns `true`, serialize operator state and call [`spill`].
/// 4. Later call [`read_spill`] to read a specific spill file back.
/// 5. On drop (or explicit [`cleanup`]) all spill files are removed.
pub struct SpillManager {
    config: SpillConfig,
    spill_files: Vec<PathBuf>,
    current_memory_usage: usize,
}

impl SpillManager {
    /// Create a new `SpillManager`. Creates the spill directory if it does not exist.
    pub fn new(config: SpillConfig) -> Result<Self> {
        fs::create_dir_all(&config.spill_dir)?;
        Ok(Self {
            config,
            spill_files: Vec::new(),
            current_memory_usage: 0,
        })
    }

    /// Returns `true` when estimated memory usage meets or exceeds the configured limit.
    pub fn should_spill(&self) -> bool {
        self.current_memory_usage >= self.config.memory_limit_bytes
    }

    /// Update the current estimated memory usage in bytes.
    pub fn update_memory_usage(&mut self, bytes: usize) {
        self.current_memory_usage = bytes;
    }

    /// Write `data` to a new spill file and return its path.
    ///
    /// Each call appends a new file; existing files are never overwritten.
    pub fn spill(&mut self, data: &[u8]) -> Result<PathBuf> {
        let file_name = format!("spill_{}.bin", self.spill_files.len());
        let path = self.config.spill_dir.join(&file_name);
        let file = fs::File::create(&path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(data)?;
        writer.flush()?;
        self.spill_files.push(path.clone());
        Ok(path)
    }

    /// Read back the contents of a spill file produced by [`spill`].
    pub fn read_spill(path: &Path) -> Result<Vec<u8>> {
        let file = fs::File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        Ok(data)
    }

    /// Return a slice of all spill file paths created so far.
    pub fn spill_files(&self) -> &[PathBuf] {
        &self.spill_files
    }

    /// Number of spill files created so far.
    pub fn spill_count(&self) -> usize {
        self.spill_files.len()
    }

    /// Delete all spill files and reset memory tracking.
    ///
    /// Errors removing individual files are silently ignored so that partial
    /// cleanup does not prevent the rest from being removed.
    pub fn cleanup(&mut self) -> Result<()> {
        for path in self.spill_files.drain(..) {
            let _ = fs::remove_file(path);
        }
        self.current_memory_usage = 0;
        Ok(())
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn test_config() -> SpillConfig {
        // Use a unique subdirectory per test invocation to avoid cross-test interference.
        let dir = env::temp_dir().join(format!(
            "streamcrab-spill-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
        ));
        SpillConfig {
            memory_limit_bytes: 1024,
            spill_dir: dir,
        }
    }

    // ── Test 1: SpillManager creation ─────────────────────────────────────────

    #[test]
    fn test_spill_manager_creation() {
        let cfg = test_config();
        let dir = cfg.spill_dir.clone();
        let _mgr = SpillManager::new(cfg).expect("SpillManager::new should succeed");
        assert!(dir.exists(), "spill directory should be created");
    }

    // ── Test 2: Write / read round-trip ──────────────────────────────────────

    #[test]
    fn test_spill_write_read_roundtrip() {
        let mut mgr = SpillManager::new(test_config()).unwrap();
        let data: Vec<u8> = (0u8..=255).collect();
        let path = mgr.spill(&data).expect("spill should succeed");
        let read_back = SpillManager::read_spill(&path).expect("read_spill should succeed");
        assert_eq!(read_back, data, "data must survive a write/read round-trip");
    }

    // ── Test 3: Cleanup removes files ─────────────────────────────────────────

    #[test]
    fn test_spill_cleanup() {
        let mut mgr = SpillManager::new(test_config()).unwrap();
        let path = mgr.spill(b"hello").unwrap();
        assert!(path.exists(), "file should exist before cleanup");
        mgr.cleanup().unwrap();
        assert!(
            !path.exists(),
            "file should be removed after cleanup"
        );
        assert_eq!(mgr.spill_count(), 0, "spill_count should be 0 after cleanup");
    }

    // ── Test 4: should_spill respects threshold ───────────────────────────────

    #[test]
    fn test_spill_should_spill_threshold() {
        let cfg = SpillConfig {
            memory_limit_bytes: 500,
            spill_dir: test_config().spill_dir,
        };
        let mut mgr = SpillManager::new(cfg).unwrap();

        mgr.update_memory_usage(499);
        assert!(!mgr.should_spill(), "499 < 500: should not spill");

        mgr.update_memory_usage(500);
        assert!(mgr.should_spill(), "500 >= 500: should spill");

        mgr.update_memory_usage(1000);
        assert!(mgr.should_spill(), "1000 >= 500: should spill");
    }

    // ── Test 5: Multiple spills create multiple files ─────────────────────────

    #[test]
    fn test_spill_multiple_files() {
        let mut mgr = SpillManager::new(test_config()).unwrap();

        let path0 = mgr.spill(b"first").unwrap();
        let path1 = mgr.spill(b"second").unwrap();
        let path2 = mgr.spill(b"third").unwrap();

        assert_eq!(mgr.spill_count(), 3);
        assert_ne!(path0, path1);
        assert_ne!(path1, path2);

        assert_eq!(SpillManager::read_spill(&path0).unwrap(), b"first");
        assert_eq!(SpillManager::read_spill(&path1).unwrap(), b"second");
        assert_eq!(SpillManager::read_spill(&path2).unwrap(), b"third");
    }

    // ── Test 6: Drop automatically cleans up ──────────────────────────────────

    #[test]
    fn test_spill_drop_cleans_up() {
        let path = {
            let mut mgr = SpillManager::new(test_config()).unwrap();
            let p = mgr.spill(b"data").unwrap();
            assert!(p.exists());
            p
        }; // mgr dropped here
        assert!(!path.exists(), "Drop should remove spill files");
    }
}
