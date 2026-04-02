//! WASM Host Runtime powered by wasmtime.
//!
//! Loads a compiled WASM module, manages linear memory, and calls the Guest
//! `process` function via the bincode ABI.

use anyhow::{Context, Result};
use tracing::debug;
use wasmtime::{Engine, Instance, Linker, Memory, Module, Store, TypedFunc};

use crate::abi::{self, WasmInput, WasmOutput};

/// Configuration for the WASM runtime.
#[derive(Debug, Clone)]
pub struct WasmRuntimeConfig {
    /// Maximum WASM linear memory in bytes (default: 64 MiB).
    pub max_memory_bytes: usize,
    /// Rebuild Instance after this many calls to prevent Guest memory leaks (I_WASM_2).
    pub rebuild_after_calls: usize,
    /// Also rebuild Instance on every checkpoint barrier (strongest leak prevention).
    ///
    /// When true, the Instance is discarded and recreated at every checkpoint, not just
    /// every `rebuild_after_calls` calls. Default: false.
    pub rebuild_on_checkpoint: bool,
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024,
            rebuild_after_calls: 100_000,
            rebuild_on_checkpoint: false,
        }
    }
}

/// WASM Host Runtime.
///
/// Manages a wasmtime Instance and provides a high-level `call` method
/// that handles memory allocation, serialization, and deserialization.
pub struct WasmRuntime {
    engine: Engine,
    module: Module,
    store: Store<()>,
    instance: Instance,
    memory: Memory,
    alloc_fn: TypedFunc<u32, u32>,
    dealloc_fn: TypedFunc<(u32, u32), ()>,
    process_fn: TypedFunc<(u32, u32), u64>,
    config: WasmRuntimeConfig,
    call_count: usize,
}

impl WasmRuntime {
    /// Load a WASM module from bytes and instantiate it.
    pub fn new(wasm_bytes: &[u8], config: WasmRuntimeConfig) -> Result<Self> {
        // I_WASM_1: Disable all Guest IO capabilities via explicit Engine config.
        // We use a bare Linker (no wasmtime_wasi::add_to_linker) so the Guest has
        // no WASI functions available. The config below additionally disables
        // threading and documents the security posture explicitly.
        let mut engine_config = wasmtime::Config::new();
        engine_config.wasm_threads(false); // No threading in Guest
        engine_config.wasm_reference_types(true); // Keep standard features
        engine_config.wasm_bulk_memory(true); // Needed for memory.copy
        let engine = Engine::new(&engine_config)?;

        let module = Module::new(&engine, wasm_bytes).context("failed to compile WASM module")?;

        let mut store = Store::new(&engine, ());
        let linker = Linker::new(&engine);
        let instance = linker
            .instantiate(&mut store, &module)
            .context("failed to instantiate WASM module")?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .context("WASM module must export 'memory'")?;
        let alloc_fn = instance
            .get_typed_func::<u32, u32>(&mut store, "alloc")
            .context("WASM module must export 'alloc(size: u32) -> u32'")?;
        let dealloc_fn = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "dealloc")
            .context("WASM module must export 'dealloc(ptr: u32, size: u32)'")?;
        let process_fn = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "process")
            .context("WASM module must export 'process(ptr: u32, len: u32) -> u64'")?;

        debug!("WASM module loaded successfully");

        Ok(Self {
            engine,
            module,
            store,
            instance,
            memory,
            alloc_fn,
            dealloc_fn,
            process_fn,
            config,
            call_count: 0,
        })
    }

    /// Call the Guest `process` function with a single input.
    ///
    /// Handles: serialize input → alloc in Guest → write → call → read output → dealloc.
    pub fn call(&mut self, input: &WasmInput) -> Result<WasmOutput> {
        // Check if we need to rebuild the Instance (I_WASM_2: prevent memory leaks)
        self.call_count += 1;
        if self.call_count % self.config.rebuild_after_calls == 0 {
            self.rebuild_instance()?;
        }

        // Serialize input
        let input_bytes = abi::encode(input)?;
        let input_len = input_bytes.len() as u32;

        // Allocate in Guest linear memory
        let input_ptr = self
            .alloc_fn
            .call(&mut self.store, input_len)
            .context("Guest alloc failed")?;

        // Write input bytes to Guest memory
        self.write_memory(input_ptr, &input_bytes)?;

        // Call Guest process function
        // Returns packed (ptr: u32, len: u32) as u64
        let result_packed = self
            .process_fn
            .call(&mut self.store, (input_ptr, input_len))
            .context("Guest process failed")?;

        // Unpack result pointer and length
        let result_ptr = (result_packed >> 32) as u32;
        let result_len = (result_packed & 0xFFFF_FFFF) as u32;

        // Read output bytes from Guest memory
        let output_bytes = self.read_memory(result_ptr, result_len as usize)?;

        // Deserialize output
        let output: WasmOutput = abi::decode(&output_bytes)?;

        // Free Guest memory
        self.dealloc_fn
            .call(&mut self.store, (input_ptr, input_len))
            .ok(); // Best-effort dealloc
        self.dealloc_fn
            .call(&mut self.store, (result_ptr, result_len))
            .ok();

        Ok(output)
    }

    /// Write bytes into Guest linear memory at the given offset.
    fn write_memory(&mut self, ptr: u32, data: &[u8]) -> Result<()> {
        let mem_data = self.memory.data_mut(&mut self.store);
        let start = ptr as usize;
        let end = start + data.len();
        if end > mem_data.len() {
            anyhow::bail!(
                "WASM memory write out of bounds: offset={start}, len={}, memory_size={}",
                data.len(),
                mem_data.len()
            );
        }
        mem_data[start..end].copy_from_slice(data);
        Ok(())
    }

    /// Read bytes from Guest linear memory at the given offset.
    fn read_memory(&self, ptr: u32, len: usize) -> Result<Vec<u8>> {
        let mem_data = self.memory.data(&self.store);
        let start = ptr as usize;
        let end = start + len;
        if end > mem_data.len() {
            anyhow::bail!(
                "WASM memory read out of bounds: offset={start}, len={len}, memory_size={}",
                mem_data.len()
            );
        }
        Ok(mem_data[start..end].to_vec())
    }

    /// Rebuild the WASM Instance to reclaim leaked Guest memory (I_WASM_2).
    ///
    /// State is in the Host StateBackend, so this is zero-cost from a
    /// correctness standpoint.
    fn rebuild_instance(&mut self) -> Result<()> {
        debug!("Rebuilding WASM instance after {} calls", self.call_count);
        let mut store = Store::new(&self.engine, ());
        let linker = Linker::new(&self.engine);
        let instance = linker.instantiate(&mut store, &self.module)?;

        self.memory = instance
            .get_memory(&mut store, "memory")
            .context("memory export missing after rebuild")?;
        self.alloc_fn = instance.get_typed_func(&mut store, "alloc")?;
        self.dealloc_fn = instance.get_typed_func(&mut store, "dealloc")?;
        self.process_fn = instance.get_typed_func(&mut store, "process")?;
        self.store = store;
        self.instance = instance;

        Ok(())
    }

    /// Low-level call: send raw bytes, get raw bytes back.
    ///
    /// Useful for testing or when the caller handles serialization externally.
    pub fn call_raw(&mut self, input_bytes: &[u8]) -> Result<Vec<u8>> {
        self.call_count += 1;
        if self.call_count % self.config.rebuild_after_calls == 0 {
            self.rebuild_instance()?;
        }

        let input_len = input_bytes.len() as u32;
        let input_ptr = self
            .alloc_fn
            .call(&mut self.store, input_len)
            .context("Guest alloc failed")?;
        self.write_memory(input_ptr, input_bytes)?;

        let result_packed = self
            .process_fn
            .call(&mut self.store, (input_ptr, input_len))
            .context("Guest process failed")?;

        let result_ptr = (result_packed >> 32) as u32;
        let result_len = (result_packed & 0xFFFF_FFFF) as u32;
        let output_bytes = self.read_memory(result_ptr, result_len as usize)?;

        self.dealloc_fn
            .call(&mut self.store, (input_ptr, input_len))
            .ok();
        self.dealloc_fn
            .call(&mut self.store, (result_ptr, result_len))
            .ok();

        Ok(output_bytes)
    }

    /// Get the number of calls made since last rebuild.
    pub fn call_count(&self) -> usize {
        self.call_count
    }

    /// Rebuild the WASM Instance now, discarding any accumulated Guest memory leaks (I_WASM_2).
    ///
    /// Public wrapper around `rebuild_instance`. Called by `WasmOperator` on checkpoint
    /// when `rebuild_on_checkpoint` is enabled.
    pub fn rebuild(&mut self) -> Result<()> {
        self.rebuild_instance()
    }

    /// Verify determinism: call `process` twice with identical input and assert identical output.
    ///
    /// Returns `Ok(())` if the Guest is deterministic (I_WASM_3). Returns `Err` if the
    /// two calls produce different outputs, indicating a non-deterministic Guest.
    pub fn verify_determinism(&mut self, input: &WasmInput) -> Result<()> {
        let out1 = self.call(input)?;
        let out2 = self.call(input)?;
        if out1 != out2 {
            anyhow::bail!("I_WASM_3 violation: same input produced different outputs");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::{WasmOutput, WasmRecord};

    /// WAT module: echo — copies input bytes to a new location and returns them.
    const ECHO_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $bump (mut i32) (i32.const 1024))

          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $size)))
            (local.get $ptr))

          (func (export "dealloc") (param $ptr i32) (param $size i32))

          (func (export "process") (param $ptr i32) (param $len i32) (result i64)
            (local $out_ptr i32)
            (local.set $out_ptr (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $len)))
            (memory.copy (local.get $out_ptr) (local.get $ptr) (local.get $len))
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $out_ptr)) (i64.const 32))
              (i64.extend_i32_u (local.get $len)))))
    "#;

    /// WAT module: always returns a hardcoded empty WasmOutput.
    /// bincode of WasmOutput { records: vec![], state: None } = 9 zero bytes.
    /// WASM linear memory is zero-initialized, so we just return a fresh region.
    const EMPTY_OUTPUT_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $bump (mut i32) (i32.const 1024))

          (func (export "alloc") (param $size i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $size)))
            (local.get $ptr))

          (func (export "dealloc") (param $ptr i32) (param $size i32))

          (func (export "process") (param $ptr i32) (param $len i32) (result i64)
            (local $out_ptr i32)
            (local.set $out_ptr (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (i32.const 9)))
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $out_ptr)) (i64.const 32))
              (i64.const 9))))
    "#;

    fn default_config() -> WasmRuntimeConfig {
        WasmRuntimeConfig {
            rebuild_after_calls: 100,
            ..Default::default()
        }
    }

    #[test]
    fn load_echo_module() {
        let rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config());
        assert!(rt.is_ok(), "Failed to load echo WAT module: {:?}", rt.err());
    }

    #[test]
    fn call_raw_echo_roundtrip() {
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config()).unwrap();
        let input = b"hello wasm world";
        let output = rt.call_raw(input).unwrap();
        assert_eq!(input.as_slice(), output.as_slice());
    }

    #[test]
    fn call_raw_echo_empty() {
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config()).unwrap();
        let output = rt.call_raw(b"").unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn call_raw_echo_large_payload() {
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config()).unwrap();
        let input: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let output = rt.call_raw(&input).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn call_empty_output() {
        let mut rt = WasmRuntime::new(EMPTY_OUTPUT_WAT.as_bytes(), default_config()).unwrap();
        let input = WasmInput {
            record: WasmRecord {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                timestamp: Some(42),
            },
            state: None,
        };
        let output = rt.call(&input).unwrap();
        assert_eq!(
            output,
            WasmOutput {
                records: vec![],
                state: None,
            }
        );
    }

    #[test]
    fn call_echo_returns_valid_wasm_output() {
        // Encode a WasmOutput, send it through echo, decode it back.
        let expected = WasmOutput {
            records: vec![WasmRecord {
                key: b"user_1".to_vec(),
                value: b"counted".to_vec(),
                timestamp: Some(999),
            }],
            state: Some(vec![7, 0, 0, 0, 0, 0, 0, 0]),
        };
        let encoded = abi::encode(&expected).unwrap();

        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config()).unwrap();
        let raw_output = rt.call_raw(&encoded).unwrap();
        let decoded: WasmOutput = abi::decode(&raw_output).unwrap();
        assert_eq!(expected, decoded);
    }

    #[test]
    fn rebuild_instance_after_threshold() {
        let config = WasmRuntimeConfig {
            rebuild_after_calls: 3,
            ..Default::default()
        };
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), config).unwrap();

        // Make 6 calls — should trigger rebuild at call 3 and 6
        for i in 0..6 {
            let data = vec![i as u8; 4];
            let out = rt.call_raw(&data).unwrap();
            assert_eq!(data, out, "Echo failed at call {i}");
        }
        assert_eq!(rt.call_count(), 6);
    }

    #[test]
    fn multiple_sequential_calls() {
        let mut rt = WasmRuntime::new(EMPTY_OUTPUT_WAT.as_bytes(), default_config()).unwrap();
        for i in 0..50 {
            let input = WasmInput {
                record: WasmRecord {
                    key: format!("key_{i}").into_bytes(),
                    value: vec![i as u8],
                    timestamp: Some(i as i64),
                },
                state: None,
            };
            let output = rt.call(&input).unwrap();
            assert!(output.records.is_empty());
            assert!(output.state.is_none());
        }
    }

    #[test]
    fn determinism_same_input_same_output() {
        // I_WASM_3: same input must produce same output
        let mut rt = WasmRuntime::new(EMPTY_OUTPUT_WAT.as_bytes(), default_config()).unwrap();
        let input = WasmInput {
            record: WasmRecord {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                timestamp: Some(1),
            },
            state: Some(vec![10]),
        };
        let out1 = rt.call(&input).unwrap();
        let out2 = rt.call(&input).unwrap();
        assert_eq!(
            out1, out2,
            "I_WASM_3 violated: same input produced different output"
        );
    }

    // ── I_WASM_1: WASI import rejection ─────────────────────────────────────────

    /// A WAT module that imports `fd_write` from `wasi_snapshot_preview1`.
    /// Instantiation must fail because the bare Linker has no WASI bindings.
    const WASI_IMPORT_WAT: &str = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (global $bump (mut i32) (i32.const 1024))
          (func (export "alloc") (param i32) (result i32) (global.get $bump))
          (func (export "dealloc") (param i32 i32))
          (func (export "process") (param i32 i32) (result i64) (i64.const 0)))
    "#;

    #[test]
    fn i_wasm_1_wasi_import_rejected() {
        // I_WASM_1: Guest that tries to import WASI functions must fail to instantiate.
        let result = WasmRuntime::new(WASI_IMPORT_WAT.as_bytes(), default_config());
        assert!(
            result.is_err(),
            "Expected instantiation to fail for WASI-importing module, but it succeeded"
        );
    }

    // ── I_WASM_2: rebuild on checkpoint ─────────────────────────────────────────

    #[test]
    fn rebuild_on_checkpoint_config_defaults_false() {
        let config = WasmRuntimeConfig::default();
        assert!(!config.rebuild_on_checkpoint);
    }

    #[test]
    fn public_rebuild_resets_instance_and_call_continues() {
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config()).unwrap();
        // Make some calls, then explicitly rebuild.
        rt.call_raw(b"before").unwrap();
        rt.rebuild().expect("rebuild() should succeed");
        // Instance is fresh; further calls must still work.
        let out = rt.call_raw(b"after").unwrap();
        assert_eq!(out, b"after");
    }

    #[test]
    fn rebuild_count_accessible_after_rebuild() {
        let config = WasmRuntimeConfig {
            rebuild_after_calls: 2,
            ..Default::default()
        };
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), config).unwrap();
        // call_count increments through rebuilds (it is never reset).
        for i in 0..4u8 {
            rt.call_raw(&[i]).unwrap();
        }
        assert_eq!(rt.call_count(), 4);
    }

    // ── I_WASM_3: verify_determinism helper ─────────────────────────────────────

    #[test]
    fn verify_determinism_echo_passes() {
        // The echo module always returns whatever bytes it received — deterministic.
        // Encode a WasmOutput so the echo roundtrip produces a decodable result.
        let encoded = abi::encode(&WasmOutput {
            records: vec![WasmRecord {
                key: b"det_key".to_vec(),
                value: b"det_val".to_vec(),
                timestamp: Some(100),
            }],
            state: None,
        })
        .unwrap();
        // call_raw twice with same bytes — output must be identical.
        let mut rt = WasmRuntime::new(ECHO_WAT.as_bytes(), default_config()).unwrap();
        let o1 = rt.call_raw(&encoded).unwrap();
        let o2 = rt.call_raw(&encoded).unwrap();
        assert_eq!(o1, o2, "echo module must be deterministic");
    }

    #[test]
    fn verify_determinism_empty_output_passes() {
        let mut rt = WasmRuntime::new(EMPTY_OUTPUT_WAT.as_bytes(), default_config()).unwrap();
        let input = WasmInput {
            record: WasmRecord {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                timestamp: Some(42),
            },
            state: Some(vec![1, 2, 3]),
        };
        rt.verify_determinism(&input)
            .expect("I_WASM_3: empty_output module should be deterministic");
    }
}
