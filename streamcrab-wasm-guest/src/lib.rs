//! # StreamCrab WASM Guest SDK
//!
//! This crate provides the building blocks for writing StreamCrab UDFs that compile
//! to WebAssembly. Link it from your UDF crate targeting `wasm32-unknown-unknown`.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use streamcrab_wasm_guest::{export_process_fn, ProcessFunction, WasmOutput, WasmRecord};
//!
//! #[derive(Default)]
//! pub struct MyUdf;
//!
//! impl ProcessFunction for MyUdf {
//!     fn process(&mut self, record: WasmRecord, _state: Option<Vec<u8>>) -> WasmOutput {
//!         WasmOutput { records: vec![record], state: None }
//!     }
//! }
//!
//! export_process_fn!(MyUdf);
//! ```
//!
//! ## WASM Invariants
//!
//! - **I_WASM_1 (No IO):** Guest must not perform network or filesystem access.
//! - **I_WASM_2 (Stateless):** The WASM instance is disposable; all persistent state is
//!   passed in via [`WasmInput::state`] and returned via [`WasmOutput::state`].
//! - **I_WASM_3 (Determinism):** Same `(record, state)` pair must always produce the same
//!   output — this enables replay-based fault tolerance.
//!
//! ## ABI overview
//!
//! The Host calls three exports on every Guest module:
//!
//! | Export    | Signature                     | Purpose                              |
//! |-----------|-------------------------------|--------------------------------------|
//! | `alloc`   | `(size: u32) -> u32`          | Allocate `size` bytes; return ptr    |
//! | `dealloc` | `(ptr: u32, size: u32)`       | Free a previous allocation           |
//! | `process` | `(ptr: u32, len: u32) -> u64` | Deserialize input, run UDF, return   |
//!
//! The `process` return value packs two `u32`s: `(result_ptr << 32) | result_len`.
//! The Host reads that region and frees it with `dealloc`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ABI types — duplicated from streamcrab-wasm to avoid a cross-compilation
// dependency on wasmtime and other host-only crates.
// ---------------------------------------------------------------------------

/// A record passed between Host and Guest.
///
/// All fields are raw bytes so the Guest can deserialize them according to its
/// own schema without pulling in complex type dependencies.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmRecord {
    /// Serialized key bytes (used for keyed-state lookup on the Host).
    pub key: Vec<u8>,
    /// Serialized value bytes (the actual user payload).
    pub value: Vec<u8>,
    /// Optional event timestamp in milliseconds since the Unix epoch.
    pub timestamp: Option<i64>,
}

/// Input bundle written into Guest linear memory by the Host before calling `process`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmInput {
    /// The record to process.
    pub record: WasmRecord,
    /// Current state bytes fetched from the Host StateBackend, or `None` if there
    /// is no prior state for this key.
    pub state: Option<Vec<u8>>,
}

/// Output bundle written into Guest linear memory and read back by the Host.
///
/// Models a pure function: `process(record, state) -> (outputs, new_state)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmOutput {
    /// Zero or more records to emit downstream.
    pub records: Vec<WasmRecord>,
    /// Updated state bytes to persist in the Host StateBackend. `None` means
    /// "leave existing state unchanged".
    pub state: Option<Vec<u8>>,
}

// ---------------------------------------------------------------------------
// Memory management exports — every Guest module must expose these so the
// Host can safely read/write Guest linear memory.
// ---------------------------------------------------------------------------

/// Allocate `size` bytes with 8-byte alignment and return the pointer.
///
/// Called by the Host before writing a serialized [`WasmInput`] into Guest memory.
/// Only exported on `wasm32` targets — on 64-bit hosts the pointer would not fit in u32.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: u32) -> u32 {
    let layout = std::alloc::Layout::from_size_align(size as usize, 8).unwrap();
    // SAFETY: layout has non-zero size (Host never allocates 0 bytes).
    unsafe { std::alloc::alloc(layout) as u32 }
}

/// Free a previously allocated region of `size` bytes starting at `ptr`.
///
/// Called by the Host after it has finished reading the serialized [`WasmOutput`].
/// Only exported on `wasm32` targets — on 64-bit hosts the pointer would not fit in u32.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn dealloc(ptr: u32, size: u32) {
    let layout = std::alloc::Layout::from_size_align(size as usize, 8).unwrap();
    // SAFETY: `ptr` was returned by a prior call to `alloc` with the same `size`.
    unsafe { std::alloc::dealloc(ptr as *mut u8, layout) }
}

// ---------------------------------------------------------------------------
// Linear-memory helpers used by the generated `process` wrapper.
// ---------------------------------------------------------------------------

/// Deserialize a bincode-encoded `T` from a Host-written region of linear memory.
///
/// # Safety
/// `ptr` must point to at least `len` valid, initialized bytes written by the Host.
/// The memory must remain valid for the duration of this call.
pub unsafe fn host_read<T: for<'de> Deserialize<'de>>(ptr: *const u8, len: usize) -> T {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    bincode::deserialize(slice).expect("bincode deserialize failed")
}

/// Serialize `value` with bincode, copy it into a freshly allocated region of
/// Guest memory, and return a packed `u64` of `(ptr << 32) | len`.
///
/// On `wasm32` targets the packed value uses the WASM linear-memory pointer (u32).
/// On native targets this function is only called from the generated `process` export
/// which itself only runs inside a WASM module; it is provided here so the crate
/// compiles on the host for documentation/testing purposes.
///
/// The Host will call `dealloc(ptr, len)` after reading the result.
#[cfg(target_arch = "wasm32")]
pub fn host_write<T: Serialize>(value: &T) -> u64 {
    let bytes = bincode::serialize(value).expect("bincode serialize failed");
    let len = bytes.len() as u32;
    let ptr = alloc(len);
    // SAFETY: `alloc` just returned a valid region of exactly `len` bytes.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, len as usize) };
    ((ptr as u64) << 32) | (len as u64)
}

/// Native stub for `host_write` — exists so the crate type-checks on 64-bit hosts
/// and the macro compiles in non-WASM contexts (e.g. IDE analysis).
///
/// On native the u32 address space assumption does not hold, so this function
/// **panics** if called at runtime outside of a WASM module.
/// Use `bincode::serialize` directly in native unit tests instead.
#[cfg(not(target_arch = "wasm32"))]
pub fn host_write<T: Serialize>(_value: &T) -> u64 {
    panic!("host_write is only valid inside a wasm32 Guest module");
}

// ---------------------------------------------------------------------------
// ProcessFunction trait — the single entry point a UDF author implements.
// ---------------------------------------------------------------------------

/// Core trait for WASM UDFs.
///
/// Implement this trait and call [`export_process_fn!`] to expose your UDF to
/// the StreamCrab Host runtime.
///
/// # Invariants
/// - Must be deterministic (I_WASM_3): same `(record, state)` produces same result.
/// - Must not perform IO (I_WASM_1): no network, file, or system calls.
/// - State is the only persistence mechanism (I_WASM_2): do not rely on
///   instance-local fields surviving a Host restart.
pub trait ProcessFunction {
    /// Process one record and return the output records plus updated state.
    ///
    /// `state` is the raw bytes previously returned by this function (or `None`
    /// on the first invocation for a given key). The Host will persist whatever
    /// bytes are returned in `WasmOutput::state`.
    fn process(&mut self, record: WasmRecord, state: Option<Vec<u8>>) -> WasmOutput;
}

// ---------------------------------------------------------------------------
// export_process_fn! — glue macro that bridges the C ABI to ProcessFunction.
// ---------------------------------------------------------------------------

/// Generate the `process` C-ABI export for a [`ProcessFunction`] implementor.
///
/// # Usage
///
/// ```rust,ignore
/// #[derive(Default)]
/// pub struct MyUdf;
///
/// impl ProcessFunction for MyUdf {
///     fn process(&mut self, record: WasmRecord, _state: Option<Vec<u8>>) -> WasmOutput {
///         WasmOutput { records: vec![record], state: None }
///     }
/// }
///
/// export_process_fn!(MyUdf);
/// ```
///
/// The macro creates a `static mut INSTANCE: Option<$ty>` and initialises it
/// lazily with `Default::default()` on the first call. This matches I_WASM_2:
/// the Host may destroy and recreate the WASM instance at any checkpoint; the
/// real state is always round-tripped through `WasmInput::state`.
#[macro_export]
macro_rules! export_process_fn {
    ($ty:ty) => {
        static mut INSTANCE: Option<$ty> = None;

        /// WASM entry point called by the StreamCrab Host for every record.
        ///
        /// Reads a bincode-encoded `WasmInput` from `(ptr, len)`, delegates to
        /// `ProcessFunction::process`, and returns a packed
        /// `(result_ptr << 32) | result_len` pointing to a bincode-encoded
        /// `WasmOutput` in Guest memory.
        ///
        /// The Host frees the returned region with `dealloc(result_ptr, result_len)`.
        #[unsafe(no_mangle)]
        pub extern "C" fn process(ptr: u32, len: u32) -> u64 {
            // SAFETY: Host allocated `(ptr, len)` via our `alloc` export and
            // wrote a valid bincode-encoded WasmInput before calling `process`.
            let input: $crate::WasmInput =
                unsafe { $crate::host_read(ptr as *const u8, len as usize) };
            // SAFETY: single-threaded WASM execution model; no concurrent access.
            let instance = unsafe { INSTANCE.get_or_insert_with(|| <$ty>::default()) };
            let output = $crate::ProcessFunction::process(instance, input.record, input.state);
            $crate::host_write(&output)
        }
    };
}

// ---------------------------------------------------------------------------
// Example / test UDF implementations (compiled only under `cargo test`).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- CounterUdf ---------------------------------------------------------

    /// Example stateful UDF: counts how many records have been processed for a key.
    ///
    /// State encoding: little-endian `u64` (8 bytes).
    /// Output value: the updated count as little-endian `u64` bytes.
    #[derive(Default)]
    struct CounterUdf;

    impl ProcessFunction for CounterUdf {
        fn process(&mut self, record: WasmRecord, state: Option<Vec<u8>>) -> WasmOutput {
            // Decode existing count from state, defaulting to 0.
            let count: u64 = state
                .as_deref()
                .and_then(|b| <&[u8; 8]>::try_from(b).ok())
                .map(|b| u64::from_le_bytes(*b))
                .unwrap_or(0);

            let new_count = count + 1;
            let new_state = new_count.to_le_bytes().to_vec();

            let out_record = WasmRecord {
                key: record.key,
                value: new_count.to_le_bytes().to_vec(),
                timestamp: record.timestamp,
            };

            WasmOutput {
                records: vec![out_record],
                state: Some(new_state),
            }
        }
    }

    // --- FilterUdf ----------------------------------------------------------

    /// Example stateless UDF: passes records through only when `value.len() > threshold`.
    struct FilterUdf {
        threshold: usize,
    }

    impl FilterUdf {
        fn with_threshold(threshold: usize) -> Self {
            Self { threshold }
        }
    }

    impl ProcessFunction for FilterUdf {
        fn process(&mut self, record: WasmRecord, state: Option<Vec<u8>>) -> WasmOutput {
            let records = if record.value.len() > self.threshold {
                vec![record]
            } else {
                vec![]
            };
            WasmOutput { records, state }
        }
    }

    // --- CounterUdf tests ---------------------------------------------------

    #[test]
    fn counter_starts_at_one_with_no_state() {
        let mut udf = CounterUdf;
        let record = WasmRecord {
            key: b"k1".to_vec(),
            value: b"hello".to_vec(),
            timestamp: None,
        };
        let out = udf.process(record, None);

        assert_eq!(out.records.len(), 1);
        assert_eq!(
            u64::from_le_bytes(out.records[0].value.as_slice().try_into().unwrap()),
            1
        );
        assert_eq!(
            out.state
                .as_deref()
                .map(|b| u64::from_le_bytes(<[u8; 8]>::try_from(b).unwrap())),
            Some(1)
        );
    }

    #[test]
    fn counter_increments_from_existing_state() {
        let mut udf = CounterUdf;
        let record = WasmRecord {
            key: b"k1".to_vec(),
            value: b"world".to_vec(),
            timestamp: Some(1000),
        };
        let initial_state = Some(42u64.to_le_bytes().to_vec());
        let out = udf.process(record, initial_state);

        assert_eq!(
            u64::from_le_bytes(out.records[0].value.as_slice().try_into().unwrap()),
            43
        );
        assert_eq!(
            out.state
                .as_deref()
                .map(|b| u64::from_le_bytes(<[u8; 8]>::try_from(b).unwrap())),
            Some(43)
        );
    }

    #[test]
    fn counter_preserves_key_and_timestamp() {
        let mut udf = CounterUdf;
        let record = WasmRecord {
            key: b"mykey".to_vec(),
            value: b"v".to_vec(),
            timestamp: Some(9999),
        };
        let out = udf.process(record, None);
        assert_eq!(out.records[0].key, b"mykey");
        assert_eq!(out.records[0].timestamp, Some(9999));
    }

    // --- FilterUdf tests ----------------------------------------------------

    #[test]
    fn filter_passes_record_above_threshold() {
        let mut udf = FilterUdf::with_threshold(3);
        let record = WasmRecord {
            key: b"k".to_vec(),
            value: b"hello".to_vec(), // len 5 > 3
            timestamp: None,
        };
        let out = udf.process(record, None);
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].value, b"hello");
    }

    #[test]
    fn filter_drops_record_at_or_below_threshold() {
        let mut udf = FilterUdf::with_threshold(3);
        let record = WasmRecord {
            key: b"k".to_vec(),
            value: b"hi".to_vec(), // len 2 <= 3
            timestamp: None,
        };
        let out = udf.process(record, None);
        assert_eq!(out.records.len(), 0);
    }

    #[test]
    fn filter_passes_state_through_unchanged() {
        let mut udf = FilterUdf::with_threshold(0);
        let state = Some(b"some-state".to_vec());
        let record = WasmRecord {
            key: b"k".to_vec(),
            value: b"x".to_vec(),
            timestamp: None,
        };
        let out = udf.process(record, state.clone());
        assert_eq!(out.state, state);
    }

    // --- ABI round-trip test ------------------------------------------------
    //
    // Verifies that WasmOutput survives a bincode serialize → deserialize cycle,
    // mirroring what host_write / host_read do inside a real WASM module.
    // We test the serialization logic directly here; the pointer-packing half of
    // the ABI only works in a wasm32 address space and is covered by integration
    // tests that compile with --target wasm32-unknown-unknown.

    #[test]
    fn abi_types_roundtrip_bincode() {
        let output = WasmOutput {
            records: vec![WasmRecord {
                key: b"rk".to_vec(),
                value: b"rv".to_vec(),
                timestamp: Some(42),
            }],
            state: Some(b"st".to_vec()),
        };

        let bytes = bincode::serialize(&output).expect("serialize");
        // SAFETY: `bytes` is valid for its entire length.
        let decoded: WasmOutput = unsafe { host_read(bytes.as_ptr(), bytes.len()) };
        assert_eq!(decoded, output);
    }

    #[test]
    fn abi_input_roundtrip_bincode() {
        let input = WasmInput {
            record: WasmRecord {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                timestamp: None,
            },
            state: Some(b"s".to_vec()),
        };

        let bytes = bincode::serialize(&input).expect("serialize");
        // SAFETY: `bytes` is valid for its entire length.
        let decoded: WasmInput = unsafe { host_read(bytes.as_ptr(), bytes.len()) };
        assert_eq!(decoded, input);
    }
}
