//! Shared ABI types for Host ↔ Guest communication.
//!
//! These types are serialized with bincode and passed through WASM linear memory.
//! Guest SDK (wasm32-wasi target) and Host runtime both use these definitions.

use serde::{Deserialize, Serialize};

/// A record passed into the WASM Guest.
///
/// Uses raw bytes to avoid complex type dependencies in the Guest.
/// The Guest deserializes `value` according to its own schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmRecord {
    /// Serialized key bytes (for keyed state lookup).
    pub key: Vec<u8>,
    /// Serialized value bytes (user data).
    pub value: Vec<u8>,
    /// Optional event timestamp in milliseconds.
    pub timestamp: Option<i64>,
}

/// Input bundle sent from Host to Guest via linear memory.
///
/// Contains the record to process and optional current state bytes.
/// The Guest's `process` function receives this as its input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmInput {
    /// The record to process.
    pub record: WasmRecord,
    /// Current state bytes (from Host StateBackend), or None if no prior state.
    pub state: Option<Vec<u8>>,
}

/// Output bundle returned from Guest to Host via linear memory.
///
/// Contains zero or more output records and optional updated state.
/// Pure function model: `process(input, state) -> (outputs, new_state)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmOutput {
    /// Output records to emit downstream.
    pub records: Vec<WasmRecord>,
    /// Updated state bytes to persist in Host StateBackend, or None to keep unchanged.
    pub state: Option<Vec<u8>>,
}

/// Encode a value to bincode bytes for linear memory transfer.
pub fn encode<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    bincode::serialize(value).map_err(|e| anyhow::anyhow!("bincode encode: {e}"))
}

/// Decode a value from bincode bytes read from linear memory.
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    bincode::deserialize(bytes).map_err(|e| anyhow::anyhow!("bincode decode: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_wasm_input() {
        let input = WasmInput {
            record: WasmRecord {
                key: b"user_1".to_vec(),
                value: b"hello".to_vec(),
                timestamp: Some(1000),
            },
            state: Some(vec![42, 0, 0, 0, 0, 0, 0, 0]),
        };
        let bytes = encode(&input).unwrap();
        let decoded: WasmInput = decode(&bytes).unwrap();
        assert_eq!(input, decoded);
    }

    #[test]
    fn roundtrip_wasm_output() {
        let output = WasmOutput {
            records: vec![WasmRecord {
                key: b"user_1".to_vec(),
                value: b"world".to_vec(),
                timestamp: None,
            }],
            state: Some(vec![43, 0, 0, 0, 0, 0, 0, 0]),
        };
        let bytes = encode(&output).unwrap();
        let decoded: WasmOutput = decode(&bytes).unwrap();
        assert_eq!(output, decoded);
    }

    #[test]
    fn empty_output() {
        let output = WasmOutput {
            records: vec![],
            state: None,
        };
        let bytes = encode(&output).unwrap();
        let decoded: WasmOutput = decode(&bytes).unwrap();
        assert_eq!(output, decoded);
    }
}
