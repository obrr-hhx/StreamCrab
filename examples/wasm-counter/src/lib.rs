use streamcrab_wasm_guest::*;

/// A simple counter UDF that increments a u64 counter for each record.
///
/// State encoding: little-endian u64 (8 bytes).
/// Output value: the updated count as little-endian u64 bytes.
#[derive(Default)]
pub struct Counter;

impl ProcessFunction for Counter {
    fn process(&mut self, record: WasmRecord, state: Option<Vec<u8>>) -> WasmOutput {
        // Read previous count from state (little-endian u64)
        let prev_count = state
            .as_ref()
            .and_then(|s| s.get(..8))
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .unwrap_or(0);

        let new_count = prev_count + 1;

        // Output record with count as value
        let out_record = WasmRecord {
            key: record.key,
            value: new_count.to_le_bytes().to_vec(),
            timestamp: record.timestamp,
        };

        WasmOutput {
            records: vec![out_record],
            state: Some(new_count.to_le_bytes().to_vec()),
        }
    }
}

export_process_fn!(Counter);
