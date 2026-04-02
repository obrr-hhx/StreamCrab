//! End-to-end integration tests using the real compiled wasm-counter guest UDF.
//!
//! The tests load `wasm_counter.wasm` (compiled from `examples/wasm-counter`)
//! via the Host `WasmRuntime` and exercise the full ABI path:
//!   Host → bincode-serialized WasmInput → Guest linear memory → process() → WasmOutput → Host
//!
//! Run with:
//!   cargo test -p streamcrab-wasm --test e2e_guest_tests

use streamcrab_core::operator_chain::Operator;
use streamcrab_core::state::HashMapStateBackend;
use streamcrab_wasm::abi::{WasmInput, WasmRecord};
use streamcrab_wasm::operator::WasmOperator;
use streamcrab_wasm::runtime::{WasmRuntime, WasmRuntimeConfig};

// ---------------------------------------------------------------------------
// Helper: load (and optionally build) the wasm-counter artifact.
// ---------------------------------------------------------------------------

fn get_counter_wasm() -> Vec<u8> {
    let wasm_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../examples/wasm-counter/target/wasm32-unknown-unknown/release/wasm_counter.wasm");

    if !wasm_path.exists() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/wasm-counter/Cargo.toml");
        let status = std::process::Command::new("cargo")
            .args(["build", "--manifest-path"])
            .arg(&manifest)
            .args(["--target", "wasm32-unknown-unknown", "--release"])
            .status()
            .expect("Failed to spawn cargo build for wasm-counter");
        assert!(status.success(), "wasm-counter build failed");
    }

    std::fs::read(&wasm_path).expect("Failed to read wasm_counter.wasm")
}

fn default_config() -> WasmRuntimeConfig {
    WasmRuntimeConfig {
        rebuild_after_calls: 100,
        ..Default::default()
    }
}

fn count_from_bytes(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().expect("output value must be 8 bytes"))
}

fn make_record(key: &[u8], value: &[u8]) -> WasmRecord {
    WasmRecord {
        key: key.to_vec(),
        value: value.to_vec(),
        timestamp: None,
    }
}

// ---------------------------------------------------------------------------
// 1. WasmRuntime — raw ABI tests
// ---------------------------------------------------------------------------

#[test]
fn runtime_loads_real_wasm() {
    let bytes = get_counter_wasm();
    let rt = WasmRuntime::new(&bytes, default_config());
    assert!(
        rt.is_ok(),
        "WasmRuntime failed to load wasm-counter: {:?}",
        rt.err()
    );
}

#[test]
fn runtime_call_count_starts_at_one_no_state() {
    let bytes = get_counter_wasm();
    let mut rt = WasmRuntime::new(&bytes, default_config()).unwrap();

    let input = WasmInput {
        record: make_record(b"key1", b"payload"),
        state: None,
    };
    let output = rt.call(&input).unwrap();

    assert_eq!(output.records.len(), 1, "Expected one output record");
    assert_eq!(
        count_from_bytes(&output.records[0].value),
        1,
        "First call with no state should yield count=1"
    );
    assert_eq!(output.records[0].key, b"key1", "Key must be preserved");
    assert!(output.state.is_some(), "Guest must return new state");
    let state_count = count_from_bytes(output.state.as_ref().unwrap());
    assert_eq!(state_count, 1, "Returned state must encode count=1");
}

#[test]
fn runtime_call_count_increments_from_state() {
    let bytes = get_counter_wasm();
    let mut rt = WasmRuntime::new(&bytes, default_config()).unwrap();

    // First call — no prior state
    let input1 = WasmInput {
        record: make_record(b"k", b"v"),
        state: None,
    };
    let out1 = rt.call(&input1).unwrap();
    assert_eq!(count_from_bytes(&out1.records[0].value), 1);

    // Second call — feed state from first call
    let input2 = WasmInput {
        record: make_record(b"k", b"v"),
        state: out1.state.clone(),
    };
    let out2 = rt.call(&input2).unwrap();
    assert_eq!(
        count_from_bytes(&out2.records[0].value),
        2,
        "Second call must yield count=2"
    );

    // Third call
    let input3 = WasmInput {
        record: make_record(b"k", b"v"),
        state: out2.state.clone(),
    };
    let out3 = rt.call(&input3).unwrap();
    assert_eq!(
        count_from_bytes(&out3.records[0].value),
        3,
        "Third call must yield count=3"
    );
}

#[test]
fn runtime_call_preserves_timestamp() {
    let bytes = get_counter_wasm();
    let mut rt = WasmRuntime::new(&bytes, default_config()).unwrap();

    let input = WasmInput {
        record: WasmRecord {
            key: b"ts_key".to_vec(),
            value: b"data".to_vec(),
            timestamp: Some(99_999),
        },
        state: None,
    };
    let output = rt.call(&input).unwrap();
    assert_eq!(
        output.records[0].timestamp,
        Some(99_999),
        "Timestamp must survive the ABI round-trip"
    );
}

#[test]
fn runtime_large_state_counter() {
    // Drive the counter to 100 to confirm state round-trips across many calls.
    let bytes = get_counter_wasm();
    let mut rt = WasmRuntime::new(&bytes, default_config()).unwrap();

    let mut state: Option<Vec<u8>> = None;
    for expected in 1u64..=100 {
        let input = WasmInput {
            record: make_record(b"loop_key", b"x"),
            state: state.take(),
        };
        let output = rt.call(&input).unwrap();
        let got = count_from_bytes(&output.records[0].value);
        assert_eq!(got, expected, "Counter mismatch at iteration {expected}");
        state = output.state;
    }
}

// ---------------------------------------------------------------------------
// 2. WasmOperator with HashMapStateBackend
// ---------------------------------------------------------------------------

#[test]
fn operator_processes_single_record() {
    let bytes = get_counter_wasm();
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(bytes, default_config(), Some(backend)).unwrap();

    op.set_current_key(b"user_1".to_vec());

    let input = vec![b"event".to_vec()];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(
        count_from_bytes(&output[0]),
        1,
        "First record for user_1 should yield count=1"
    );
}

#[test]
fn operator_increments_count_across_batches() {
    let bytes = get_counter_wasm();
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(bytes, default_config(), Some(backend)).unwrap();

    op.set_current_key(b"user_a".to_vec());

    for expected in 1u64..=5 {
        let mut output = Vec::new();
        op.process_batch(&[b"rec".to_vec()], &mut output).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(
            count_from_bytes(&output[0]),
            expected,
            "Batch {expected}: expected count={expected}"
        );
    }
}

#[test]
fn operator_independent_state_per_key() {
    // Different keys must maintain independent counters.
    let bytes = get_counter_wasm();
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(bytes, default_config(), Some(backend)).unwrap();

    // Process 3 records for key_a
    op.set_current_key(b"key_a".to_vec());
    for _ in 0..3 {
        let mut out = Vec::new();
        op.process_batch(&[b"x".to_vec()], &mut out).unwrap();
    }

    // Process 1 record for key_b — must start at 1, not 4
    op.set_current_key(b"key_b".to_vec());
    let mut out = Vec::new();
    op.process_batch(&[b"y".to_vec()], &mut out).unwrap();
    assert_eq!(
        count_from_bytes(&out[0]),
        1,
        "key_b must start its own counter at 1"
    );

    // key_a should continue from 3
    op.set_current_key(b"key_a".to_vec());
    let mut out = Vec::new();
    op.process_batch(&[b"z".to_vec()], &mut out).unwrap();
    assert_eq!(count_from_bytes(&out[0]), 4, "key_a should continue at 4");
}

// ---------------------------------------------------------------------------
// 3. Snapshot / Restore — state continuity across operator restart
// ---------------------------------------------------------------------------

#[test]
fn operator_snapshot_restore_continues_count() {
    let bytes = get_counter_wasm();

    // --- Phase A: process 5 records and snapshot ---
    let backend_a = HashMapStateBackend::new();
    let mut op_a = WasmOperator::new(bytes.clone(), default_config(), Some(backend_a)).unwrap();

    op_a.set_current_key(b"persistent_key".to_vec());
    for _ in 0..5 {
        let mut out = Vec::new();
        op_a.process_batch(&[b"event".to_vec()], &mut out).unwrap();
    }

    let snapshot = op_a.snapshot_state().unwrap();

    // --- Phase B: restore into a fresh operator and continue ---
    let backend_b = HashMapStateBackend::new();
    let mut op_b = WasmOperator::new(bytes, default_config(), Some(backend_b)).unwrap();
    op_b.restore_state(&snapshot).unwrap();

    op_b.set_current_key(b"persistent_key".to_vec());
    let mut out = Vec::new();
    op_b.process_batch(&[b"event".to_vec()], &mut out).unwrap();

    assert_eq!(
        count_from_bytes(&out[0]),
        6,
        "After restore the counter must continue from 5, yielding count=6"
    );
}

#[test]
fn operator_snapshot_restore_multiple_keys() {
    let bytes = get_counter_wasm();

    // Build state for two keys.
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(bytes.clone(), default_config(), Some(backend)).unwrap();

    op.set_current_key(b"alpha".to_vec());
    for _ in 0..3 {
        let mut out = Vec::new();
        op.process_batch(&[b"e".to_vec()], &mut out).unwrap();
    }
    op.set_current_key(b"beta".to_vec());
    for _ in 0..7 {
        let mut out = Vec::new();
        op.process_batch(&[b"e".to_vec()], &mut out).unwrap();
    }

    let snapshot = op.snapshot_state().unwrap();

    // Restore into fresh operator.
    let backend2 = HashMapStateBackend::new();
    let mut op2 = WasmOperator::new(bytes, default_config(), Some(backend2)).unwrap();
    op2.restore_state(&snapshot).unwrap();

    op2.set_current_key(b"alpha".to_vec());
    let mut out = Vec::new();
    op2.process_batch(&[b"e".to_vec()], &mut out).unwrap();
    assert_eq!(
        count_from_bytes(&out[0]),
        4,
        "alpha must continue at 4 after restore"
    );

    op2.set_current_key(b"beta".to_vec());
    let mut out = Vec::new();
    op2.process_batch(&[b"e".to_vec()], &mut out).unwrap();
    assert_eq!(
        count_from_bytes(&out[0]),
        8,
        "beta must continue at 8 after restore"
    );
}

// ---------------------------------------------------------------------------
// 4. Checkpoint barrier integration
// ---------------------------------------------------------------------------

#[test]
fn operator_checkpoint_barrier_does_not_reset_count() {
    let bytes = get_counter_wasm();
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(bytes, default_config(), Some(backend)).unwrap();

    op.set_current_key(b"ck_key".to_vec());
    for _ in 0..3 {
        let mut out = Vec::new();
        op.process_batch(&[b"e".to_vec()], &mut out).unwrap();
    }

    op.on_checkpoint_barrier(1).unwrap();

    // After checkpoint the counter must NOT reset.
    let mut out = Vec::new();
    op.set_current_key(b"ck_key".to_vec());
    op.process_batch(&[b"e".to_vec()], &mut out).unwrap();
    assert_eq!(
        count_from_bytes(&out[0]),
        4,
        "Checkpoint barrier must not reset state; count should be 4"
    );
}
