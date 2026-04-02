//! Integration tests for WasmOperator with HashMapStateBackend.

use streamcrab_core::operator_chain::Operator;
use streamcrab_core::state::HashMapStateBackend;
use streamcrab_wasm::operator::WasmOperator;
use streamcrab_wasm::runtime::WasmRuntimeConfig;

/// WAT: always returns empty WasmOutput (9 zero bytes = bincode of { records: [], state: None }).
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

// ============================================================================
// Stateless WasmOperator tests
// ============================================================================

#[test]
fn operator_process_batch_empty_input() {
    let mut op: WasmOperator<HashMapStateBackend> =
        WasmOperator::new(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), default_config(), None).unwrap();

    let mut output = Vec::new();
    op.process_batch(&[], &mut output).unwrap();
    assert!(output.is_empty());
}

#[test]
fn operator_process_batch_multiple_records() {
    let mut op: WasmOperator<HashMapStateBackend> =
        WasmOperator::new(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), default_config(), None).unwrap();

    let input = vec![
        b"record_1".to_vec(),
        b"record_2".to_vec(),
        b"record_3".to_vec(),
    ];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();
    // Empty output WAT produces no records
    assert!(output.is_empty());
}

// ============================================================================
// Stateful WasmOperator tests
// ============================================================================

#[test]
fn operator_with_state_backend() {
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend),
    )
    .unwrap();

    op.set_current_key(b"key_1".to_vec());
    let input = vec![b"data".to_vec()];
    let mut output = Vec::new();
    op.process_batch(&input, &mut output).unwrap();
    assert!(output.is_empty());
}

// ============================================================================
// Snapshot / Restore tests (I_WASM_2)
// ============================================================================

#[test]
fn operator_snapshot_stateless() {
    let op: WasmOperator<HashMapStateBackend> =
        WasmOperator::new(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), default_config(), None).unwrap();

    let snapshot = op.snapshot_state().unwrap();
    assert!(
        snapshot.is_empty(),
        "Stateless operator should produce empty snapshot"
    );
}

#[test]
fn operator_snapshot_with_backend() {
    let backend = HashMapStateBackend::new();
    let op = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend),
    )
    .unwrap();

    let snapshot = op.snapshot_state().unwrap();
    // Fresh backend snapshot is non-empty (contains serialized empty maps)
    assert!(!snapshot.is_empty());
}

#[test]
fn operator_restore_rebuilds_instance() {
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend),
    )
    .unwrap();

    // Process some records
    op.set_current_key(b"key_1".to_vec());
    let mut output = Vec::new();
    op.process_batch(&[b"a".to_vec()], &mut output).unwrap();

    // Snapshot
    let snapshot = op.snapshot_state().unwrap();

    // Restore — this should rebuild the WASM Instance (I_WASM_2)
    op.restore_state(&snapshot).unwrap();

    // Should still work after restore
    op.set_current_key(b"key_1".to_vec());
    output.clear();
    op.process_batch(&[b"b".to_vec()], &mut output).unwrap();
    // Empty output WAT still produces no output records
    assert!(output.is_empty());
}

#[test]
fn operator_snapshot_restore_roundtrip() {
    let backend1 = HashMapStateBackend::new();
    let op1 = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend1),
    )
    .unwrap();

    let snapshot = op1.snapshot_state().unwrap();

    // Create a new operator and restore from snapshot
    let backend2 = HashMapStateBackend::new();
    let mut op2 = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend2),
    )
    .unwrap();
    op2.restore_state(&snapshot).unwrap();

    // Restored operator should work correctly
    op2.set_current_key(b"key_1".to_vec());
    let mut output = Vec::new();
    op2.process_batch(&[b"test".to_vec()], &mut output).unwrap();
    assert!(output.is_empty());
}

// ============================================================================
// Checkpoint barrier test
// ============================================================================

#[test]
fn operator_checkpoint_barrier() {
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend),
    )
    .unwrap();

    // Should not error on checkpoint barrier
    op.on_checkpoint_barrier(1).unwrap();
    op.on_checkpoint_barrier(2).unwrap();
}

#[test]
fn operator_checkpoint_barrier_no_backend() {
    let mut op: WasmOperator<HashMapStateBackend> =
        WasmOperator::new(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), default_config(), None).unwrap();

    // No backend — checkpoint barrier is a no-op
    op.on_checkpoint_barrier(1).unwrap();
}

// ============================================================================
// Hot update tests
// ============================================================================

/// A second valid WAT module with bump start at 2048 (distinct from EMPTY_OUTPUT_WAT).
const EMPTY_OUTPUT_WAT_V2: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $bump (mut i32) (i32.const 2048))
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

#[test]
fn test_hot_update_at_checkpoint() {
    // Schedule an update — it must NOT take effect until on_checkpoint_barrier is called.
    let mut op: WasmOperator<HashMapStateBackend> =
        WasmOperator::new(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), default_config(), None).unwrap();

    assert!(!op.has_pending_update());

    op.schedule_update(EMPTY_OUTPUT_WAT_V2.as_bytes().to_vec());
    assert!(
        op.has_pending_update(),
        "Update should be pending after schedule_update"
    );

    // The module has NOT been swapped yet — operator should still work
    let mut output = Vec::new();
    op.process_batch(&[b"before_swap".to_vec()], &mut output)
        .unwrap();
    assert!(output.is_empty());
    assert!(
        op.has_pending_update(),
        "Still pending before checkpoint barrier"
    );

    // Trigger checkpoint barrier — swap should happen here
    op.on_checkpoint_barrier(42).unwrap();
    assert!(
        !op.has_pending_update(),
        "Pending update should be consumed after barrier"
    );

    // Operator should still work after the swap
    output.clear();
    op.process_batch(&[b"after_swap".to_vec()], &mut output)
        .unwrap();
    assert!(output.is_empty());
}

#[test]
fn test_hot_update_preserves_state() {
    // Verify the state backend survives a WASM module hot-swap.
    let backend = HashMapStateBackend::new();
    let mut op = WasmOperator::new(
        EMPTY_OUTPUT_WAT.as_bytes().to_vec(),
        default_config(),
        Some(backend),
    )
    .unwrap();

    op.set_current_key(b"key_a".to_vec());
    let mut output = Vec::new();
    op.process_batch(&[b"data".to_vec()], &mut output).unwrap();

    // Take a snapshot before the swap
    let snapshot_before = op.snapshot_state().unwrap();

    op.schedule_update(EMPTY_OUTPUT_WAT_V2.as_bytes().to_vec());
    op.on_checkpoint_barrier(1).unwrap();

    // Snapshot after swap must equal snapshot before (state backend untouched)
    let snapshot_after = op.snapshot_state().unwrap();
    assert_eq!(
        snapshot_before, snapshot_after,
        "State backend must be identical after WASM module hot-swap"
    );

    // Operator is fully functional after the swap
    op.set_current_key(b"key_a".to_vec());
    output.clear();
    op.process_batch(&[b"more_data".to_vec()], &mut output)
        .unwrap();
    assert!(output.is_empty());
}

// ============================================================================
// Stress / rebuild test
// ============================================================================

#[test]
fn operator_many_batches_with_rebuild() {
    let config = WasmRuntimeConfig {
        rebuild_after_calls: 5,
        ..Default::default()
    };
    let mut op: WasmOperator<HashMapStateBackend> =
        WasmOperator::new(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), config, None).unwrap();

    // Process 20 batches — should trigger multiple rebuilds at call 5, 10, 15, 20
    for _ in 0..20 {
        let mut output = Vec::new();
        op.process_batch(&[b"x".to_vec()], &mut output).unwrap();
    }
}
