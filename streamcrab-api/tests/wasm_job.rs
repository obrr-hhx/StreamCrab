//! Integration tests for KeyedStream::process_wasm / WasmJob.

use streamcrab_api::environment::StreamExecutionEnvironment;

/// WAT module that always returns an empty WasmOutput.
/// 9 zero bytes = bincode encoding of WasmOutput { records: [], state: None }.
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

#[test]
fn process_wasm_compiles_and_returns_empty_results() {
    let env = StreamExecutionEnvironment::new("wasm_test");

    let words = vec![("hello".to_string(), 1i32), ("world".to_string(), 1)];

    let results = env
        .from_iter(words)
        .key_by(|(word, _): &(String, i32)| word.clone())
        .process_wasm(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), None)
        .execute_with_parallelism(1)
        .unwrap();

    // EMPTY_OUTPUT_WAT returns no output records for any input
    assert!(
        results.is_empty(),
        "expected empty results from empty-output WASM UDF, got {} records",
        results.len()
    );
}

#[test]
fn process_wasm_with_explicit_config_returns_empty_results() {
    use streamcrab_wasm::runtime::WasmRuntimeConfig;

    let env = StreamExecutionEnvironment::new("wasm_config_test");

    let records = vec![42i32, 7, 100];

    let config = WasmRuntimeConfig {
        rebuild_after_calls: 10,
        ..Default::default()
    };

    let results = env
        .from_iter(records)
        .key_by(|n: &i32| *n)
        .process_wasm(EMPTY_OUTPUT_WAT.as_bytes().to_vec(), Some(config))
        .execute_with_parallelism(1)
        .unwrap();

    assert!(results.is_empty());
}
