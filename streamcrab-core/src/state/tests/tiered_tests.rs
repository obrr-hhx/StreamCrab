use std::sync::{Arc, Mutex};

use crate::state::{
    InMemoryStateClient, KeyedStateBackend, StateServiceCore, StateServiceCoreConfig,
    TieredStateBackend, TieredStateBackendConfig, ValueStateHandle,
};

fn new_backend() -> (
    TieredStateBackend<InMemoryStateClient>,
    Arc<Mutex<StateServiceCore>>,
) {
    let core = Arc::new(Mutex::new(StateServiceCore::new(
        StateServiceCoreConfig::default(),
    )));
    let client = Arc::new(InMemoryStateClient::new(Arc::clone(&core)));
    (
        TieredStateBackend::new(client, TieredStateBackendConfig::default()),
        core,
    )
}

#[test]
fn test_tiered_backend_write_buffer_priority() {
    let (mut backend, _core) = new_backend();
    backend.set_current_key(b"user1".to_vec());
    let handle = ValueStateHandle::<i32>::new("count");

    handle.put(&mut backend, 42).unwrap();
    assert_eq!(handle.get(&backend).unwrap(), Some(42));
}

#[test]
fn test_tiered_backend_flush_persists_remote() {
    let (mut backend, core) = new_backend();
    backend.set_current_key(b"user1".to_vec());
    let handle = ValueStateHandle::<i32>::new("count");
    handle.put(&mut backend, 7).unwrap();
    backend.flush().unwrap();

    let core_guard = core.lock().unwrap();
    assert_eq!(core_guard.pending_epochs(), vec![1]);
}

#[test]
fn test_tiered_backend_snapshot_restore() {
    let (mut backend, _core) = new_backend();
    backend.set_current_key(b"user1".to_vec());
    let handle = ValueStateHandle::<i32>::new("count");
    handle.put(&mut backend, 88).unwrap();
    let snapshot = backend.snapshot().unwrap();

    let (mut restored, _core2) = new_backend();
    restored.restore(&snapshot).unwrap();
    restored.set_current_key(b"user1".to_vec());
    assert_eq!(handle.get(&restored).unwrap(), Some(88));
}

#[test]
fn test_tiered_backend_prefetch_hot_keys_and_rescale_warmup() {
    let core = Arc::new(Mutex::new(StateServiceCore::new(
        StateServiceCoreConfig::default(),
    )));
    {
        let mut guard = core.lock().unwrap();
        guard.put(b"k_hot".to_vec(), b"v_hot".to_vec(), 1).unwrap();
        guard
            .put(b"k_cold".to_vec(), b"v_cold".to_vec(), 1)
            .unwrap();
        let _ = guard.get(b"k_hot", 1);
        let _ = guard.get(b"k_hot", 1);
        let _ = guard.get(b"k_hot", 1);
    }

    let client = Arc::new(InMemoryStateClient::new(Arc::clone(&core)));
    let mut backend = TieredStateBackend::new(
        client,
        TieredStateBackendConfig {
            cache_capacity: 16,
            flush_batch_size: 64,
            auto_flush_interval: std::time::Duration::from_millis(10),
            warmup_top_keys: 1,
            require_remote_flush: false,
        },
    );

    assert_eq!(backend.prefetch_hot_keys(1).unwrap(), 1);
    backend.on_rescale_activate(2).unwrap();
}

#[test]
fn test_tiered_backend_auto_flush_interval_triggers_flush() {
    let core = Arc::new(Mutex::new(StateServiceCore::new(
        StateServiceCoreConfig::default(),
    )));
    let client = Arc::new(InMemoryStateClient::new(Arc::clone(&core)));
    let mut backend = TieredStateBackend::new(
        client,
        TieredStateBackendConfig {
            cache_capacity: 16,
            flush_batch_size: usize::MAX,
            auto_flush_interval: std::time::Duration::from_millis(0),
            warmup_top_keys: 0,
            require_remote_flush: false,
        },
    );
    backend.set_current_key(b"user-auto".to_vec());
    let handle = ValueStateHandle::<i32>::new("cnt");
    handle.put(&mut backend, 1).unwrap();

    let core_guard = core.lock().unwrap();
    assert_eq!(core_guard.pending_epochs(), vec![1]);
}

#[test]
fn test_tiered_backend_trigger_remote_snapshot_marks_epoch() {
    let (mut backend, core) = new_backend();
    backend.set_current_key(b"user-snapshot".to_vec());
    let handle = ValueStateHandle::<i32>::new("count");
    handle.put(&mut backend, 9).unwrap();

    backend.set_epoch(1);
    backend.flush().unwrap();
    backend.trigger_remote_snapshot(1).unwrap();

    let core_guard = core.lock().unwrap();
    assert_eq!(core_guard.triggered_snapshot_epochs(), vec![1]);
}

#[test]
fn test_tiered_backend_rescale_activate_keeps_state_continuity() {
    let (mut backend, _core) = new_backend();
    let handle = ValueStateHandle::<i32>::new("sum");

    backend.set_current_key(b"user-a".to_vec());
    handle.put(&mut backend, 10).unwrap();
    backend.flush().unwrap();

    backend.on_rescale_activate(2).unwrap();

    backend.set_current_key(b"user-a".to_vec());
    assert_eq!(
        handle.get(&backend).unwrap(),
        Some(10),
        "value should remain readable after rescale activation"
    );
}
