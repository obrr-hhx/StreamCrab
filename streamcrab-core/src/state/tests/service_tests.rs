use crate::state::{StateServiceCore, StateServiceCoreConfig};

#[test]
fn test_state_service_epoch_monotonic_and_pending_bound() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig {
        max_pending_epochs: 2,
        ..StateServiceCoreConfig::default()
    });
    core.put(b"k1".to_vec(), b"v1".to_vec(), 1).unwrap();
    core.put(b"k2".to_vec(), b"v2".to_vec(), 2).unwrap();
    assert!(core.put(b"k3".to_vec(), b"v3".to_vec(), 3).is_err());
}

#[test]
fn test_state_service_seal_is_idempotent() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig::default());
    core.put(b"k".to_vec(), b"v".to_vec(), 1).unwrap();
    core.seal(1).unwrap();
    core.seal(1).unwrap();
    assert_eq!(core.sealed_epoch(), 1);
    assert_eq!(core.get(b"k", 2), Some(b"v".to_vec()));
}

#[test]
fn test_state_service_rollback_clears_pending_without_polluting_committed() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig::default());
    core.put(b"k".to_vec(), b"v1".to_vec(), 1).unwrap();
    core.seal(1).unwrap();
    core.put(b"k".to_vec(), b"v2".to_vec(), 2).unwrap();
    assert_eq!(core.get(b"k", 2), Some(b"v2".to_vec()));
    core.rollback();
    assert_eq!(core.get(b"k", 2), Some(b"v1".to_vec()));
}

#[test]
fn test_state_service_timer_index_tracks_timer_keys() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig::default());
    core.put(b"__timer_event_100|user-a".to_vec(), b"payload".to_vec(), 1)
        .unwrap();
    let timers = core.timer_entries();
    assert_eq!(timers.len(), 1);
    assert_eq!(timers[0].fire_at, 100);
    assert_eq!(timers[0].user_key, b"user-a".to_vec());
}

#[test]
fn test_state_service_top_keys_prefers_hot_entries() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig::default());
    core.put(b"k_hot".to_vec(), b"v1".to_vec(), 1).unwrap();
    core.put(b"k_cold".to_vec(), b"v2".to_vec(), 1).unwrap();

    // k_hot gets more accesses than k_cold.
    assert_eq!(core.get(b"k_hot", 1), Some(b"v1".to_vec()));
    assert_eq!(core.get(b"k_hot", 1), Some(b"v1".to_vec()));
    assert_eq!(core.get(b"k_hot", 1), Some(b"v1".to_vec()));
    assert_eq!(core.get(b"k_cold", 1), Some(b"v2".to_vec()));

    let top = core.top_keys(1, 1);
    assert_eq!(top, vec![b"k_hot".to_vec()]);
}

#[test]
fn test_state_service_oom_key_limit_rejects_new_keys() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig {
        max_pending_epochs: 2,
        max_total_keys: 1,
        max_total_bytes: 1024 * 1024,
        state_ttl_ms: None,
    });
    core.put(b"k1".to_vec(), b"v1".to_vec(), 1).unwrap();
    let err = core.put(b"k2".to_vec(), b"v2".to_vec(), 1).unwrap_err();
    assert!(err.to_string().contains("key limit"));
}

#[test]
fn test_state_service_ttl_evicts_expired_keys() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig {
        max_pending_epochs: 2,
        max_total_keys: 100,
        max_total_bytes: 1024 * 1024,
        state_ttl_ms: Some(1),
    });
    core.put(b"k".to_vec(), b"v".to_vec(), 1).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(3));
    assert_eq!(core.get(b"k", 1), None);
}

#[test]
fn test_state_service_put_sequence_idempotency() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig::default());
    let accepted = core
        .put_with_meta(
            b"k".to_vec(),
            b"v1".to_vec(),
            1,
            Some("task-a"),
            Some(1),
        )
        .unwrap();
    assert!(accepted);
    let dup = core
        .put_with_meta(
            b"k".to_vec(),
            b"v2".to_vec(),
            1,
            Some("task-a"),
            Some(1),
        )
        .unwrap();
    assert!(!dup);
    assert_eq!(core.get(b"k", 1), Some(b"v1".to_vec()));

    let accepted_next = core
        .put_with_meta(
            b"k".to_vec(),
            b"v3".to_vec(),
            1,
            Some("task-a"),
            Some(2),
        )
        .unwrap();
    assert!(accepted_next);
    assert_eq!(core.get(b"k", 1), Some(b"v3".to_vec()));
}

#[test]
fn test_state_service_trigger_snapshot_records_epoch() {
    let mut core = StateServiceCore::new(StateServiceCoreConfig::default());
    core.put(b"k".to_vec(), b"v".to_vec(), 1).unwrap();
    core.flush(1).unwrap();
    core.trigger_snapshot(1).unwrap();
    assert_eq!(core.triggered_snapshot_epochs(), vec![1]);
}

#[tokio::test]
async fn test_state_service_grpc_roundtrip() {
    use crate::cluster::rpc;
    use crate::cluster::rpc::state_service_server::StateService;
    use crate::state::service::StateServiceRpc;
    use std::sync::{Arc, Mutex};
    use tonic::Request;

    let core = Arc::new(Mutex::new(StateServiceCore::new(
        StateServiceCoreConfig::default(),
    )));
    let rpc_impl = StateServiceRpc::new(core);

    rpc_impl
        .put(Request::new(rpc::PutRequest {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            epoch: 1,
            task_id: "test".to_string(),
            seq_no: 1,
        }))
        .await
        .unwrap();
    let response = rpc_impl
        .get(Request::new(rpc::GetRequest {
            key: b"k".to_vec(),
            epoch: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.found);
    assert_eq!(response.value, b"v".to_vec());

    let snapshot = rpc_impl
        .trigger_snapshot(Request::new(rpc::TriggerSnapshotRequest { epoch: 1 }))
        .await
        .unwrap()
        .into_inner();
    assert!(snapshot.accepted);
}
