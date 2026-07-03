use super::*;
use crate::task::{TaskId, VertexId};
use opendal::services::Memory;

fn memory_storage() -> ObjectStoreCheckpointStorage {
    let op = Operator::new(Memory::default()).unwrap().finish();
    ObjectStoreCheckpointStorage::new(op).unwrap()
}

fn meta(checkpoint_id: CheckpointId, task_id: TaskId) -> CheckpointMetadata {
    CheckpointMetadata {
        checkpoint_id,
        timestamp: 42,
        task_ids: vec![task_id],
    }
}

#[test]
fn test_object_store_checkpoint_roundtrip() {
    let storage = memory_storage();
    let t0 = TaskId::new(VertexId::new(1), 0);
    let m = meta(7, t0);

    storage.save_checkpoint(m.clone()).unwrap();
    storage.save_task_state(7, t0, vec![1, 2, 3]).unwrap();

    assert_eq!(storage.load_checkpoint(7).unwrap(), m);
    assert_eq!(storage.load_task_state(7, t0).unwrap(), vec![1, 2, 3]);
    assert_eq!(storage.list_checkpoints().unwrap(), vec![7]);
}

#[test]
fn test_object_store_load_missing_checkpoint_fails() {
    let storage = memory_storage();
    assert!(storage.load_checkpoint(999).is_err());
    assert!(
        storage
            .load_task_state(999, TaskId::new(VertexId::new(1), 0))
            .is_err()
    );
}

#[test]
fn test_object_store_purge_keeps_latest_n() {
    let storage = memory_storage();
    let t0 = TaskId::new(VertexId::new(3), 0);
    for id in 1..=5 {
        storage.save_checkpoint(meta(id, t0)).unwrap();
        storage.save_task_state(id, t0, vec![id as u8]).unwrap();
    }

    storage.purge(2).unwrap();

    assert_eq!(storage.list_checkpoints().unwrap(), vec![4, 5]);
    assert!(storage.load_checkpoint(3).is_err());
    assert!(storage.load_task_state(3, t0).is_err());
    assert_eq!(storage.load_task_state(5, t0).unwrap(), vec![5]);
}

#[test]
fn test_checkpoint_storage_config_builds_all_backends() {
    let mem = CheckpointStorageConfig::InMemory.build().unwrap();
    let t0 = TaskId::new(VertexId::new(5), 0);
    mem.save_checkpoint(meta(1, t0)).unwrap();
    assert_eq!(mem.list_checkpoints().unwrap(), vec![1]);

    let dir = std::env::temp_dir().join(format!("streamcrab-cfg-{}", std::process::id()));
    let fs_storage = CheckpointStorageConfig::Fs { path: dir.clone() }.build().unwrap();
    fs_storage.save_checkpoint(meta(2, t0)).unwrap();
    assert_eq!(fs_storage.list_checkpoints().unwrap(), vec![2]);
    std::fs::remove_dir_all(&dir).unwrap();

    // S3 config builds offline; credentials/requests are lazy.
    CheckpointStorageConfig::S3 {
        bucket: "b".into(),
        root: "chk".into(),
        endpoint: Some("http://127.0.0.1:9000".into()),
        region: Some("us-east-1".into()),
    }
    .build()
    .unwrap();
}

#[test]
fn test_object_store_callable_from_async_context() {
    // The cluster JobManager invokes CheckpointStorage from inside tokio;
    // a nested block_on would panic without the scoped-thread indirection.
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let storage = memory_storage();
        let t0 = TaskId::new(VertexId::new(9), 0);
        storage.save_checkpoint(meta(1, t0)).unwrap();
        assert_eq!(storage.list_checkpoints().unwrap(), vec![1]);
    });
}
