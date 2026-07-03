//! End-to-end test for S3-backed checkpoint storage against a real
//! S3-compatible endpoint (MinIO).
//!
//! Setup:
//!   docker run -d --name streamcrab-minio -p 9000:9000 \
//!     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!     minio/minio server /data
//!   curl --aws-sigv4 "aws:amz:us-east-1:s3" --user minioadmin:minioadmin \
//!     -X PUT http://127.0.0.1:9000/streamcrab-test
//!
//! Run:
//!   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//!     cargo test -p streamcrab-core --test s3_checkpoint_e2e -- --ignored --nocapture

use streamcrab_core::checkpoint::{CheckpointMetadata, CheckpointStorageConfig};
use streamcrab_core::runtime::task::{TaskId, VertexId};

#[test]
#[ignore = "requires a running MinIO/S3 endpoint (see module docs)"]
fn s3_checkpoint_roundtrip_recovery_and_purge() {
    let endpoint =
        std::env::var("STREAMCRAB_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let bucket =
        std::env::var("STREAMCRAB_S3_BUCKET").unwrap_or_else(|_| "streamcrab-test".into());

    let config = CheckpointStorageConfig::S3 {
        bucket,
        root: "e2e-checkpoints".into(),
        endpoint: Some(endpoint),
        region: Some("us-east-1".into()),
    };
    let storage = config.build().unwrap();

    let t0 = TaskId::new(VertexId::new(1), 0);
    let t1 = TaskId::new(VertexId::new(1), 1);
    for id in 1..=4u64 {
        storage
            .save_checkpoint(CheckpointMetadata {
                checkpoint_id: id,
                timestamp: (id * 100) as i64,
                task_ids: vec![t0, t1],
            })
            .unwrap();
        storage.save_task_state(id, t0, vec![id as u8; 64]).unwrap();
        storage
            .save_task_state(id, t1, vec![id as u8 + 100; 64])
            .unwrap();
    }

    // A recovering JobManager builds a fresh client and must see everything
    // the previous incarnation persisted.
    let recovered = config.build().unwrap();
    assert_eq!(recovered.list_checkpoints().unwrap(), vec![1, 2, 3, 4]);
    let meta = recovered.load_checkpoint(3).unwrap();
    assert_eq!(meta.timestamp, 300);
    assert_eq!(meta.task_ids, vec![t0, t1]);
    assert_eq!(recovered.load_task_state(3, t1).unwrap(), vec![103u8; 64]);

    recovered.purge(1).unwrap();
    assert_eq!(recovered.list_checkpoints().unwrap(), vec![4]);
    assert!(recovered.load_task_state(3, t0).is_err());
    assert_eq!(recovered.load_task_state(4, t0).unwrap(), vec![4u8; 64]);

    // Leave the bucket clean so the test is repeatable.
    recovered.purge(0).unwrap();
    assert!(recovered.list_checkpoints().unwrap().is_empty());
}
