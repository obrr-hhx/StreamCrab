use super::*;
use crate::task::{TaskId, VertexId};
use crate::types::StreamElement;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "streamcrab-{prefix}-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn test_checkpoint_barrier_with_timestamp_roundtrip() {
    let elem = StreamElement::<i32>::barrier_with_timestamp(10, 123);
    match elem {
        StreamElement::CheckpointBarrier(barrier) => {
            assert_eq!(barrier.checkpoint_id, 10);
            assert_eq!(barrier.timestamp, 123);
        }
        _ => panic!("expected barrier element"),
    }
}

#[test]
fn test_in_memory_checkpoint_storage_roundtrip() {
    let storage = InMemoryCheckpointStorage::new();
    let t0 = TaskId::new(VertexId::new(1), 0);
    let meta = CheckpointMetadata {
        checkpoint_id: 7,
        timestamp: 42,
        task_ids: vec![t0],
    };

    storage.save_checkpoint(meta.clone()).unwrap();
    storage.save_task_state(7, t0, vec![1, 2, 3]).unwrap();

    assert_eq!(storage.load_checkpoint(7).unwrap(), meta);
    assert_eq!(storage.load_task_state(7, t0).unwrap(), vec![1, 2, 3]);
    assert_eq!(storage.list_checkpoints().unwrap(), vec![7]);
}

#[test]
fn test_fs_checkpoint_storage_roundtrip() {
    let path = unique_temp_dir("checkpoint-storage");
    let storage = FsCheckpointStorage::new(&path).unwrap();

    let t0 = TaskId::new(VertexId::new(2), 1);
    let meta = CheckpointMetadata {
        checkpoint_id: 9,
        timestamp: 77,
        task_ids: vec![t0],
    };

    storage.save_checkpoint(meta.clone()).unwrap();
    storage.save_task_state(9, t0, vec![9, 8, 7]).unwrap();

    assert_eq!(storage.load_checkpoint(9).unwrap(), meta);
    assert_eq!(storage.load_task_state(9, t0).unwrap(), vec![9, 8, 7]);
    assert_eq!(storage.list_checkpoints().unwrap(), vec![9]);

    fs::remove_dir_all(&path).unwrap();
}

#[test]
fn test_in_memory_storage_purge_keeps_latest_n() {
    let storage = InMemoryCheckpointStorage::new();
    let t0 = TaskId::new(VertexId::new(1), 0);
    for id in 1..=4 {
        storage
            .save_checkpoint(CheckpointMetadata {
                checkpoint_id: id,
                timestamp: id as i64,
                task_ids: vec![t0],
            })
            .unwrap();
    }

    storage.purge(2).unwrap();
    assert_eq!(storage.list_checkpoints().unwrap(), vec![3, 4]);
}

#[test]
fn test_barrier_aligner_single_input_aligned_immediately() {
    let mut aligner = BarrierAligner::<i32>::new(1);
    let result = aligner
        .process_element(0, StreamElement::barrier_with_timestamp(1, 100))
        .unwrap();
    match result {
        BarrierAlignResult::Aligned { barrier, buffered } => {
            assert_eq!(barrier.checkpoint_id, 1);
            assert!(buffered.is_empty());
        }
        _ => panic!("expected aligned result"),
    }
}

#[test]
fn test_barrier_aligner_channel_out_of_bounds_error() {
    let mut aligner = BarrierAligner::<i32>::new(2);
    let err = aligner
        .process_element(2, StreamElement::record(1))
        .unwrap_err();
    assert!(
        err.to_string().contains("out of bounds"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_barrier_aligner_duplicate_barrier_error() {
    let mut aligner = BarrierAligner::<i32>::new(2);
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(8, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));

    let err = aligner
        .process_element(0, StreamElement::barrier_with_timestamp(8, 100))
        .unwrap_err();
    assert!(
        err.to_string().contains("duplicate barrier"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_barrier_aligner_multi_input_blocks_and_releases() {
    let mut aligner = BarrierAligner::<i32>::new(2);
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(7, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::record(10))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    match aligner
        .process_element(1, StreamElement::record(20))
        .unwrap()
    {
        BarrierAlignResult::Forward(StreamElement::Record(r)) => assert_eq!(r.value, 20),
        _ => panic!("expected forward record"),
    }

    match aligner
        .process_element(1, StreamElement::barrier_with_timestamp(7, 100))
        .unwrap()
    {
        BarrierAlignResult::Aligned { barrier, buffered } => {
            assert_eq!(barrier.checkpoint_id, 7);
            assert_eq!(buffered.len(), 1);
            assert_eq!(buffered[0].0, 0);
        }
        _ => panic!("expected aligned"),
    }

    match aligner
        .process_element(0, StreamElement::record(30))
        .unwrap()
    {
        BarrierAlignResult::Forward(StreamElement::Record(r)) => assert_eq!(r.value, 30),
        _ => panic!("expected forward after alignment"),
    }
}

#[test]
fn test_barrier_aligner_abort_on_buffer_overflow() {
    let mut aligner = BarrierAligner::<i32>::new(2).with_max_buffer_size(1);
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(9, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::record(1))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));

    match aligner
        .process_element(0, StreamElement::record(2))
        .unwrap()
    {
        BarrierAlignResult::Aborted {
            checkpoint_id,
            drained,
        } => {
            assert_eq!(checkpoint_id, 9);
            assert_eq!(drained.len(), 2);
            match &drained[0].1 {
                StreamElement::Record(r) => assert_eq!(r.value, 1),
                _ => panic!("expected record"),
            }
            match &drained[1].1 {
                StreamElement::Record(r) => assert_eq!(r.value, 2),
                _ => panic!("expected record"),
            }
        }
        _ => panic!("expected aborted"),
    }
}

#[test]
fn test_barrier_aligner_ignores_late_barrier_after_abort() {
    let mut aligner = BarrierAligner::<i32>::new(2).with_max_buffer_size(1);
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(9, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::record(1))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::record(2))
            .unwrap(),
        BarrierAlignResult::Aborted {
            checkpoint_id: 9,
            ..
        }
    ));

    // Late barrier from the aborted checkpoint must be ignored.
    assert!(matches!(
        aligner
            .process_element(1, StreamElement::barrier_with_timestamp(9, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));

    // New checkpoint can still align normally.
    assert!(matches!(
        aligner
            .process_element(1, StreamElement::barrier_with_timestamp(10, 200))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    match aligner
        .process_element(0, StreamElement::barrier_with_timestamp(10, 200))
        .unwrap()
    {
        BarrierAlignResult::Aligned { barrier, buffered } => {
            assert_eq!(barrier.checkpoint_id, 10);
            assert!(buffered.is_empty());
        }
        _ => panic!("expected aligned result"),
    }
}

#[test]
fn test_barrier_aligner_ignores_late_barrier_after_aligned() {
    let mut aligner = BarrierAligner::<i32>::new(2);
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(5, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    assert!(matches!(
        aligner
            .process_element(1, StreamElement::barrier_with_timestamp(5, 100))
            .unwrap(),
        BarrierAlignResult::Aligned {
            barrier: Barrier {
                checkpoint_id: 5,
                ..
            },
            ..
        }
    ));

    // Late barrier from completed checkpoint must be ignored.
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(5, 100))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));

    // Next checkpoint should still align.
    assert!(matches!(
        aligner
            .process_element(0, StreamElement::barrier_with_timestamp(6, 110))
            .unwrap(),
        BarrierAlignResult::Buffering
    ));
    assert!(matches!(
        aligner
            .process_element(1, StreamElement::barrier_with_timestamp(6, 110))
            .unwrap(),
        BarrierAlignResult::Aligned {
            barrier: Barrier {
                checkpoint_id: 6,
                ..
            },
            ..
        }
    ));
}

#[test]
fn test_barrier_aligner_out_of_order_barrier_error() {
    let mut aligner = BarrierAligner::<i32>::new(2);
    aligner
        .process_element(0, StreamElement::barrier_with_timestamp(10, 100))
        .unwrap();

    let err = aligner
        .process_element(1, StreamElement::barrier_with_timestamp(11, 110))
        .unwrap_err();
    assert!(
        err.to_string().contains("out-of-order barrier"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_checkpoint_coordinator_trigger_ack_finalize() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage.clone());

    let t0 = TaskId::new(VertexId::new(1), 0);
    let t1 = TaskId::new(VertexId::new(1), 1);
    let barrier = coordinator.trigger_checkpoint(1234, vec![t0, t1]).unwrap();
    assert_eq!(barrier.checkpoint_id, 1);
    assert_eq!(barrier.timestamp, 1234);

    let done = coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: 1,
            task_id: t0,
            state: vec![1, 2],
        })
        .unwrap();
    assert!(!done);

    let done = coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: 1,
            task_id: t1,
            state: vec![3, 4],
        })
        .unwrap();
    assert!(done);

    assert_eq!(coordinator.completed_checkpoint_ids().unwrap(), vec![1]);
    let meta = storage.load_checkpoint(1).unwrap();
    assert_eq!(meta.checkpoint_id, 1);
    assert_eq!(meta.timestamp, 1234);
    assert_eq!(meta.task_ids.len(), 2);
    assert_eq!(storage.load_task_state(1, t0).unwrap(), vec![1, 2]);
    assert_eq!(storage.load_task_state(1, t1).unwrap(), vec![3, 4]);
}

#[test]
fn test_checkpoint_coordinator_trigger_rejects_empty_expected_tasks() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage);
    let err = coordinator.trigger_checkpoint(123, Vec::new()).unwrap_err();
    assert!(
        err.to_string().contains("expected_tasks"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_checkpoint_coordinator_rejects_duplicate_ack() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage);
    let t0 = TaskId::new(VertexId::new(2), 0);

    coordinator.trigger_checkpoint(10, vec![t0]).unwrap();
    coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: 1,
            task_id: t0,
            state: vec![9],
        })
        .unwrap();

    let err = coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: 1,
            task_id: t0,
            state: vec![9],
        })
        .unwrap_err();
    assert!(
        err.to_string().contains("not pending") || err.to_string().contains("duplicate ack"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_checkpoint_coordinator_retention_purges_old_checkpoints() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage.clone()).with_retained_checkpoints(2);
    let t0 = TaskId::new(VertexId::new(3), 0);

    for ts in [100, 200, 300] {
        let barrier = coordinator.trigger_checkpoint(ts, vec![t0]).unwrap();
        coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: barrier.checkpoint_id,
                task_id: t0,
                state: vec![ts as u8],
            })
            .unwrap();
    }

    assert_eq!(coordinator.completed_checkpoint_ids().unwrap(), vec![2, 3]);
    assert_eq!(storage.list_checkpoints().unwrap(), vec![2, 3]);
}

#[test]
fn test_checkpoint_coordinator_abort_removes_pending_and_ignores_late_ack() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage);
    let t0 = TaskId::new(VertexId::new(5), 0);
    let t1 = TaskId::new(VertexId::new(5), 1);

    coordinator.trigger_checkpoint(50, vec![t0, t1]).unwrap();
    assert!(
        coordinator
            .abort_checkpoint(1, t0, "alignment buffer overflow")
            .unwrap()
    );
    assert_eq!(
        coordinator.completed_checkpoint_ids().unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(coordinator.aborted_checkpoint_ids().unwrap(), vec![1]);

    let pending_guard = coordinator.pending_checkpoints.lock().unwrap();
    assert!(!pending_guard.contains_key(&1));
    drop(pending_guard);

    // Acks arriving after abort should be ignored.
    let done = coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: 1,
            task_id: t1,
            state: vec![1],
        })
        .unwrap();
    assert!(!done);
}

#[test]
fn test_checkpoint_coordinator_abort_non_pending_returns_false() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage);
    let t0 = TaskId::new(VertexId::new(6), 0);
    assert!(!coordinator.abort_checkpoint(42, t0, "noop").unwrap());
}

#[test]
fn test_checkpoint_coordinator_abort_rejects_unexpected_task() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage);
    let t0 = TaskId::new(VertexId::new(7), 0);
    let t1 = TaskId::new(VertexId::new(7), 1);

    coordinator.trigger_checkpoint(88, vec![t0]).unwrap();
    let err = coordinator
        .abort_checkpoint(1, t1, "unexpected")
        .unwrap_err();
    assert!(
        err.to_string().contains("is not expected"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_checkpoint_coordinator_retained_minimum_is_one() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let coordinator = CheckpointCoordinator::new(storage.clone()).with_retained_checkpoints(0);
    let t0 = TaskId::new(VertexId::new(8), 0);

    for ts in [11, 22] {
        let barrier = coordinator.trigger_checkpoint(ts, vec![t0]).unwrap();
        coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id: barrier.checkpoint_id,
                task_id: t0,
                state: vec![ts as u8],
            })
            .unwrap();
    }

    assert_eq!(coordinator.completed_checkpoint_ids().unwrap(), vec![2]);
    assert_eq!(storage.list_checkpoints().unwrap(), vec![2]);
}
