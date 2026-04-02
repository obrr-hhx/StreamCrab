//! Cluster acceptance tests for StreamCrab P7.
//!
//! Tests 3–5 verify the checkpoint/recovery and rescale protocols at the
//! component level without requiring a real distributed cluster.

use std::sync::Arc;

use streamcrab_core::checkpoint::{
    CheckpointCoordinator, CheckpointStorage, InMemoryCheckpointStorage, TaskCheckpointAck,
};
use streamcrab_core::cluster::{RescaleCoordinator, RescalePhase};
use streamcrab_core::elastic::RescalePlan;
use streamcrab_core::task::{TaskId, VertexId};

// ---------------------------------------------------------------------------
// Test 3: Crash & Recovery
// ---------------------------------------------------------------------------

/// Verifies the checkpoint/recovery protocol at the coordinator level.
///
/// Scenario:
///   1. Create a coordinator with two tasks.
///   2. Trigger a checkpoint and have both tasks acknowledge with state.
///   3. Verify the checkpoint completes and state is persisted in storage.
///   4. Simulate a "crash" by dropping the coordinator.
///   5. Create a new coordinator backed by the same storage.
///   6. Load the latest checkpoint and restore task state.
///   7. Verify restored state matches the pre-crash state.
#[test]
fn test_crash_and_recovery() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());

    let task0 = TaskId::new(VertexId::new(1), 0);
    let task1 = TaskId::new(VertexId::new(1), 1);

    let task0_state = vec![0x01, 0x02, 0x03];
    let task1_state = vec![0x04, 0x05, 0x06];

    // Phase 1: trigger checkpoint and collect acks from both tasks.
    let completed_checkpoint_id = {
        let coordinator = CheckpointCoordinator::new(Arc::clone(&storage));
        let barrier = coordinator
            .trigger_checkpoint(1000, vec![task0, task1])
            .expect("trigger_checkpoint should succeed");

        let checkpoint_id = barrier.checkpoint_id;

        let still_pending = coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id,
                task_id: task0,
                state: task0_state.clone(),
            })
            .expect("first ack should succeed");
        assert!(!still_pending, "checkpoint should still be pending after first ack");

        let completed = coordinator
            .acknowledge_checkpoint(TaskCheckpointAck {
                checkpoint_id,
                task_id: task1,
                state: task1_state.clone(),
            })
            .expect("second ack should succeed");
        assert!(completed, "checkpoint should complete after all tasks ack");

        let ids = coordinator
            .completed_checkpoint_ids()
            .expect("completed_checkpoint_ids should succeed");
        assert_eq!(ids, vec![checkpoint_id], "checkpoint should be listed as completed");

        checkpoint_id
    };
    // coordinator dropped here — simulates crash.

    // Phase 2: recover from storage using a fresh coordinator.
    let recovered_coordinator = CheckpointCoordinator::new(Arc::clone(&storage));

    // The new coordinator's in-memory list is empty (it was dropped), but
    // state is durably stored in the shared storage.
    let stored_ids = storage
        .list_checkpoints()
        .expect("list_checkpoints should succeed");
    assert!(
        stored_ids.contains(&completed_checkpoint_id),
        "checkpoint should be present in storage after crash"
    );

    let metadata = storage
        .load_checkpoint(completed_checkpoint_id)
        .expect("load_checkpoint should succeed");
    assert_eq!(metadata.checkpoint_id, completed_checkpoint_id);
    assert_eq!(metadata.timestamp, 1000);

    let recovered_state0 = storage
        .load_task_state(completed_checkpoint_id, task0)
        .expect("load_task_state for task0 should succeed");
    let recovered_state1 = storage
        .load_task_state(completed_checkpoint_id, task1)
        .expect("load_task_state for task1 should succeed");

    assert_eq!(recovered_state0, task0_state, "task0 state must match pre-crash state");
    assert_eq!(recovered_state1, task1_state, "task1 state must match pre-crash state");

    // The recovered coordinator can trigger and complete new checkpoints.
    let new_barrier = recovered_coordinator
        .trigger_checkpoint(2000, vec![task0, task1])
        .expect("post-recovery trigger should succeed");
    let new_cp_id = new_barrier.checkpoint_id;
    recovered_coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: new_cp_id,
            task_id: task0,
            state: vec![0xAA],
        })
        .expect("post-recovery ack task0 should succeed");
    let done = recovered_coordinator
        .acknowledge_checkpoint(TaskCheckpointAck {
            checkpoint_id: new_cp_id,
            task_id: task1,
            state: vec![0xBB],
        })
        .expect("post-recovery ack task1 should succeed");
    assert!(done, "post-recovery checkpoint should complete successfully");
}

// ---------------------------------------------------------------------------
// Test 4: Rescale
// ---------------------------------------------------------------------------

/// Verifies the full rescale phase-transition protocol (parallelism 2 → 4).
///
/// Walks through every phase in order and asserts the session state at each
/// step, then confirms the final activated state and monotonic generation
/// assignment.
#[test]
fn test_rescale_full_protocol() {
    let mut coordinator = RescaleCoordinator::new();
    let plan = RescalePlan {
        job_id: "acceptance-job".to_string(),
        operator_id: 42,
        new_parallelism: 4,
    };

    // Phase S1: Preparing — begin_rescale allocates a generation.
    let generation = coordinator
        .begin_rescale(&plan)
        .expect("begin_rescale should succeed");
    assert_eq!(
        coordinator.session("acceptance-job").unwrap().phase,
        RescalePhase::S1Preparing,
        "should start in S1Preparing"
    );

    // Phase S2: Prepared — new task vertices are ready.
    let new_task_ids = vec![
        "acceptance-job::vertex_42_2".to_string(),
        "acceptance-job::vertex_42_3".to_string(),
    ];
    coordinator
        .mark_prepared("acceptance-job", generation, new_task_ids)
        .expect("mark_prepared should succeed");

    // Phase S2: BarrierInjected — rescale barrier has been sent into the pipeline.
    let rescale_checkpoint_id: u64 = 9001;
    coordinator
        .mark_barrier_injected("acceptance-job", generation, rescale_checkpoint_id)
        .expect("mark_barrier_injected should succeed");
    assert_eq!(
        coordinator.session("acceptance-job").unwrap().phase,
        RescalePhase::S2BarrierInjected,
        "should be in S2BarrierInjected after barrier injection"
    );
    assert_eq!(
        coordinator.session("acceptance-job").unwrap().checkpoint_id,
        Some(rescale_checkpoint_id),
        "barrier checkpoint id should be recorded"
    );

    // Phase S3: Aligned — all upstream tasks aligned on the barrier.
    coordinator
        .mark_aligned("acceptance-job", generation)
        .expect("mark_aligned should succeed");

    // Phase S4: Flushed — old tasks flushed their write buffers.
    coordinator
        .mark_flushed("acceptance-job", generation)
        .expect("mark_flushed should succeed");

    // Phase S5: Switched — router atomically switched to new_parallelism.
    coordinator
        .mark_switched("acceptance-job", generation)
        .expect("mark_switched should succeed");

    // Phase S6: Activated — new tasks are consuming data.
    coordinator
        .mark_activated("acceptance-job", generation)
        .expect("mark_activated should succeed");

    let session = coordinator
        .session("acceptance-job")
        .expect("session must exist");
    assert_eq!(session.phase, RescalePhase::S6Activated, "final phase must be S6Activated");
    assert_eq!(
        session.checkpoint_id,
        Some(rescale_checkpoint_id),
        "checkpoint id should be preserved through all phases"
    );
    assert_eq!(
        coordinator.latest_generation("acceptance-job"),
        Some(generation),
        "latest_generation should match"
    );
}

/// Verifies that a second rescale receives a strictly greater generation number.
#[test]
fn test_rescale_generation_monotonically_increasing() {
    let mut coordinator = RescaleCoordinator::new();
    let base_plan = RescalePlan {
        job_id: "gen-job".to_string(),
        operator_id: 1,
        new_parallelism: 4,
    };

    let g1 = coordinator.begin_rescale(&base_plan).unwrap();
    coordinator.mark_prepared("gen-job", g1, vec![]).unwrap();
    coordinator.mark_barrier_injected("gen-job", g1, 100).unwrap();
    coordinator.mark_aligned("gen-job", g1).unwrap();
    coordinator.mark_flushed("gen-job", g1).unwrap();
    coordinator.mark_switched("gen-job", g1).unwrap();
    coordinator.mark_activated("gen-job", g1).unwrap();

    let g2 = coordinator
        .begin_rescale(&RescalePlan { new_parallelism: 8, ..base_plan })
        .unwrap();
    assert!(g2 > g1, "second generation must be strictly greater than first");
}

/// Verifies that attempting an invalid phase transition is rejected.
#[test]
fn test_rescale_rejects_out_of_order_transition() {
    let mut coordinator = RescaleCoordinator::new();
    let generation = coordinator
        .begin_rescale(&RescalePlan {
            job_id: "ooo-job".to_string(),
            operator_id: 7,
            new_parallelism: 2,
        })
        .unwrap();

    // Attempt to switch before flush — must fail.
    let err = coordinator
        .mark_switched("ooo-job", generation)
        .expect_err("switch before flush must be rejected");
    assert!(
        err.to_string().contains("cannot switch"),
        "error should mention 'cannot switch', got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Test 5: State Loss & Recovery from Storage
// ---------------------------------------------------------------------------

/// Verifies that a coordinator can be fully reconstructed from durable storage
/// after all in-memory state is lost (simulating a complete process restart).
///
/// Scenario:
///   1. Create a coordinator and complete three checkpoints.
///   2. Drop the coordinator (all in-memory state lost).
///   3. Create a brand-new coordinator backed by the same storage.
///   4. Load the latest checkpoint from storage.
///   5. Verify state for every task matches what was originally saved.
#[test]
fn test_state_loss_and_recovery_from_storage() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());

    let task0 = TaskId::new(VertexId::new(10), 0);
    let task1 = TaskId::new(VertexId::new(10), 1);

    // Save three checkpoints to simulate a running pipeline.
    let mut last_checkpoint_id = 0u64;
    {
        let coordinator = CheckpointCoordinator::new(Arc::clone(&storage));

        for round in 1u64..=3 {
            let timestamp = round * 1000;
            let barrier = coordinator
                .trigger_checkpoint(timestamp as i64, vec![task0, task1])
                .expect("trigger_checkpoint should succeed");
            let cp_id = barrier.checkpoint_id;

            coordinator
                .acknowledge_checkpoint(TaskCheckpointAck {
                    checkpoint_id: cp_id,
                    task_id: task0,
                    state: format!("task0-round-{round}").into_bytes(),
                })
                .expect("ack task0 should succeed");

            let done = coordinator
                .acknowledge_checkpoint(TaskCheckpointAck {
                    checkpoint_id: cp_id,
                    task_id: task1,
                    state: format!("task1-round-{round}").into_bytes(),
                })
                .expect("ack task1 should succeed");
            assert!(done, "checkpoint {cp_id} should complete");

            last_checkpoint_id = cp_id;
        }
    }
    // All in-memory coordinator state is now gone.

    // Recover: discover latest checkpoint from storage.
    let _recovered_coordinator = CheckpointCoordinator::new(Arc::clone(&storage));

    let mut stored_ids = storage
        .list_checkpoints()
        .expect("list_checkpoints should succeed");
    assert!(!stored_ids.is_empty(), "storage must contain at least one checkpoint");

    stored_ids.sort_unstable();
    let latest_id = *stored_ids.last().unwrap();
    assert_eq!(
        latest_id, last_checkpoint_id,
        "latest checkpoint in storage must match the last completed checkpoint"
    );

    // Load metadata and verify it describes both tasks.
    let metadata = storage
        .load_checkpoint(latest_id)
        .expect("load_checkpoint should succeed");
    assert_eq!(metadata.checkpoint_id, latest_id);
    assert_eq!(metadata.task_ids.len(), 2, "metadata should reference both tasks");

    // Restore state for each task from the latest checkpoint.
    let state0 = storage
        .load_task_state(latest_id, task0)
        .expect("load_task_state for task0 should succeed");
    let state1 = storage
        .load_task_state(latest_id, task1)
        .expect("load_task_state for task1 should succeed");

    assert_eq!(
        state0,
        b"task0-round-3",
        "recovered task0 state must match round 3"
    );
    assert_eq!(
        state1,
        b"task1-round-3",
        "recovered task1 state must match round 3"
    );
}

/// Verifies that checkpoint storage correctly purges old checkpoints and that
/// recovery always targets the newest retained entry.
#[test]
fn test_state_loss_recovery_with_purge() {
    let storage = Arc::new(InMemoryCheckpointStorage::new());
    let task0 = TaskId::new(VertexId::new(20), 0);

    // Coordinator retains only 2 checkpoints.
    {
        let coordinator =
            CheckpointCoordinator::new(Arc::clone(&storage)).with_retained_checkpoints(2);

        for round in 1u64..=4 {
            let barrier = coordinator
                .trigger_checkpoint(round as i64 * 500, vec![task0])
                .expect("trigger should succeed");
            let cp_id = barrier.checkpoint_id;
            coordinator
                .acknowledge_checkpoint(TaskCheckpointAck {
                    checkpoint_id: cp_id,
                    task_id: task0,
                    state: vec![round as u8],
                })
                .expect("ack should succeed");
        }
    }

    // After purge, only the 2 most recent checkpoints remain in storage.
    let ids = storage.list_checkpoints().expect("list_checkpoints should succeed");
    assert_eq!(ids.len(), 2, "storage should retain exactly 2 checkpoints after purge");

    // The latest must hold the state from round 4.
    let latest_id = *ids.iter().max().unwrap();
    let recovered_state = storage
        .load_task_state(latest_id, task0)
        .expect("load_task_state should succeed");
    assert_eq!(recovered_state, vec![4u8], "recovered state must be from the latest round");
}
