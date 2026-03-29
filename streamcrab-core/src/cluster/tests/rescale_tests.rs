use crate::cluster::{RescaleCoordinator, RescalePhase};
use crate::elastic::RescalePlan;

#[test]
fn test_rescale_coordinator_allocates_monotonic_generations() {
    let mut coordinator = RescaleCoordinator::new();
    let plan = RescalePlan {
        job_id: "job-1".to_string(),
        operator_id: 1,
        new_parallelism: 8,
    };
    let g1 = coordinator.begin_rescale(&plan).unwrap();
    coordinator.mark_prepared("job-1", g1, vec![]).unwrap();
    coordinator.mark_barrier_injected("job-1", g1, 101).unwrap();
    coordinator.mark_aligned("job-1", g1).unwrap();
    coordinator.mark_flushed("job-1", g1).unwrap();
    coordinator.mark_switched("job-1", g1).unwrap();
    coordinator.mark_activated("job-1", g1).unwrap();

    let g2 = coordinator
        .begin_rescale(&RescalePlan {
            new_parallelism: 16,
            ..plan
        })
        .unwrap();
    assert!(g2 > g1);
}

#[test]
fn test_rescale_coordinator_tracks_full_phase_transitions() {
    let mut coordinator = RescaleCoordinator::new();
    let plan = RescalePlan {
        job_id: "job-2".to_string(),
        operator_id: 3,
        new_parallelism: 4,
    };
    let generation = coordinator.begin_rescale(&plan).unwrap();
    assert_eq!(
        coordinator.session("job-2").unwrap().phase,
        RescalePhase::S1Preparing
    );

    coordinator
        .mark_prepared("job-2", generation, vec!["job-2::vertex_0_2".to_string()])
        .unwrap();
    coordinator
        .mark_barrier_injected("job-2", generation, 2001)
        .unwrap();
    assert_eq!(
        coordinator.session("job-2").unwrap().phase,
        RescalePhase::S2BarrierInjected
    );
    coordinator.mark_aligned("job-2", generation).unwrap();
    coordinator.mark_flushed("job-2", generation).unwrap();
    coordinator.mark_switched("job-2", generation).unwrap();
    coordinator.mark_activated("job-2", generation).unwrap();

    let session = coordinator.session("job-2").unwrap();
    assert_eq!(session.phase, RescalePhase::S6Activated);
    assert_eq!(session.checkpoint_id, Some(2001));
    assert_eq!(coordinator.latest_generation("job-2"), Some(generation));
}

#[test]
fn test_rescale_coordinator_rejects_invalid_transition_order() {
    let mut coordinator = RescaleCoordinator::new();
    let generation = coordinator
        .begin_rescale(&RescalePlan {
            job_id: "job-3".to_string(),
            operator_id: 2,
            new_parallelism: 2,
        })
        .unwrap();
    let err = coordinator
        .mark_switched("job-3", generation)
        .expect_err("switch before flush must fail");
    assert!(err.to_string().contains("cannot switch"));
}

#[test]
fn test_rescale_coordinator_failure_marks_session() {
    let mut coordinator = RescaleCoordinator::new();
    let generation = coordinator
        .begin_rescale(&RescalePlan {
            job_id: "job-4".to_string(),
            operator_id: 9,
            new_parallelism: 5,
        })
        .unwrap();
    coordinator
        .mark_failed("job-4", generation, "inject barrier failed")
        .unwrap();
    let session = coordinator.session("job-4").unwrap();
    assert_eq!(session.phase, RescalePhase::Failed);
    assert!(
        session
            .failure_reason
            .unwrap_or_default()
            .contains("inject barrier failed")
    );
}

#[test]
fn test_rescale_coordinator_rejects_zero_parallelism() {
    let mut coordinator = RescaleCoordinator::new();
    let err = coordinator
        .begin_rescale(&RescalePlan {
            job_id: "job-1".to_string(),
            operator_id: 1,
            new_parallelism: 0,
        })
        .unwrap_err();
    assert!(err.to_string().contains("new_parallelism"));
}
