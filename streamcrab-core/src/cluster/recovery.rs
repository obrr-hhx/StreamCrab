use std::collections::{HashMap, HashSet};

use super::{JobId, TaskLocation, TmId};

/// Recovery helper result for a failed job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryDecision {
    pub job_id: JobId,
    pub latest_checkpoint_id: Option<u64>,
}

pub fn collect_affected_jobs(
    job_locations: &HashMap<JobId, Vec<TaskLocation>>,
    removed_tm_ids: &HashSet<TmId>,
) -> Vec<JobId> {
    job_locations
        .iter()
        .filter_map(|(job_id, locations)| {
            if locations
                .iter()
                .any(|loc| removed_tm_ids.contains(&loc.tm_id))
            {
                Some(job_id.clone())
            } else {
                None
            }
        })
        .collect()
}

pub fn latest_completed_checkpoint_for_job(
    completed_ids: &[u64],
    checkpoint_to_job: &HashMap<u64, JobId>,
    job_id: &str,
) -> Option<u64> {
    completed_ids
        .iter()
        .copied()
        .filter(|id| {
            checkpoint_to_job
                .get(id)
                .map(|j| j == job_id)
                .unwrap_or(false)
        })
        .max()
}
