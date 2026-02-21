use super::*;

/// Persisted checkpoint metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointMetadata {
    pub checkpoint_id: CheckpointId,
    pub timestamp: EventTime,
    pub task_ids: Vec<TaskId>,
}
