use super::*;

/// Task-level checkpoint acknowledgement payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCheckpointAck {
    pub checkpoint_id: CheckpointId,
    pub task_id: TaskId,
    pub state: Vec<u8>,
}

/// Task-level checkpoint abort payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCheckpointAbort {
    pub checkpoint_id: CheckpointId,
    pub task_id: TaskId,
    pub reason: String,
}

/// Task -> coordinator checkpoint control event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskCheckpointEvent {
    Ack(TaskCheckpointAck),
    Aborted(TaskCheckpointAbort),
}
