use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cluster::rpc;

pub type TmId = String;
pub type JobId = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLocation {
    pub task_id: String,
    pub tm_id: TmId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resources {
    pub cpu_cores: u32,
    pub memory_mb: u64,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            memory_mb: 512,
        }
    }
}

impl From<Option<rpc::Resources>> for Resources {
    fn from(value: Option<rpc::Resources>) -> Self {
        if let Some(r) = value {
            Self {
                cpu_cores: r.cpu_cores,
                memory_mb: r.memory_mb,
            }
        } else {
            Self::default()
        }
    }
}

impl From<Resources> for rpc::Resources {
    fn from(value: Resources) -> Self {
        Self {
            cpu_cores: value.cpu_cores,
            memory_mb: value.memory_mb,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClusterConfig {
    pub heartbeat: HeartbeatConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Unspecified,
    Created,
    Running,
    Failed,
    Finished,
    Cancelling,
    Cancelled,
}

impl From<JobStatus> for i32 {
    fn from(value: JobStatus) -> Self {
        match value {
            JobStatus::Unspecified => 0,
            JobStatus::Created => 1,
            JobStatus::Running => 2,
            JobStatus::Failed => 3,
            JobStatus::Finished => 4,
            JobStatus::Cancelling => 5,
            JobStatus::Cancelled => 6,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskManagerInfo {
    pub tm_id: TmId,
    pub address: String,
    pub num_slots: usize,
    pub resources: Resources,
    pub last_heartbeat: Instant,
}

impl TaskManagerInfo {
    pub fn new(tm_id: TmId, address: String, num_slots: usize, resources: Resources) -> Self {
        Self {
            tm_id,
            address,
            num_slots,
            resources,
            last_heartbeat: Instant::now(),
        }
    }

    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    pub fn is_alive(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() <= timeout
    }

    pub fn to_summary(&self) -> rpc::TaskManagerSummary {
        let now_ms = current_unix_millis();
        let elapsed_ms = self.last_heartbeat.elapsed().as_millis() as i64;
        let last_heartbeat_unix_ms = now_ms.saturating_sub(elapsed_ms);
        rpc::TaskManagerSummary {
            tm_id: self.tm_id.clone(),
            address: self.address.clone(),
            num_slots: self.num_slots as u32,
            last_heartbeat_unix_ms,
        }
    }
}

pub(crate) fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
