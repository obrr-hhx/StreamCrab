use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Serializable partition strategy for distributed job plans.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionDescriptor {
    Forward,
    Hash,
    Broadcast,
}

/// Serializable edge for distributed deployment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EdgePlan {
    pub source_node_id: u32,
    pub target_node_id: u32,
    pub partition: PartitionDescriptor,
}

/// Serializable operator descriptor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OperatorDescriptor {
    Source {
        source_id: String,
    },
    Map {
        udf_id: String,
        config: Vec<u8>,
    },
    Filter {
        udf_id: String,
        config: Vec<u8>,
    },
    FlatMap {
        udf_id: String,
        config: Vec<u8>,
    },
    KeyBy {
        key_selector_id: String,
    },
    Window {
        assigner: String,
        trigger: String,
        function_id: String,
    },
    Reduce {
        udf_id: String,
    },
    Sink {
        sink_id: String,
    },
}

/// Serializable deployment plan consumed by TaskManagers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobPlan {
    pub job_name: String,
    pub parallelism: u32,
    pub operators: Vec<OperatorDescriptor>,
    pub edges: Vec<EdgePlan>,
}

impl JobPlan {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

#[cfg(test)]
#[path = "tests/descriptors_tests.rs"]
mod tests;
