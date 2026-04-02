//! Vectorized operator implementations inspired by Velox's operator model.
//!
//! Operators follow Velox's hybrid push/pull pattern:
//! - `add_input()` pushes a batch into the operator
//! - `get_output()` pulls results out
//!
//! This decoupling is essential for operators like HashJoin where the build
//! side accumulates data before the probe side produces output.

pub mod project_filter;
pub use project_filter::{FilterOperator, ProjectOperator, Projection};

pub mod hash_aggregate;
pub use hash_aggregate::{AggregateDescriptor, AggregateFunction, HashAggregateOperator};

pub mod hash_join;
pub use hash_join::{HashJoinOperator, JoinType};

pub mod window_aggregate;
pub use window_aggregate::{
    WindowAggFunction, WindowAggregateDescriptor, WindowAggregateOperator, WindowType,
};

use crate::batch::VeloxBatch;
use anyhow::Result;

/// Core trait for vectorized streaming operators.
///
/// Mirrors Velox's Operator interface (addInput/getOutput/isBlocked/finished)
/// adapted to Rust ownership semantics and StreamCrab's checkpoint protocol.
///
/// All operators must be `Send` to support Flink's per-subtask threading model.
pub trait VectorizedOperator: Send {
    /// Push an input batch into this operator for processing.
    fn add_input(&mut self, batch: VeloxBatch) -> Result<()>;

    /// Pull the next output batch, if available.
    /// Returns `None` when the operator has no pending output.
    fn get_output(&mut self) -> Result<Option<VeloxBatch>>;

    /// Returns true when the operator has finished and will produce no more output.
    fn is_finished(&self) -> bool;

    /// Serialize the operator's internal state for checkpointing.
    /// Stateless operators return an empty Vec.
    fn snapshot_state(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    /// Restore the operator's internal state from a previous checkpoint.
    fn restore_state(&mut self, _data: &[u8]) -> Result<()> {
        Ok(())
    }

    /// Called when a watermark advances. The operator should fire any pending
    /// timers/windows and return the resulting output batches.
    fn on_watermark(&mut self, _watermark: i64) -> Result<Vec<VeloxBatch>> {
        Ok(Vec::new())
    }
}
