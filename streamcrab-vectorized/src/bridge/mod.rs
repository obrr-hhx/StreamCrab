//! Bridge: run vectorized operators inside StreamCrab's own runtime (P9).
//!
//! The pieces compose into a pipeline segment over `RecordBatch` elements:
//!
//! ```text
//! [row source] → Batcher → VectorOp(Filter) → VectorOp(Project) → Unbatcher → [row sink]
//!                                    └→ BatchRepartition → (partition, sub-batch) → keyed stage
//! ```
//!
//! Watermarks and checkpoint barriers stay task-level concerns: batches flow
//! as ordinary records, so barrier alignment and the rescale protocol are
//! untouched. `Batcher` checkpoints its pending rows; stateful vectorized
//! operators delegate snapshot/restore to their own state.

mod batcher;
mod repartition;
mod vector_op;

pub use batcher::{Batcher, Unbatcher};
pub use repartition::{BatchRepartition, SubBatchPartitioner};
pub use vector_op::VectorOp;
