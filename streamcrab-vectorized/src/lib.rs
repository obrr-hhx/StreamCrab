//! Velox-inspired vectorized execution engine for StreamCrab.
//!
//! Provides Arrow-columnar batch processing with stateful streaming operators,
//! designed to bridge to Apache Flink via JNI.

pub mod batch;
pub mod expression;
pub mod hash_table;
pub mod memory;
pub mod operators;
pub mod spill;

pub use batch::VeloxBatch;
