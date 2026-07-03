//! Apache Paimon source connector for StreamCrab.
//!
//! Reads Paimon tables (bounded snapshot scan) into Arrow `RecordBatch`es,
//! feeding StreamCrab's vectorized pipeline with zero row conversion.

pub mod source;

pub use source::PaimonSource;
