//! Vectorized expression evaluation framework.
//!
//! Provides a simple expression AST that is evaluated over Arrow RecordBatches.
//! Intentionally minimal covering arithmetic, comparison, logical, and null-check
//! operators. DataFusion integration is deferred to v2.

pub mod eval;
pub use eval::*;
