//! # StreamCrab Core
//!
//! Core runtime components for the StreamCrab stream processing engine.
//!
//! This crate provides the foundational types and infrastructure:
//!
//! - [`types`] — Core data types: [`StreamElement`](types::StreamElement),
//!   [`StreamRecord`](types::StreamRecord), [`Watermark`](types::Watermark),
//!   [`Barrier`](types::Barrier), and the [`StreamData`](types::StreamData) trait bound.
//! - [`graph`] — Logical + physical plans:
//!   [`StreamGraph`](graph::StreamGraph), [`JobGraph`](graph::JobGraph).
//! - [`runtime`] — Task loop, operator chain, process/reduce operators, partitioners.
//! - [`network`] — Local channels and gate abstractions.
//! - [`state`] — State backends: [`HashMapStateBackend`](state::HashMapStateBackend).
//! - [`checkpoint`] — Barrier aligner, coordinator, and checkpoint storage.
//! - [`time`] / [`window`] — Event-time/watermark and windowing semantics.

pub mod checkpoint;
pub mod cluster;
pub mod graph;
pub mod network;
pub mod runtime;
pub mod state;
pub mod time;
pub mod types;
pub mod window;

/// Backward-compatible alias: `streamcrab_core::channel::*`.
pub mod channel {
    pub use crate::network::channel::*;
}

/// Backward-compatible alias: `streamcrab_core::input_gate::*`.
pub mod input_gate {
    pub use crate::network::input_gate::*;
}

/// Backward-compatible alias: `streamcrab_core::output_gate::*`.
pub mod output_gate {
    pub use crate::network::output_gate::*;
}

/// Backward-compatible alias: `streamcrab_core::job_graph::*`.
pub mod job_graph {
    pub use crate::graph::job_graph::*;
}

/// Backward-compatible alias: `streamcrab_core::operator_chain::*`.
pub mod operator_chain {
    pub use crate::runtime::operator_chain::*;
}

/// Backward-compatible alias: `streamcrab_core::partitioner::*`.
pub mod partitioner {
    pub use crate::runtime::partitioner::*;
}

/// Backward-compatible alias: `streamcrab_core::process::*`.
pub mod process {
    pub use crate::runtime::process::*;
}

/// Backward-compatible alias: `streamcrab_core::task::*`.
pub mod task {
    pub use crate::runtime::task::*;
}
