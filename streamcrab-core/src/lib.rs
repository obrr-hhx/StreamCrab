//! # StreamCrab Core
//!
//! Core runtime components for the StreamCrab stream processing engine.
//!
//! This crate provides the foundational types and infrastructure:
//!
//! - [`types`] — Core data types: [`StreamElement`](types::StreamElement),
//!   [`StreamRecord`](types::StreamRecord), [`Watermark`](types::Watermark),
//!   [`Barrier`](types::Barrier), and the [`StreamData`](types::StreamData) trait bound.
//! - [`graph`] — Logical DAG representation: [`StreamGraph`](graph::StreamGraph),
//!   [`StreamNode`](graph::StreamNode), [`StreamEdge`](graph::StreamEdge).
//! - [`job_graph`] — Physical execution plan: [`JobGraph`](job_graph::JobGraph),
//!   [`JobVertex`](job_graph::JobVertex) with operator chaining.
//! - [`partitioner`] — Data partitioning: [`HashPartitioner`](partitioner::HashPartitioner),
//!   [`RoundRobinPartitioner`](partitioner::RoundRobinPartitioner).
//! - [`runtime`] — Execution utilities: topological sort, downstream routing.
//! - [`state`] — State backends: [`LocalStateBackend`](state::LocalStateBackend).

pub mod channel;
pub mod graph;
pub mod input_gate;
pub mod job_graph;
pub mod operator_chain;
pub mod output_gate;
pub mod partitioner;
pub mod runtime;
pub mod state;
pub mod task;
pub mod types;
