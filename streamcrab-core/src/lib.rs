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
//! - [`state`] — State backends: [`HashMapStateBackend`](state::HashMapStateBackend).
//! - [`process`] — Keyed process functions: [`KeyedProcessFunction`](process::KeyedProcessFunction).

pub mod channel;
pub mod checkpoint;
pub mod graph;
pub mod input_gate;
pub mod job_graph;
pub mod operator_chain;
pub mod output_gate;
pub mod partitioner;
pub mod process;
pub mod runtime;
pub mod state;
pub mod task;
pub mod time;
pub mod types;
pub mod window;
