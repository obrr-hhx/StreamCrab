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
//! - [`runtime`] — Execution utilities: topological sort, downstream routing.
//! - [`state`] — State backends: [`LocalStateBackend`](state::LocalStateBackend).

pub mod graph;
pub mod runtime;
pub mod state;
pub mod types;
