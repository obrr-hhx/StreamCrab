//! Distributed-runtime RPC surface for P4.
//!
//! The Rust types in this module are generated from `proto/streamcrab.proto`.

pub mod rpc {
    tonic::include_proto!("streamcrab");
}

mod job_manager;
mod operator_factory;
mod recovery;
mod scheduler;
mod task_manager;
mod types;

pub use job_manager::*;
pub use operator_factory::*;
pub use recovery::*;
pub use scheduler::*;
pub use task_manager::*;
pub use types::*;

#[cfg(test)]
#[path = "tests/cluster_tests.rs"]
mod tests;
