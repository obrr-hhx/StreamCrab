//! # StreamCrab WASM Integration
//!
//! WASM sandbox for safe, isolated UDF execution via wasmtime.
//!
//! ## Design Principles
//!
//! - **Logic, not Runtime**: WASM executes pure computation; Host manages state/IO.
//! - **I_WASM_1 (No IO)**: Guest cannot do network/file IO.
//! - **I_WASM_2 (Stateless)**: WASM Instance is disposable; state lives in Host.
//! - **I_WASM_3 (Determinism)**: Same input + state = same output.
//!
//! ## ABI
//!
//! Host and Guest communicate via linear memory using bincode-serialized bytes.
//! Guest exports: `alloc`, `dealloc`, `process`.

pub mod abi;
pub mod operator;
pub mod runtime;

pub use abi::{WasmInput, WasmOutput, WasmRecord};
pub use operator::WasmOperator;
pub use runtime::{WasmRuntime, WasmRuntimeConfig};
