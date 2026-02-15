//! # StreamCrab API
//!
//! User-facing DataStream API for building stream processing pipelines.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use streamcrab_api::environment::StreamExecutionEnvironment;
//!
//! let env = StreamExecutionEnvironment::new("wordcount");
//! env.from_iter(vec!["hello world".to_string(), "hello streamcrab".to_string()])
//!     .flat_map(|line: String| line.split_whitespace().map(String::from).collect::<Vec<_>>())
//!     .map(|word: String| (word, 1i32))
//!     .key_by::<String, _>(|(w, _)| w.clone())
//!     .reduce(|(w, c1), (_, c2)| (w, c1 + c2))
//!     .print();
//! env.execute().unwrap();
//! ```
//!
//! - [`environment`] — [`StreamExecutionEnvironment`](environment::StreamExecutionEnvironment):
//!   entry point for creating sources and executing pipelines.
//! - [`datastream`] — [`DataStream`](datastream::DataStream) and
//!   [`KeyedStream`](datastream::KeyedStream): fluent builder API for transformations.

pub mod datastream;
pub mod environment;

pub use streamcrab_core;
