//! Checkpoint primitives and storage implementations for P3.

use crate::input_gate::ChannelIndex;
use crate::task::TaskId;
use crate::types::{Barrier, CheckpointId, EventTime, StreamElement};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod aligner;
mod coordinator;
mod events;
mod metadata;
mod storage;

pub use aligner::*;
pub use coordinator::*;
pub use events::*;
pub use metadata::*;
pub use storage::*;

#[cfg(test)]
#[path = "tests/checkpoint_tests.rs"]
mod tests;
