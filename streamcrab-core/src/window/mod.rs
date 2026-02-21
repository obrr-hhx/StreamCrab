use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use anyhow::Result;

use crate::time::{EVENT_TIME_MAX, EVENT_TIME_MIN, TimerService};
use crate::types::{EventTime, StreamData, StreamElement};

mod assigners;
mod functions;
mod operator;
mod primitives;
mod triggers;

pub use assigners::*;
pub use functions::*;
pub use operator::*;
pub use primitives::*;
pub use triggers::*;

#[cfg(test)]
#[path = "tests/window_tests.rs"]
mod tests;
