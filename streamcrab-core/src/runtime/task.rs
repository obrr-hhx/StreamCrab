//! Task execution model for parallel stream processing.
//!
//! Implements Flink-style runtime:
//! - Single-threaded Task (event loop per Task instance)
//! - Multi-instance parallelism (one Task per thread)
//! - Each Task owns its StateBackend (no sharing)
//!
//! # Task Execution Loop
//!
//! The Task is the fundamental unit of execution in StreamCrab.
//! Each Task runs in its own thread and executes a chain of operators.
//!
//! ```text
//! loop {
//!     element = input_gate.next()
//!     match element {
//!         Record => operator_chain.process() -> output_gate.emit()
//!         Watermark => operator_chain.on_timer(EventTime) -> output_gate.broadcast()
//!         Barrier => snapshot_state() -> ack_checkpoint() -> output_gate.broadcast()
//!         End => cleanup() -> break
//!     }
//! }
//! ```

use crate::checkpoint::{
    BarrierAlignResult, BarrierAligner, TaskCheckpointAbort, TaskCheckpointAck, TaskCheckpointEvent,
};
use crate::input_gate::InputGate;
use crate::operator_chain::{Operator, TimerDomain};
use crate::output_gate::OutputGate;
use crate::time::WatermarkTracker;
use crate::types::{Barrier, EventTime, NodeId, StreamElement, Watermark};
use anyhow::Result;
use crossbeam_channel::Sender;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Unique identifier for a Task instance.
///
/// Format: `{vertex_id}_{subtask_index}`
/// Example: `vertex_1_0`, `vertex_1_1`, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub vertex_id: VertexId,
    pub subtask_index: usize,
}

impl TaskId {
    pub fn new(vertex_id: VertexId, subtask_index: usize) -> Self {
        Self {
            vertex_id,
            subtask_index,
        }
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "vertex_{}_{}", self.vertex_id.0, self.subtask_index)
    }
}

/// Unique identifier for a JobVertex (after chaining).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VertexId(pub u32);

impl VertexId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

/// Unique identifier for a channel between Tasks.
///
/// Format: `{source_task}_{target_task}`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChannelId {
    pub source: TaskId,
    pub target: TaskId,
}

impl ChannelId {
    pub fn new(source: TaskId, target: TaskId) -> Self {
        Self { source, target }
    }
}

/// Deployment descriptor for a Task instance.
///
/// Contains all information needed to spawn and run a Task:
/// - Task identity (via task_id: vertex_id + subtask_index)
/// - Total parallelism (for routing decisions)
/// - Operator chain to execute
/// - Input/output channel configuration (TODO: P1 Turn 3-4)
#[derive(Debug, Clone)]
pub struct TaskDeploymentDescriptor {
    /// Unique task identifier (contains vertex_id + subtask_index)
    pub task_id: TaskId,

    /// Total parallelism of this vertex
    ///
    /// Used for:
    /// - Hash partitioning: `target_subtask = hash(key) % parallelism`
    /// - Validation: `subtask_index < parallelism`
    pub parallelism: usize,

    /// Chained operators to execute (from JobGraph)
    ///
    /// Example: [Source, Map, Filter] if chained together
    pub chained_operators: Vec<NodeId>,
}

impl TaskDeploymentDescriptor {
    /// Create a new task deployment descriptor.
    ///
    /// # Arguments
    /// - `vertex_id`: JobVertex this task belongs to
    /// - `subtask_index`: Index of this task instance (0 to parallelism-1)
    /// - `parallelism`: Total number of parallel instances
    /// - `chained_operators`: Operators to execute in this task
    pub fn new(
        vertex_id: VertexId,
        subtask_index: usize,
        parallelism: usize,
        chained_operators: Vec<NodeId>,
    ) -> Self {
        Self {
            task_id: TaskId::new(vertex_id, subtask_index),
            parallelism,
            chained_operators,
        }
    }
}

// ============================================================================
// Task: The Execution Unit
// ============================================================================

/// Task is the fundamental execution unit in StreamCrab.
///
/// Each Task:
/// - Runs in a single thread (no internal concurrency)
/// - Executes a chain of operators (from JobGraph)
/// - Reads from InputGate (multiple upstream channels)
/// - Writes to OutputGate (multiple downstream channels)
/// - Owns its StateBackend (no sharing between tasks)
///
/// # Performance Characteristics
///
/// - **Single-threaded**: No locks, no contention, cache-friendly
/// - **Operator chaining**: Zero serialization between chained operators
/// - **Batch processing**: Amortize function call overhead
/// - **Push-based**: Zero intermediate allocations
///
/// # Event Loop
///
/// ```text
/// loop {
///     element = input_gate.next()  // Fair selection from all inputs
///     match element {
///         Record(rec) => {
///             outputs = operator_chain.process_batch([rec])
///             for output in outputs {
///                 output_gate.emit(output)
///             }
///         }
///         Watermark(wm) => {
///             operator_chain.on_timer(wm.timestamp, EventTime)
///             output_gate.broadcast(Watermark(wm))
///         }
///         CheckpointBarrier(barrier) => {
///             snapshot = operator_chain.snapshot_state()
///             ack_checkpoint(barrier.id, snapshot)
///             output_gate.broadcast(CheckpointBarrier(barrier))
///         }
///         End => break
///     }
/// }
/// ```
pub struct Task<Op> {
    /// Task identity and configuration
    pub descriptor: TaskDeploymentDescriptor,

    /// Input gate for reading from upstream tasks
    pub input_gate: InputGate<Vec<u8>>,

    /// Output gate for writing to downstream tasks
    pub output_gate: OutputGate<Vec<u8>>,

    /// Operator chain to execute
    pub operator_chain: Op,

    /// Multi-input watermark alignment tracker.
    ///
    /// Enabled by default when the task has more than one input channel.
    pub watermark_tracker: Option<WatermarkTracker>,

    /// Optional processing-time tick interval.
    pub processing_time_tick_interval: Option<Duration>,

    /// Optional checkpoint event sender.
    pub checkpoint_event_sender: Option<Sender<TaskCheckpointEvent>>,

    /// Optional barrier aligner used for checkpoint barriers.
    pub barrier_aligner: Option<BarrierAligner<Vec<u8>>>,
}

impl<Op> Task<Op>
where
    Op: Operator<Vec<u8>, OUT = Vec<u8>>,
{
    /// Create a new task instance.
    pub fn new(
        descriptor: TaskDeploymentDescriptor,
        input_gate: InputGate<Vec<u8>>,
        output_gate: OutputGate<Vec<u8>>,
        operator_chain: Op,
    ) -> Self {
        let watermark_tracker = if input_gate.num_channels() > 1 {
            Some(WatermarkTracker::new(input_gate.num_channels()))
        } else {
            None
        };
        Self {
            descriptor,
            input_gate,
            output_gate,
            operator_chain,
            watermark_tracker,
            processing_time_tick_interval: None,
            checkpoint_event_sender: None,
            barrier_aligner: None,
        }
    }

    /// Enable processing-time ticks for this task.
    pub fn with_processing_time_tick(mut self, interval: Duration) -> Self {
        self.processing_time_tick_interval = Some(interval);
        self
    }

    /// Enable watermark idle-timeout detection for multi-input alignment.
    ///
    /// For single-input tasks this is a no-op.
    pub fn with_watermark_idle_timeout(mut self, timeout: Duration) -> Self {
        if self.input_gate.num_channels() > 1 {
            self.watermark_tracker = Some(WatermarkTracker::new_with_idle_timeout(
                self.input_gate.num_channels(),
                timeout,
            ));
        }
        self
    }

    /// Enable checkpoint handling for this task.
    ///
    /// - `ack_sender`: checkpoint ack output channel (Task -> Coordinator)
    /// - `max_align_buffer_size`: max buffered elements during barrier alignment
    pub fn with_checkpointing(
        mut self,
        checkpoint_event_sender: Sender<TaskCheckpointEvent>,
        max_align_buffer_size: usize,
    ) -> Self {
        self.checkpoint_event_sender = Some(checkpoint_event_sender);
        self.barrier_aligner = Some(
            BarrierAligner::new(self.input_gate.num_channels())
                .with_max_buffer_size(max_align_buffer_size),
        );
        self
    }

    fn handle_aligned_checkpoint(&mut self, barrier: Barrier) -> Result<()> {
        let snapshot = self.operator_chain.snapshot_state()?;
        if let Some(sender) = &self.checkpoint_event_sender {
            sender.send(TaskCheckpointEvent::Ack(TaskCheckpointAck {
                checkpoint_id: barrier.checkpoint_id,
                task_id: self.descriptor.task_id,
                state: snapshot,
            }))?;
        }
        self.output_gate
            .broadcast(StreamElement::CheckpointBarrier(barrier))?;
        Ok(())
    }

    fn report_aborted_checkpoint(&self, checkpoint_id: u64, reason: &str) -> Result<()> {
        if let Some(sender) = &self.checkpoint_event_sender {
            sender.send(TaskCheckpointEvent::Aborted(TaskCheckpointAbort {
                checkpoint_id,
                task_id: self.descriptor.task_id,
                reason: reason.to_string(),
            }))?;
        }
        Ok(())
    }

    fn process_element(
        &mut self,
        channel_idx: usize,
        element: StreamElement<Vec<u8>>,
        input_batch: &mut Vec<Vec<u8>>,
        output_batch: &mut Vec<Vec<u8>>,
    ) -> Result<bool> {
        match element {
            StreamElement::Record(record) => {
                // Process data record through operator chain
                input_batch.clear();
                input_batch.push(record.value);

                output_batch.clear();
                self.operator_chain
                    .process_batch(&input_batch, output_batch)?;

                // Emit outputs to downstream
                for output in &*output_batch {
                    let out_element = StreamElement::record(output.clone());
                    self.output_gate.forward(out_element)?;
                }
            }

            StreamElement::Watermark(wm) => {
                let aligned_wm = if let Some(tracker) = self.watermark_tracker.as_mut() {
                    tracker.advance(channel_idx, wm)
                } else {
                    // Single-input fast path keeps previous direct semantics.
                    Some(wm)
                };
                if let Some(aligned_wm) = aligned_wm {
                    self.emit_aligned_watermark(aligned_wm, output_batch)?;
                }
            }

            StreamElement::CheckpointBarrier(barrier) => {
                // Non-aligned fallback path (single-input / checkpointing disabled).
                self.handle_aligned_checkpoint(barrier)?;
            }

            StreamElement::End => {
                // All inputs ended, propagate End and terminate
                self.output_gate.broadcast(StreamElement::End)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn emit_aligned_watermark(
        &mut self,
        watermark: Watermark,
        output_batch: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        // Fire event-time timers before forwarding watermark downstream.
        output_batch.clear();
        self.operator_chain
            .on_timer(watermark.timestamp, TimerDomain::EventTime, output_batch)?;
        for output in output_batch.iter() {
            self.output_gate
                .forward(StreamElement::record(output.clone()))?;
        }
        self.output_gate
            .broadcast(StreamElement::Watermark(watermark))?;
        Ok(())
    }

    fn process_newly_ended_channels(&mut self, output_batch: &mut Vec<Vec<u8>>) -> Result<()> {
        let ended_channels = self.input_gate.drain_newly_ended_channels();
        if ended_channels.is_empty() {
            return Ok(());
        }

        let mut to_emit = Vec::new();
        if let Some(tracker) = self.watermark_tracker.as_mut() {
            for channel_idx in ended_channels {
                let before = tracker.current_min_timestamp();
                tracker.mark_idle(channel_idx);
                let after = tracker.current_min_timestamp();
                if after > before {
                    to_emit.push(Watermark::new(after));
                }
            }
        }

        for wm in to_emit {
            self.emit_aligned_watermark(wm, output_batch)?;
        }
        Ok(())
    }

    /// Run the task event loop.
    ///
    /// This is the main execution loop that processes stream elements.
    /// Returns when the End element is received from all input channels.
    pub fn run(&mut self) -> Result<()> {
        let mut input_batch = Vec::with_capacity(1);
        let mut output_batch = Vec::with_capacity(1);

        loop {
            // Read next element from any input channel (fair selection).
            // When processing-time ticks are enabled, this becomes a timed poll.
            let maybe = match self.processing_time_tick_interval {
                Some(interval) => self.input_gate.next_timeout(interval)?,
                None => Some(self.input_gate.next()?),
            };

            // Observe channels that ended during InputGate internal loops.
            self.process_newly_ended_channels(&mut output_batch)?;

            if maybe.is_none() {
                output_batch.clear();
                self.operator_chain.on_timer(
                    current_processing_time_ms(),
                    TimerDomain::ProcessingTime,
                    &mut output_batch,
                )?;
                for output in &output_batch {
                    let out_element = StreamElement::record(output.clone());
                    self.output_gate.forward(out_element)?;
                }

                if let Some(wm) = self
                    .watermark_tracker
                    .as_mut()
                    .and_then(|tracker| tracker.detect_idle_channels())
                {
                    self.emit_aligned_watermark(wm, &mut output_batch)?;
                }
                continue;
            }

            let (channel_idx, element) = maybe.unwrap();

            if self.barrier_aligner.is_some() {
                let align_result = self
                    .barrier_aligner
                    .as_mut()
                    .expect("barrier_aligner checked")
                    .process_element(channel_idx, element)?;
                match align_result {
                    BarrierAlignResult::Forward(element) => {
                        let should_end = self.process_element(
                            channel_idx,
                            element,
                            &mut input_batch,
                            &mut output_batch,
                        )?;
                        if should_end {
                            break;
                        }
                    }
                    BarrierAlignResult::Buffering => {
                        continue;
                    }
                    BarrierAlignResult::Aligned { barrier, buffered } => {
                        self.handle_aligned_checkpoint(barrier)?;
                        let mut should_stop = false;
                        for (buffered_channel, buffered_element) in buffered {
                            let should_end = self.process_element(
                                buffered_channel,
                                buffered_element,
                                &mut input_batch,
                                &mut output_batch,
                            )?;
                            if should_end {
                                should_stop = true;
                                break;
                            }
                        }
                        if should_stop {
                            break;
                        }
                    }
                    BarrierAlignResult::Aborted {
                        checkpoint_id,
                        drained,
                    } => {
                        self.report_aborted_checkpoint(
                            checkpoint_id,
                            "barrier alignment aborted due to buffer overflow",
                        )?;
                        let mut should_stop = false;
                        for (buffered_channel, buffered_element) in drained {
                            let should_end = self.process_element(
                                buffered_channel,
                                buffered_element,
                                &mut input_batch,
                                &mut output_batch,
                            )?;
                            if should_end {
                                should_stop = true;
                                break;
                            }
                        }
                        if should_stop {
                            break;
                        }
                    }
                }
                continue;
            }

            let should_end =
                self.process_element(channel_idx, element, &mut input_batch, &mut output_batch)?;
            if should_end {
                break;
            }
        }

        Ok(())
    }
}

fn current_processing_time_ms() -> EventTime {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as EventTime)
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "tests/task_tests.rs"]
mod tests;
