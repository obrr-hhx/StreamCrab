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
mod tests {
    use super::*;
    use crate::channel::local_channel_default;
    use crate::checkpoint::TaskCheckpointEvent;
    use crate::operator_chain::{ChainEnd, Operator};
    use crate::types::Watermark;
    use std::time::Instant;

    #[test]
    fn test_task_id_display() {
        let task_id = TaskId::new(VertexId::new(1), 3);
        assert_eq!(task_id.to_string(), "vertex_1_3");
    }

    #[test]
    fn test_task_deployment_descriptor() {
        let desc = TaskDeploymentDescriptor::new(VertexId::new(2), 1, 4, vec![10, 11]);

        // Access vertex_id and subtask_index via task_id
        assert_eq!(desc.task_id.vertex_id, VertexId::new(2));
        assert_eq!(desc.task_id.subtask_index, 1);
        assert_eq!(desc.parallelism, 4);
        assert_eq!(desc.chained_operators.len(), 2);
    }

    #[test]
    fn test_task_execution_simple() {
        // Create a simple task with identity operator (pass-through)
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(1), 0, 1, vec![1]);

        // Create input/output channels
        let (input_sender, input_receiver) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();

        // Create gates
        let input_gate = InputGate::new(vec![input_receiver]);
        let output_gate = OutputGate::new(vec![output_sender]);

        // Create operator chain (identity: ChainEnd just passes through)
        let operator_chain = ChainEnd;

        // Create task
        let mut task = Task::new(descriptor, input_gate, output_gate, operator_chain);

        // Send test data
        input_sender
            .send(StreamElement::record(vec![1u8, 2, 3]))
            .unwrap();
        input_sender
            .send(StreamElement::record(vec![4u8, 5, 6]))
            .unwrap();
        input_sender.send(StreamElement::End).unwrap();

        // Run task in separate thread
        let handle = std::thread::spawn(move || task.run());

        // Verify outputs
        let elem1 = output_receiver.recv().unwrap();
        match elem1 {
            StreamElement::Record(rec) => assert_eq!(rec.value, vec![1u8, 2, 3]),
            _ => panic!("expected Record"),
        }

        let elem2 = output_receiver.recv().unwrap();
        match elem2 {
            StreamElement::Record(rec) => assert_eq!(rec.value, vec![4u8, 5, 6]),
            _ => panic!("expected Record"),
        }

        let elem3 = output_receiver.recv().unwrap();
        assert!(matches!(elem3, StreamElement::End));

        // Task should complete successfully
        handle.join().unwrap().unwrap();
    }

    struct TickOnlyOperator;

    impl Operator<Vec<u8>> for TickOnlyOperator {
        type OUT = Vec<u8>;

        fn process_batch(&mut self, _input: &[Vec<u8>], _output: &mut Vec<Vec<u8>>) -> Result<()> {
            Ok(())
        }

        fn on_processing_time(
            &mut self,
            _processing_time: EventTime,
            output: &mut Vec<Vec<u8>>,
        ) -> Result<()> {
            output.push(vec![9u8]);
            Ok(())
        }
    }

    #[test]
    fn test_task_processing_time_tick_emits() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(1), 0, 1, vec![1]);
        let (input_sender, input_receiver) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let input_gate = InputGate::new(vec![input_receiver]);
        let output_gate = OutputGate::new(vec![output_sender]);

        let mut task = Task::new(descriptor, input_gate, output_gate, TickOnlyOperator)
            .with_processing_time_tick(Duration::from_millis(1));

        let sender_handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            input_sender.send(StreamElement::End).unwrap();
        });

        let task_handle = std::thread::spawn(move || task.run());

        let mut tick_records = 0usize;
        loop {
            let elem = output_receiver.recv().unwrap();
            match elem {
                StreamElement::Record(rec) => {
                    if rec.value == vec![9u8] {
                        tick_records += 1;
                    }
                }
                StreamElement::End => break,
                _ => {}
            }
        }

        sender_handle.join().unwrap();
        task_handle.join().unwrap().unwrap();
        assert!(
            tick_records >= 1,
            "expected at least one processing-time tick output"
        );
    }

    struct EventTimeOnlyOperator;

    impl Operator<Vec<u8>> for EventTimeOnlyOperator {
        type OUT = Vec<u8>;

        fn process_batch(&mut self, _input: &[Vec<u8>], _output: &mut Vec<Vec<u8>>) -> Result<()> {
            Ok(())
        }

        fn on_event_time(
            &mut self,
            event_time: EventTime,
            output: &mut Vec<Vec<u8>>,
        ) -> Result<()> {
            output.push(vec![event_time as u8]);
            Ok(())
        }
    }

    struct SnapshotCounterOp {
        count: u32,
    }

    impl Operator<Vec<u8>> for SnapshotCounterOp {
        type OUT = Vec<u8>;

        fn process_batch(&mut self, input: &[Vec<u8>], output: &mut Vec<Vec<u8>>) -> Result<()> {
            self.count += input.len() as u32;
            output.extend(input.iter().cloned());
            Ok(())
        }

        fn snapshot_state(&self) -> Result<Vec<u8>> {
            Ok(bincode::serialize(&self.count)?)
        }

        fn restore_state(&mut self, data: &[u8]) -> Result<()> {
            self.count = bincode::deserialize(data)?;
            Ok(())
        }
    }

    #[test]
    fn test_task_watermark_triggers_event_time_timer_before_forwarding() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(1), 0, 1, vec![1]);
        let (input_sender, input_receiver) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let input_gate = InputGate::new(vec![input_receiver]);
        let output_gate = OutputGate::new(vec![output_sender]);

        let mut task = Task::new(descriptor, input_gate, output_gate, EventTimeOnlyOperator);

        input_sender
            .send(StreamElement::Watermark(Watermark::new(42)))
            .unwrap();
        input_sender.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());

        let first = output_receiver.recv().unwrap();
        match first {
            StreamElement::Record(rec) => assert_eq!(rec.value, vec![42u8]),
            _ => panic!("expected timer Record before Watermark"),
        }

        let second = output_receiver.recv().unwrap();
        match second {
            StreamElement::Watermark(wm) => assert_eq!(wm.timestamp, 42),
            _ => panic!("expected forwarded Watermark"),
        }

        let third = output_receiver.recv().unwrap();
        assert!(matches!(third, StreamElement::End));

        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_task_multi_input_watermark_alignment() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(1), 0, 1, vec![1]);
        let (sender0, receiver0) = local_channel_default();
        let (sender1, receiver1) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let input_gate = InputGate::new(vec![receiver0, receiver1]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(descriptor, input_gate, output_gate, EventTimeOnlyOperator);

        sender0
            .send(StreamElement::Watermark(Watermark::new(100)))
            .unwrap();
        sender1
            .send(StreamElement::Watermark(Watermark::new(50)))
            .unwrap();
        sender1
            .send(StreamElement::Watermark(Watermark::new(120)))
            .unwrap();
        sender0.send(StreamElement::End).unwrap();
        sender1.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());
        let mut forwarded_wms = Vec::new();
        loop {
            match output_receiver.recv().unwrap() {
                StreamElement::Watermark(wm) => forwarded_wms.push(wm.timestamp),
                StreamElement::End => break,
                _ => {}
            }
        }
        handle.join().unwrap().unwrap();

        assert!(forwarded_wms.windows(2).all(|w| w[0] <= w[1]));
        let pos_50 = forwarded_wms
            .iter()
            .position(|&ts| ts == 50)
            .expect("expected aligned watermark 50");
        let pos_100 = forwarded_wms
            .iter()
            .position(|&ts| ts == 100)
            .expect("expected aligned watermark 100");
        assert!(pos_50 < pos_100, "50 must be emitted before 100");
    }

    #[test]
    fn test_task_ended_channel_unblocks_watermark_alignment() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(1), 0, 1, vec![1]);
        let (sender0, receiver0) = local_channel_default();
        let (sender1, receiver1) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let input_gate = InputGate::new(vec![receiver0, receiver1]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(descriptor, input_gate, output_gate, EventTimeOnlyOperator);

        sender0
            .send(StreamElement::Watermark(Watermark::new(100)))
            .unwrap();
        sender1.send(StreamElement::End).unwrap();
        sender0.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());
        let mut forwarded_wms = Vec::new();
        loop {
            match output_receiver.recv().unwrap() {
                StreamElement::Watermark(wm) => forwarded_wms.push(wm.timestamp),
                StreamElement::End => break,
                _ => {}
            }
        }
        handle.join().unwrap().unwrap();

        assert!(
            forwarded_wms.contains(&100),
            "channel end should mark idle and unblock watermark 100"
        );
    }

    #[test]
    fn test_task_idle_timeout_advances_watermark_with_processing_tick() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(1), 0, 1, vec![1]);
        let (sender0, receiver0) = local_channel_default();
        let (sender1, receiver1) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let input_gate = InputGate::new(vec![receiver0, receiver1]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(descriptor, input_gate, output_gate, EventTimeOnlyOperator)
            .with_processing_time_tick(Duration::from_millis(1))
            .with_watermark_idle_timeout(Duration::from_millis(1));

        let end_sender = std::thread::spawn(move || {
            // Let idle-timeout mark silent channel 1 as idle, then emit watermark on channel 0.
            std::thread::sleep(Duration::from_millis(10));
            sender0
                .send(StreamElement::Watermark(Watermark::new(100)))
                .unwrap();
            std::thread::sleep(Duration::from_millis(30));
            sender0.send(StreamElement::End).unwrap();
            sender1.send(StreamElement::End).unwrap();
        });

        let handle = std::thread::spawn(move || task.run());

        // End is sent at ~40ms. Seeing watermark 100 before then means idle-timeout path worked.
        let deadline = Instant::now() + Duration::from_millis(30);
        let mut saw_wm_100_before_end = false;
        while Instant::now() < deadline {
            match output_receiver.try_recv().unwrap() {
                Some(StreamElement::Watermark(wm)) if wm.timestamp == 100 => {
                    saw_wm_100_before_end = true;
                    break;
                }
                Some(_) => {}
                None => std::thread::sleep(Duration::from_millis(1)),
            }
        }

        loop {
            let elem = output_receiver.recv().unwrap();
            if matches!(elem, StreamElement::End) {
                break;
            }
        }
        end_sender.join().unwrap();
        handle.join().unwrap().unwrap();

        assert!(
            saw_wm_100_before_end,
            "expected watermark 100 before End via idle-timeout detection"
        );
    }

    #[test]
    fn test_task_barrier_passthrough_without_checkpointing() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(2), 0, 1, vec![1]);
        let (input_sender, input_receiver) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let input_gate = InputGate::new(vec![input_receiver]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(descriptor, input_gate, output_gate, ChainEnd);

        input_sender
            .send(StreamElement::barrier_with_timestamp(3, 123))
            .unwrap();
        input_sender.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());

        match output_receiver.recv().unwrap() {
            StreamElement::CheckpointBarrier(barrier) => {
                assert_eq!(barrier.checkpoint_id, 3);
                assert_eq!(barrier.timestamp, 123);
            }
            other => panic!("expected checkpoint barrier, got {other:?}"),
        }
        assert!(matches!(
            output_receiver.recv().unwrap(),
            StreamElement::End
        ));
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_task_single_input_checkpoint_barrier_emits_ack() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(2), 1, 1, vec![1]);
        let (input_sender, input_receiver) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let (event_sender, event_receiver) = crossbeam_channel::unbounded();
        let input_gate = InputGate::new(vec![input_receiver]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(descriptor, input_gate, output_gate, ChainEnd)
            .with_checkpointing(event_sender, 16);

        input_sender
            .send(StreamElement::barrier_with_timestamp(7, 777))
            .unwrap();
        input_sender.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());

        assert!(matches!(
            output_receiver.recv().unwrap(),
            StreamElement::CheckpointBarrier(Barrier {
                checkpoint_id: 7,
                ..
            })
        ));
        assert!(matches!(
            output_receiver.recv().unwrap(),
            StreamElement::End
        ));
        handle.join().unwrap().unwrap();

        let event = event_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        match event {
            TaskCheckpointEvent::Ack(ack) => {
                assert_eq!(ack.checkpoint_id, 7);
                assert_eq!(ack.task_id, TaskId::new(VertexId::new(2), 1));
            }
            other => panic!("expected ack event, got {other:?}"),
        }
        assert!(event_receiver.try_recv().is_err());
    }

    #[test]
    fn test_task_checkpoint_alignment_ack_and_buffer_release() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(3), 0, 1, vec![1]);
        let (sender0, receiver0) = local_channel_default();
        let (sender1, receiver1) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let (event_sender, event_receiver) = crossbeam_channel::unbounded();

        let input_gate = InputGate::new(vec![receiver0, receiver1]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(
            descriptor,
            input_gate,
            output_gate,
            SnapshotCounterOp { count: 0 },
        )
        .with_checkpointing(event_sender, 10_000);

        sender0
            .send(StreamElement::barrier_with_timestamp(1, 111))
            .unwrap();
        sender0.send(StreamElement::record(vec![1u8])).unwrap();
        sender1.send(StreamElement::record(vec![2u8])).unwrap();
        sender1
            .send(StreamElement::barrier_with_timestamp(1, 111))
            .unwrap();
        sender0.send(StreamElement::End).unwrap();
        sender1.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());

        let mut seen = Vec::new();
        loop {
            let elem = output_receiver.recv().unwrap();
            if matches!(elem, StreamElement::End) {
                break;
            }
            seen.push(elem);
        }
        handle.join().unwrap().unwrap();

        let barrier_pos = seen
            .iter()
            .position(|e| matches!(e, StreamElement::CheckpointBarrier(b) if b.checkpoint_id == 1))
            .expect("barrier should be forwarded");
        let rec1_pos = seen
            .iter()
            .position(|e| matches!(e, StreamElement::Record(r) if r.value == vec![1u8]))
            .expect("buffered record should be forwarded");
        let rec2_pos = seen
            .iter()
            .position(|e| matches!(e, StreamElement::Record(r) if r.value == vec![2u8]))
            .expect("unblocked channel record should be forwarded");

        assert!(
            rec2_pos < barrier_pos,
            "record from unblocked channel should pass through"
        );
        assert!(
            barrier_pos < rec1_pos,
            "buffered record should drain after barrier"
        );

        let event = event_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("expected checkpoint event");
        let ack = match event {
            TaskCheckpointEvent::Ack(ack) => ack,
            other => panic!("expected ack event, got {other:?}"),
        };
        assert_eq!(ack.checkpoint_id, 1);
        assert_eq!(ack.task_id, TaskId::new(VertexId::new(3), 0));
        let snap_count: u32 = bincode::deserialize(&ack.state).unwrap();
        assert_eq!(snap_count, 1, "snapshot happens before buffered replay");
        assert!(
            event_receiver.try_recv().is_err(),
            "only one event expected"
        );
    }

    #[test]
    fn test_task_checkpoint_overflow_aborts_without_data_loss() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(4), 0, 1, vec![1]);
        let (sender0, receiver0) = local_channel_default();
        let (sender1, receiver1) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let (event_sender, event_receiver) = crossbeam_channel::unbounded();

        let input_gate = InputGate::new(vec![receiver0, receiver1]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(
            descriptor,
            input_gate,
            output_gate,
            SnapshotCounterOp { count: 0 },
        )
        .with_checkpointing(event_sender, 1);

        sender0
            .send(StreamElement::barrier_with_timestamp(9, 200))
            .unwrap();
        sender0.send(StreamElement::record(vec![1u8])).unwrap();
        sender0.send(StreamElement::record(vec![2u8])).unwrap();
        sender0.send(StreamElement::End).unwrap();
        sender1.send(StreamElement::End).unwrap();

        let handle = std::thread::spawn(move || task.run());

        let mut records = Vec::new();
        let mut seen_barrier = false;
        loop {
            match output_receiver.recv().unwrap() {
                StreamElement::Record(rec) => records.push(rec.value),
                StreamElement::CheckpointBarrier(_) => seen_barrier = true,
                StreamElement::End => break,
                _ => {}
            }
        }
        handle.join().unwrap().unwrap();

        assert_eq!(records, vec![vec![1u8], vec![2u8]]);
        assert!(
            !seen_barrier,
            "aborted checkpoint should not forward barrier"
        );
        let aborted = event_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("aborted checkpoint should emit aborted event");
        match aborted {
            TaskCheckpointEvent::Aborted(abort) => {
                assert_eq!(abort.checkpoint_id, 9);
                assert_eq!(abort.task_id, TaskId::new(VertexId::new(4), 0));
            }
            other => panic!("expected aborted event, got {other:?}"),
        }
        assert!(
            event_receiver.try_recv().is_err(),
            "only one event expected"
        );
    }

    #[test]
    fn test_task_checkpoint_abort_then_next_checkpoint_still_acks() {
        let descriptor = TaskDeploymentDescriptor::new(VertexId::new(5), 0, 1, vec![1]);
        let (sender0, receiver0) = local_channel_default();
        let (sender1, receiver1) = local_channel_default();
        let (output_sender, output_receiver) = local_channel_default();
        let (event_sender, event_receiver) = crossbeam_channel::unbounded();

        let input_gate = InputGate::new(vec![receiver0, receiver1]);
        let output_gate = OutputGate::new(vec![output_sender]);
        let mut task = Task::new(descriptor, input_gate, output_gate, ChainEnd)
            .with_checkpointing(event_sender, 1);

        let handle = std::thread::spawn(move || task.run());
        let sender_handle = std::thread::spawn(move || {
            // Force checkpoint 9 overflow before channel 1 emits barrier 9.
            sender0
                .send(StreamElement::barrier_with_timestamp(9, 900))
                .unwrap();
            sender0.send(StreamElement::record(vec![1u8])).unwrap();
            sender0.send(StreamElement::record(vec![2u8])).unwrap();
            std::thread::sleep(Duration::from_millis(10));

            sender1
                .send(StreamElement::barrier_with_timestamp(9, 900))
                .unwrap();
            sender1
                .send(StreamElement::barrier_with_timestamp(10, 1000))
                .unwrap();
            sender0
                .send(StreamElement::barrier_with_timestamp(10, 1000))
                .unwrap();
            sender0.send(StreamElement::End).unwrap();
            sender1.send(StreamElement::End).unwrap();
        });

        let mut barrier_ids = Vec::new();
        let mut records = Vec::new();
        loop {
            match output_receiver.recv().unwrap() {
                StreamElement::CheckpointBarrier(barrier) => {
                    barrier_ids.push(barrier.checkpoint_id)
                }
                StreamElement::Record(record) => records.push(record.value),
                StreamElement::End => break,
                _ => {}
            }
        }
        sender_handle.join().unwrap();
        handle.join().unwrap().unwrap();

        assert_eq!(records, vec![vec![1u8], vec![2u8]]);
        assert!(
            !barrier_ids.contains(&9),
            "aborted checkpoint barrier must not forward"
        );
        assert_eq!(
            barrier_ids,
            vec![10],
            "checkpoint after abort should still align and forward"
        );

        let first = event_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        let second = event_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        let events = vec![first, second];
        assert!(events.iter().any(|e| matches!(
            e,
            TaskCheckpointEvent::Aborted(abort) if abort.checkpoint_id == 9
        )));
        assert!(events.iter().any(|e| matches!(
            e,
            TaskCheckpointEvent::Ack(ack) if ack.checkpoint_id == 10
        )));
        assert!(event_receiver.try_recv().is_err());
    }
}
