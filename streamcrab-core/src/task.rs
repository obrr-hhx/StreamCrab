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
//!         Watermark => operator_chain.on_watermark() -> output_gate.broadcast()
//!         Barrier => snapshot_state() -> ack_checkpoint() -> output_gate.broadcast()
//!         End => cleanup() -> break
//!     }
//! }
//! ```

use crate::input_gate::InputGate;
use crate::operator_chain::Operator;
use crate::output_gate::OutputGate;
use crate::types::{NodeId, StreamElement};
use anyhow::Result;
use serde::{Deserialize, Serialize};

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
///             operator_chain.on_watermark(wm)
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
        Self {
            descriptor,
            input_gate,
            output_gate,
            operator_chain,
        }
    }

    /// Run the task event loop.
    ///
    /// This is the main execution loop that processes stream elements.
    /// Returns when the End element is received from all input channels.
    pub fn run(&mut self) -> Result<()> {
        let mut input_batch = Vec::with_capacity(1);
        let mut output_batch = Vec::with_capacity(1);

        loop {
            // Read next element from any input channel (fair selection)
            let (_channel_idx, element) = self.input_gate.next()?;

            match element {
                StreamElement::Record(record) => {
                    // Process data record through operator chain
                    input_batch.clear();
                    input_batch.push(record.value);

                    output_batch.clear();
                    self.operator_chain
                        .process_batch(&input_batch, &mut output_batch)?;

                    // Emit outputs to downstream
                    for output in &output_batch {
                        let out_element = StreamElement::record(output.clone());
                        self.output_gate.forward(out_element)?;
                    }
                }

                StreamElement::Watermark(wm) => {
                    // Broadcast watermark to all downstream tasks
                    // TODO: Watermark alignment and timer triggering (P2)
                    self.output_gate.broadcast(StreamElement::Watermark(wm))?;
                }

                StreamElement::CheckpointBarrier(barrier) => {
                    // Snapshot state and broadcast barrier
                    // TODO: Barrier alignment and state snapshot (P3)
                    self.output_gate
                        .broadcast(StreamElement::CheckpointBarrier(barrier))?;
                }

                StreamElement::End => {
                    // All inputs ended, propagate End and terminate
                    self.output_gate.broadcast(StreamElement::End)?;
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::local_channel_default;
    use crate::operator_chain::ChainEnd;

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
}
