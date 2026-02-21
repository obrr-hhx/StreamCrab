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

    fn on_event_time(&mut self, event_time: EventTime, output: &mut Vec<Vec<u8>>) -> Result<()> {
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
            StreamElement::CheckpointBarrier(barrier) => barrier_ids.push(barrier.checkpoint_id),
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
