use super::*;
use crate::channel::local_channel;

#[test]
fn test_input_gate_single_channel() {
    let (sender, receiver) = local_channel::<i32>(10);
    let mut gate = InputGate::new(vec![receiver]);

    sender.send(StreamElement::record(42)).unwrap();
    sender.send(StreamElement::End).unwrap();

    let (ch, elem) = gate.next().unwrap();
    assert_eq!(ch, 0);
    match elem {
        StreamElement::Record(rec) => assert_eq!(rec.value, 42),
        _ => panic!("Expected Record"),
    }

    let (ch, elem) = gate.next().unwrap();
    assert_eq!(ch, 0);
    assert_eq!(elem, StreamElement::End);
    assert!(gate.all_ended());
}

#[test]
fn test_input_gate_multiple_channels() {
    let (sender1, receiver1) = local_channel::<i32>(10);
    let (sender2, receiver2) = local_channel::<i32>(10);
    let mut gate = InputGate::new(vec![receiver1, receiver2]);

    assert_eq!(gate.num_channels(), 2);
    assert_eq!(gate.num_ended(), 0);

    // Send from both channels
    sender1.send(StreamElement::record(1)).unwrap();
    sender2.send(StreamElement::record(2)).unwrap();

    // Should receive from both (order may vary)
    let (_, elem1) = gate.next().unwrap();
    let (_, elem2) = gate.next().unwrap();

    let mut values = vec![];
    if let StreamElement::Record(rec) = elem1 {
        values.push(rec.value);
    }
    if let StreamElement::Record(rec) = elem2 {
        values.push(rec.value);
    }
    values.sort();
    assert_eq!(values, vec![1, 2]);
}

#[test]
fn test_input_gate_partial_end() {
    let (sender1, receiver1) = local_channel::<i32>(10);
    let (sender2, receiver2) = local_channel::<i32>(10);
    let mut gate = InputGate::new(vec![receiver1, receiver2]);

    // Send data from channel 1
    sender2.send(StreamElement::record(1)).unwrap();
    sender2.send(StreamElement::record(2)).unwrap();

    // Channel 0 ends
    sender1.send(StreamElement::End).unwrap();
    drop(sender1);

    // Read all elements (should get 2 records and skip the End)
    let mut received = vec![];
    for _ in 0..2 {
        let (_, elem) = gate.next().unwrap();
        if let StreamElement::Record(rec) = elem {
            received.push(rec.value);
        }
    }
    received.sort();
    assert_eq!(received, vec![1, 2]);

    // After reading, channel 0 should be marked as ended
    assert_eq!(gate.num_ended(), 1);
    assert!(!gate.all_ended());

    // Now channel 1 ends
    sender2.send(StreamElement::End).unwrap();
    let (ch, elem) = gate.next().unwrap();
    assert_eq!(ch, 1);
    assert_eq!(elem, StreamElement::End);
    assert!(gate.all_ended());
}

#[test]
fn test_input_gate_all_ended_error() {
    let (sender, receiver) = local_channel::<i32>(10);
    let mut gate = InputGate::new(vec![receiver]);

    sender.send(StreamElement::End).unwrap();
    gate.next().unwrap(); // Consume the End

    // Next call should error
    let result = gate.next();
    assert!(result.is_err());
}

#[test]
fn test_input_gate_next_timeout() {
    let (sender, receiver) = local_channel::<i32>(10);
    let mut gate = InputGate::new(vec![receiver]);

    // No input available yet -> timeout.
    let timed = gate
        .next_timeout(std::time::Duration::from_millis(1))
        .unwrap();
    assert!(timed.is_none());

    // Send data and ensure timeout API can read it.
    sender.send(StreamElement::record(7)).unwrap();
    let got = gate
        .next_timeout(std::time::Duration::from_millis(10))
        .unwrap();
    assert!(matches!(got, Some((0, StreamElement::Record(_)))));
}

#[test]
fn test_input_gate_drain_newly_ended_channels_once() {
    let (_sender1, receiver1) = local_channel::<i32>(10);
    let (_sender2, receiver2) = local_channel::<i32>(10);
    let mut gate = InputGate::new(vec![receiver1, receiver2]);

    gate.mark_ended(0);
    let ended = gate.drain_newly_ended_channels();
    assert_eq!(ended, vec![0]);
    assert!(gate.drain_newly_ended_channels().is_empty());

    gate.mark_ended(1);
    let ended = gate.drain_newly_ended_channels();
    assert_eq!(ended, vec![1]);
}
