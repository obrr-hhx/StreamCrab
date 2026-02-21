use super::*;
use crate::channel::local_channel;
use crate::partitioner::HashPartitioner;

#[test]
fn test_output_gate_emit_to() {
    let (sender1, receiver1) = local_channel::<i32>(10);
    let (sender2, receiver2) = local_channel::<i32>(10);
    let gate = OutputGate::new(vec![sender1, sender2]);

    gate.emit_to(0, StreamElement::record(1)).unwrap();
    gate.emit_to(1, StreamElement::record(2)).unwrap();

    let elem1 = receiver1.recv().unwrap();
    match elem1 {
        StreamElement::Record(rec) => assert_eq!(rec.value, 1),
        _ => panic!("Expected Record"),
    }

    let elem2 = receiver2.recv().unwrap();
    match elem2 {
        StreamElement::Record(rec) => assert_eq!(rec.value, 2),
        _ => panic!("Expected Record"),
    }
}

#[test]
fn test_output_gate_broadcast() {
    let (sender1, receiver1) = local_channel::<i32>(10);
    let (sender2, receiver2) = local_channel::<i32>(10);
    let gate = OutputGate::new(vec![sender1, sender2]);

    gate.broadcast(StreamElement::watermark(1000)).unwrap();

    let elem1 = receiver1.recv().unwrap();
    assert_eq!(elem1, StreamElement::watermark(1000));

    let elem2 = receiver2.recv().unwrap();
    assert_eq!(elem2, StreamElement::watermark(1000));
}

#[test]
fn test_output_gate_forward() {
    let (sender, receiver) = local_channel::<i32>(10);
    let gate = OutputGate::new(vec![sender]);

    gate.forward(StreamElement::record(42)).unwrap();

    let elem = receiver.recv().unwrap();
    match elem {
        StreamElement::Record(rec) => assert_eq!(rec.value, 42),
        _ => panic!("Expected Record"),
    }
}

#[test]
fn test_output_gate_emit_partitioned() {
    let (sender1, receiver1) = local_channel::<i32>(10);
    let (sender2, receiver2) = local_channel::<i32>(10);
    let gate = OutputGate::new(vec![sender1, sender2]);

    let partitioner = HashPartitioner::new(|x: &i32| *x);

    // Emit multiple records with partitioning
    for i in 0..10 {
        gate.emit_partitioned(StreamElement::record(i), &i, &partitioner)
            .unwrap();
    }

    // Verify that records are distributed across channels
    let mut count1 = 0;
    let mut count2 = 0;

    while let Ok(Some(_)) = receiver1.try_recv() {
        count1 += 1;
    }
    while let Ok(Some(_)) = receiver2.try_recv() {
        count2 += 1;
    }

    // Should have distributed all 10 records
    assert_eq!(count1 + count2, 10);
    // Both channels should have received some records (with high probability)
    assert!(count1 > 0);
    assert!(count2 > 0);
}
