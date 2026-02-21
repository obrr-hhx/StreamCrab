use super::*;
use crate::types::StreamRecord;

#[test]
fn test_local_channel_send_recv() {
    let (sender, receiver) = local_channel::<i32>(10);

    // Send a record
    sender.send(StreamElement::record(42)).unwrap();

    // Receive it
    let elem = receiver.recv().unwrap();
    match elem {
        StreamElement::Record(rec) => assert_eq!(rec.value, 42),
        _ => panic!("Expected Record"),
    }
}

#[test]
fn test_local_channel_watermark() {
    let (sender, receiver) = local_channel::<i32>(10);

    sender.send(StreamElement::watermark(1000)).unwrap();

    let elem = receiver.recv().unwrap();
    match elem {
        StreamElement::Watermark(wm) => assert_eq!(wm.timestamp, 1000),
        _ => panic!("Expected Watermark"),
    }
}

#[test]
fn test_local_channel_end() {
    let (sender, receiver) = local_channel::<i32>(10);

    sender.send(StreamElement::End).unwrap();

    let elem = receiver.recv().unwrap();
    assert_eq!(elem, StreamElement::End);
}

#[test]
fn test_local_channel_backpressure() {
    let (sender, receiver) = local_channel::<i32>(2);

    // Fill the channel
    sender.send(StreamElement::record(1)).unwrap();
    sender.send(StreamElement::record(2)).unwrap();

    // try_send should fail (channel full)
    let result = sender.try_send(StreamElement::record(3));
    assert!(result.is_err());

    // Consume one element
    receiver.recv().unwrap();

    // Now try_send should succeed
    sender.try_send(StreamElement::record(3)).unwrap();
}

#[test]
fn test_local_channel_closed() {
    let (sender, receiver) = local_channel::<i32>(10);

    sender.send(StreamElement::record(42)).unwrap();

    // Drop sender
    drop(sender);

    // Can still receive buffered element
    let elem = receiver.recv().unwrap();
    match elem {
        StreamElement::Record(rec) => assert_eq!(rec.value, 42),
        _ => panic!("Expected Record"),
    }

    // Next recv should fail (channel closed)
    let result = receiver.recv();
    assert!(result.is_err());
}

#[test]
fn test_local_channel_clone_sender() {
    let (sender, receiver) = local_channel::<i32>(10);

    // Clone sender
    let sender2 = sender.clone();

    sender.send(StreamElement::record(1)).unwrap();
    sender2.send(StreamElement::record(2)).unwrap();

    assert_eq!(
        receiver.recv().unwrap(),
        StreamElement::Record(StreamRecord::new(1))
    );
    assert_eq!(
        receiver.recv().unwrap(),
        StreamElement::Record(StreamRecord::new(2))
    );
}
