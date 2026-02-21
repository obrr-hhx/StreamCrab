use std::collections::HashMap;

use crate::channel::local_channel_default;
use crate::network::frame::{Frame, FrameType};
use crate::network::remote_gate::{
    RemoteInputGate, RemoteOutputGate, decode_stream_element, encode_stream_element,
};
use crate::types::{StreamElement, StreamRecord};

#[test]
fn test_stream_element_frame_roundtrip() {
    let samples = vec![
        StreamElement::Record(StreamRecord::with_timestamp(vec![1, 2, 3], 99)),
        StreamElement::watermark(1234),
        StreamElement::barrier_with_timestamp(7, 88),
        StreamElement::End,
    ];

    for elem in samples {
        let frame = encode_stream_element(42, elem.clone()).unwrap();
        let (channel_id, decoded) = decode_stream_element(frame).unwrap();
        assert_eq!(channel_id, 42);
        assert_eq!(decoded, elem);
    }
}

#[test]
fn test_decode_rejects_frame_type_mismatch() {
    let mut frame = encode_stream_element(1, StreamElement::watermark(10)).unwrap();
    frame.frame_type = FrameType::Data;
    let err = decode_stream_element(frame).unwrap_err();
    assert!(err.to_string().contains("frame type mismatch"));
}

#[test]
fn test_remote_input_gate_routes_by_channel_id() {
    let (sender0, receiver0) = local_channel_default();
    let (sender1, receiver1) = local_channel_default();

    let mut channels = HashMap::new();
    channels.insert(11, sender0);
    channels.insert(29, sender1);
    let gate = RemoteInputGate::new(channels);

    gate.ingest_frame(encode_stream_element(29, StreamElement::record(vec![9u8])).unwrap())
        .unwrap();
    gate.ingest_frame(encode_stream_element(11, StreamElement::watermark(77)).unwrap())
        .unwrap();

    assert_eq!(gate.channel_count(), 2);
    assert_eq!(receiver1.recv().unwrap(), StreamElement::record(vec![9u8]));
    assert_eq!(receiver0.recv().unwrap(), StreamElement::watermark(77));
}

#[test]
fn test_remote_output_gate_maps_output_index_to_channel_id() {
    let gate = RemoteOutputGate::new(vec![5, 7]);

    let frame = gate
        .encode_for_output(1, StreamElement::barrier_with_timestamp(3, 100))
        .unwrap();
    assert_eq!(frame.channel_id, 7);
    assert_eq!(frame.frame_type, FrameType::Barrier);

    let (channel_id, elem) = decode_stream_element(frame).unwrap();
    assert_eq!(channel_id, 7);
    assert_eq!(elem, StreamElement::barrier_with_timestamp(3, 100));
    assert_eq!(gate.output_count(), 2);
}

#[test]
fn test_decode_rejects_control_frame() {
    let frame = Frame::new(FrameType::Control, 9, vec![]);
    let err = decode_stream_element(frame).unwrap_err();
    assert!(err.to_string().contains("control frames"));
}
