use super::*;

#[test]
fn test_frame_encode_decode_roundtrip() {
    let frame = Frame::new(FrameType::Barrier, 7, vec![1, 2, 3, 4]);
    let encoded = frame.encode();
    let decoded = Frame::decode(&encoded[4..]).unwrap();
    assert_eq!(decoded, frame);
}

#[test]
fn test_frame_decode_rejects_unknown_type() {
    let mut body = vec![99u8];
    body.extend_from_slice(&7u32.to_be_bytes());
    body.extend_from_slice(&[1, 2, 3]);
    let err = Frame::decode(&body).unwrap_err();
    assert!(err.to_string().contains("unknown frame type"));
}

#[test]
fn test_try_decode_from_sticky_buffer() {
    let f1 = Frame::new(FrameType::Data, 1, vec![10]);
    let f2 = Frame::new(FrameType::Control, 2, vec![20, 21]);
    let mut buffer = f1.encode();
    buffer.extend_from_slice(&f2.encode());

    let d1 = try_decode_from_buffer(&mut buffer).unwrap().unwrap();
    let d2 = try_decode_from_buffer(&mut buffer).unwrap().unwrap();
    let d3 = try_decode_from_buffer(&mut buffer).unwrap();
    assert_eq!(d1, f1);
    assert_eq!(d2, f2);
    assert!(d3.is_none());
}
