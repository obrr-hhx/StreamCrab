use super::*;

#[test]
fn test_stream_element_record() {
    let elem = StreamElement::record(42i32);
    match &elem {
        StreamElement::Record(rec) => {
            assert_eq!(rec.value, 42);
            assert_eq!(rec.timestamp, None);
        }
        _ => panic!("expected Record"),
    }
}

#[test]
fn test_stream_element_watermark() {
    let elem = StreamElement::<i32>::watermark(1000);
    match elem {
        StreamElement::Watermark(wm) => assert_eq!(wm.timestamp, 1000),
        _ => panic!("expected Watermark"),
    }
}

#[test]
fn test_stream_element_barrier() {
    let elem = StreamElement::<i32>::barrier(5);
    match elem {
        StreamElement::CheckpointBarrier(b) => {
            assert_eq!(b.checkpoint_id, 5);
            assert_eq!(b.timestamp, 0);
        }
        _ => panic!("expected Barrier"),
    }
}

#[test]
fn test_stream_element_barrier_with_timestamp() {
    let elem = StreamElement::<i32>::barrier_with_timestamp(7, 1234);
    match elem {
        StreamElement::CheckpointBarrier(b) => {
            assert_eq!(b.checkpoint_id, 7);
            assert_eq!(b.timestamp, 1234);
        }
        _ => panic!("expected Barrier"),
    }
}

#[test]
fn test_stream_element_rescale_barrier() {
    let barrier = RescaleBarrier::new(9, 3, 16, 11, vec![1, 2, 3]);
    let elem = StreamElement::<i32>::rescale_barrier(barrier.clone());
    match elem {
        StreamElement::RescaleBarrier(b) => {
            assert_eq!(b.checkpoint_id, 9);
            assert_eq!(b.operator_id, 3);
            assert_eq!(b.new_parallelism, 16);
            assert_eq!(b.generation, 11);
            assert_eq!(b.global_watermark, None);
            assert_eq!(b.router_state, vec![1, 2, 3]);
        }
        _ => panic!("expected RescaleBarrier"),
    }
}

#[test]
fn test_rescale_barrier_with_global_watermark() {
    let barrier = RescaleBarrier::new(10, 1, 8, 22, vec![]).with_global_watermark(1234);
    assert_eq!(barrier.global_watermark, Some(1234));
}

#[test]
fn test_stream_record_with_timestamp() {
    let rec = StreamRecord::with_timestamp("hello", 999);
    assert_eq!(rec.value, "hello");
    assert_eq!(rec.timestamp, Some(999));
}

#[test]
fn test_stream_data_trait() {
    // Verify common types satisfy StreamData.
    fn assert_stream_data<T: StreamData>() {}
    assert_stream_data::<i32>();
    assert_stream_data::<String>();
    assert_stream_data::<(String, i32)>();
    assert_stream_data::<Vec<u8>>();
}
