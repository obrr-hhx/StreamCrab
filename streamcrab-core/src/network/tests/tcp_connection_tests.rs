use super::*;
use crate::network::frame::{Frame, FrameType};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn test_tcp_connection_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        TcpConnection::from_stream(stream, 8).await.unwrap()
    });

    let (client_conn, _client_rx) = TcpConnection::connect(addr, 8).await.unwrap();
    let (_server_conn, mut server_rx) = server.await.unwrap();

    client_conn
        .send_data(Frame::new(FrameType::Data, 1, vec![42]))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.frame_type, FrameType::Data);
    assert_eq!(received.payload, vec![42]);
}

#[tokio::test]
async fn test_tcp_connection_control_priority() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        TcpConnection::from_stream(stream, 1).await.unwrap()
    });

    let (client_conn, _client_rx) = TcpConnection::connect(addr, 1).await.unwrap();
    let (_server_conn, mut server_rx) = server.await.unwrap();

    client_conn
        .send_data(Frame::new(FrameType::Data, 1, vec![1]))
        .await
        .unwrap();

    let client_clone = client_conn.clone();
    let send_second_data = tokio::spawn(async move {
        client_clone
            .send_data(Frame::new(FrameType::Data, 1, vec![2]))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    client_conn
        .send_control(Frame::new(FrameType::Barrier, 1, vec![9]))
        .unwrap();
    client_conn
        .send_data(Frame::new(FrameType::Data, 1, vec![3]))
        .await
        .unwrap();

    let first = timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let third = timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let fourth = timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();

    send_second_data.await.unwrap();

    let frames = [first, second, third, fourth];
    let barrier_pos = frames
        .iter()
        .position(|f| f.frame_type == FrameType::Barrier)
        .unwrap();
    let data3_pos = frames.iter().position(|f| f.payload == vec![3]).unwrap();

    assert!(
        barrier_pos < data3_pos,
        "control frame should be prioritized before later data frames"
    );
}
