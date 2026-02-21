use crate::network::frame::{Frame, FrameType};
use crate::network::network_manager::NetworkManager;
use crate::network::tcp_connection::TcpConnection;
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn test_network_manager_connect_and_send() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        TcpConnection::from_stream(stream, 8).await.unwrap()
    });

    let manager = NetworkManager::new(8);
    let _client_rx = manager.connect("tm-1".to_string(), addr).await.unwrap();
    let (_server_conn, mut server_rx) = server.await.unwrap();

    manager
        .send_data("tm-1", Frame::new(FrameType::Data, 3, vec![7]))
        .await
        .unwrap();
    manager
        .send_control("tm-1", Frame::new(FrameType::Control, 3, vec![8]))
        .unwrap();

    let first = timeout(Duration::from_secs(1), server_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.channel_id, 3);
}

#[test]
fn test_network_manager_remove_connection() {
    let manager = NetworkManager::new(8);
    assert_eq!(manager.connection_count(), 0);
    assert!(manager.remove_connection("missing").is_none());
}
