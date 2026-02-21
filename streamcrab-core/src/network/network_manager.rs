use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use anyhow::{Result, anyhow};
use tokio::sync::mpsc;

use super::frame::Frame;
use super::tcp_connection::TcpConnection;

/// Maintains outgoing TCP connections to peer task managers.
pub struct NetworkManager {
    data_capacity: usize,
    connections: RwLock<HashMap<String, TcpConnection>>,
}

#[cfg(test)]
#[path = "tests/network_manager_tests.rs"]
mod tests;

impl NetworkManager {
    pub fn new(data_capacity: usize) -> Self {
        Self {
            data_capacity,
            connections: RwLock::new(HashMap::new()),
        }
    }

    pub async fn connect(&self, tm_id: String, addr: SocketAddr) -> Result<mpsc::Receiver<Frame>> {
        let (conn, rx) = TcpConnection::connect(addr, self.data_capacity).await?;
        self.connections
            .write()
            .expect("connections poisoned")
            .insert(tm_id, conn);
        Ok(rx)
    }

    pub fn insert_connection(&self, tm_id: String, connection: TcpConnection) {
        self.connections
            .write()
            .expect("connections poisoned")
            .insert(tm_id, connection);
    }

    pub fn remove_connection(&self, tm_id: &str) -> Option<TcpConnection> {
        self.connections
            .write()
            .expect("connections poisoned")
            .remove(tm_id)
    }

    pub fn connection_count(&self) -> usize {
        self.connections.read().expect("connections poisoned").len()
    }

    pub async fn send_data(&self, tm_id: &str, frame: Frame) -> Result<()> {
        let conn = self
            .connections
            .read()
            .expect("connections poisoned")
            .get(tm_id)
            .cloned()
            .ok_or_else(|| anyhow!("connection to {} not found", tm_id))?;
        conn.send_data(frame).await
    }

    pub fn send_control(&self, tm_id: &str, frame: Frame) -> Result<()> {
        let conn = self
            .connections
            .read()
            .expect("connections poisoned")
            .get(tm_id)
            .cloned()
            .ok_or_else(|| anyhow!("connection to {} not found", tm_id))?;
        conn.send_control(frame)
    }
}
