use std::net::SocketAddr;

use anyhow::{Result, anyhow};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::frame::{Frame, read_frame, write_frame};

/// Bidirectional TCP connection with control/data priority queues.
#[derive(Clone)]
pub struct TcpConnection {
    peer_addr: SocketAddr,
    control_tx: mpsc::UnboundedSender<Frame>,
    data_tx: mpsc::Sender<Frame>,
}

impl TcpConnection {
    pub async fn connect(
        peer_addr: SocketAddr,
        data_capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<Frame>)> {
        let stream = TcpStream::connect(peer_addr).await?;
        Self::from_stream(stream, data_capacity).await
    }

    pub async fn from_stream(
        stream: TcpStream,
        data_capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<Frame>)> {
        let peer_addr = stream.peer_addr()?;
        let (mut read_half, mut write_half) = stream.into_split();

        let (incoming_tx, incoming_rx) = mpsc::channel::<Frame>(data_capacity);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel::<Frame>();
        let (data_tx, mut data_rx) = mpsc::channel::<Frame>(data_capacity);

        tokio::spawn(async move {
            loop {
                match read_frame(&mut read_half).await {
                    Ok(frame) => {
                        if incoming_tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::spawn(async move {
            loop {
                while let Ok(frame) = control_rx.try_recv() {
                    if write_frame(&mut write_half, &frame).await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    biased;
                    maybe_control = control_rx.recv() => {
                        match maybe_control {
                            Some(frame) => {
                                if write_frame(&mut write_half, &frame).await.is_err() {
                                    return;
                                }
                            }
                            None => {
                                if data_rx.is_closed() {
                                    return;
                                }
                            }
                        }
                    }
                    maybe_data = data_rx.recv() => {
                        match maybe_data {
                            Some(frame) => {
                                // Preempt data write if control frames arrived after select.
                                while let Ok(control_frame) = control_rx.try_recv() {
                                    if write_frame(&mut write_half, &control_frame).await.is_err() {
                                        return;
                                    }
                                }
                                if write_frame(&mut write_half, &frame).await.is_err() {
                                    return;
                                }
                            }
                            None => {
                                if control_rx.is_closed() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok((
            Self {
                peer_addr,
                control_tx,
                data_tx,
            },
            incoming_rx,
        ))
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn send_control(&self, frame: Frame) -> Result<()> {
        self.control_tx
            .send(frame)
            .map_err(|_| anyhow!("control queue closed"))
    }

    pub async fn send_data(&self, frame: Frame) -> Result<()> {
        self.data_tx
            .send(frame)
            .await
            .map_err(|_| anyhow!("data queue closed"))
    }
}

#[cfg(test)]
#[path = "tests/tcp_connection_tests.rs"]
mod tests;
