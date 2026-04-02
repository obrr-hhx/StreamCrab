//! Graceful shutdown coordinator for StreamCrab.
//!
//! Graceful shutdown sequence:
//! 1. Receive SIGTERM/Ctrl-C
//! 2. Source stops consuming (no more poll)
//! 3. Drain all in-flight data
//! 4. Trigger final checkpoint
//! 5. Wait for checkpoint completion
//! 6. Sink commit
//! 7. Close all tasks

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Shutdown signal that can be shared across tasks.
#[derive(Clone)]
pub struct ShutdownSignal {
    /// Whether shutdown has been requested.
    requested: Arc<AtomicBool>,
    /// Notifier to wake tasks blocked on input.
    notify: Arc<Notify>,
}

impl ShutdownSignal {
    pub fn new() -> Self {
        Self {
            requested: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Request shutdown. This is idempotent.
    pub fn request_shutdown(&self) {
        self.requested.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// Wait until shutdown is requested.
    pub async fn wait_for_shutdown(&self) {
        if self.is_shutdown_requested() {
            return;
        }
        self.notify.notified().await;
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Install a Ctrl-C / SIGTERM handler that triggers the given shutdown signal.
///
/// Returns a JoinHandle for the signal listener task.
pub fn install_signal_handler(signal: ShutdownSignal) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl-C handler");
        tracing::info!("Shutdown signal received, initiating graceful shutdown...");
        signal.request_shutdown();
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[test]
    fn test_new_signal_not_requested() {
        let signal = ShutdownSignal::new();
        assert!(!signal.is_shutdown_requested());
    }

    #[test]
    fn test_request_shutdown() {
        let signal = ShutdownSignal::new();
        signal.request_shutdown();
        assert!(signal.is_shutdown_requested());
    }

    #[test]
    fn test_request_shutdown_idempotent() {
        let signal = ShutdownSignal::new();
        signal.request_shutdown();
        signal.request_shutdown();
        assert!(signal.is_shutdown_requested());
    }

    #[test]
    fn test_clone_shares_state() {
        let signal = ShutdownSignal::new();
        let cloned = signal.clone();
        signal.request_shutdown();
        assert!(cloned.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_wait_for_shutdown_returns_immediately_if_already_requested() {
        let signal = ShutdownSignal::new();
        signal.request_shutdown();
        // Should return without blocking since shutdown is already requested.
        timeout(Duration::from_millis(100), signal.wait_for_shutdown())
            .await
            .expect("wait_for_shutdown should return immediately");
    }

    #[tokio::test]
    async fn test_wait_for_shutdown_wakes_on_request() {
        let signal = ShutdownSignal::new();
        let waiter = signal.clone();

        let handle = tokio::spawn(async move {
            waiter.wait_for_shutdown().await;
        });

        // Give the spawned task time to block on notified().
        tokio::time::sleep(Duration::from_millis(10)).await;
        signal.request_shutdown();

        timeout(Duration::from_millis(200), handle)
            .await
            .expect("task should complete after shutdown request")
            .expect("task should not panic");
    }
}
