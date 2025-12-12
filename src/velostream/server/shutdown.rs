//! Signal handling and graceful shutdown support
//!
//! This module provides comprehensive signal handling for production deployments,
//! supporting SIGINT (Ctrl+C), SIGTERM (kill/Kubernetes/Docker), SIGHUP (hangup),
//! and SIGQUIT (quit with diagnostics).
//!
//! ## Usage
//!
//! ```rust,no_run
//! use velostream::velostream::server::shutdown::{ShutdownSignal, shutdown_signal};
//! use std::time::Duration;
//!
//! # async fn example() {
//! // Wait for any shutdown signal
//! let signal = shutdown_signal().await;
//! println!("Received {:?}, initiating graceful shutdown...", signal);
//!
//! // Perform cleanup with timeout
//! // server.graceful_shutdown(Duration::from_secs(30)).await;
//! # }
//! ```
//!
//! ## Signals Handled
//!
//! | Signal | Trigger | Default Action |
//! |--------|---------|----------------|
//! | SIGINT | Ctrl+C | Graceful shutdown |
//! | SIGTERM | kill, K8s, Docker | Graceful shutdown |
//! | SIGHUP | Terminal hangup | Graceful shutdown (could be config reload) |
//! | SIGQUIT | Ctrl+\ | Graceful shutdown with state dump |
//!
//! ## Kubernetes/Docker Compatibility
//!
//! Both platforms send SIGTERM first, then SIGKILL after a grace period
//! (default 30s in Kubernetes). This implementation ensures the service
//! responds to SIGTERM by committing Kafka offsets and flushing sinks
//! before the grace period expires.

use log::{info, warn};
use std::fmt;
use std::time::Duration;
use tokio::sync::broadcast;

/// The type of shutdown signal received
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// SIGINT - User interrupt (Ctrl+C)
    Interrupt,
    /// SIGTERM - Termination request (kill, Kubernetes, Docker)
    Terminate,
    /// SIGHUP - Terminal hangup (could trigger config reload)
    Hangup,
    /// SIGQUIT - Quit with core dump (Ctrl+\)
    Quit,
}

impl fmt::Display for ShutdownSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShutdownSignal::Interrupt => write!(f, "SIGINT (Ctrl+C)"),
            ShutdownSignal::Terminate => write!(f, "SIGTERM"),
            ShutdownSignal::Hangup => write!(f, "SIGHUP"),
            ShutdownSignal::Quit => write!(f, "SIGQUIT"),
        }
    }
}

/// Wait for any shutdown signal (SIGINT, SIGTERM, SIGHUP, SIGQUIT)
///
/// Returns the specific signal that was received so the caller can
/// take appropriate action (e.g., dump state on SIGQUIT).
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() {
/// use velostream::velostream::server::shutdown::shutdown_signal;
///
/// let signal = shutdown_signal().await;
/// println!("Received {}, shutting down...", signal);
/// # }
/// ```
#[cfg(unix)]
pub async fn shutdown_signal() -> ShutdownSignal {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
    let mut sighup = signal(SignalKind::hangup()).expect("Failed to install SIGHUP handler");
    let mut sigquit = signal(SignalKind::quit()).expect("Failed to install SIGQUIT handler");

    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM - initiating graceful shutdown");
            ShutdownSignal::Terminate
        }
        _ = sigint.recv() => {
            info!("Received SIGINT (Ctrl+C) - initiating graceful shutdown");
            ShutdownSignal::Interrupt
        }
        _ = sighup.recv() => {
            info!("Received SIGHUP - initiating graceful shutdown");
            ShutdownSignal::Hangup
        }
        _ = sigquit.recv() => {
            info!("Received SIGQUIT - initiating graceful shutdown with diagnostics");
            ShutdownSignal::Quit
        }
    }
}

/// Windows-compatible shutdown signal handler (only handles Ctrl+C)
#[cfg(not(unix))]
pub async fn shutdown_signal() -> ShutdownSignal {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl+C handler");
    info!("Received Ctrl+C - initiating graceful shutdown");
    ShutdownSignal::Interrupt
}

/// Shutdown coordinator for managing graceful shutdown across multiple components
///
/// This provides a broadcast channel that can notify multiple listeners
/// when a shutdown signal is received.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    /// Sender for broadcasting shutdown signal
    sender: broadcast::Sender<ShutdownSignal>,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        Self { sender }
    }

    /// Get a receiver that will be notified when shutdown is triggered
    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.sender.subscribe()
    }

    /// Trigger a shutdown with the given signal
    pub fn trigger(&self, signal: ShutdownSignal) {
        if let Err(e) = self.sender.send(signal) {
            warn!("No shutdown listeners registered: {:?}", e);
        }
    }

    /// Wait for a shutdown signal and then broadcast it to all subscribers
    ///
    /// This is typically run as a background task:
    /// ```rust,no_run
    /// # async fn example() {
    /// use velostream::velostream::server::shutdown::ShutdownCoordinator;
    ///
    /// let coordinator = ShutdownCoordinator::new();
    /// let coord_clone = coordinator.clone();
    ///
    /// tokio::spawn(async move {
    ///     coord_clone.wait_for_signal().await;
    /// });
    ///
    /// // Meanwhile, subscribe to shutdown notifications
    /// let mut rx = coordinator.subscribe();
    /// // ... do work ...
    /// // if let Ok(signal) = rx.recv().await { ... }
    /// # }
    /// ```
    pub async fn wait_for_signal(&self) {
        let signal = shutdown_signal().await;
        self.trigger(signal);
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for graceful shutdown behavior
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Maximum time to wait for graceful shutdown before force killing
    pub timeout: Duration,
    /// Whether to dump diagnostics on SIGQUIT
    pub dump_on_quit: bool,
    /// Whether to log detailed shutdown progress
    pub verbose: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            dump_on_quit: true,
            verbose: true,
        }
    }
}

impl ShutdownConfig {
    /// Create a new shutdown config with the specified timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Default::default()
        }
    }

    /// Set whether to dump diagnostics on SIGQUIT
    pub fn dump_on_quit(mut self, dump: bool) -> Self {
        self.dump_on_quit = dump;
        self
    }

    /// Set verbose logging
    pub fn verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }
}

/// Result of a graceful shutdown attempt
#[derive(Debug, Clone)]
pub struct ShutdownResult {
    /// The signal that triggered the shutdown
    pub signal: ShutdownSignal,
    /// Number of jobs that were successfully stopped
    pub jobs_stopped: usize,
    /// Number of jobs that had to be force-killed
    pub jobs_force_killed: usize,
    /// Whether the shutdown completed within the timeout
    pub completed_gracefully: bool,
    /// Total time taken for shutdown
    pub elapsed: Duration,
}

impl ShutdownResult {
    /// Check if all jobs were stopped gracefully (none force-killed)
    pub fn all_graceful(&self) -> bool {
        self.completed_gracefully && self.jobs_force_killed == 0
    }
}

impl fmt::Display for ShutdownResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.all_graceful() {
            write!(
                f,
                "Graceful shutdown complete: {} jobs stopped in {:?}",
                self.jobs_stopped, self.elapsed
            )
        } else {
            write!(
                f,
                "Shutdown complete: {} graceful, {} force-killed in {:?}",
                self.jobs_stopped, self.jobs_force_killed, self.elapsed
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_signal_display() {
        assert_eq!(format!("{}", ShutdownSignal::Interrupt), "SIGINT (Ctrl+C)");
        assert_eq!(format!("{}", ShutdownSignal::Terminate), "SIGTERM");
        assert_eq!(format!("{}", ShutdownSignal::Hangup), "SIGHUP");
        assert_eq!(format!("{}", ShutdownSignal::Quit), "SIGQUIT");
    }

    #[test]
    fn test_shutdown_config_default() {
        let config = ShutdownConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert!(config.dump_on_quit);
        assert!(config.verbose);
    }

    #[test]
    fn test_shutdown_config_builder() {
        let config = ShutdownConfig::with_timeout(Duration::from_secs(60))
            .dump_on_quit(false)
            .verbose(false);
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert!(!config.dump_on_quit);
        assert!(!config.verbose);
    }

    #[test]
    fn test_shutdown_result_display() {
        let result = ShutdownResult {
            signal: ShutdownSignal::Terminate,
            jobs_stopped: 5,
            jobs_force_killed: 0,
            completed_gracefully: true,
            elapsed: Duration::from_secs(2),
        };
        assert!(result.all_graceful());
        assert!(format!("{}", result).contains("5 jobs stopped"));

        let partial = ShutdownResult {
            signal: ShutdownSignal::Interrupt,
            jobs_stopped: 3,
            jobs_force_killed: 2,
            completed_gracefully: false,
            elapsed: Duration::from_secs(30),
        };
        assert!(!partial.all_graceful());
        assert!(format!("{}", partial).contains("force-killed"));
    }

    #[tokio::test]
    async fn test_shutdown_coordinator() {
        let coordinator = ShutdownCoordinator::new();
        let mut rx = coordinator.subscribe();

        // Trigger shutdown in background
        let coord = coordinator.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coord.trigger(ShutdownSignal::Terminate);
        });

        // Should receive the signal
        let signal = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("Timeout waiting for signal")
            .expect("Channel closed");

        assert_eq!(signal, ShutdownSignal::Terminate);
    }
}
