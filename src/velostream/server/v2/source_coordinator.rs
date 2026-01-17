//! Source Coordinator for Stream-Stream Joins
//!
//! Coordinates concurrent reading from multiple data sources for stream-stream joins.
//! Each source runs in its own async task, sending records to a central channel
//! for join processing.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │  Source A   │     │  Source B   │     │  Source N   │
//! │  (async)    │     │  (async)    │     │  (async)    │
//! └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
//!        │                   │                   │
//!        │    ┌──────────────┴──────────────┐    │
//!        └───►│     mpsc::channel           │◄───┘
//!             │   (SourceRecord stream)     │
//!             └──────────────┬──────────────┘
//!                            │
//!                            ▼
//!                   ┌─────────────────┐
//!                   │ Join Processor  │
//!                   └─────────────────┘
//! ```

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::velostream::datasource::DataReader;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::join::JoinSide;

/// Errors that can occur during source coordination
#[derive(Debug)]
pub enum SourceCoordinatorError {
    /// Attempted to add a source after close_sender() was called
    SenderClosed,
    /// Source with the given name already exists
    DuplicateSource(String),
}

impl fmt::Display for SourceCoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceCoordinatorError::SenderClosed => {
                write!(f, "Cannot add source after close_sender() was called")
            }
            SourceCoordinatorError::DuplicateSource(name) => {
                write!(f, "Source '{}' already exists", name)
            }
        }
    }
}

impl Error for SourceCoordinatorError {}

/// A record tagged with its source information
#[derive(Debug, Clone)]
pub struct SourceRecord {
    /// Name of the source this record came from
    pub source_name: String,
    /// Which side of the join this source represents
    pub side: JoinSide,
    /// The actual record
    pub record: StreamRecord,
    /// Sequence number for ordering (per source)
    pub sequence: u64,
}

/// Statistics for source coordination
#[derive(Debug, Default)]
pub struct SourceCoordinatorStats {
    /// Records read from left source
    pub left_records: AtomicU64,
    /// Records read from right source
    pub right_records: AtomicU64,
    /// Total records sent to join processor
    pub records_sent: AtomicU64,
    /// Records dropped due to channel full
    pub records_dropped: AtomicU64,
    /// Number of read errors encountered
    pub read_errors: AtomicU64,
}

impl SourceCoordinatorStats {
    /// Create new stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful read from a side
    pub fn record_read(&self, side: JoinSide) {
        match side {
            JoinSide::Left => self.left_records.fetch_add(1, Ordering::Relaxed),
            JoinSide::Right => self.right_records.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Record a successful send
    pub fn record_sent(&self) {
        self.records_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a dropped record
    pub fn record_dropped(&self) {
        self.records_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a read error
    pub fn record_error(&self) {
        self.read_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get snapshot of stats
    pub fn snapshot(&self) -> SourceCoordinatorStatsSnapshot {
        SourceCoordinatorStatsSnapshot {
            left_records: self.left_records.load(Ordering::Relaxed),
            right_records: self.right_records.load(Ordering::Relaxed),
            records_sent: self.records_sent.load(Ordering::Relaxed),
            records_dropped: self.records_dropped.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of coordinator stats (for reporting)
#[derive(Debug, Clone, Default)]
pub struct SourceCoordinatorStatsSnapshot {
    pub left_records: u64,
    pub right_records: u64,
    pub records_sent: u64,
    pub records_dropped: u64,
    pub read_errors: u64,
}

/// Behavior when channel is full
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureMode {
    /// Block until space is available (recommended for data integrity)
    Block,
    /// Drop records after timeout (fast but loses data)
    DropOnTimeout,
    /// Block with periodic warnings (default - balances integrity with visibility)
    BlockWithWarning,
}

impl Default for BackpressureMode {
    fn default() -> Self {
        BackpressureMode::BlockWithWarning
    }
}

/// Configuration for source coordination
#[derive(Debug, Clone)]
pub struct SourceCoordinatorConfig {
    /// Channel buffer size for record queue
    pub channel_buffer_size: usize,
    /// Timeout for checking stop flag during blocking send
    pub send_check_interval: Duration,
    /// Whether to stop all sources on first error
    pub stop_on_error: bool,
    /// Maximum records to read per batch from each source
    pub batch_size: usize,
    /// Polling interval when source has no data
    pub poll_interval: Duration,
    /// Behavior when channel is full
    pub backpressure_mode: BackpressureMode,
    /// Interval for logging warnings during backpressure (0 = no warnings)
    pub backpressure_warning_interval: Duration,
}

impl Default for SourceCoordinatorConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: 10_000,
            send_check_interval: Duration::from_millis(100),
            stop_on_error: false,
            batch_size: 1000,
            poll_interval: Duration::from_millis(10),
            backpressure_mode: BackpressureMode::BlockWithWarning,
            backpressure_warning_interval: Duration::from_secs(5),
        }
    }
}

impl SourceCoordinatorConfig {
    /// Create config that drops records on timeout (not recommended for production)
    pub fn with_drop_on_timeout(timeout: Duration) -> Self {
        Self {
            backpressure_mode: BackpressureMode::DropOnTimeout,
            send_check_interval: timeout,
            ..Default::default()
        }
    }

    /// Create config that blocks until space is available
    pub fn with_blocking() -> Self {
        Self {
            backpressure_mode: BackpressureMode::Block,
            ..Default::default()
        }
    }
}

/// Coordinates concurrent reading from multiple data sources
///
/// The coordinator spawns one async task per source, each reading records
/// and sending them to a shared channel for join processing.
pub struct SourceCoordinator {
    /// Configuration
    config: SourceCoordinatorConfig,
    /// Sender side of the record channel (Option to allow dropping after sources start)
    record_tx: Option<mpsc::Sender<SourceRecord>>,
    /// Stop flag for graceful shutdown
    stop_flag: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<SourceCoordinatorStats>,
    /// Handles to spawned reader tasks
    reader_handles: Vec<JoinHandle<()>>,
    /// Names of registered sources (to prevent duplicates)
    source_names: HashSet<String>,
}

impl SourceCoordinator {
    /// Create a new source coordinator
    ///
    /// Returns the coordinator and a receiver for processing records.
    pub fn new(config: SourceCoordinatorConfig) -> (Self, mpsc::Receiver<SourceRecord>) {
        let (tx, rx) = mpsc::channel(config.channel_buffer_size);

        let coordinator = Self {
            config,
            record_tx: Some(tx),
            stop_flag: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(SourceCoordinatorStats::new()),
            reader_handles: Vec::new(),
            source_names: HashSet::new(),
        };

        (coordinator, rx)
    }

    /// Add a source reader and start reading from it
    ///
    /// Spawns an async task that reads from the source and sends records
    /// to the shared channel.
    ///
    /// # Errors
    /// - `SenderClosed`: Called after `close_sender()` was invoked
    /// - `DuplicateSource`: A source with this name already exists
    pub fn add_source(
        &mut self,
        name: String,
        side: JoinSide,
        reader: Box<dyn DataReader>,
    ) -> Result<(), SourceCoordinatorError> {
        // Check for duplicate source name
        if self.source_names.contains(&name) {
            return Err(SourceCoordinatorError::DuplicateSource(name));
        }

        let tx = self
            .record_tx
            .as_ref()
            .ok_or(SourceCoordinatorError::SenderClosed)?
            .clone();

        // Register the source name
        self.source_names.insert(name.clone());
        let stop_flag = self.stop_flag.clone();
        let stats = self.stats.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            Self::reader_task(name, side, reader, tx, stop_flag, stats, config).await;
        });

        self.reader_handles.push(handle);
        Ok(())
    }

    /// Close the coordinator's sender after all sources have been added.
    ///
    /// This allows the channel to close naturally when all reader tasks
    /// complete, signaling that all sources are exhausted.
    pub fn close_sender(&mut self) {
        self.record_tx = None;
    }

    /// Start reading from multiple sources concurrently
    ///
    /// Convenience method to add multiple sources at once.
    ///
    /// # Errors
    /// Returns an error if called after `close_sender()` was invoked.
    pub fn start_sources(
        &mut self,
        sources: Vec<(String, JoinSide, Box<dyn DataReader>)>,
    ) -> Result<(), SourceCoordinatorError> {
        for (name, side, reader) in sources {
            self.add_source(name, side, reader)?;
        }
        Ok(())
    }

    /// The reader task that runs for each source
    async fn reader_task(
        source_name: String,
        side: JoinSide,
        mut reader: Box<dyn DataReader>,
        tx: mpsc::Sender<SourceRecord>,
        stop_flag: Arc<AtomicBool>,
        stats: Arc<SourceCoordinatorStats>,
        config: SourceCoordinatorConfig,
    ) {
        let mut sequence: u64 = 0;

        info!(
            "SourceCoordinator: Starting reader for '{}' ({:?})",
            source_name, side
        );

        loop {
            // Check stop flag
            if stop_flag.load(Ordering::Relaxed) {
                debug!(
                    "SourceCoordinator: Stop flag set, shutting down '{}'",
                    source_name
                );
                break;
            }

            // Check if channel is closed
            if tx.is_closed() {
                debug!(
                    "SourceCoordinator: Channel closed, shutting down '{}'",
                    source_name
                );
                break;
            }

            // Read batch of records
            let records = match reader.read().await {
                Ok(records) => records,
                Err(e) => {
                    error!(
                        "SourceCoordinator: Error reading from '{}': {}",
                        source_name, e
                    );
                    stats.record_error();

                    if config.stop_on_error {
                        stop_flag.store(true, Ordering::Relaxed);
                        break;
                    }

                    // Brief pause before retry
                    tokio::time::sleep(config.poll_interval).await;
                    continue;
                }
            };

            // If no records, check if source is exhausted or just idle
            if records.is_empty() {
                match reader.has_more().await {
                    Ok(true) => {
                        // Source is idle, wait briefly
                        tokio::time::sleep(config.poll_interval).await;
                        continue;
                    }
                    Ok(false) => {
                        // Source is exhausted
                        info!(
                            "SourceCoordinator: Source '{}' exhausted after {} records",
                            source_name,
                            match side {
                                JoinSide::Left => stats.left_records.load(Ordering::Relaxed),
                                JoinSide::Right => stats.right_records.load(Ordering::Relaxed),
                            }
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "SourceCoordinator: Error checking has_more for '{}': {}",
                            source_name, e
                        );
                        // Assume more data might be coming
                        tokio::time::sleep(config.poll_interval).await;
                        continue;
                    }
                }
            }

            // Send each record to the channel
            for record in records {
                stats.record_read(side);
                sequence += 1;

                let source_record = SourceRecord {
                    source_name: source_name.clone(),
                    side,
                    record,
                    sequence,
                };

                // Send with backpressure handling
                if !Self::send_with_backpressure(
                    &tx,
                    source_record,
                    &stop_flag,
                    &stats,
                    &config,
                    &source_name,
                )
                .await
                {
                    // Channel closed or stop requested
                    return;
                }
            }
        }

        info!("SourceCoordinator: Reader '{}' finished", source_name);
    }

    /// Send a record with backpressure handling based on configuration
    ///
    /// Returns `true` if send was successful, `false` if channel closed or stop requested.
    async fn send_with_backpressure(
        tx: &mpsc::Sender<SourceRecord>,
        record: SourceRecord,
        stop_flag: &Arc<AtomicBool>,
        stats: &Arc<SourceCoordinatorStats>,
        config: &SourceCoordinatorConfig,
        source_name: &str,
    ) -> bool {
        match config.backpressure_mode {
            BackpressureMode::DropOnTimeout => {
                // Original behavior: drop on timeout
                match tokio::time::timeout(config.send_check_interval, tx.send(record)).await {
                    Ok(Ok(())) => {
                        stats.record_sent();
                        true
                    }
                    Ok(Err(_)) => {
                        debug!(
                            "SourceCoordinator: Channel closed while sending from '{}'",
                            source_name
                        );
                        false
                    }
                    Err(_) => {
                        stats.record_dropped();
                        warn!(
                            "SourceCoordinator: Dropping record from '{}' - channel full (DropOnTimeout mode)",
                            source_name
                        );
                        true // Continue reading even though we dropped
                    }
                }
            }
            BackpressureMode::Block => {
                // Block until space available, checking stop flag periodically
                // Use reserve() to avoid losing the record on timeout
                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        return false;
                    }

                    match tokio::time::timeout(config.send_check_interval, tx.reserve()).await {
                        Ok(Ok(permit)) => {
                            permit.send(record);
                            stats.record_sent();
                            return true;
                        }
                        Ok(Err(_)) => {
                            debug!(
                                "SourceCoordinator: Channel closed while sending from '{}'",
                                source_name
                            );
                            return false;
                        }
                        Err(_) => {
                            // Timeout - retry (record not consumed yet)
                            continue;
                        }
                    }
                }
            }
            BackpressureMode::BlockWithWarning => {
                // Block with periodic warnings
                // Use reserve() to avoid losing the record on timeout
                let mut last_warning = std::time::Instant::now();
                let mut blocked_count = 0u64;

                loop {
                    if stop_flag.load(Ordering::Relaxed) {
                        return false;
                    }

                    match tokio::time::timeout(config.send_check_interval, tx.reserve()).await {
                        Ok(Ok(permit)) => {
                            if blocked_count > 0 {
                                debug!(
                                    "SourceCoordinator: Backpressure released for '{}' after {} retries",
                                    source_name, blocked_count
                                );
                            }
                            permit.send(record);
                            stats.record_sent();
                            return true;
                        }
                        Ok(Err(_)) => {
                            debug!(
                                "SourceCoordinator: Channel closed while sending from '{}'",
                                source_name
                            );
                            return false;
                        }
                        Err(_) => {
                            // Timeout - retry (record not consumed yet)
                            blocked_count += 1;

                            // Log warning periodically
                            if config.backpressure_warning_interval > Duration::ZERO
                                && last_warning.elapsed() >= config.backpressure_warning_interval
                            {
                                warn!(
                                    "SourceCoordinator: Backpressure on '{}' - channel full, blocked {} times. Consider increasing channel_buffer_size or processing faster.",
                                    source_name, blocked_count
                                );
                                last_warning = std::time::Instant::now();
                            }
                        }
                    }
                }
            }
        }
    }

    /// Signal all readers to stop
    pub fn stop(&self) {
        info!("SourceCoordinator: Signaling stop to all readers");
        self.stop_flag.store(true, Ordering::Relaxed);
    }

    /// Check if stop has been signaled
    pub fn is_stopped(&self) -> bool {
        self.stop_flag.load(Ordering::Relaxed)
    }

    /// Wait for all readers to complete
    pub async fn wait_for_completion(&mut self) {
        for handle in self.reader_handles.drain(..) {
            let _ = handle.await;
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &SourceCoordinatorStats {
        &self.stats
    }

    /// Get a snapshot of current statistics
    pub fn stats_snapshot(&self) -> SourceCoordinatorStatsSnapshot {
        self.stats.snapshot()
    }
}

impl Drop for SourceCoordinator {
    fn drop(&mut self) {
        // Signal stop on drop
        self.stop();
    }
}
