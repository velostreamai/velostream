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

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::velostream::datasource::DataReader;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::join::JoinSide;

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

/// Configuration for source coordination
#[derive(Debug, Clone)]
pub struct SourceCoordinatorConfig {
    /// Channel buffer size for record queue
    pub channel_buffer_size: usize,
    /// Maximum time to wait when channel is full before dropping
    pub send_timeout: Duration,
    /// Whether to stop all sources on first error
    pub stop_on_error: bool,
    /// Maximum records to read per batch from each source
    pub batch_size: usize,
    /// Polling interval when source has no data
    pub poll_interval: Duration,
}

impl Default for SourceCoordinatorConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: 10_000,
            send_timeout: Duration::from_millis(100),
            stop_on_error: false,
            batch_size: 1000,
            poll_interval: Duration::from_millis(10),
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
    /// Sender side of the record channel
    record_tx: mpsc::Sender<SourceRecord>,
    /// Stop flag for graceful shutdown
    stop_flag: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<SourceCoordinatorStats>,
    /// Handles to spawned reader tasks
    reader_handles: Vec<JoinHandle<()>>,
}

impl SourceCoordinator {
    /// Create a new source coordinator
    ///
    /// Returns the coordinator and a receiver for processing records.
    pub fn new(config: SourceCoordinatorConfig) -> (Self, mpsc::Receiver<SourceRecord>) {
        let (tx, rx) = mpsc::channel(config.channel_buffer_size);

        let coordinator = Self {
            config,
            record_tx: tx,
            stop_flag: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(SourceCoordinatorStats::new()),
            reader_handles: Vec::new(),
        };

        (coordinator, rx)
    }

    /// Add a source reader and start reading from it
    ///
    /// Spawns an async task that reads from the source and sends records
    /// to the shared channel.
    pub fn add_source(&mut self, name: String, side: JoinSide, reader: Box<dyn DataReader>) {
        let tx = self.record_tx.clone();
        let stop_flag = self.stop_flag.clone();
        let stats = self.stats.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            Self::reader_task(name, side, reader, tx, stop_flag, stats, config).await;
        });

        self.reader_handles.push(handle);
    }

    /// Start reading from multiple sources concurrently
    ///
    /// Convenience method to add multiple sources at once.
    pub fn start_sources(&mut self, sources: Vec<(String, JoinSide, Box<dyn DataReader>)>) {
        for (name, side, reader) in sources {
            self.add_source(name, side, reader);
        }
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

                // Try to send with timeout
                match tokio::time::timeout(config.send_timeout, tx.send(source_record)).await {
                    Ok(Ok(())) => {
                        stats.record_sent();
                    }
                    Ok(Err(_)) => {
                        // Channel closed
                        debug!(
                            "SourceCoordinator: Channel closed while sending from '{}'",
                            source_name
                        );
                        return;
                    }
                    Err(_) => {
                        // Timeout - channel full, drop record
                        stats.record_dropped();
                        warn!(
                            "SourceCoordinator: Dropping record from '{}' - channel full",
                            source_name
                        );
                    }
                }
            }
        }

        info!("SourceCoordinator: Reader '{}' finished", source_name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::FieldValue;
    use async_trait::async_trait;
    use std::collections::HashMap;

    /// Mock reader for testing
    struct MockReader {
        records: Vec<StreamRecord>,
        position: usize,
    }

    impl MockReader {
        fn new(records: Vec<StreamRecord>) -> Self {
            Self {
                records,
                position: 0,
            }
        }

        fn from_count(count: usize) -> Self {
            let records: Vec<StreamRecord> = (0..count)
                .map(|i| {
                    let mut fields = HashMap::new();
                    fields.insert("id".to_string(), FieldValue::Integer(i as i64));
                    StreamRecord::new(fields)
                })
                .collect();
            Self::new(records)
        }
    }

    #[async_trait]
    impl DataReader for MockReader {
        async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
            if self.position >= self.records.len() {
                return Ok(vec![]);
            }

            let batch_end = (self.position + 10).min(self.records.len());
            let batch = self.records[self.position..batch_end].to_vec();
            self.position = batch_end;
            Ok(batch)
        }

        async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        async fn seek(
            &mut self,
            _offset: crate::velostream::datasource::SourceOffset,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
            Ok(self.position < self.records.len())
        }
    }

    #[tokio::test]
    async fn test_single_source_reading() {
        let config = SourceCoordinatorConfig::default();
        let (mut coordinator, mut rx) = SourceCoordinator::new(config);

        let reader = Box::new(MockReader::from_count(25));
        coordinator.add_source("test_source".to_string(), JoinSide::Left, reader);

        // Collect records
        let mut received = Vec::new();
        while let Ok(record) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            if let Some(r) = record {
                received.push(r);
            } else {
                break;
            }
        }

        assert_eq!(received.len(), 25);
        assert!(received.iter().all(|r| r.side == JoinSide::Left));
        assert!(received.iter().all(|r| r.source_name == "test_source"));
    }

    #[tokio::test]
    async fn test_dual_source_concurrent_reading() {
        let config = SourceCoordinatorConfig::default();
        let (mut coordinator, mut rx) = SourceCoordinator::new(config);

        let left_reader = Box::new(MockReader::from_count(20));
        let right_reader = Box::new(MockReader::from_count(15));

        coordinator.add_source("left".to_string(), JoinSide::Left, left_reader);
        coordinator.add_source("right".to_string(), JoinSide::Right, right_reader);

        // Collect records
        let mut left_count = 0;
        let mut right_count = 0;

        while let Ok(record) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            if let Some(r) = record {
                match r.side {
                    JoinSide::Left => left_count += 1,
                    JoinSide::Right => right_count += 1,
                }
            } else {
                break;
            }
        }

        assert_eq!(left_count, 20);
        assert_eq!(right_count, 15);
    }

    #[tokio::test]
    async fn test_stop_signal() {
        let config = SourceCoordinatorConfig::default();
        let (mut coordinator, mut rx) = SourceCoordinator::new(config);

        // Create a reader with many records
        let reader = Box::new(MockReader::from_count(1000));
        coordinator.add_source("test".to_string(), JoinSide::Left, reader);

        // Read a few records then stop
        let mut count = 0;
        while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
            count += 1;
            if count >= 10 {
                coordinator.stop();
                break;
            }
        }

        assert!(coordinator.is_stopped());
        assert!(count >= 10);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let config = SourceCoordinatorConfig::default();
        let (mut coordinator, mut rx) = SourceCoordinator::new(config);

        let left_reader = Box::new(MockReader::from_count(10));
        let right_reader = Box::new(MockReader::from_count(5));

        coordinator.add_source("left".to_string(), JoinSide::Left, left_reader);
        coordinator.add_source("right".to_string(), JoinSide::Right, right_reader);

        // Drain the channel
        while let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {}

        let stats = coordinator.stats_snapshot();
        assert_eq!(stats.left_records, 10);
        assert_eq!(stats.right_records, 5);
        assert_eq!(stats.records_sent, 15);
        assert_eq!(stats.records_dropped, 0);
    }

    #[tokio::test]
    async fn test_sequence_numbers() {
        let config = SourceCoordinatorConfig::default();
        let (mut coordinator, mut rx) = SourceCoordinator::new(config);

        let reader = Box::new(MockReader::from_count(10));
        coordinator.add_source("test".to_string(), JoinSide::Left, reader);

        // Collect and verify sequence numbers
        let mut sequences = Vec::new();
        while let Ok(Some(record)) =
            tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
        {
            sequences.push(record.sequence);
        }

        // Sequences should be 1, 2, 3, ... (monotonically increasing)
        assert_eq!(sequences.len(), 10);
        for (i, seq) in sequences.iter().enumerate() {
            assert_eq!(*seq, (i + 1) as u64);
        }
    }
}
