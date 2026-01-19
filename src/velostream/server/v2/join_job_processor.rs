//! Join Job Processor
//!
//! Specialized processor for stream-stream join queries that:
//! 1. Uses SourceCoordinator for concurrent source reading
//! 2. Processes records through IntervalJoinProcessor
//! 3. Writes joined results to output
//!
//! This processor is used when a query contains a stream-stream join
//! (as opposed to stream-table joins which use the standard processor).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use log::{debug, error, info, warn};
use tokio::sync::Mutex;

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::server::processors::common::JobExecutionStats;
use crate::velostream::server::v2::source_coordinator::{
    SourceCoordinator, SourceCoordinatorConfig,
};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::join::{JoinCoordinatorStats, JoinSide};
use crate::velostream::sql::execution::processors::{IntervalJoinConfig, IntervalJoinProcessor};

/// Configuration for join job processing
#[derive(Debug, Clone)]
pub struct JoinJobConfig {
    /// Join configuration
    pub join_config: IntervalJoinConfig,
    /// Source coordinator configuration
    pub source_config: SourceCoordinatorConfig,
    /// Batch size for writing output
    pub output_batch_size: usize,
    /// Maximum time to run (0 = unlimited)
    pub timeout: Duration,
}

impl Default for JoinJobConfig {
    fn default() -> Self {
        Self {
            join_config: IntervalJoinConfig::new("left", "right"),
            source_config: SourceCoordinatorConfig::default(),
            output_batch_size: 1000,
            timeout: Duration::ZERO,
        }
    }
}

/// Statistics for join job execution
#[derive(Debug, Clone, Default)]
pub struct JoinJobStats {
    // === Basic Processing Stats ===
    pub left_records_read: u64,
    pub right_records_read: u64,
    pub join_matches: u64,
    pub records_written: u64,
    pub processing_time_ms: u64,

    // === State Store Stats (from JoinCoordinatorStats) ===
    /// Current records in left state store
    pub left_store_size: usize,
    /// Current records in right state store
    pub right_store_size: usize,
    /// Records evicted from left store due to limits
    pub left_evictions: u64,
    /// Records evicted from right store due to limits
    pub right_evictions: u64,

    // === Memory Interning Stats ===
    /// Number of unique keys interned
    pub interned_key_count: usize,
    /// Estimated memory saved by string interning (bytes)
    pub interning_memory_saved: usize,

    // === Error Stats ===
    /// Records with missing join keys
    pub missing_key_count: u64,
    /// Records with missing event time
    pub missing_time_count: u64,

    // === Window Stats (for window joins) ===
    /// Windows closed
    pub windows_closed: u64,
    /// Currently active windows
    pub active_windows: usize,
}

impl JoinJobStats {
    /// Convert to JobExecutionStats for compatibility
    pub fn to_execution_stats(&self) -> JobExecutionStats {
        let mut stats = JobExecutionStats::new();
        stats.records_processed = self.left_records_read + self.right_records_read;
        // Note: JobExecutionStats doesn't have records_output, but records_processed covers both
        stats.total_processing_time = std::time::Duration::from_millis(self.processing_time_ms);
        stats
    }

    /// Update stats from JoinCoordinatorStats
    ///
    /// This copies state store metrics, eviction counts, and interning stats
    /// from the join coordinator into these job-level stats.
    pub fn update_from_coordinator(&mut self, coord_stats: &JoinCoordinatorStats) {
        // State store sizes
        self.left_store_size = coord_stats.left_store_size;
        self.right_store_size = coord_stats.right_store_size;

        // Eviction counts
        self.left_evictions = coord_stats.left_evictions;
        self.right_evictions = coord_stats.right_evictions;

        // Memory interning stats
        self.interned_key_count = coord_stats.interned_key_count;
        self.interning_memory_saved = coord_stats.interning_memory_saved;

        // Error counts
        self.missing_key_count = coord_stats.missing_key_count;
        self.missing_time_count = coord_stats.missing_time_count;

        // Window stats
        self.windows_closed = coord_stats.windows_closed;
        self.active_windows = coord_stats.active_windows;
    }
}

/// Processor for stream-stream join jobs
///
/// Returns `JoinJobStats` which can be pushed to `MetricsProvider.update_join_metrics()`
/// for Prometheus exposure.
pub struct JoinJobProcessor {
    /// Configuration
    config: JoinJobConfig,
    /// Stop flag for graceful shutdown
    stop_flag: Arc<AtomicBool>,
}

impl JoinJobProcessor {
    /// Create a new join job processor
    pub fn new(config: JoinJobConfig) -> Self {
        Self {
            config,
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create with default configuration
    pub fn with_join_config(join_config: IntervalJoinConfig) -> Self {
        Self::new(JoinJobConfig {
            join_config,
            ..Default::default()
        })
    }

    /// Process a stream-stream join job
    ///
    /// # Arguments
    /// * `left_reader` - Reader for the left source
    /// * `right_reader` - Reader for the right source
    /// * `writer` - Writer for join output (optional)
    ///
    /// # Returns
    /// Statistics about the join execution
    pub async fn process_join(
        &self,
        left_name: String,
        left_reader: Box<dyn DataReader>,
        right_name: String,
        right_reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
    ) -> Result<JoinJobStats, SqlError> {
        let start_time = Instant::now();
        let mut stats = JoinJobStats::default();

        info!(
            "JoinJobProcessor: Starting join between '{}' and '{}'",
            left_name, right_name
        );

        // Create the interval join processor
        let mut join_processor = IntervalJoinProcessor::new(self.config.join_config.clone());

        // Create the source coordinator
        let (mut coordinator, mut rx) = SourceCoordinator::new(self.config.source_config.clone());

        // Add both sources
        coordinator
            .add_source(left_name.clone(), JoinSide::Left, left_reader)
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to add left source: {}", e),
                query: None,
            })?;
        coordinator
            .add_source(right_name.clone(), JoinSide::Right, right_reader)
            .map_err(|e| SqlError::ExecutionError {
                message: format!("Failed to add right source: {}", e),
                query: None,
            })?;

        // Close coordinator's sender so channel closes when readers finish
        coordinator.close_sender();

        // Wrap writer for async access
        let writer = writer.map(|w| Arc::new(Mutex::new(w)));

        // Output buffer for batching writes
        let mut output_buffer: Vec<StreamRecord> =
            Vec::with_capacity(self.config.output_batch_size);

        // Process records as they arrive
        loop {
            // Check stop conditions
            if self.stop_flag.load(Ordering::Relaxed) {
                info!("JoinJobProcessor: Stop signal received");
                coordinator.stop();
                break;
            }

            if self.config.timeout > Duration::ZERO && start_time.elapsed() > self.config.timeout {
                info!("JoinJobProcessor: Timeout reached");
                coordinator.stop();
                break;
            }

            // Receive next record (with timeout to check stop conditions)
            let record = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;

            match record {
                Ok(Some(source_record)) => {
                    // Update stats
                    match source_record.side {
                        JoinSide::Left => stats.left_records_read += 1,
                        JoinSide::Right => stats.right_records_read += 1,
                    }

                    // Process through join
                    let joined = join_processor
                        .process(source_record.side, source_record.record)
                        .map_err(|e| SqlError::ExecutionError {
                            message: format!("Join processing error: {}", e),
                            query: None,
                        })?;

                    stats.join_matches += joined.len() as u64;

                    // Buffer output
                    for record in joined {
                        output_buffer.push(record);

                        // Flush when buffer is full
                        if output_buffer.len() >= self.config.output_batch_size {
                            if let Some(ref writer) = writer {
                                let mut w = writer.lock().await;
                                for rec in output_buffer.drain(..) {
                                    w.write(rec).await.map_err(|e| SqlError::ExecutionError {
                                        message: format!("Write error: {}", e),
                                        query: None,
                                    })?;
                                    stats.records_written += 1;
                                }
                            } else {
                                stats.records_written += output_buffer.len() as u64;
                                output_buffer.clear();
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Channel closed - all sources exhausted
                    debug!("JoinJobProcessor: All sources exhausted");
                    break;
                }
                Err(_) => {
                    // Timeout - continue polling (channel will close when sources finish)
                    continue;
                }
            }
        }

        // Flush remaining output
        if !output_buffer.is_empty() {
            if let Some(ref writer) = writer {
                let mut w = writer.lock().await;
                for rec in output_buffer.drain(..) {
                    w.write(rec).await.map_err(|e| SqlError::ExecutionError {
                        message: format!("Write error: {}", e),
                        query: None,
                    })?;
                    stats.records_written += 1;
                }
                w.flush().await.map_err(|e| SqlError::ExecutionError {
                    message: format!("Flush error: {}", e),
                    query: None,
                })?;
            } else {
                stats.records_written += output_buffer.len() as u64;
            }
        }

        // Wait for coordinator to finish
        coordinator.wait_for_completion().await;

        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;

        // Copy coordinator stats (state store sizes, evictions, interning)
        // Caller can push these stats to MetricsProvider.update_join_metrics()
        stats.update_from_coordinator(join_processor.stats());

        info!(
            "JoinJobProcessor: Completed - {} left + {} right records, {} joins, {} output in {}ms",
            stats.left_records_read,
            stats.right_records_read,
            stats.join_matches,
            stats.records_written,
            stats.processing_time_ms
        );
        debug!(
            "JoinJobProcessor: State store: left={}, right={}, evicted: left={}, right={}, interned keys={}, memory saved={} bytes",
            stats.left_store_size,
            stats.right_store_size,
            stats.left_evictions,
            stats.right_evictions,
            stats.interned_key_count,
            stats.interning_memory_saved
        );

        Ok(stats)
    }

    /// Signal the processor to stop
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }

    /// Check if stop has been signaled
    pub fn is_stopped(&self) -> bool {
        self.stop_flag.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::FieldValue;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::error::Error;

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

        fn with_key_field(count: usize, key_name: &str, key_start: i64) -> Self {
            let records: Vec<StreamRecord> = (0..count)
                .map(|i| {
                    let mut fields = HashMap::new();
                    fields.insert(
                        key_name.to_string(),
                        FieldValue::Integer(key_start + i as i64),
                    );
                    fields.insert(
                        "event_time".to_string(),
                        FieldValue::Integer(1000 + i as i64 * 100),
                    );
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
            let batch_end = (self.position + 5).min(self.records.len());
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

    /// Mock writer for testing
    struct MockWriter {
        records: Arc<Mutex<Vec<StreamRecord>>>,
    }

    impl MockWriter {
        fn new() -> (Self, Arc<Mutex<Vec<StreamRecord>>>) {
            let records = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    records: records.clone(),
                },
                records,
            )
        }
    }

    #[async_trait]
    impl DataWriter for MockWriter {
        async fn write(
            &mut self,
            record: StreamRecord,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.records.lock().await.push(record);
            Ok(())
        }

        async fn write_batch(
            &mut self,
            records: Vec<std::sync::Arc<StreamRecord>>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            let mut locked = self.records.lock().await;
            for record in records {
                locked.push((*record).clone());
            }
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        async fn update(
            &mut self,
            _key: &str,
            record: StreamRecord,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.records.lock().await.push(record);
            Ok(())
        }

        async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }

        async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_join_job_basic() {
        // Create matching records on both sides
        let left_reader = Box::new(MockReader::with_key_field(5, "order_id", 100));
        let right_reader = Box::new(MockReader::with_key_field(5, "order_id", 100));

        let join_config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let processor = JoinJobProcessor::with_join_config(join_config);

        let stats = processor
            .process_join(
                "orders".to_string(),
                left_reader,
                "shipments".to_string(),
                right_reader,
                None,
            )
            .await
            .unwrap();

        assert_eq!(stats.left_records_read, 5);
        assert_eq!(stats.right_records_read, 5);
        // Should have some joins (exact number depends on timing)
        assert!(stats.join_matches > 0);
    }

    #[tokio::test]
    async fn test_join_job_with_writer() {
        let left_reader = Box::new(MockReader::with_key_field(3, "id", 1));
        let right_reader = Box::new(MockReader::with_key_field(3, "id", 1));
        let (writer, output) = MockWriter::new();

        let join_config = IntervalJoinConfig::new("left", "right")
            .with_key("id", "id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let processor = JoinJobProcessor::with_join_config(join_config);

        let stats = processor
            .process_join(
                "left".to_string(),
                left_reader,
                "right".to_string(),
                right_reader,
                Some(Box::new(writer)),
            )
            .await
            .unwrap();

        // Verify output was written
        let written = output.lock().await;
        assert_eq!(written.len(), stats.records_written as usize);
    }

    #[tokio::test]
    async fn test_join_job_no_matches() {
        // Create non-matching records (different key ranges)
        let left_reader = Box::new(MockReader::with_key_field(5, "id", 1));
        let right_reader = Box::new(MockReader::with_key_field(5, "id", 100)); // Different keys

        let join_config = IntervalJoinConfig::new("left", "right")
            .with_key("id", "id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        let processor = JoinJobProcessor::with_join_config(join_config);

        let stats = processor
            .process_join(
                "left".to_string(),
                left_reader,
                "right".to_string(),
                right_reader,
                None,
            )
            .await
            .unwrap();

        assert_eq!(stats.left_records_read, 5);
        assert_eq!(stats.right_records_read, 5);
        assert_eq!(stats.join_matches, 0); // No matches
    }
}
