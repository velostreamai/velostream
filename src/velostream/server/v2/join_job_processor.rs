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

use log::{debug, info};
use tokio::sync::Mutex;

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::observability::trace_propagation;
use crate::velostream::observability::trace_propagation::SamplingDecision;
use crate::velostream::server::processors::SharedJobStats;
use crate::velostream::server::processors::common::JobExecutionStats;
use crate::velostream::server::processors::metrics_helper::{
    ProcessorMetricsHelper, extract_job_name,
};
use crate::velostream::server::processors::observability_helper::ObservabilityHelper;
use crate::velostream::server::v2::source_coordinator::{
    SourceCoordinator, SourceCoordinatorConfig,
};
use crate::velostream::sql::StreamingQuery;
use crate::velostream::sql::ast::SelectField;
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
    /// SQL SELECT projection to apply after join (optional)
    /// When set, only the specified fields will be output with aliases applied
    pub projection: Option<Vec<SelectField>>,
}

impl Default for JoinJobConfig {
    fn default() -> Self {
        Self {
            join_config: IntervalJoinConfig::new("left", "right"),
            source_config: SourceCoordinatorConfig::default(),
            output_batch_size: 1000,
            timeout: Duration::ZERO,
            projection: None,
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

    /// Flush the output buffer: emit metrics, write records, optionally flush the writer.
    ///
    /// Join outputs already carry traceparent headers via FR-090 (join processor uses
    /// `with_headers_from`), so no span creation or injection is needed at flush time.
    async fn flush_output_buffer(
        output_buffer: &mut Vec<Arc<StreamRecord>>,
        writer: &Option<Arc<Mutex<Box<dyn DataWriter>>>>,
        metrics_helper: &ProcessorMetricsHelper,
        query: &Option<Arc<StreamingQuery>>,
        observability: &Option<SharedObservabilityManager>,
        queue: &Option<
            std::sync::Arc<crate::velostream::observability::async_queue::ObservabilityQueue>,
        >,
        job_name_for_metrics: &Option<String>,
        stats: &mut JoinJobStats,
        flush_writer: bool,
    ) -> Result<(), SqlError> {
        // Emit metrics for the batch
        if let (Some(q), Some(jn)) = (query.as_ref(), job_name_for_metrics.as_ref()) {
            metrics_helper
                .emit_all_metrics(q, output_buffer, observability, queue, jn)
                .await;
        }

        let record_count = output_buffer.len();
        let job_name = job_name_for_metrics.as_deref().unwrap_or("join");

        if let Some(writer) = writer {
            let ser_start = Instant::now();
            let mut w = writer.lock().await;
            for rec in output_buffer.drain(..) {
                let owned = Arc::try_unwrap(rec).unwrap_or_else(|arc| (*arc).clone());
                w.write(owned).await.map_err(|e| SqlError::ExecutionError {
                    message: format!("Write error: {}", e),
                    query: None,
                })?;
                stats.records_written += 1;
            }
            if flush_writer {
                w.flush().await.map_err(|e| SqlError::ExecutionError {
                    message: format!("Flush error: {}", e),
                    query: None,
                })?;
            }
            let ser_elapsed = ser_start.elapsed();
            // Record serialization metrics
            if record_count > 0 {
                ObservabilityHelper::record_serialization_success(
                    observability,
                    job_name,
                    record_count,
                    ser_elapsed.as_millis() as u64,
                );
            }
        } else {
            stats.records_written += output_buffer.len() as u64;
            output_buffer.clear();
        }

        Ok(())
    }

    /// Process a stream-stream join job
    ///
    /// # Arguments
    /// * `left_reader` - Reader for the left source
    /// * `right_reader` - Reader for the right source
    /// * `writer` - Writer for join output (optional)
    /// * `shared_stats` - Shared stats for real-time monitoring (optional)
    /// * `observability` - Optional observability manager for Prometheus metrics
    /// * `query` - Optional parsed query with @metric annotations
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
        shared_stats: Option<SharedJobStats>,
        observability: Option<SharedObservabilityManager>,
        query: Option<Arc<StreamingQuery>>,
        app_name: Option<String>,
    ) -> Result<JoinJobStats, SqlError> {
        let start_time = Instant::now();
        let mut stats = JoinJobStats::default();
        let mut last_stats_update = Instant::now();

        info!(
            "JoinJobProcessor: Starting join between '{}' and '{}'",
            left_name, right_name
        );

        // Set up metrics helper and register SQL-annotated metrics if query is provided
        let mut metrics_helper = ProcessorMetricsHelper::new();
        if let Some(name) = app_name {
            metrics_helper.set_app_name(name);
        }
        let job_name_for_metrics = query.as_ref().map(|q| extract_job_name(q));

        if let (Some(q), Some(jn)) = (query.as_ref(), job_name_for_metrics.as_ref()) {
            metrics_helper
                .register_all_metrics(q, &observability, jn)
                .await;
        }

        // Create the interval join processor
        let mut join_processor = IntervalJoinProcessor::new(self.config.join_config.clone());

        // Apply projection if specified
        if let Some(projection) = &self.config.projection {
            join_processor = join_processor.with_projection(projection.clone());
        }

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

        // Output buffer for batching writes — stored as Arc to avoid cloning for metrics emission
        let mut output_buffer: Vec<Arc<StreamRecord>> =
            Vec::with_capacity(self.config.output_batch_size);

        // Get sampling ratio once for the entire join job.
        // Only run sampling/injection when tracing is enabled.
        let tracing_enabled = observability.is_some();
        let sampling_ratio = ObservabilityHelper::sampling_ratio(&observability);
        let job_name_for_spans = job_name_for_metrics.as_deref().unwrap_or("join");

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

                    // Periodically update shared_stats for real-time monitoring (every 500ms)
                    if let Some(ref shared) = shared_stats {
                        if last_stats_update.elapsed() > Duration::from_millis(500) {
                            if let Ok(mut s) = shared.write() {
                                s.records_processed =
                                    stats.left_records_read + stats.right_records_read;
                            }
                            last_stats_update = Instant::now();
                        }
                    }

                    // Process through join
                    let joined = join_processor
                        .process(source_record.side, source_record.record)
                        .map_err(|e| SqlError::ExecutionError {
                            message: format!("Join processing error: {}", e),
                            query: None,
                        })?;

                    stats.join_matches += joined.len() as u64;

                    // Buffer output with per-record sampling
                    for record in joined {
                        let mut out = Arc::new(record);

                        if tracing_enabled {
                            // Per-record sampling on join output: check merged traceparent
                            let sampling_decision =
                                trace_propagation::extract_trace_context_if_sampled(
                                    &out.headers,
                                    sampling_ratio,
                                );

                            match &sampling_decision {
                                SamplingDecision::Sampled(upstream_ctx) => {
                                    // Create a per-record span and inject sampled context
                                    if let Some(span) = ObservabilityHelper::start_record_span(
                                        &observability,
                                        job_name_for_spans,
                                        upstream_ctx.as_ref(),
                                    ) {
                                        ObservabilityHelper::inject_record_trace_context(
                                            &span, &mut out,
                                        );
                                        // span drops here → ends the span
                                    }
                                }
                                SamplingDecision::NotSampledNew => {
                                    // No traceparent after header merge — inject flag=00
                                    ObservabilityHelper::inject_not_sampled_context(&mut out);
                                }
                                SamplingDecision::NotSampled => {
                                    // Upstream traceparent already has flag=00, no action needed
                                }
                            }
                        }

                        output_buffer.push(out);

                        // Flush when buffer is full
                        if output_buffer.len() >= self.config.output_batch_size {
                            // Note: JoinJobProcessor doesn't have access to ObservabilityWrapper,
                            // so queue is not available here. Pass None for now.
                            let queue: Option<std::sync::Arc<crate::velostream::observability::async_queue::ObservabilityQueue>> = None;
                            Self::flush_output_buffer(
                                &mut output_buffer,
                                &writer,
                                &metrics_helper,
                                &query,
                                &observability,
                                &queue,
                                &job_name_for_metrics,
                                &mut stats,
                                false,
                            )
                            .await?;
                        }
                    }
                }
                Ok(None) => {
                    // Channel closed - all sources exhausted
                    debug!("JoinJobProcessor: All sources exhausted");
                    break;
                }
                Err(_) => {
                    // Timeout - flush any pending output then continue polling
                    // This ensures data is written even when batch size isn't reached
                    if !output_buffer.is_empty() {
                        debug!(
                            "JoinJobProcessor: Flushing {} buffered records on idle",
                            output_buffer.len()
                        );
                        let queue: Option<
                            std::sync::Arc<
                                crate::velostream::observability::async_queue::ObservabilityQueue,
                            >,
                        > = None;
                        Self::flush_output_buffer(
                            &mut output_buffer,
                            &writer,
                            &metrics_helper,
                            &query,
                            &observability,
                            &queue,
                            &job_name_for_metrics,
                            &mut stats,
                            true,
                        )
                        .await?;
                    }

                    // Update shared_stats on idle too, so the test harness can detect
                    // that records have been processed even when new records arrive
                    // faster than the 500ms periodic update interval.
                    let total = stats.left_records_read + stats.right_records_read;
                    if total > 0 {
                        if let Some(ref shared) = shared_stats {
                            if let Ok(mut s) = shared.write() {
                                s.records_processed = total;
                            }
                            last_stats_update = Instant::now();
                        }
                    }

                    continue;
                }
            }
        }

        // Flush remaining output
        if !output_buffer.is_empty() {
            let queue: Option<
                std::sync::Arc<crate::velostream::observability::async_queue::ObservabilityQueue>,
            > = None;
            Self::flush_output_buffer(
                &mut output_buffer,
                &writer,
                &metrics_helper,
                &query,
                &observability,
                &queue,
                &job_name_for_metrics,
                &mut stats,
                true,
            )
            .await?;
        }

        // Wait for coordinator to finish
        coordinator.wait_for_completion().await;

        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;

        // Final update of shared_stats with complete totals
        if let Some(ref shared) = shared_stats {
            if let Ok(mut s) = shared.write() {
                s.records_processed = stats.left_records_read + stats.right_records_read;
            }
        }

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
    use std::sync::Arc;

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

        // Create shared stats like production code
        let shared_stats: SharedJobStats =
            Arc::new(std::sync::RwLock::new(JobExecutionStats::new()));

        let stats = processor
            .process_join(
                "orders".to_string(),
                left_reader,
                "shipments".to_string(),
                right_reader,
                None,
                Some(shared_stats.clone()),
                None,
                None,
                None, // no app name
            )
            .await
            .unwrap();

        assert_eq!(stats.left_records_read, 5);
        assert_eq!(stats.right_records_read, 5);
        // Should have some joins (exact number depends on timing)
        assert!(stats.join_matches > 0);

        // Verify shared_stats was updated
        let final_shared = shared_stats.read().unwrap();
        assert_eq!(final_shared.records_processed, 10); // 5 left + 5 right
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

        // Create shared stats like production code
        let shared_stats: SharedJobStats =
            Arc::new(std::sync::RwLock::new(JobExecutionStats::new()));

        let stats = processor
            .process_join(
                "left".to_string(),
                left_reader,
                "right".to_string(),
                right_reader,
                Some(Box::new(writer)),
                Some(shared_stats.clone()),
                None,
                None,
                None, // no app name
            )
            .await
            .unwrap();

        // Verify output was written
        let written = output.lock().await;
        assert_eq!(written.len(), stats.records_written as usize);

        // Verify shared_stats was updated
        let final_shared = shared_stats.read().unwrap();
        assert_eq!(final_shared.records_processed, 6); // 3 left + 3 right
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

        // Create shared stats like production code
        let shared_stats: SharedJobStats =
            Arc::new(std::sync::RwLock::new(JobExecutionStats::new()));

        let stats = processor
            .process_join(
                "left".to_string(),
                left_reader,
                "right".to_string(),
                right_reader,
                None,
                Some(shared_stats.clone()),
                None,
                None,
                None, // no app name
            )
            .await
            .unwrap();

        assert_eq!(stats.left_records_read, 5);
        assert_eq!(stats.right_records_read, 5);
        assert_eq!(stats.join_matches, 0); // No matches

        // Verify shared_stats was updated
        let final_shared = shared_stats.read().unwrap();
        assert_eq!(final_shared.records_processed, 10); // 5 left + 5 right
    }

    #[tokio::test]
    async fn test_join_job_with_projection() {
        // Create matching records with multiple fields
        let left_records: Vec<StreamRecord> = (0..3)
            .map(|i| {
                let mut fields = HashMap::new();
                fields.insert("order_id".to_string(), FieldValue::Integer(100 + i));
                fields.insert(
                    "customer".to_string(),
                    FieldValue::String(format!("Customer{}", i)),
                );
                fields.insert("amount".to_string(), FieldValue::Integer(500 + i * 100));
                fields.insert(
                    "event_time".to_string(),
                    FieldValue::Integer(1000 + i * 100),
                );
                StreamRecord::new(fields)
            })
            .collect();

        let right_records: Vec<StreamRecord> = (0..3)
            .map(|i| {
                let mut fields = HashMap::new();
                fields.insert("order_id".to_string(), FieldValue::Integer(100 + i));
                fields.insert(
                    "carrier".to_string(),
                    FieldValue::String(format!("Carrier{}", i)),
                );
                fields.insert(
                    "tracking".to_string(),
                    FieldValue::String(format!("TRK{}", i)),
                );
                fields.insert(
                    "event_time".to_string(),
                    FieldValue::Integer(1100 + i * 100),
                );
                StreamRecord::new(fields)
            })
            .collect();

        let left_reader = Box::new(MockReader::new(left_records));
        let right_reader = Box::new(MockReader::new(right_records));
        let (writer, output) = MockWriter::new();

        let join_config = IntervalJoinConfig::new("orders", "shipments")
            .with_key("order_id", "order_id")
            .with_bounds(Duration::ZERO, Duration::from_secs(3600));

        // Set projection: only customer (aliased) and carrier
        let projection = vec![
            SelectField::AliasedColumn {
                column: "orders.customer".to_string(),
                alias: "customer_name".to_string(),
            },
            SelectField::Column("shipments.carrier".to_string()),
        ];

        let config = JoinJobConfig {
            join_config,
            projection: Some(projection),
            ..Default::default()
        };

        let processor = JoinJobProcessor::new(config);

        // Create shared stats like production code
        let shared_stats: SharedJobStats =
            Arc::new(std::sync::RwLock::new(JobExecutionStats::new()));

        let stats = processor
            .process_join(
                "orders".to_string(),
                left_reader,
                "shipments".to_string(),
                right_reader,
                Some(Box::new(writer)),
                Some(shared_stats.clone()),
                None,
                None,
                None, // no app name
            )
            .await
            .unwrap();

        // Should have some join matches
        assert!(stats.join_matches > 0, "Expected join matches");

        // Verify shared_stats was updated
        {
            let final_shared = shared_stats.read().unwrap();
            assert_eq!(final_shared.records_processed, 6); // 3 left + 3 right
        }

        // Check output records have only projected fields
        let written = output.lock().await;
        assert!(!written.is_empty(), "Expected some output records");

        for record in written.iter() {
            // Should have the aliased field
            assert!(
                record.fields.contains_key("customer_name"),
                "Expected 'customer_name' alias, got fields: {:?}",
                record.fields.keys().collect::<Vec<_>>()
            );
            // Should have the carrier field
            assert!(
                record.fields.contains_key("shipments.carrier"),
                "Expected 'shipments.carrier', got fields: {:?}",
                record.fields.keys().collect::<Vec<_>>()
            );
            // Should NOT have non-projected fields
            assert!(
                !record.fields.contains_key("orders.amount"),
                "Should not have 'orders.amount' which wasn't projected"
            );
            assert!(
                !record.fields.contains_key("shipments.tracking"),
                "Should not have 'shipments.tracking' which wasn't projected"
            );
            // Should have exactly 2 fields
            assert_eq!(
                record.fields.len(),
                2,
                "Should have exactly 2 projected fields, got: {:?}",
                record.fields.keys().collect::<Vec<_>>()
            );
        }
    }

    /// Helper to create a test observability manager with in-memory span collection
    async fn create_test_observability_manager() -> SharedObservabilityManager {
        use crate::velostream::observability::ObservabilityManager;
        use crate::velostream::sql::execution::config::{StreamingConfig, TracingConfig};

        let tracing_config = TracingConfig {
            service_name: "join-flush-test".to_string(),
            otlp_endpoint: None,
            ..Default::default()
        };

        let streaming_config = StreamingConfig::default().with_tracing_config(tracing_config);

        let mut manager = ObservabilityManager::from_streaming_config(streaming_config);
        manager
            .initialize()
            .await
            .expect("Failed to initialize observability manager");

        Arc::new(tokio::sync::RwLock::new(manager))
    }

    /// Helper to create a StreamRecord with an upstream traceparent
    fn create_record_with_traceparent(traceparent: &str) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        let mut headers = HashMap::new();
        headers.insert("traceparent".to_string(), traceparent.to_string());
        StreamRecord {
            fields,
            timestamp: 1700000000000,
            offset: 0,
            partition: 0,
            event_time: None,
            headers,
            topic: None,
            key: None,
        }
    }

    #[tokio::test]
    async fn test_flush_output_buffer_with_observability() {
        let obs_manager = create_test_observability_manager().await;
        let observability: Option<SharedObservabilityManager> = Some(obs_manager.clone());
        let metrics_helper = ProcessorMetricsHelper::new();
        let mut stats = JoinJobStats::default();

        let upstream_traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        // Create output buffer with first record carrying upstream traceparent
        let mut output_buffer: Vec<Arc<StreamRecord>> = vec![
            Arc::new(create_record_with_traceparent(upstream_traceparent)),
            Arc::new(StreamRecord::new(HashMap::from([(
                "id".to_string(),
                FieldValue::Integer(2),
            )]))),
        ];

        // Call flush_output_buffer with writer=None, observability=Some
        JoinJobProcessor::flush_output_buffer(
            &mut output_buffer,
            &None, // no writer
            &metrics_helper,
            &None, // no query
            &observability,
            &None, // no queue
            &Some("test-flush-job".to_string()),
            &mut stats,
            false,
        )
        .await
        .expect("flush_output_buffer should succeed");

        // Since writer=None, records_written should be updated and buffer cleared
        assert_eq!(stats.records_written, 2, "Should count 2 written records");
        assert!(output_buffer.is_empty(), "Buffer should be cleared");
    }

    #[tokio::test]
    async fn test_flush_output_buffer_no_observability() {
        let metrics_helper = ProcessorMetricsHelper::new();
        let mut stats = JoinJobStats::default();

        // Create output buffer without any trace context
        let mut output_buffer: Vec<Arc<StreamRecord>> = vec![Arc::new(StreamRecord::new(
            HashMap::from([("id".to_string(), FieldValue::Integer(1))]),
        ))];

        // Call flush_output_buffer with observability=None
        JoinJobProcessor::flush_output_buffer(
            &mut output_buffer,
            &None, // no writer
            &metrics_helper,
            &None, // no query
            &None, // no observability
            &None, // no queue
            &Some("test-no-obs-job".to_string()),
            &mut stats,
            false,
        )
        .await
        .expect("flush_output_buffer should succeed without observability");

        // Should process records without panic
        assert_eq!(stats.records_written, 1, "Should count 1 written record");
    }

    #[tokio::test]
    async fn test_flush_output_buffer_job_name_fallback() {
        let obs_manager = create_test_observability_manager().await;
        let observability: Option<SharedObservabilityManager> = Some(obs_manager.clone());
        let metrics_helper = ProcessorMetricsHelper::new();
        let mut stats = JoinJobStats::default();

        let upstream_traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        // Create output buffer with a record carrying upstream traceparent
        let mut output_buffer: Vec<Arc<StreamRecord>> = vec![Arc::new(
            create_record_with_traceparent(upstream_traceparent),
        )];

        // Call flush_output_buffer with job_name_for_metrics=None
        // This should use "join" as the default job name
        JoinJobProcessor::flush_output_buffer(
            &mut output_buffer,
            &None, // no writer
            &metrics_helper,
            &None, // no query
            &observability,
            &None, // no queue
            &None, // no job name -- should fall back to "join"
            &mut stats,
            false,
        )
        .await
        .expect("flush_output_buffer should succeed with None job_name");

        assert_eq!(
            stats.records_written, 1,
            "Should count 1 written record with fallback job name"
        );
    }
}
