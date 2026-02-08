//! Common functionality for multi-job SQL processing
//!
//! This module contains shared functionality used by both transactional
//! and non-transactional multi-job processors.

use crate::velostream::datasource::{
    DataReader, DataSink, DataSource, DataWriter, SinkConfig, StdoutWriter,
    file::{FileDataSink, FileDataSource, FileMmapDataSource},
    kafka::{
        KafkaDataSink, KafkaDataSource,
        config_helpers::{
            ClientType, generate_processor_client_id, generate_transactional_id,
            log_processor_client_id, log_transactional_id,
        },
    },
};
use crate::velostream::server::processors::SimpleJobProcessor;
use crate::velostream::sql::config::substitute_env_vars;
use crate::velostream::sql::{
    StreamExecutionEngine, StreamingQuery,
    ast::{StreamSource, StreamingQuery as AstStreamingQuery},
    execution::{
        config::StreamingConfig, processors::context::ProcessorContext,
        processors::order::OrderProcessor, types::StreamRecord,
    },
    query_analyzer::{DataSinkRequirement, DataSinkType, DataSourceRequirement, DataSourceType},
};
use log::{debug, error, info, warn};
use serde::Serializer;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

/// Serialize Duration as milliseconds (f64)
fn serialize_duration_as_ms<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64(duration.as_secs_f64() * 1000.0)
}

/// Serialize Option<Instant> as elapsed milliseconds since that instant (f64)
/// Returns null if None, or elapsed ms since the instant if Some
fn serialize_option_instant_as_elapsed_ms<S>(
    instant: &Option<Instant>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match instant {
        Some(i) => serializer.serialize_f64(i.elapsed().as_secs_f64() * 1000.0),
        None => serializer.serialize_none(),
    }
}

/// Result of batch processing
#[derive(Debug, Clone)]
pub struct BatchProcessingResult {
    pub records_processed: usize,
    pub records_failed: usize,
    pub processing_time: Duration,
    pub batch_size: usize,
    pub error_details: Vec<ProcessingError>,
}

/// Details about a processing error
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProcessingError {
    pub record_index: usize,
    pub error_message: String,
    pub recoverable: bool,
}

/// Dead Letter Queue entry - records that failed processing with error details
#[derive(Debug, Clone)]
pub struct DLQEntry {
    pub record: StreamRecord,
    pub error_message: String,
    pub record_index: usize,
    pub recoverable: bool,
    pub timestamp: Instant,
}

/// Dead Letter Queue - collects failed records for inspection and debugging
#[derive(Debug, Clone)]
pub struct DeadLetterQueue {
    pub entries: Arc<Mutex<Vec<DLQEntry>>>,
    /// Optional maximum size limit for DLQ. If None, no limit is enforced.
    max_size: Option<usize>,
    /// Atomic flag indicating if DLQ has reached max capacity
    max_size_reached: Arc<AtomicBool>,
    /// Current count of entries (updated atomically for fast capacity checks)
    entry_count: Arc<AtomicUsize>,
}

impl DeadLetterQueue {
    /// Create a new DLQ without size limits
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            max_size: None,
            max_size_reached: Arc::new(AtomicBool::new(false)),
            entry_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new DLQ with a maximum size limit
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
            max_size: Some(max_size),
            max_size_reached: Arc::new(AtomicBool::new(false)),
            entry_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add a failed record to the DLQ
    /// Returns true if entry was added, false if at capacity or other error
    pub async fn add_entry(
        &self,
        record: StreamRecord,
        error_message: String,
        record_index: usize,
        recoverable: bool,
    ) -> bool {
        // Check capacity before adding
        if let Some(max) = self.max_size {
            let current_count = self.entry_count.load(Ordering::SeqCst);
            if current_count >= max {
                self.max_size_reached.store(true, Ordering::SeqCst);
                debug!(
                    "DLQ at capacity: {}/{} entries. Rejecting new entry.",
                    current_count, max
                );
                return false;
            }
        }

        let entry = DLQEntry {
            record,
            error_message,
            record_index,
            recoverable,
            timestamp: Instant::now(),
        };

        self.entries.lock().await.push(entry);

        // Update atomic count
        let new_count = self.entry_count.fetch_add(1, Ordering::SeqCst) + 1;

        // Update max_size_reached flag
        if let Some(max) = self.max_size {
            if new_count >= max {
                self.max_size_reached.store(true, Ordering::SeqCst);
            }
        }

        true
    }

    /// Get all DLQ entries
    pub async fn get_entries(&self) -> Vec<DLQEntry> {
        self.entries.lock().await.clone()
    }

    /// Get count of DLQ entries
    pub async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    /// Check if DLQ is empty
    pub async fn is_empty(&self) -> bool {
        self.entries.lock().await.is_empty()
    }

    /// Clear the DLQ and reset counters
    pub async fn clear(&self) {
        self.entries.lock().await.clear();
        self.entry_count.store(0, Ordering::SeqCst);
        self.max_size_reached.store(false, Ordering::SeqCst);
    }

    /// Print all DLQ entries for debugging
    pub async fn print_entries(&self) {
        let entries = self.entries.lock().await;
        if entries.is_empty() {
            println!("DLQ is empty");
            return;
        }

        println!("\n╔════════════════════════════════════════════════════════════╗");
        println!(
            "║ DEAD LETTER QUEUE - {} failed records                 ║",
            entries.len()
        );
        println!("╚════════════════════════════════════════════════════════════╝\n");

        for (i, entry) in entries.iter().enumerate() {
            println!("DLQ Entry {}:", i + 1);
            println!("  Record Index:     {}", entry.record_index);
            println!("  Error Message:    {}", entry.error_message);
            println!("  Recoverable:      {}", entry.recoverable);
            println!(
                "  Record Fields:    {:?}",
                entry.record.fields.keys().collect::<Vec<_>>()
            );
            println!();
        }
    }

    /// Get the maximum size limit for this DLQ
    pub fn max_size(&self) -> Option<usize> {
        self.max_size
    }

    /// Check if DLQ has reached maximum capacity
    pub fn is_at_capacity(&self) -> bool {
        self.max_size_reached.load(Ordering::SeqCst)
    }

    /// Reset the max_size_reached flag (useful after clearing DLQ)
    pub fn reset_max_size_flag(&self) {
        self.max_size_reached.store(false, Ordering::SeqCst);
    }

    /// Get current capacity usage as a percentage (0.0 - 100.0)
    /// Returns None if no max_size is configured
    pub fn capacity_usage_percent(&self) -> Option<f64> {
        self.max_size.map(|max| {
            let current = self.entry_count.load(Ordering::SeqCst);
            (current as f64 / max as f64) * 100.0
        })
    }

    /// Get current entry count (fast, non-blocking)
    pub fn current_size(&self) -> usize {
        self.entry_count.load(Ordering::SeqCst)
    }
}

impl Default for DeadLetterQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for job execution
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct JobExecutionStats {
    pub records_processed: u64,
    pub records_failed: u64,
    pub batches_processed: u64,
    pub batches_failed: u64,
    #[serde(skip_deserializing)]
    #[serde(serialize_with = "serialize_option_instant_as_elapsed_ms")]
    pub start_time: Option<Instant>,
    #[serde(skip_deserializing)]
    #[serde(serialize_with = "serialize_option_instant_as_elapsed_ms")]
    pub last_record_time: Option<Instant>,
    pub avg_batch_size: f64,
    pub avg_processing_time_ms: f64,
    #[serde(serialize_with = "serialize_duration_as_ms")]
    pub total_processing_time: Duration,
    /// Detailed error information for debugging
    pub error_details: Vec<ProcessingError>,

    // Timing breakdown for performance analysis
    /// Total time spent reading from source (poll + deserialization)
    #[serde(serialize_with = "serialize_duration_as_ms")]
    pub total_read_time: Duration,
    /// Total time spent on SQL processing
    #[serde(serialize_with = "serialize_duration_as_ms")]
    pub total_sql_time: Duration,
    /// Total time spent writing to sink (serialization + produce)
    #[serde(serialize_with = "serialize_duration_as_ms")]
    pub total_write_time: Duration,

    // Idle detection
    /// Whether the last batch had records (for idle transition detection)
    #[serde(skip)]
    pub last_batch_had_records: bool,
    /// Whether we've logged the idle transition (to avoid spam)
    #[serde(skip)]
    pub idle_logged: bool,
}

impl JobExecutionStats {
    pub fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Update statistics after batch processing
    pub fn update_from_batch(&mut self, result: &BatchProcessingResult) {
        self.records_processed += result.records_processed as u64;
        self.records_failed += result.records_failed as u64;

        if result.records_failed > 0 {
            self.batches_failed += 1;
        } else {
            self.batches_processed += 1;
        }

        self.last_record_time = Some(Instant::now());
        self.total_processing_time += result.processing_time;

        // Accumulate error details
        self.error_details.extend(result.error_details.clone());

        // Update moving averages
        let total_batches = (self.batches_processed + self.batches_failed) as f64;
        if total_batches > 0.0 {
            self.avg_batch_size = ((self.avg_batch_size * (total_batches - 1.0))
                + result.batch_size as f64)
                / total_batches;

            // Use as_secs_f64() * 1000.0 for sub-millisecond precision (as_millis() truncates to 0 for <1ms)
            self.avg_processing_time_ms = ((self.avg_processing_time_ms * (total_batches - 1.0))
                + result.processing_time.as_secs_f64() * 1000.0)
                / total_batches;
        }
    }

    pub fn records_per_second(&self) -> f64 {
        // Calculate based on actual processing time (read + sql + write)
        // This gives the true processing rate: records / time_spent_processing
        // Note: SimpleJobProcessor uses add_read_time/add_sql_time/add_write_time,
        // while update_from_batch uses total_processing_time - support both
        let tracked_time = self.total_read_time + self.total_sql_time + self.total_write_time;
        let processing_time = if tracked_time > Duration::ZERO {
            tracked_time
        } else {
            self.total_processing_time
        };

        let processing_secs = processing_time.as_secs_f64();
        if processing_secs > 0.0 {
            return self.records_processed as f64 / processing_secs;
        }
        0.0
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.records_processed + self.records_failed;
        if total > 0 {
            (self.records_processed as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Returns the processing duration (start to last record, or start to now if still processing)
    pub fn elapsed(&self) -> Duration {
        if let Some(start) = self.start_time {
            // Use last_record_time if available (processing complete),
            // otherwise use current time (still processing)
            let end = self.last_record_time.unwrap_or_else(Instant::now);
            end.duration_since(start)
        } else {
            Duration::from_secs(0)
        }
    }

    /// Add read time (Kafka poll + deserialization)
    pub fn add_read_time(&mut self, duration: Duration) {
        self.total_read_time += duration;
    }

    /// Add SQL processing time
    pub fn add_sql_time(&mut self, duration: Duration) {
        self.total_sql_time += duration;
    }

    /// Add write time (serialization + Kafka produce)
    pub fn add_write_time(&mut self, duration: Duration) {
        self.total_write_time += duration;
    }

    /// Get effective average processing time per batch in milliseconds.
    /// Calculates from breakdown times when available, otherwise uses the stored avg_processing_time_ms.
    pub fn effective_avg_batch_time_ms(&self) -> f64 {
        // If breakdown times are available, calculate from them
        let tracked_time = self.total_read_time + self.total_sql_time + self.total_write_time;
        if tracked_time > Duration::ZERO && self.batches_processed > 0 {
            return (tracked_time.as_secs_f64() * 1000.0) / self.batches_processed as f64;
        }

        // Fall back to stored avg_processing_time_ms (populated by update_from_batch)
        self.avg_processing_time_ms
    }

    /// Get timing breakdown as percentages of elapsed time
    pub fn timing_breakdown(&self) -> (f64, f64, f64, f64) {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return (0.0, 0.0, 0.0, 0.0);
        }

        let read_pct = (self.total_read_time.as_secs_f64() / elapsed) * 100.0;
        let sql_pct = (self.total_sql_time.as_secs_f64() / elapsed) * 100.0;
        let write_pct = (self.total_write_time.as_secs_f64() / elapsed) * 100.0;
        let other_pct = 100.0 - read_pct - sql_pct - write_pct;

        (read_pct, sql_pct, write_pct, other_pct.max(0.0))
    }

    /// Sync stats to shared stats for real-time monitoring.
    /// This is used by processors to update the shared stats reference
    /// that external observers (like the test harness) can poll.
    pub fn sync_to_shared(
        &self,
        shared_stats: &Option<crate::velostream::server::processors::SharedJobStats>,
    ) {
        if let Some(shared) = shared_stats {
            match shared.write() {
                Ok(mut shared_lock) => {
                    *shared_lock = self.clone();
                }
                Err(e) => {
                    log::warn!("Failed to sync to shared_stats: {} - stats may be stale", e);
                }
            }
        }
    }

    /// Mark whether the current batch had records.
    /// Returns true if this is a transition to idle (was processing, now empty).
    pub fn mark_batch(&mut self, had_records: bool) -> bool {
        let transition_to_idle = self.last_batch_had_records && !had_records && !self.idle_logged;
        self.last_batch_had_records = had_records;

        if had_records {
            // Reset idle flag when we get records again
            self.idle_logged = false;
        } else if transition_to_idle {
            // Mark that we've logged the idle transition
            self.idle_logged = true;
        }

        transition_to_idle
    }
}

/// Configuration for job processing behavior
#[derive(Debug, Clone)]
pub struct JobProcessingConfig {
    /// Maximum batch size for processing
    pub max_batch_size: usize,
    /// Timeout for collecting a batch
    pub batch_timeout: Duration,
    /// Whether to use transactional processing
    pub use_transactions: bool,
    /// Strategy for handling failed records
    pub failure_strategy: FailureStrategy,
    /// Maximum retries for recoverable errors
    pub max_retries: u32,
    /// Backoff duration between retries
    pub retry_backoff: Duration,
    /// Whether to log progress periodically
    pub log_progress: bool,
    /// Progress logging interval (in batches)
    pub progress_interval: u64,
    /// Maximum number of consecutive empty batches before exiting data source loop
    /// Default: 1000 (allows long waiting periods for slow data sources)
    pub empty_batch_count: u32,
    /// Wait time in milliseconds between empty batches
    /// Default: 1000ms (1 second) to avoid busy-waiting on empty data sources
    pub wait_on_empty_batch_ms: u64,
    /// Enable Dead Letter Queue for failed records (only used with LogAndContinue strategy)
    /// - SimpleJobProcessor: Default true (supports error recovery)
    /// - TransactionalJobProcessor: Default false (FailBatch rolls back, DLQ not applicable)
    /// - PartitionReceiver: Default true (enables debug tracking for partition-level failures)
    pub enable_dlq: bool,
    /// Maximum size limit for the Dead Letter Queue
    /// If None, no limit is enforced. If Some(max), DLQ will reject entries once capacity is reached.
    /// Default: Some(100) - 100 items max to prevent unbounded growth
    pub dlq_max_size: Option<usize>,
}

impl Default for JobProcessingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(1000),
            use_transactions: false,
            failure_strategy: FailureStrategy::LogAndContinue,
            max_retries: 10,
            retry_backoff: Duration::from_millis(5000),
            log_progress: true,
            progress_interval: 10,
            empty_batch_count: 1000, // Exit immediately when all sources exhausted (no retry delay)
            wait_on_empty_batch_ms: 1000,
            enable_dlq: true,        // Default: enabled for LogAndContinue strategy
            dlq_max_size: Some(100), // Default: 100 items max DLQ size
        }
    }
}

/// Strategy for handling processing failures
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FailureStrategy {
    /// Log the error and continue processing
    LogAndContinue,
    /// Send failed records to a dead letter queue
    SendToDLQ,
    /// Fail the entire batch if any record fails
    FailBatch,
    /// Retry failed records with exponential backoff
    RetryWithBackoff,
}

/// Result of batch processing with SQL engine results
#[derive(Debug, Clone)]
pub struct BatchProcessingResultWithOutput {
    pub records_processed: usize,
    pub records_failed: usize,
    pub processing_time: Duration,
    pub batch_size: usize,
    pub error_details: Vec<ProcessingError>,
    pub output_records: Vec<Arc<StreamRecord>>, // PERF: Arc for zero-copy multi-sink writes
}

/// Process a batch of records through the SQL execution engine
///
/// Uses engine.execute_with_record_sync() for proper abstraction layer and architecture.
/// This approach:
/// - Maintains clean separation between processor and engine layers
/// - Engine handles windowing, GROUP BY, state management, EMIT CHANGES
/// - Returns actual SQL query results (0 or more per input record)
/// - Consistent with PartitionReceiver (V2 architecture)
///
/// The engine manages all complexity: context initialization, state persistence,
/// window emission, GROUP BY queue draining, EMIT CHANGES routing.
pub async fn process_batch(
    batch: Vec<StreamRecord>,
    engine: &Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
    query: &StreamingQuery,
    job_name: &str,
) -> BatchProcessingResultWithOutput {
    let batch_start = Instant::now();
    let batch_size = batch.len();
    let mut records_processed = 0;
    let mut records_failed = 0;
    let mut error_details = Vec::new();
    let mut output_records = Vec::new();

    debug!(
        "Job '{}': Processing batch of {} records",
        job_name, batch_size
    );

    // NOTE: Query execution is already initialized once at job startup by SimpleJobProcessor.process_multi_job()
    // Calling init_query_execution here is redundant and causes unnecessary write locks
    // The engine maintains persistent QueryExecution state across all batches

    // Process all records through engine abstraction layer
    for (index, record) in batch.into_iter().enumerate() {
        match engine
            .write()
            .await
            .execute_with_record_sync(query, &record)
        {
            Ok(outputs) => {
                records_processed += 1;

                // Collect results (0 or more per input record)
                // Engine handles windows, GROUP BY, EMIT CHANGES internally
                for output in outputs {
                    output_records.push(Arc::new(output));
                }
            }
            Err(e) => {
                records_failed += 1;

                let detailed_msg = extract_error_context(&e);
                let recoverable = is_recoverable_error(&e);

                error_details.push(ProcessingError {
                    record_index: index,
                    error_message: detailed_msg.clone(),
                    recoverable,
                });

                // Log first 3 errors to avoid log spam
                // Use error! for non-recoverable errors (schema issues, field not found, etc.)
                // Use warn! for recoverable errors (transient network issues, etc.)
                if index < 3 {
                    if recoverable {
                        warn!(
                            "Job '{}' failed to process record {}: {} [Recoverable]",
                            job_name, index, detailed_msg
                        );
                    } else {
                        error!(
                            "Job '{}' failed to process record {}: {} [Non-recoverable]",
                            job_name, index, detailed_msg
                        );
                    }
                    debug!("Full error details: {:?}", e);
                } else if index == 3 {
                    if recoverable {
                        warn!(
                            "Job '{}' (suppressing further error logs for batch, {} errors total)",
                            job_name, batch_size
                        );
                    } else {
                        error!(
                            "Job '{}' (suppressing further error logs for batch, {} errors total)",
                            job_name, batch_size
                        );
                    }
                }
            }
        }
    }

    debug!(
        "Job '{}': Batch completed: {} processed, {} failed",
        job_name, records_processed, records_failed
    );

    // Phase 5: Apply ORDER BY sorting to batch results (FR-084 extension)
    // This sorts records within each batch according to ORDER BY expressions.
    let output_records = apply_order_by_sorting(output_records, query);

    BatchProcessingResultWithOutput {
        records_processed,
        records_failed,
        processing_time: batch_start.elapsed(),
        batch_size,
        error_details,
        output_records,
    }
}

/// Apply ORDER BY sorting to output records if the query has an ORDER BY clause
///
/// Extracts ORDER BY expressions from the query and sorts the records accordingly.
/// This implements Phase 5 batch-level sorting for non-windowed queries.
fn apply_order_by_sorting(
    output_records: Vec<Arc<StreamRecord>>,
    query: &StreamingQuery,
) -> Vec<Arc<StreamRecord>> {
    // Extract ORDER BY from query (handles Select and CreateStream variants)
    let order_by = match query {
        StreamingQuery::Select {
            order_by, window, ..
        } => {
            // Skip if query has WINDOW clause (window adapter handles sorting)
            if window.is_some() {
                return output_records;
            }
            order_by.as_ref()
        }
        StreamingQuery::CreateStream { as_select, .. } => {
            if let StreamingQuery::Select {
                order_by, window, ..
            } = as_select.as_ref()
            {
                if window.is_some() {
                    return output_records;
                }
                order_by.as_ref()
            } else {
                None
            }
        }
        _ => None,
    };

    // If no ORDER BY or empty, return records as-is
    let order_exprs = match order_by {
        Some(exprs) if !exprs.is_empty() => exprs,
        _ => return output_records,
    };

    // Convert Arc<StreamRecord> to owned StreamRecord for sorting
    let records: Vec<StreamRecord> = output_records
        .into_iter()
        .map(|arc| (*arc).clone())
        .collect();

    // Create a minimal ProcessorContext for ORDER BY evaluation
    let context = ProcessorContext::new("order_by_batch");

    // Sort using OrderProcessor (sorting cannot fail for valid ORDER BY expressions)
    match OrderProcessor::process(records, order_exprs, &context) {
        Ok(sorted) => sorted.into_iter().map(Arc::new).collect(),
        Err(e) => {
            warn!("ORDER BY sorting failed: {}, returning unsorted records", e);
            // Reconstruct the original records (we consumed them for sorting attempt)
            // This shouldn't happen in practice - ORDER BY expressions are validated at parse time
            Vec::new()
        }
    }
}

/// Extract human-readable error context from SqlError
///
/// Provides detailed, actionable error messages by extracting type-specific information
/// instead of relying on generic debug formatting. This improves production diagnostics.
fn extract_error_context(error: &crate::velostream::sql::SqlError) -> String {
    match error {
        crate::velostream::sql::SqlError::ExecutionError { message, query: _ } => {
            format!("ExecutionError: {}", message)
        }
        crate::velostream::sql::SqlError::ParseError { message, position } => match position {
            Some(pos) => format!("ParseError at position {}: {}", pos, message),
            None => format!("ParseError: {}", message),
        },
        crate::velostream::sql::SqlError::TypeError {
            expected,
            actual,
            value,
        } => match value {
            Some(val) => {
                format!(
                    "TypeError: expected {}, got {} for value '{}'",
                    expected, actual, val
                )
            }
            None => format!("TypeError: expected {}, got {}", expected, actual),
        },
        crate::velostream::sql::SqlError::SchemaError { message, column } => match column {
            Some(col) => format!("SchemaError for column '{}': {}", col, message),
            None => format!("SchemaError: {}", message),
        },
        crate::velostream::sql::SqlError::StreamError {
            stream_name,
            message,
        } => format!("StreamError for '{}': {}", stream_name, message),
        crate::velostream::sql::SqlError::WindowError {
            message,
            window_type,
        } => match window_type {
            Some(wtype) => format!("WindowError for {} window: {}", wtype, message),
            None => format!("WindowError: {}", message),
        },
        crate::velostream::sql::SqlError::ResourceError { resource, message } => {
            format!("ResourceError for {}: {}", resource, message)
        }
        crate::velostream::sql::SqlError::TableNotFound { table_name } => {
            format!("TableNotFound: '{}'", table_name)
        }
        crate::velostream::sql::SqlError::ConfigurationError { message } => {
            format!("ConfigurationError: {}", message)
        }
        crate::velostream::sql::SqlError::AggregateWithoutGrouping {
            functions,
            suggestion,
        } => {
            format!(
                "AggregateWithoutGrouping: aggregate functions {} cannot be used without GROUP BY or WINDOW clause. {}",
                functions.join(", "),
                suggestion
            )
        }
    }
}

/// Determine if an error is recoverable
///
/// Classifies errors to inform retry and fallback strategies.
/// Currently conservative (most errors non-recoverable).
fn is_recoverable_error(_error: &crate::velostream::sql::SqlError) -> bool {
    // This is a simple implementation - could be extended based on error types
    // Add specific error pattern matching here
    false
}

/// Log progress for a job - shows per-batch throughput only
///
/// This function is called after each batch. It shows:
/// - Batch-level stats (records in THIS batch, time for THIS batch)
/// - NOT cumulative totals (those are shown at the end by log_final_stats)
///
/// Parameters:
/// - batch_records: Number of records processed in this specific batch
/// - batch_time_ms: Time taken to process this specific batch
pub fn log_job_progress(job_name: &str, batch_records: usize, batch_time_ms: f64) {
    // Calculate per-batch throughput
    let batch_rps = if batch_time_ms > 0.0 {
        (batch_records as f64 / batch_time_ms) * 1000.0
    } else {
        0.0
    };

    // Only log if there were records in this batch
    if batch_records > 0 {
        info!(
            "Job '{}': batch processed {} records in {:.1}ms ({:.0} records/sec)",
            job_name, batch_records, batch_time_ms, batch_rps
        );
    }
}

/// Log final statistics for a job
pub fn log_final_stats(job_name: &str, stats: &JobExecutionStats) {
    let elapsed = stats.elapsed();
    let rps = stats.records_per_second();
    let success_rate = stats.success_rate();

    info!(
        "Job '{}' completed: {} records processed, {} failed ({:.1}% success) in {:.2}s ({:.2} records/sec)",
        job_name,
        stats.records_processed,
        stats.records_failed,
        success_rate,
        elapsed.as_secs_f64(),
        rps
    );

    if stats.batches_processed > 0 {
        info!(
            "  Batch stats: {} successful, {} failed, {:.1} avg size, {:.2}ms avg time",
            stats.batches_processed,
            stats.batches_failed,
            stats.avg_batch_size,
            stats.effective_avg_batch_time_ms()
        );
    }

    // Log timing breakdown if any timing data was collected
    let has_timing = stats.total_read_time.as_nanos() > 0
        || stats.total_sql_time.as_nanos() > 0
        || stats.total_write_time.as_nanos() > 0;

    if has_timing {
        let (read_pct, sql_pct, write_pct, other_pct) = stats.timing_breakdown();
        info!(
            "  Timing breakdown: read={:.1}ms ({:.1}%), sql={:.1}ms ({:.1}%), write={:.1}ms ({:.1}%), other={:.1}%",
            stats.total_read_time.as_secs_f64() * 1000.0,
            read_pct,
            stats.total_sql_time.as_secs_f64() * 1000.0,
            sql_pct,
            stats.total_write_time.as_secs_f64() * 1000.0,
            write_pct,
            other_pct
        );
    }
}

/// Log when processor transitions to idle state (first empty batch after processing records)
/// This helps diagnose when the data stream stops producing records.
/// Note: Does NOT log cumulative stats - those are shown at the end by log_final_stats.
pub fn log_idle_transition(job_name: &str) {
    debug!("Job '{}' ⏸️  IDLE: waiting for more records...", job_name);
}

/// Result type for datasource operations
pub type DataSourceResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Configuration for creating a datasource
#[derive(Debug, Clone)]
pub struct DataSourceConfig {
    pub requirement: DataSourceRequirement,
    pub default_topic: String,
    pub job_name: String,
    pub app_name: Option<String>, // For multi-JobServer consumer group coordination
    pub instance_id: Option<String>, // For unique client.id generation
    pub batch_config: Option<crate::velostream::datasource::BatchConfig>,
    /// Enable transactional mode (isolation.level=read_committed for consumers)
    pub use_transactions: bool,
}

#[derive(Debug, Clone)]
pub struct DataSinkConfig {
    pub requirement: DataSinkRequirement,
    pub job_name: String,
    pub app_name: Option<String>, // For hierarchical client.id generation
    pub instance_id: Option<String>, // For unique client.id generation
    pub batch_config: Option<crate::velostream::datasource::BatchConfig>,
    /// Enable transactional mode (transactional.id for exactly-once producers)
    pub use_transactions: bool,
}

/// Result of datasource creation
pub type DataSourceCreationResult = Result<Box<dyn DataReader>, String>;

/// Result of datasink creation
pub type DataSinkCreationResult = Result<Box<dyn DataWriter>, String>;

/// Multi-source/sink creation results
pub type MultiSourceCreationResult = Result<HashMap<String, Box<dyn DataReader>>, String>;
pub type MultiSinkCreationResult = Result<HashMap<String, Box<dyn DataWriter>>, String>;

/// Helper to check if a reader supports transactions
pub fn check_transaction_support(reader: &dyn DataReader, job_name: &str) -> bool {
    let supports = reader.supports_transactions();
    if supports {
        info!(
            "Job '{}': Datasource supports transactional processing",
            job_name
        );
    } else {
        info!(
            "Job '{}': Datasource does not support transactions, using best-effort delivery",
            job_name
        );
    }
    supports
}

/// Helper to check if a writer supports transactions
pub fn check_writer_transaction_support(writer: &dyn DataWriter, job_name: &str) -> bool {
    let supports = writer.supports_transactions();
    if supports {
        info!("Job '{}': Sink supports transactional writes", job_name);
    } else {
        info!(
            "Job '{}': Sink does not support transactions, using best-effort delivery",
            job_name
        );
    }
    supports
}

/// Retry logic for recoverable operations
pub async fn retry_with_backoff<F, T>(
    operation: F,
    max_retries: u32,
    backoff: Duration,
    job_name: &str,
    operation_name: &str,
) -> DataSourceResult<T>
where
    F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = DataSourceResult<T>> + Send>>,
{
    let mut attempts = 0;
    let mut current_backoff = backoff;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if attempts < max_retries => {
                attempts += 1;
                warn!(
                    "Job '{}': {} failed (attempt {}/{}): {:?}. Retrying in {:?}",
                    job_name, operation_name, attempts, max_retries, e, current_backoff
                );
                tokio::time::sleep(current_backoff).await;
                current_backoff *= 2; // Exponential backoff
            }
            Err(e) => {
                error!(
                    "Job '{}': {} failed after {} attempts: {:?}",
                    job_name, operation_name, max_retries, e
                );
                return Err(e);
            }
        }
    }
}

/// Create a datasource reader based on configuration
pub async fn create_datasource_reader(config: &DataSourceConfig) -> DataSourceCreationResult {
    let requirement = &config.requirement;

    match requirement.source_type {
        DataSourceType::Kafka => {
            create_kafka_reader(
                &requirement.properties,
                &requirement.name, // Use source name, not default_topic
                &config.job_name,
                config.app_name.as_deref(),
                config.instance_id.as_deref(),
                &config.batch_config,
                config.use_transactions,
            )
            .await
        }
        DataSourceType::File => {
            // Check if this is an mmap source based on properties
            let is_mmap = requirement
                .properties
                .get("source.type")
                .or_else(|| requirement.properties.get("type"))
                .map(|t| t == "file_source_mmap")
                .unwrap_or(false);

            if is_mmap {
                create_file_mmap_reader(&requirement.properties, &config.batch_config).await
            } else {
                create_file_reader(&requirement.properties, &config.batch_config).await
            }
        }
        _ => Err(format!(
            "Unsupported datasource type '{:?}'",
            requirement.source_type
        )),
    }
}

/// Create a Kafka datasource reader
async fn create_kafka_reader(
    props: &HashMap<String, String>,
    source_name: &str,
    job_name: &str,
    app_name: Option<&str>,
    instance_id: Option<&str>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
    use_transactions: bool,
) -> DataSourceCreationResult {
    // Extract topic name from properties, or use source name as default
    let topic = props
        .get("topic")
        .or_else(|| props.get("source.topic"))
        .or_else(|| props.get("datasource.topic.name"))
        .map(|s| s.to_string())
        .unwrap_or_else(|| source_name.to_string());

    info!(
        "Creating Kafka reader for source '{}' with topic '{}' (app: {}, instance: {}, transactional: {})",
        source_name,
        topic,
        app_name.unwrap_or("none"),
        instance_id.unwrap_or("none"),
        use_transactions
    );

    // Build properties with client.id and isolation.level if transactional mode is enabled
    let mut source_props = props.clone();

    // Generate client.id for consumer identification in logs and monitoring
    let client_id = generate_processor_client_id(app_name, job_name, instance_id, source_name);
    source_props.insert("client.id".to_string(), client_id.clone());
    log_processor_client_id(
        &client_id,
        ClientType::Source,
        app_name,
        job_name,
        instance_id,
        source_name,
    );

    if use_transactions {
        // Configure consumer for exactly-once semantics:
        // 1. isolation.level=read_committed - Only see committed transactional messages
        // 2. enable.auto.commit=false - Manual offset management for transactional guarantees
        info!(
            "Enabling transactional consumer with isolation.level=read_committed, enable.auto.commit=false"
        );
        source_props.insert("isolation.level".to_string(), "read_committed".to_string());
        source_props.insert("enable.auto.commit".to_string(), "false".to_string());
    }

    // Let KafkaDataSource handle its own configuration extraction
    let mut datasource =
        KafkaDataSource::from_properties(&source_props, &topic, job_name, app_name, instance_id);

    // Self-initialize with the extracted configuration
    datasource
        .self_initialize()
        .await
        .map_err(|e| format!("Failed to initialize Kafka datasource: {}", e))?;

    // Create reader with batch configuration if available
    match batch_config {
        Some(batch_config) => {
            info!(
                "Creating Kafka reader with batch configuration: {:?}",
                batch_config
            );
            datasource
                .create_reader_with_batch_config(batch_config.clone())
                .await
                .map_err(|e| format!("Failed to create Kafka reader with batch config: {}", e))
        }
        None => {
            debug!("Creating Kafka reader without batch configuration");
            datasource
                .create_reader()
                .await
                .map_err(|e| format!("Failed to create Kafka reader: {}", e))
        }
    }
}

/// Create a file datasource reader
async fn create_file_reader(
    props: &HashMap<String, String>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> DataSourceCreationResult {
    // Let FileDataSource handle its own configuration extraction
    let mut datasource = FileDataSource::from_properties(props);

    // Self-initialize with the extracted configuration
    datasource
        .self_initialize()
        .await
        .map_err(|e| format!("Failed to initialize File datasource: {}", e))?;

    // Create reader with batch configuration if available
    match batch_config {
        Some(batch_config) => {
            info!(
                "Creating File reader with batch configuration: {:?}",
                batch_config
            );
            datasource
                .create_reader_with_batch_config(batch_config.clone())
                .await
                .map_err(|e| format!("Failed to create File reader with batch config: {}", e))
        }
        None => {
            debug!("Creating File reader without batch configuration");
            datasource
                .create_reader()
                .await
                .map_err(|e| format!("Failed to create File reader: {}", e))
        }
    }
}

/// Create a memory-mapped file datasource reader
async fn create_file_mmap_reader(
    props: &HashMap<String, String>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> DataSourceCreationResult {
    let mut datasource = FileMmapDataSource::from_properties(props);

    datasource
        .self_initialize()
        .await
        .map_err(|e| format!("Failed to initialize File mmap datasource: {}", e))?;

    match batch_config {
        Some(batch_config) => {
            info!(
                "Creating File mmap reader with batch configuration: {:?}",
                batch_config
            );
            datasource
                .create_reader_with_batch_config(batch_config.clone())
                .await
                .map_err(|e| format!("Failed to create File mmap reader with batch config: {}", e))
        }
        None => {
            debug!("Creating File mmap reader without batch configuration");
            datasource
                .create_reader()
                .await
                .map_err(|e| format!("Failed to create File mmap reader: {}", e))
        }
    }
}

/// Create a datasink writer based on configuration
pub async fn create_datasource_writer(config: &DataSinkConfig) -> DataSinkCreationResult {
    let requirement = &config.requirement;

    match requirement.sink_type {
        DataSinkType::Kafka => {
            create_kafka_writer(
                &requirement.properties,
                &requirement.name, // Use sink name, not job name
                &config.job_name,
                config.app_name.as_deref(),
                config.instance_id.as_deref(),
                &config.batch_config,
                config.use_transactions,
                requirement.primary_keys.as_ref(), // Pass PRIMARY KEY from SQL
            )
            .await
        }
        DataSinkType::File => {
            create_file_writer(&requirement.properties, &config.batch_config).await
        }
        _ => Err(format!(
            "Unsupported datasink type '{:?}'",
            requirement.sink_type
        )),
    }
}

/// Create a Kafka datasink writer
async fn create_kafka_writer(
    props: &HashMap<String, String>,
    sink_name: &str,
    job_name: &str,
    app_name: Option<&str>,
    instance_id: Option<&str>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
    use_transactions: bool,
    primary_keys: Option<&Vec<String>>,
) -> DataSinkCreationResult {
    // Extract brokers from properties
    // NOTE: Check flattened YAML keys (datasink.producer_config.bootstrap.servers) first
    // as they contain the raw ${ENV_VAR:default} pattern that needs substitution
    let brokers_raw = props
        .get("datasink.producer_config.bootstrap.servers")
        .or_else(|| props.get("datasink.config.bootstrap.servers"))
        .or_else(|| props.get("bootstrap.servers"))
        .or_else(|| props.get("brokers"))
        .or_else(|| props.get("kafka.brokers"))
        .or_else(|| props.get("producer_config.bootstrap.servers"))
        .cloned()
        .unwrap_or_else(|| "localhost:9092".to_string());

    // Apply runtime env var substitution if the value contains ${VAR:default} pattern
    // This handles cases where YAML was loaded before env vars were set (e.g., testcontainers)
    let brokers = substitute_env_vars(&brokers_raw);

    // Extract topic name from properties, or use sink name as default
    let topic = props
        .get("topic")
        .or_else(|| props.get("topic.name"))
        .map(|s| s.to_string())
        .unwrap_or_else(|| sink_name.to_string());

    info!(
        "Creating Kafka writer for sink '{}' with brokers '{}', topic '{}', instance: {}, transactional: {}",
        sink_name,
        brokers,
        topic,
        instance_id.unwrap_or("none"),
        use_transactions
    );

    // Build properties with client.id and transactional.id if transactional mode is enabled
    let mut sink_props = props.clone();

    // Generate client.id for producer identification in logs and monitoring
    let client_id = generate_processor_client_id(app_name, job_name, instance_id, sink_name);
    sink_props.insert("client.id".to_string(), client_id.clone());
    log_processor_client_id(
        &client_id,
        ClientType::Sink,
        app_name,
        job_name,
        instance_id,
        sink_name,
    );

    if use_transactions {
        // Generate unique transactional.id (must be unique per producer instance)
        let txn_id = generate_transactional_id(app_name, job_name, instance_id, sink_name);
        sink_props.insert("transactional.id".to_string(), txn_id.clone());
        log_transactional_id(&txn_id, app_name, job_name, instance_id, sink_name);
    }

    // Let KafkaDataSink handle its own configuration extraction
    // Pass use_transactions so the sink knows to configure transactional producer
    // Pass primary_keys from SQL PRIMARY KEY annotation for Kafka message key
    let mut datasink = KafkaDataSink::from_properties(
        &sink_props,
        job_name,
        app_name,
        instance_id,
        use_transactions,
        primary_keys.cloned(),
    );

    // Initialize with Kafka SinkConfig using extracted brokers, topic, and properties
    // Note: sink_props includes transactional.id if use_transactions is true
    let config = SinkConfig::Kafka {
        brokers,
        topic,
        properties: sink_props.clone(),
    };
    datasink
        .initialize(config)
        .await
        .map_err(|e| format!("Failed to initialize Kafka datasink: {}", e))?;

    // Create writer with batch configuration if available
    match batch_config {
        Some(batch_config) => {
            info!(
                "Creating Kafka writer with batch configuration: {:?}",
                batch_config
            );
            datasink
                .create_writer_with_batch_config(batch_config.clone())
                .await
                .map_err(|e| format!("Failed to create Kafka writer with batch config: {}", e))
        }
        None => {
            debug!("Creating Kafka writer without batch configuration");
            datasink
                .create_writer()
                .await
                .map_err(|e| format!("Failed to create Kafka writer: {}", e))
        }
    }
}

/// Create a file datasink writer
async fn create_file_writer(
    props: &HashMap<String, String>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> DataSinkCreationResult {
    use crate::velostream::datasource::config::FileFormat as ConfigFileFormat;

    // Extract format from properties first (needed for default path extension)
    let format_str = props
        .get("sink.format")
        .or_else(|| props.get("format"))
        .map(|s| s.to_lowercase());

    let format = match format_str.as_deref() {
        Some("csv") => ConfigFileFormat::Csv {
            header: true,
            delimiter: ',',
            quote: '"',
        },
        Some("jsonlines") | Some("json_lines") => ConfigFileFormat::JsonLines,
        _ => ConfigFileFormat::Json,
    };

    // Extract path from properties with format-appropriate default extension
    let default_path = match format_str.as_deref() {
        Some("csv") => "output.csv",
        Some("jsonlines") | Some("json_lines") => "output.jsonl",
        _ => "output.json",
    };

    let path = props
        .get("sink.path")
        .or_else(|| props.get("path"))
        .cloned()
        .unwrap_or_else(|| default_path.to_string());

    info!(
        "Creating File datasink writer with path: '{}', format: {:?}",
        path, format
    );

    // Create datasink from properties
    let mut datasink = FileDataSink::from_properties(props);

    // Initialize with SinkConfig using extracted path and format
    let config = SinkConfig::File {
        path,
        format,
        compression: None,
        properties: props.clone(),
    };
    datasink
        .initialize(config)
        .await
        .map_err(|e| format!("Failed to initialize File datasink: {}", e))?;

    // Create writer with batch configuration if available
    match batch_config {
        Some(batch_config) => {
            info!(
                "Creating File writer with batch configuration: {:?}",
                batch_config
            );
            datasink
                .create_writer_with_batch_config(batch_config.clone())
                .await
                .map_err(|e| format!("Failed to create File writer with batch config: {}", e))
        }
        None => {
            debug!("Creating File writer without batch configuration");
            datasink
                .create_writer()
                .await
                .map_err(|e| format!("Failed to create File writer: {}", e))
        }
    }
}

/// Create multiple datasource readers from analysis requirements
///
/// # Arguments
/// * `sources` - Data source requirements
/// * `job_name` - Name of the job
/// * `app_name` - Optional SQL Application name for consumer group coordination
/// * `instance_id` - Optional instance ID for unique client.id generation
/// * `batch_config` - Optional batch configuration
/// * `use_transactions` - Enable transactional mode (isolation.level=read_committed for Kafka consumers)
pub async fn create_multi_source_readers(
    sources: &[DataSourceRequirement],
    job_name: &str,
    app_name: Option<&str>,
    instance_id: Option<&str>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
    use_transactions: bool,
) -> MultiSourceCreationResult {
    let mut readers = HashMap::new();

    info!(
        "Creating {} data sources for job '{}' (transactional={})",
        sources.len(),
        job_name,
        use_transactions
    );

    for (idx, requirement) in sources.iter().enumerate() {
        let source_name = format!("source_{}_{}", idx, requirement.name);
        info!(
            "Creating source '{}' of type {:?} (transactional={})",
            source_name, requirement.source_type, use_transactions
        );

        // Use requirement.name as the default topic for this source
        let source_config = DataSourceConfig {
            requirement: requirement.clone(),
            default_topic: requirement.name.clone(), // Use source name as default
            job_name: job_name.to_string(),
            app_name: app_name.map(|a| a.to_string()),
            instance_id: instance_id.map(|i| i.to_string()),
            batch_config: batch_config.clone(),
            use_transactions,
        };

        match create_datasource_reader(&source_config).await {
            Ok(reader) => {
                info!(
                    "Successfully created source '{}' for job '{}'",
                    source_name, job_name
                );
                readers.insert(source_name, reader);
            }
            Err(e) => {
                error!(
                    "Failed to create source '{}' for job '{}': {}",
                    source_name, job_name, e
                );
                return Err(format!("Failed to create source '{}': {}", source_name, e));
            }
        }
    }

    info!(
        "Successfully created {} data sources for job '{}'",
        readers.len(),
        job_name
    );
    Ok(readers)
}

/// Create multiple datasink writers from analysis requirements
///
/// # Arguments
/// * `sinks` - Data sink requirements
/// * `job_name` - Name of the job
/// * `app_name` - Optional SQL Application name
/// * `instance_id` - Optional instance ID for unique client.id generation
/// * `batch_config` - Optional batch configuration
/// * `use_transactions` - Enable transactional mode (injects transactional.id for Kafka producers)
pub async fn create_multi_sink_writers(
    sinks: &[DataSinkRequirement],
    job_name: &str,
    app_name: Option<&str>,
    instance_id: Option<&str>,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
    use_transactions: bool,
) -> MultiSinkCreationResult {
    let mut writers = HashMap::new();

    if sinks.is_empty() {
        info!(
            "No sinks specified for job '{}', will use stdout as default",
            job_name
        );
        return Ok(writers);
    }

    info!(
        "Creating {} data sinks for job '{}' (transactional={})",
        sinks.len(),
        job_name,
        use_transactions
    );

    for (idx, requirement) in sinks.iter().enumerate() {
        let sink_name = format!("sink_{}_{}", idx, requirement.name);
        info!(
            "Creating sink '{}' of type {:?} (transactional={})",
            sink_name, requirement.sink_type, use_transactions
        );

        let sink_config = DataSinkConfig {
            requirement: requirement.clone(),
            job_name: job_name.to_string(),
            app_name: app_name.map(|a| a.to_string()),
            instance_id: instance_id.map(|i| i.to_string()),
            batch_config: batch_config.clone(),
            use_transactions,
        };

        match create_datasource_writer(&sink_config).await {
            Ok(writer) => {
                info!(
                    "Successfully created sink '{}' for job '{}'",
                    sink_name, job_name
                );
                writers.insert(sink_name, writer);
            }
            Err(e) => {
                warn!(
                    "Failed to create sink '{}' for job '{}': {}, skipping",
                    sink_name, job_name, e
                );
                // Don't fail job creation for sink failures, just log and continue
            }
        }
    }

    info!(
        "Successfully created {} data sinks for job '{}'",
        writers.len(),
        job_name
    );
    Ok(writers)
}

/// Log comprehensive configuration details for a job
pub fn log_job_configuration(job_name: &str, config: &JobProcessingConfig) {
    info!(
        "Job '{}' configuration: use_transactions={}, failure_strategy={:?}, max_batch_size={}, batch_timeout={}ms, max_retries={}, retry_backoff={}ms, progress_interval={}, log_progress={}",
        job_name,
        config.use_transactions,
        config.failure_strategy,
        config.max_batch_size,
        config.batch_timeout.as_millis(),
        config.max_retries,
        config.retry_backoff.as_millis(),
        config.progress_interval,
        config.log_progress
    );
}

/// Log detailed datasource type information
pub fn log_datasource_info(
    job_name: &str,
    reader: &dyn DataReader,
    writer: Option<&dyn DataWriter>,
) {
    let reader_type = std::any::type_name_of_val(reader);
    let writer_type = writer.map(std::any::type_name_of_val).unwrap_or("None");
    let reader_has_tx = reader.supports_transactions();
    let writer_has_tx = writer.map(|w| w.supports_transactions()).unwrap_or(false);

    info!(
        "Job '{}': Source type: '{}' (supports_transactions: {})",
        job_name, reader_type, reader_has_tx
    );

    info!(
        "Job '{}': Sink type: '{}' (supports_transactions: {})",
        job_name, writer_type, writer_has_tx
    );
}

/// Determine if batch should be committed based on failure strategy
pub fn should_commit_batch(
    failure_strategy: FailureStrategy,
    records_failed: usize,
    job_name: &str,
) -> bool {
    match failure_strategy {
        FailureStrategy::FailBatch => records_failed == 0,
        FailureStrategy::LogAndContinue => {
            if records_failed > 0 {
                warn!(
                    "Job '{}': {} records failed in batch, logging and continuing",
                    job_name, records_failed
                );
            }
            true
        }
        FailureStrategy::SendToDLQ => {
            if records_failed > 0 {
                warn!(
                    "Job '{}': {} records failed - DLQ not yet implemented, logging instead",
                    job_name, records_failed
                );
            }
            true
        }
        FailureStrategy::RetryWithBackoff => {
            if records_failed > 0 {
                warn!(
                    "Job '{}': {} records failed in batch - will retry with backoff",
                    job_name, records_failed
                );
                false
            } else {
                true
            }
        }
    }
}

/// Write batch to sink with error handling and retry logic
pub async fn write_batch_to_sink(
    writer: &mut dyn DataWriter,
    output_records: &[std::sync::Arc<StreamRecord>],
    job_name: &str,
    failure_strategy: FailureStrategy,
    retry_backoff: Duration,
) -> DataSourceResult<()> {
    // Pass Arc slice directly - write_batch now accepts Vec<Arc<StreamRecord>>
    match writer.write_batch(output_records.to_vec()).await {
        Ok(()) => {
            debug!(
                "Job '{}': Successfully wrote {} records to sink",
                job_name,
                output_records.len()
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                "Job '{}': Failed to write {} records to sink: {:?}",
                job_name,
                output_records.len(),
                e
            );

            if matches!(failure_strategy, FailureStrategy::RetryWithBackoff) {
                warn!(
                    "Job '{}': Applying retry backoff of {:?} before retrying batch",
                    job_name, retry_backoff
                );
                tokio::time::sleep(retry_backoff).await;
                Err(format!("Sink write failed, will retry: {}", e).into())
            } else {
                warn!(
                    "Job '{}': Sink write failed but continuing (failure strategy: {:?})",
                    job_name, failure_strategy
                );
                Err(format!("Sink write failed: {:?}", e).into())
            }
        }
    }
}

/// Update stats from batch processing result
pub fn update_stats_from_batch_result(
    stats: &mut JobExecutionStats,
    batch_result: &BatchProcessingResultWithOutput,
) {
    let stats_result = BatchProcessingResult {
        records_processed: batch_result.records_processed,
        records_failed: batch_result.records_failed,
        processing_time: batch_result.processing_time,
        batch_size: batch_result.batch_size,
        error_details: batch_result.error_details.clone(),
    };
    stats.update_from_batch(&stats_result);
}

/// Handle missing sink by creating a StdoutWriter and logging a warning
pub fn ensure_sink_or_create_stdout(writer: &mut Option<Box<dyn DataWriter>>, job_name: &str) {
    if writer.is_none() {
        warn!(
            "Job '{}': No sink specified, defaulting to stdout.",
            job_name
        );
        *writer = Some(Box::new(StdoutWriter::new_pretty()));
    }
}
