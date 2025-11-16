//! Common functionality for multi-job SQL processing
//!
//! This module contains shared functionality used by both transactional
//! and non-transactional multi-job processors.

use crate::velostream::datasource::{
    DataReader, DataSink, DataSource, DataWriter, SinkConfig, StdoutWriter,
    file::{FileDataSink, FileDataSource},
    kafka::{KafkaDataSink, KafkaDataSource},
};
use crate::velostream::server::processors::SimpleJobProcessor;
use crate::velostream::sql::{
    StreamExecutionEngine, StreamingQuery,
    ast::{StreamSource, StreamingQuery as AstStreamingQuery},
    execution::{
        config::StreamingConfig,
        processors::{QueryProcessor, context::ProcessorContext},
        types::StreamRecord,
    },
    query_analyzer::{DataSinkRequirement, DataSinkType, DataSourceRequirement, DataSourceType},
};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

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
#[derive(Debug, Clone)]
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
}

impl DeadLetterQueue {
    /// Create a new DLQ
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add a failed record to the DLQ
    pub async fn add_entry(
        &self,
        record: StreamRecord,
        error_message: String,
        record_index: usize,
        recoverable: bool,
    ) {
        let entry = DLQEntry {
            record,
            error_message,
            record_index,
            recoverable,
            timestamp: Instant::now(),
        };
        self.entries.lock().await.push(entry);
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

    /// Clear the DLQ
    pub async fn clear(&self) {
        self.entries.lock().await.clear();
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
}

impl Default for DeadLetterQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for job execution
#[derive(Debug, Clone, Default)]
pub struct JobExecutionStats {
    pub records_processed: u64,
    pub records_failed: u64,
    pub batches_processed: u64,
    pub batches_failed: u64,
    pub start_time: Option<Instant>,
    pub last_record_time: Option<Instant>,
    pub avg_batch_size: f64,
    pub avg_processing_time_ms: f64,
    pub total_processing_time: Duration,
    /// Detailed error information for debugging
    pub error_details: Vec<ProcessingError>,
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

            self.avg_processing_time_ms = ((self.avg_processing_time_ms * (total_batches - 1.0))
                + result.processing_time.as_millis() as f64)
                / total_batches;
        }
    }

    pub fn records_per_second(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.records_processed as f64 / elapsed;
            }
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

    pub fn elapsed(&self) -> Duration {
        self.start_time
            .map(|s| s.elapsed())
            .unwrap_or(Duration::from_secs(0))
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
            empty_batch_count: 1000,
            wait_on_empty_batch_ms: 1000,
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
/// Process a batch of records and capture SQL engine output for sink writing
///
/// Uses direct QueryProcessor calls for low-latency processing without holding
/// engine locks during batch processing. This approach:
/// - Eliminates per-record engine lock contention
/// - Returns actual SQL query results (not input passthroughs)
/// - Supports GROUP BY/Window aggregations via ProcessorContext state
/// - Achieves 2x+ performance improvement over execute_with_record()
///
/// ## Architecture Pattern
/// 1. Get state once at batch start (minimal lock time)
/// 2. Process batch with local state copies (no locks)
/// 3. Sync state back once at batch end (minimal lock time)
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

    // Generate query ID for state management
    let query_id = generate_query_id(query);

    // FR-082 Phase 5: Detect EMIT CHANGES mode for hybrid routing
    let uses_emit_changes = is_emit_changes_query(query);

    if uses_emit_changes {
        // FR-082 Week 8 Optimization 2: Lock-free batch processing for EMIT CHANGES
        // Previous: Per-record locking (1,000 locks per 1,000 records)
        // New: 2 locks total (batch start + batch end), process without lock
        // Expected improvement: 2-3x from reduced lock contention
        debug!(
            "Job '{}': Using lock-free EMIT CHANGES path for {} records (Optimization 2)",
            job_name, batch_size
        );

        // Lock 1: Get output channel ONCE at batch start
        let mut temp_receiver = {
            let mut engine_lock = engine.write().await;
            engine_lock.take_output_receiver()
        };

        // Get output sender (cheap clone operation, minimal lock time)
        let output_sender = {
            let engine_lock = engine.read().await;
            engine_lock.get_output_sender_for_batch()
        };

        // Phase 6.5: Get mutable context from QueryExecution
        // Context persists for the lifetime of the query, accumulating state
        // Get streaming config once at batch start
        let streaming_config = {
            let engine_lock = engine.read().await;
            engine_lock.streaming_config().clone()
        };

        let output_sender = {
            let engine_lock = engine.read().await;
            engine_lock.get_output_sender_for_batch()
        };

        // Get Arc<Mutex<ProcessorContext>> from QueryExecution - NOT holding engine lock during processing
        // FR-082 STP: Arc is cheap to clone for ownership transfer, Mutex lock held only during processing
        // FR-082 Phase 6.8: Try read-only first, fall back to lazy init if needed
        // This eliminates the write lock for queries that have been initialized at startup
        let processor_context_arc = {
            // Try to get without lazy initialization (fast path for normal operation)
            {
                let engine_lock = engine.read().await;
                if let Some(context_arc) = engine_lock.get_query_execution_context(query) {
                    context_arc
                } else {
                    // Slow path: query not yet initialized, need write lock for lazy init
                    // This happens when process_batch() is called directly without process_job()
                    drop(engine_lock); // Release read lock before acquiring write lock
                    let mut engine_lock = engine.write().await;
                    match engine_lock.ensure_query_execution(query) {
                        Some(context_arc) => context_arc,
                        None => {
                            error!("Failed to initialize query execution: {}", query_id);
                            return BatchProcessingResultWithOutput {
                                records_processed: 0,
                                records_failed: batch.len(),
                                processing_time: Duration::from_secs(0),
                                batch_size: batch.len(),
                                error_details: vec![ProcessingError {
                                    record_index: 0,
                                    error_message: format!(
                                        "Failed to initialize query execution: {}",
                                        query_id
                                    ),
                                    recoverable: false,
                                }],
                                output_records: vec![],
                            };
                        }
                    }
                }
            }
        };

        // Set streaming config on the persistent context
        {
            let mut ctx = processor_context_arc.lock().unwrap();
            ctx.streaming_config = Some(streaming_config.clone());
        }

        // Acquire Mutex lock only during record processing (minimal contention)
        // CRITICAL: Lock is held only during batch processing, released before any await
        {
            let mut ctx = processor_context_arc.lock().unwrap();

            // FR-082 Week 8 Optimization 1+2: Batch emission with lock-free processing
            // Process all records WITHOUT holding engine lock
            for (index, record) in batch.into_iter().enumerate() {
                match QueryProcessor::process_query(query, &record, &mut ctx) {
                    Ok(result) => {
                        records_processed += 1;

                        // For EMIT CHANGES: emit each output record through the channel
                        if let Some(output) = result.record {
                            let _ = output_sender.send(output);
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

                        warn!(
                            "Job '{}' EMIT CHANGES (lock-free) failed to process record {}: {} [Recoverable: {}]",
                            job_name, index, detailed_msg, recoverable
                        );
                        debug!("Full error details: {:?}", e);
                    }
                }
            }
        } // MutexGuard explicitly dropped here before any await

        // Collect any emitted results from receiver if still available
        if let Some(rx) = &mut temp_receiver {
            while let Ok(emitted_record) = rx.try_recv() {
                output_records.push(Arc::new(emitted_record));
            }
        }

        // No sync needed - context is Arc<Mutex>, mutations are already persisted in-place
        if let Some(rx) = temp_receiver {
            let mut engine_lock = engine.write().await;
            engine_lock.return_output_receiver(rx);
        }
    } else {
        // Standard Path: Batch optimization for non-EMIT queries (high performance)
        // Track null output records for debugging
        let mut null_output_count = 0;
        let mut null_output_example_idx = None;

        // Phase 6.5: Get Arc<Mutex<ProcessorContext>> from QueryExecution
        // Context persists for lifetime of query - acquired by reference, never cloned
        // FR-082 STP: Arc is cheap to clone for ownership transfer, Mutex lock held only during processing
        // FR-082 Phase 6.8: Try read-only first, fall back to lazy init if needed
        // This eliminates the write lock for queries that have been initialized at startup
        let processor_context_arc = {
            // Try to get without lazy initialization (fast path for normal operation)
            {
                let engine_lock = engine.read().await;
                if let Some(context_arc) = engine_lock.get_query_execution_context(query) {
                    context_arc
                } else {
                    // Slow path: query not yet initialized, need write lock for lazy init
                    // This happens when process_batch() is called directly without process_job()
                    drop(engine_lock); // Release read lock before acquiring write lock
                    let mut engine_lock = engine.write().await;
                    match engine_lock.ensure_query_execution(query) {
                        Some(context_arc) => context_arc,
                        None => {
                            error!("Failed to initialize query execution: {}", query_id);
                            return BatchProcessingResultWithOutput {
                                records_processed: 0,
                                records_failed: batch.len(),
                                processing_time: Duration::from_secs(0),
                                batch_size: batch.len(),
                                error_details: vec![ProcessingError {
                                    record_index: 0,
                                    error_message: format!(
                                        "Failed to initialize query execution: {}",
                                        query_id
                                    ),
                                    recoverable: false,
                                }],
                                output_records: vec![],
                            };
                        }
                    }
                }
            }
        };

        // Set streaming config on the persistent context
        {
            let streaming_config = {
                let engine_lock = engine.read().await;
                engine_lock.streaming_config().clone()
            };
            let mut ctx = processor_context_arc.lock().unwrap();
            ctx.streaming_config = Some(streaming_config);
        }

        // Process batch WITHOUT holding engine lock (high performance)
        debug!(
            "Job '{}': Standard path processing {} records (uses_emit_changes = false)",
            job_name, batch_size
        );

        // Acquire Mutex lock only during record processing (minimal contention)
        // CRITICAL: Lock is held only during batch processing, released before any await
        // Context state accumulates (GROUP BY, window state) across all records
        {
            let mut ctx = processor_context_arc.lock().unwrap();

            for (index, record) in batch.into_iter().enumerate() {
                match QueryProcessor::process_query(query, &record, &mut ctx) {
                    Ok(result) => {
                        records_processed += 1;

                        // Collect ACTUAL SQL query results for sink writing
                        // (not input passthrough - this is the critical fix!)
                        // PERF: Wrap in Arc for zero-copy multi-sink writes (7x faster)
                        if let Some(output) = result.record {
                            output_records.push(Arc::new(output));
                        } else {
                            // DIAGNOSTIC: Record when result.record is None
                            // This indicates the query executed but produced no output for this record
                            null_output_count += 1;
                            if null_output_example_idx.is_none() {
                                null_output_example_idx = Some(index);
                            }

                            debug!(
                                "Job '{}' record {} processed successfully but produced no output (result.record is None)",
                                job_name, index
                            );
                        }

                        // NOTE: State accumulation happens automatically in context
                        // No need to copy state back - it's already mutated in place
                    }
                    Err(e) => {
                        records_failed += 1;

                        // Extract and log detailed error context
                        let detailed_msg = extract_error_context(&e);
                        let recoverable = is_recoverable_error(&e);

                        error_details.push(ProcessingError {
                            record_index: index,
                            error_message: detailed_msg.clone(),
                            recoverable,
                        });

                        // Log with human-readable context and full debug info
                        if index < 3 {
                            // Log first 3 errors to avoid log spam
                            warn!(
                                "Job '{}' failed to process record {}: {} [Recoverable: {}]",
                                job_name, index, detailed_msg, recoverable
                            );
                            debug!("Full error details: {:?}", e);
                        } else if index == 3 {
                            warn!(
                                "Job '{}' (suppressing further error logs for batch, {} errors total)",
                                job_name, batch_size
                            );
                        }
                    }
                }
            }
        } // MutexGuard explicitly dropped here

        debug!(
            "Job '{}': Standard path completed: {} processed, {} failed",
            job_name, records_processed, records_failed
        );

        // DIAGNOSTIC: Log summary of null output records
        if null_output_count > 0 {
            warn!(
                "Job '{}': ⚠️  DIAGNOSTIC: {} out of {} processed records produced no output (result.record was None)",
                job_name, null_output_count, records_processed
            );
            if let Some(idx) = null_output_example_idx {
                warn!(
                    "Job '{}': First example of null output at record index {}",
                    job_name, idx
                );
                warn!("Job '{}': Possible causes:", job_name);
                warn!(
                    "   1. Query produces no output (e.g., no rows match filters, window not complete)"
                );
                warn!("   2. Stream/JOIN produces no output by design");
                warn!("   3. Aggregation hasn't collected enough records to emit");
                warn!("   4. Record is filtered out (WHERE/HAVING clauses exclude it)");
            }
        }

        // No sync needed - context is Arc<Mutex>, mutations are already persisted in-place
    }

    BatchProcessingResultWithOutput {
        records_processed,
        records_failed,
        processing_time: batch_start.elapsed(),
        batch_size,
        error_details,
        output_records,
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

/// Log progress for a job
pub fn log_job_progress(job_name: &str, stats: &JobExecutionStats) {
    let rps = stats.records_per_second();
    let success_rate = stats.success_rate();
    let elapsed = stats.elapsed();

    // Check for data starvation - job running but processing 0 records
    if stats.records_processed == 0 && elapsed.as_secs() >= 60 {
        error!(
            "🚨 DATA STARVATION DETECTED: Job '{}' has processed 0 records for {} seconds",
            job_name,
            elapsed.as_secs()
        );
        error!("   Possible causes:");
        error!("   1. Source Kafka topic is empty or not producing messages");
        error!("   2. Schema deserialization is failing (check for missing schema files)");
        error!("   3. Consumer group offset is stuck or invalid");
        error!("   4. Network connectivity issues with data source");
        error!("   Action required: Investigate source configuration and data flow immediately");
        error!("   Check: curl http://localhost:9091/metrics | grep velo_streaming_throughput_rps");
    } else if stats.records_processed == 0 {
        warn!(
            "Job '{}': 0 records processed after {} seconds (waiting for data...)",
            job_name,
            elapsed.as_secs()
        );
    }

    info!(
        "Job '{}': {} records processed ({} batches), {:.2} records/sec, {:.1}% success rate, {:.1}ms avg batch time",
        job_name,
        stats.records_processed,
        stats.batches_processed,
        rps,
        success_rate,
        stats.avg_processing_time_ms
    );
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
            stats.avg_processing_time_ms
        );
    }
}

/// Result type for datasource operations
pub type DataSourceResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Configuration for creating a datasource
#[derive(Debug, Clone)]
pub struct DataSourceConfig {
    pub requirement: DataSourceRequirement,
    pub default_topic: String,
    pub job_name: String,
    pub batch_config: Option<crate::velostream::datasource::BatchConfig>,
}

#[derive(Debug, Clone)]
pub struct DataSinkConfig {
    pub requirement: DataSinkRequirement,
    pub job_name: String,
    pub batch_config: Option<crate::velostream::datasource::BatchConfig>,
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
                &config.batch_config,
            )
            .await
        }
        DataSourceType::File => {
            create_file_reader(&requirement.properties, &config.batch_config).await
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
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> DataSourceCreationResult {
    // Extract topic name from properties, or use source name as default
    let topic = props
        .get("topic")
        .or_else(|| props.get("source.topic"))
        .or_else(|| props.get("datasource.topic.name"))
        .map(|s| s.to_string())
        .unwrap_or_else(|| source_name.to_string());

    info!(
        "Creating Kafka reader for source '{}' with topic '{}'",
        source_name, topic
    );

    // Let KafkaDataSource handle its own configuration extraction
    let mut datasource = KafkaDataSource::from_properties(props, &topic, job_name);

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

/// Create a datasink writer based on configuration
pub async fn create_datasource_writer(config: &DataSinkConfig) -> DataSinkCreationResult {
    let requirement = &config.requirement;

    match requirement.sink_type {
        DataSinkType::Kafka => {
            create_kafka_writer(
                &requirement.properties,
                &requirement.name, // Use sink name, not job name
                &config.batch_config,
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
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> DataSinkCreationResult {
    // Extract brokers from properties
    let brokers = props
        .get("bootstrap.servers")
        .or_else(|| props.get("brokers"))
        .or_else(|| props.get("kafka.brokers"))
        .or_else(|| props.get("producer_config.bootstrap.servers"))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "localhost:9092".to_string());

    // Extract topic name from properties, or use sink name as default
    let topic = props
        .get("topic")
        .or_else(|| props.get("topic.name"))
        .map(|s| s.to_string())
        .unwrap_or_else(|| sink_name.to_string());

    info!(
        "Creating Kafka writer for sink '{}' with brokers '{}', topic '{}'",
        sink_name, brokers, topic
    );

    // Let KafkaDataSink handle its own configuration extraction
    let mut datasink = KafkaDataSink::from_properties(props, sink_name);

    // Initialize with Kafka SinkConfig using extracted brokers, topic, and properties
    let config = SinkConfig::Kafka {
        brokers,
        topic,
        properties: props.clone(),
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
    // Let FileSink handle its own configuration extraction
    let mut datasink = FileDataSink::from_properties(props);

    // Initialize with File SinkConfig
    let config = SinkConfig::File {
        path: "output.json".to_string(),
        format: crate::velostream::datasource::FileFormat::Json,
        compression: None,
        properties: HashMap::new(),
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
pub async fn create_multi_source_readers(
    sources: &[DataSourceRequirement],
    job_name: &str,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> MultiSourceCreationResult {
    let mut readers = HashMap::new();

    info!(
        "Creating {} data sources for job '{}'",
        sources.len(),
        job_name
    );

    for (idx, requirement) in sources.iter().enumerate() {
        let source_name = format!("source_{}_{}", idx, requirement.name);
        info!(
            "Creating source '{}' of type {:?}",
            source_name, requirement.source_type
        );

        // Use requirement.name as the default topic for this source
        let source_config = DataSourceConfig {
            requirement: requirement.clone(),
            default_topic: requirement.name.clone(), // Use source name as default
            job_name: job_name.to_string(),
            batch_config: batch_config.clone(),
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
pub async fn create_multi_sink_writers(
    sinks: &[DataSinkRequirement],
    job_name: &str,
    batch_config: &Option<crate::velostream::datasource::BatchConfig>,
) -> MultiSinkCreationResult {
    let mut writers = HashMap::new();

    if sinks.is_empty() {
        info!(
            "No sinks specified for job '{}', will use stdout as default",
            job_name
        );
        return Ok(writers);
    }

    info!("Creating {} data sinks for job '{}'", sinks.len(), job_name);

    for (idx, requirement) in sinks.iter().enumerate() {
        let sink_name = format!("sink_{}_{}", idx, requirement.name);
        info!(
            "Creating sink '{}' of type {:?}",
            sink_name, requirement.sink_type
        );

        let sink_config = DataSinkConfig {
            requirement: requirement.clone(),
            job_name: job_name.to_string(),
            batch_config: batch_config.clone(),
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
