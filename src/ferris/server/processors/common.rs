//! Common functionality for multi-job SQL processing
//!
//! This module contains shared functionality used by both transactional
//! and non-transactional multi-job processors.

use crate::ferris::datasource::{
    file::FileDataSource, kafka::KafkaDataSource, DataReader, DataSource, DataWriter,
};
use crate::ferris::sql::{
    execution::types::StreamRecord,
    query_analyzer::{DataSourceRequirement, DataSourceType},
    StreamExecutionEngine, StreamingQuery,
};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

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
}

impl Default for JobProcessingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(1000),
            use_transactions: false,
            failure_strategy: FailureStrategy::LogAndContinue,
            max_retries: 3,
            retry_backoff: Duration::from_millis(1000),
            log_progress: true,
            progress_interval: 10,
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
    pub output_records: Vec<StreamRecord>, // SQL engine results ready for sink
}

/// Process a batch of records through the SQL execution engine
pub async fn process_batch_common(
    batch: Vec<StreamRecord>,
    engine: &Arc<tokio::sync::Mutex<StreamExecutionEngine>>,
    query: &StreamingQuery,
    job_name: &str,
) -> BatchProcessingResult {
    let result = process_batch_with_output(batch, engine, query, job_name).await;

    // Convert to the original result type (without output records)
    BatchProcessingResult {
        records_processed: result.records_processed,
        records_failed: result.records_failed,
        processing_time: result.processing_time,
        batch_size: result.batch_size,
        error_details: result.error_details,
    }
}

/// Process a batch of records and capture SQL engine output for sink writing
/// Uses the engine's existing execute_from_batch method to capture results
pub async fn process_batch_with_output(
    batch: Vec<StreamRecord>,
    engine: &Arc<tokio::sync::Mutex<StreamExecutionEngine>>,
    query: &StreamingQuery,
    job_name: &str,
) -> BatchProcessingResultWithOutput {
    let batch_start = Instant::now();
    let batch_size = batch.len();
    let mut records_processed = 0;
    let mut records_failed = 0;
    let mut error_details = Vec::new();
    let mut output_records = Vec::new();

    // Process each record individually to capture output and handle errors
    for (index, record) in batch.into_iter().enumerate() {
        let mut engine_lock = engine.lock().await;

        // Try to execute the record and capture the result
        match engine_lock.execute_with_record(query, record.clone()).await {
            Ok(()) => {
                records_processed += 1;
                // For now, we'll add the processed record to output
                // TODO: This is a placeholder - ideally we'd capture the actual SQL output
                // For simple SELECT * queries, the output would be the input record
                // For more complex queries, we'd need the transformed result
                output_records.push(record);
            }
            Err(e) => {
                records_failed += 1;
                error_details.push(ProcessingError {
                    record_index: index,
                    error_message: format!("{:?}", e),
                    recoverable: is_recoverable_error(&e),
                });
                warn!(
                    "Job '{}' failed to process record {}: {:?}",
                    job_name, index, e
                );
            }
        }
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

/// Determine if an error is recoverable
fn is_recoverable_error(_error: &crate::ferris::sql::SqlError) -> bool {
    // This is a simple implementation - could be extended based on error types
    // Add specific error pattern matching here
    false
}

/// Log progress for a job
pub fn log_job_progress(job_name: &str, stats: &JobExecutionStats) {
    let rps = stats.records_per_second();
    let success_rate = stats.success_rate();

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
}

/// Result of datasource creation
pub type DataSourceCreationResult = Result<Box<dyn DataReader>, String>;

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
                &config.default_topic,
                &config.job_name,
            )
            .await
        }
        DataSourceType::File => create_file_reader(&requirement.properties).await,
        _ => Err(format!(
            "Unsupported datasource type '{:?}'",
            requirement.source_type
        )),
    }
}

/// Create a Kafka datasource reader
async fn create_kafka_reader(
    props: &HashMap<String, String>,
    default_topic: &str,
    job_name: &str,
) -> DataSourceCreationResult {
    // Let KafkaDataSource handle its own configuration extraction
    let mut datasource = KafkaDataSource::from_properties(props, default_topic, job_name);

    // Self-initialize with the extracted configuration
    datasource
        .self_initialize()
        .await
        .map_err(|e| format!("Failed to initialize Kafka datasource: {}", e))?;

    datasource
        .create_reader()
        .await
        .map_err(|e| format!("Failed to create Kafka reader: {}", e))
}

/// Create a file datasource reader
async fn create_file_reader(props: &HashMap<String, String>) -> DataSourceCreationResult {
    // Let FileDataSource handle its own configuration extraction
    let mut datasource = FileDataSource::from_properties(props);

    // Self-initialize with the extracted configuration
    datasource
        .self_initialize()
        .await
        .map_err(|e| format!("Failed to initialize File datasource: {}", e))?;

    datasource
        .create_reader()
        .await
        .map_err(|e| format!("Failed to create File reader: {}", e))
}

/// Process records from a datasource using the modern multi-job processors
/// This is the recommended entry point that automatically chooses between
/// simple and transactional processing based on datasource capabilities
pub async fn process_datasource_records(
    reader: Box<dyn DataReader>,
    writer: Option<Box<dyn DataWriter>>,
    execution_engine: Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: StreamingQuery,
    job_name: String,
    shutdown_receiver: mpsc::Receiver<()>,
    config: JobProcessingConfig,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
    // Choose processor based on configuration and datasource capabilities
    if config.use_transactions && reader.supports_transactions() {
        use crate::ferris::server::processors::transactional::TransactionalJobProcessor;
        let processor = TransactionalJobProcessor::new(config);
        processor
            .process_job(
                reader,
                writer,
                execution_engine,
                parsed_query,
                job_name,
                shutdown_receiver,
            )
            .await
    } else {
        use crate::ferris::server::processors::simple::SimpleJobProcessor;
        let processor = SimpleJobProcessor::new(config);
        processor
            .process_job(
                reader,
                writer,
                execution_engine,
                parsed_query,
                job_name,
                shutdown_receiver,
            )
            .await
    }
}
