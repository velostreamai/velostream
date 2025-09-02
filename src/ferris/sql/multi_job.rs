//! Multi-job SQL server support
//!
//! This module provides functionality for running multiple SQL jobs concurrently,
//! with support for different datasources and execution monitoring.

use crate::ferris::datasource::{
    config::BatchConfig, file::FileDataSource, kafka::KafkaDataSource, DataReader, DataSource,
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

/// Configuration for creating a datasource
#[derive(Debug, Clone)]
pub struct DataSourceConfig {
    pub requirement: DataSourceRequirement,
    pub default_topic: String,
    pub job_name: String,
}

/// Result of datasource creation
pub type DataSourceResult = Result<Box<dyn DataReader>, String>;

/// Result of batch processing
#[derive(Debug)]
pub struct BatchResult {
    pub records_processed: usize,
    pub records_failed: usize,
    pub processing_time: Duration,
    pub batch_size: usize,
}

/// Statistics for job execution
#[derive(Debug, Clone, Default)]
pub struct JobExecutionStats {
    pub records_processed: u64,
    pub start_time: Option<Instant>,
    pub errors: u64,
    pub last_record_time: Option<Instant>,
    // Batch-specific stats
    pub batches_processed: u64,
    pub avg_batch_size: f64,
    pub total_batch_processing_time: Duration,
}

impl JobExecutionStats {
    pub fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Update statistics after batch processing
    pub fn update_batch_stats(&mut self, batch_result: &BatchResult) {
        self.batches_processed += 1;
        self.records_processed += batch_result.records_processed as u64;
        self.errors += batch_result.records_failed as u64;
        self.last_record_time = Some(Instant::now());
        self.total_batch_processing_time += batch_result.processing_time;

        // Update moving average of batch size
        let total_records = self.batches_processed as f64;
        self.avg_batch_size = ((self.avg_batch_size * (total_records - 1.0))
            + batch_result.batch_size as f64)
            / total_records;
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

    pub fn elapsed(&self) -> Duration {
        self.start_time
            .map(|s| s.elapsed())
            .unwrap_or(Duration::from_secs(0))
    }
}

/// Create a datasource reader based on configuration
pub async fn create_datasource_reader(config: &DataSourceConfig) -> DataSourceResult {
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
) -> DataSourceResult {
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
async fn create_file_reader(props: &HashMap<String, String>) -> DataSourceResult {
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

/// Process records from a datasource (enhanced with batch processing)
pub async fn process_datasource_records(
    reader: Box<dyn DataReader>,
    execution_engine: Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: StreamingQuery,
    job_name: String,
    shutdown_receiver: mpsc::UnboundedReceiver<()>,
) -> JobExecutionStats {
    // Use default batch configuration for now - can be made configurable later
    let batch_config = BatchConfig::default();
    process_datasource_records_with_batch_config(
        reader,
        execution_engine,
        parsed_query,
        job_name,
        shutdown_receiver,
        batch_config,
    )
    .await
}

/// Process records from a datasource with configurable batch processing
pub async fn process_datasource_records_with_batch_config(
    mut reader: Box<dyn DataReader>,
    execution_engine: Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: StreamingQuery,
    job_name: String,
    mut shutdown_receiver: mpsc::UnboundedReceiver<()>,
    batch_config: BatchConfig,
) -> JobExecutionStats {
    let mut stats = JobExecutionStats::new();

    if batch_config.enable_batching {
        // Batch processing mode - use datasource's native read_batch() method
        loop {
            // Check for shutdown signal
            if shutdown_receiver.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Check if datasource supports transactions for exactly-once processing
            let supports_transactions = reader.supports_transactions();

            let batch_start = Instant::now();

            // Begin transaction if supported (Kafka exactly-once semantics)
            if supports_transactions {
                match reader.begin_transaction().await {
                    Ok(true) => {
                        // Transaction started successfully
                        info!(
                            "Job '{}' started transaction for batch processing",
                            job_name
                        );
                    }
                    Ok(false) => {
                        warn!("Job '{}' datasource claimed transaction support but begin_transaction returned false", job_name);
                    }
                    Err(e) => {
                        warn!("Job '{}' failed to begin transaction: {:?}", job_name, e);
                        stats.errors += 1;
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                }
            }

            // Use read() method which returns configured batch size
            // The batch size is configured in the datasource during initialization
            match reader.read().await {
                Ok(batch) if batch.is_empty() => {
                    // No data available, abort transaction if active
                    if supports_transactions {
                        if let Err(e) = reader.abort_transaction().await {
                            warn!(
                                "Job '{}' failed to abort empty batch transaction: {:?}",
                                job_name, e
                            );
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Ok(batch) => {
                    // Process the batch using SQL engine
                    let batch_result = process_batch(
                        batch,
                        &execution_engine,
                        &parsed_query,
                        &job_name,
                        batch_start,
                    )
                    .await;

                    // Check if batch processing succeeded
                    let batch_succeeded = batch_result.records_failed == 0;

                    if batch_succeeded && supports_transactions {
                        // Commit transaction on successful batch processing
                        match reader.commit_transaction().await {
                            Ok(()) => {
                                info!("Job '{}' successfully committed transaction for batch of {} records", 
                                      job_name, batch_result.records_processed);
                            }
                            Err(e) => {
                                error!("Job '{}' failed to commit transaction: {:?}", job_name, e);
                                stats.errors += 1;

                                // Try to abort the transaction
                                if let Err(abort_err) = reader.abort_transaction().await {
                                    error!(
                                        "Job '{}' also failed to abort transaction: {:?}",
                                        job_name, abort_err
                                    );
                                }
                                tokio::time::sleep(Duration::from_millis(1000)).await;
                                continue;
                            }
                        }
                    } else if !batch_succeeded && supports_transactions {
                        // Abort transaction on failed batch processing
                        warn!(
                            "Job '{}' aborting transaction due to {} failed records",
                            job_name, batch_result.records_failed
                        );

                        if let Err(e) = reader.abort_transaction().await {
                            error!("Job '{}' failed to abort transaction: {:?}", job_name, e);
                        }
                        stats.errors += 1;
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    } else if !supports_transactions {
                        // Non-transactional commit for datasources without transaction support
                        if let Err(e) = reader.commit().await {
                            warn!("Job '{}' failed to commit batch: {:?}", job_name, e);
                            stats.errors += 1;
                        }
                    }

                    stats.update_batch_stats(&batch_result);

                    // Log progress periodically
                    if stats.batches_processed % 10 == 0 {
                        log_batch_progress(&job_name, &stats);
                    }
                }
                Err(e) => {
                    warn!("Job '{}' error reading batch: {:?}", job_name, e);

                    // Abort transaction on read error
                    if supports_transactions {
                        if let Err(abort_err) = reader.abort_transaction().await {
                            error!(
                                "Job '{}' failed to abort transaction after read error: {:?}",
                                job_name, abort_err
                            );
                        }
                    }

                    stats.errors += 1;
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
    } else {
        // Single record processing mode (fallback)
        loop {
            // Check for shutdown signal
            if shutdown_receiver.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Process next record
            match process_single_record(
                &mut reader,
                &execution_engine,
                &parsed_query,
                &job_name,
                &mut stats,
            )
            .await
            {
                ProcessResult::Continue => continue,
                ProcessResult::NoData => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                ProcessResult::Error => {
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
    }

    log_final_stats(&job_name, &stats);
    stats
}

/// Result of processing a single record
#[derive(Debug)]
enum ProcessResult {
    Continue,
    NoData,
    Error,
}

/// Process a batch of records through the execution engine
async fn process_batch(
    batch: Vec<StreamRecord>,
    execution_engine: &Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: &StreamingQuery,
    job_name: &str,
    batch_start: Instant,
) -> BatchResult {
    let batch_size = batch.len();
    let mut records_processed = 0;
    let mut records_failed = 0;

    // Process each record in the batch
    let mut engine = execution_engine.lock().await;

    for record in batch {
        match engine.execute_with_record(parsed_query, record).await {
            Ok(_) => records_processed += 1,
            Err(e) => {
                error!(
                    "Job '{}' failed to process record in batch: {:?}",
                    job_name, e
                );
                records_failed += 1;
            }
        }
    }

    drop(engine);

    let processing_time = batch_start.elapsed();

    BatchResult {
        records_processed,
        records_failed,
        processing_time,
        batch_size,
    }
}

/// Process a single record from the datasource
async fn process_single_record(
    reader: &mut Box<dyn DataReader>,
    execution_engine: &Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: &StreamingQuery,
    job_name: &str,
    stats: &mut JobExecutionStats,
) -> ProcessResult {
    match reader.read().await {
        Ok(records) if records.is_empty() => ProcessResult::NoData,
        Ok(records) => {
            // Process each record in the batch
            let mut engine = execution_engine.lock().await;
            for record in records {
                stats.records_processed += 1;
                stats.last_record_time = Some(Instant::now());

                // Execute the query directly with StreamRecord (most efficient)
                if let Err(e) = engine.execute_with_record(parsed_query, record).await {
                    error!("Job '{}' failed to process record: {:?}", job_name, e);
                    stats.errors += 1;
                }

                // Log progress periodically
                if stats.records_processed % 1000 == 0 {
                    log_progress(job_name, stats);
                }
            }
            drop(engine);

            ProcessResult::Continue
        }
        Err(e) => {
            warn!("Job '{}' error reading from datasource: {:?}", job_name, e);
            stats.errors += 1;
            ProcessResult::Error
        }
    }
}

/// Log progress information
fn log_progress(job_name: &str, stats: &JobExecutionStats) {
    let rps = stats.records_per_second();
    info!(
        "Job '{}': processed {} records ({:.2} records/sec)",
        job_name, stats.records_processed, rps
    );
}

/// Log batch processing progress
fn log_batch_progress(job_name: &str, stats: &JobExecutionStats) {
    let rps = stats.records_per_second();
    let avg_processing_time = if stats.batches_processed > 0 {
        stats.total_batch_processing_time.as_millis() as f64 / stats.batches_processed as f64
    } else {
        0.0
    };

    info!(
        "Job '{}': processed {} batches ({} records, {:.2} records/sec, {:.1} avg batch size, {:.2}ms avg batch time)",
        job_name,
        stats.batches_processed,
        stats.records_processed,
        rps,
        stats.avg_batch_size,
        avg_processing_time
    );
}

/// Log final execution statistics
fn log_final_stats(job_name: &str, stats: &JobExecutionStats) {
    let elapsed = stats.elapsed().as_secs_f64();
    let rps = stats.records_per_second();

    if stats.batches_processed > 0 {
        let avg_processing_time =
            stats.total_batch_processing_time.as_millis() as f64 / stats.batches_processed as f64;
        info!(
            "Job '{}' completed: processed {} batches ({} records) in {:.2}s ({:.2} records/sec, {:.1} avg batch size, {:.2}ms avg batch time), {} errors",
            job_name,
            stats.batches_processed,
            stats.records_processed,
            elapsed,
            rps,
            stats.avg_batch_size,
            avg_processing_time,
            stats.errors
        );
    } else {
        info!(
            "Job '{}' completed: processed {} records in {:.2}s ({:.2} records/sec), {} errors",
            job_name, stats.records_processed, elapsed, rps, stats.errors
        );
    }
}
