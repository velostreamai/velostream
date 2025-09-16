//! Simple (non-transactional) streaming job processing
//!
//! This module provides best-effort job processing without transactional semantics.
//! It's optimized for throughput and simplicity, using basic commit/flush operations.

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::server::processors::common::*;
use crate::velostream::sql::{StreamExecutionEngine, StreamingQuery};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Simple (non-transactional) job processor
pub struct SimpleJobProcessor {
    config: JobProcessingConfig,
}

impl SimpleJobProcessor {
    pub fn new(config: JobProcessingConfig) -> Self {
        Self { config }
    }

    /// Get reference to the job processing configuration
    pub fn get_config(&self) -> &JobProcessingConfig {
        &self.config
    }

    /// Process records from multiple datasources with multiple sinks (multi-source/sink processing)
    pub async fn process_multi_job(
        &self,
        readers: HashMap<String, Box<dyn DataReader>>,
        writers: HashMap<String, Box<dyn DataWriter>>,
        engine: Arc<Mutex<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        let mut stats = JobExecutionStats::new();

        info!(
            "Job '{}' starting multi-source simple processing with {} sources and {} sinks",
            job_name,
            readers.len(),
            writers.len()
        );

        // Log comprehensive configuration details
        log_job_configuration(&job_name, &self.config);

        // Create enhanced context with multiple sources and sinks
        let mut context =
            crate::velostream::sql::execution::processors::ProcessorContext::new_with_sources(
                &job_name, readers, writers,
            );

        // Copy engine state to context
        {
            let _engine_lock = engine.lock().await;
            // Context is already prepared by engine.prepare_context() above
        }

        loop {
            // Check for shutdown signal
            if shutdown_rx.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Process from all sources
            match self
                .process_multi_source_batch(&mut context, &engine, &query, &job_name, &mut stats)
                .await
            {
                Ok(()) => {
                    if self.config.log_progress
                        && stats.batches_processed % self.config.progress_interval == 0
                    {
                        log_job_progress(&job_name, &stats);
                    }
                }
                Err(e) => {
                    warn!(
                        "Job '{}' multi-source batch processing failed: {:?}",
                        job_name, e
                    );
                    stats.batches_failed += 1;

                    // Apply retry backoff
                    tokio::time::sleep(self.config.retry_backoff).await;
                }
            }
        }

        // Commit all sources and flush all sinks
        info!(
            "Job '{}' shutting down, committing sources and flushing sinks",
            job_name
        );

        for source_name in context.list_sources() {
            if let Err(e) = context.commit_source(&source_name).await {
                warn!(
                    "Job '{}': Failed to commit source '{}': {:?}",
                    job_name, source_name, e
                );
            } else {
                info!(
                    "Job '{}': Successfully committed source '{}'",
                    job_name, source_name
                );
            }
        }

        if let Err(e) = context.flush_all().await {
            warn!("Job '{}': Failed to flush all sinks: {:?}", job_name, e);
        } else {
            info!("Job '{}': Successfully flushed all sinks", job_name);
        }

        log_final_stats(&job_name, &stats);
        Ok(stats)
    }

    /// Process records from a datasource with best-effort semantics
    pub async fn process_job(
        &self,
        mut reader: Box<dyn DataReader>,
        mut writer: Option<Box<dyn DataWriter>>,
        engine: Arc<Mutex<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        let mut stats = JobExecutionStats::new();

        // Create a StdoutWriter if no sink is provided
        ensure_sink_or_create_stdout(&mut writer, &job_name);

        info!(
            "Job '{}' starting simple (non-transactional) processing",
            job_name
        );

        // Log comprehensive configuration details
        log_job_configuration(&job_name, &self.config);

        // Log detailed information about the source and sink types
        log_datasource_info(&job_name, reader.as_ref(), writer.as_deref());

        if reader.supports_transactions()
            || writer
                .as_ref()
                .map(|w| w.supports_transactions())
                .unwrap_or(false)
        {
            info!(
                "Job '{}': Note - datasources support transactions but running in simple mode",
                job_name
            );
        }

        loop {
            // Check for shutdown signal
            if shutdown_rx.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Check if there are more records to process
            if !reader.has_more().await? {
                info!("Job '{}' no more records to process", job_name);
                break;
            }

            // Process one simple batch
            match self
                .process_simple_batch(
                    reader.as_mut(),
                    writer.as_deref_mut(),
                    &engine,
                    &query,
                    &job_name,
                    &mut stats,
                )
                .await
            {
                Ok(()) => {
                    // Successful batch processing
                    if self.config.log_progress
                        && stats.batches_processed % self.config.progress_interval == 0
                    {
                        log_job_progress(&job_name, &stats);
                    }
                }
                Err(e) => {
                    warn!("Job '{}' batch processing failed: {:?}", job_name, e);
                    stats.batches_failed += 1;

                    // Apply retry backoff
                    warn!(
                        "Job '{}': Applying retry backoff of {:?} due to batch failure",
                        job_name, self.config.retry_backoff
                    );
                    tokio::time::sleep(self.config.retry_backoff).await;
                    debug!(
                        "Job '{}': Backoff complete, retrying batch processing",
                        job_name
                    );
                }
            }
        }

        log_final_stats(&job_name, &stats);
        Ok(stats)
    }

    /// Process a single batch with simple (non-transactional) semantics
    async fn process_simple_batch(
        &self,
        reader: &mut dyn DataReader,
        mut writer: Option<&mut dyn DataWriter>,
        engine: &Arc<Mutex<StreamExecutionEngine>>,
        query: &StreamingQuery,
        job_name: &str,
        stats: &mut JobExecutionStats,
    ) -> DataSourceResult<()> {
        debug!("Job '{}': Starting batch processing cycle", job_name);

        // Step 1: Read batch from datasource
        let batch = reader.read().await?;
        if batch.is_empty() {
            debug!("Job '{}': No data available, waiting 100ms", job_name);
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        debug!(
            "Job '{}': Read {} records from datasource",
            job_name,
            batch.len()
        );

        // Step 2: Process batch through SQL engine and capture output
        let batch_result = process_batch_with_output(batch, engine, query, job_name).await;
        debug!(
            "Job '{}': SQL processing complete - {} records processed, {} failed",
            job_name, batch_result.records_processed, batch_result.records_failed
        );

        // Step 3: Handle results based on failure strategy
        let should_commit = should_commit_batch(
            self.config.failure_strategy,
            batch_result.records_failed,
            job_name,
        );

        // Step 4: Write processed data to sink if we have one
        if let Some(w) = writer.as_mut() {
            if should_commit && !batch_result.output_records.is_empty() {
                debug!(
                    "Job '{}': Writing {} output records to sink",
                    job_name,
                    batch_result.output_records.len()
                );

                // Attempt to write to sink with retry logic
                match w.write_batch(batch_result.output_records.clone()).await {
                    Ok(()) => {
                        debug!(
                            "Job '{}': Successfully wrote {} records to sink",
                            job_name,
                            batch_result.output_records.len()
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Job '{}': Failed to write {} records to sink: {:?}",
                            job_name,
                            batch_result.output_records.len(),
                            e
                        );

                        // Apply backoff and return error to trigger retry at batch level
                        if matches!(
                            self.config.failure_strategy,
                            FailureStrategy::RetryWithBackoff
                        ) {
                            warn!(
                                "Job '{}': Applying retry backoff of {:?} before retrying batch",
                                job_name, self.config.retry_backoff
                            );
                            tokio::time::sleep(self.config.retry_backoff).await;
                            return Err(format!("Sink write failed, will retry: {}", e).into());
                        } else {
                            warn!("Job '{}': Sink write failed but continuing (failure strategy: {:?})", 
                                  job_name, self.config.failure_strategy);
                        }
                    }
                }
            }
        }

        // Step 5: Commit with simple semantics (no rollback if sink fails)
        if should_commit {
            self.commit_simple(reader, writer, job_name).await?;

            // Update stats from batch result
            update_stats_from_batch_result(stats, &batch_result);

            if batch_result.records_failed > 0 {
                debug!(
                    "Job '{}': Committed batch with {} failures",
                    job_name, batch_result.records_failed
                );
            }
        } else {
            // Batch failed or RetryWithBackoff strategy triggered
            match self.config.failure_strategy {
                FailureStrategy::RetryWithBackoff => {
                    warn!(
                        "Job '{}': Batch failed with {} record failures - applying retry backoff and will retry",
                        job_name, batch_result.records_failed
                    );
                    stats.batches_failed += 1;
                    // Return error to trigger retry at the calling level
                    return Err(format!(
                        "Batch processing failed with {} record failures - will retry with backoff",
                        batch_result.records_failed
                    )
                    .into());
                }
                _ => {
                    // FailBatch strategy - don't commit, just log
                    warn!(
                        "Job '{}': Skipping commit due to {} batch failures",
                        job_name, batch_result.records_failed
                    );
                    stats.batches_failed += 1;
                }
            }
        }

        Ok(())
    }

    /// Simple commit operation - flush sink first, then commit source
    /// Note: Unlike transactional mode, we don't rollback source if sink fails
    async fn commit_simple(
        &self,
        reader: &mut dyn DataReader,
        writer: Option<&mut dyn DataWriter>,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Step 1: Flush writer/sink (best effort)
        if let Some(w) = writer {
            match w.flush().await {
                Ok(()) => {
                    debug!("Job '{}': Sink flushed successfully", job_name);
                }
                Err(e) => {
                    // In simple mode, we log sink failures but still commit source
                    // This prioritizes not losing read position over guaranteed delivery
                    error!(
                        "Job '{}': Sink flush failed (continuing anyway): {:?}",
                        job_name, e
                    );
                }
            }
        }

        // Step 2: Commit reader/source (always attempt)
        match reader.commit().await {
            Ok(()) => {
                debug!("Job '{}': Source committed", job_name);
            }
            Err(e) => {
                error!("Job '{}': Source commit failed: {:?}", job_name, e);
                return Err(format!("Source commit failed: {:?}", e).into());
            }
        }

        Ok(())
    }

    /// Process a batch from multiple sources using StreamExecutionEngine's multi-source support
    async fn process_multi_source_batch(
        &self,
        context: &mut crate::velostream::sql::execution::processors::ProcessorContext,
        engine: &Arc<Mutex<StreamExecutionEngine>>,
        query: &StreamingQuery,
        job_name: &str,
        stats: &mut JobExecutionStats,
    ) -> DataSourceResult<()> {
        debug!(
            "Job '{}': Starting multi-source batch processing cycle",
            job_name
        );

        let source_names = context.list_sources();
        if source_names.is_empty() {
            warn!("Job '{}': No sources available for processing", job_name);
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        let mut total_records_processed = 0;
        let mut total_records_failed = 0;
        let _all_output_records: Vec<crate::velostream::sql::execution::StreamRecord> = Vec::new();

        // Process records from each source
        for source_name in &source_names {
            context.set_active_reader(source_name)?;

            // Read batch from current source
            let batch = context.read().await?;
            if batch.is_empty() {
                debug!(
                    "Job '{}': No data from source '{}', skipping",
                    job_name, source_name
                );
                continue;
            }

            debug!(
                "Job '{}': Read {} records from source '{}'",
                job_name,
                batch.len(),
                source_name
            );

            // Process batch through SQL engine using execute_with_sources
            {
                let mut engine_lock = engine.lock().await;

                for record in batch {
                    match engine_lock.execute_with_record(query, record).await {
                        Ok(()) => {
                            total_records_processed += 1;
                        }
                        Err(e) => {
                            total_records_failed += 1;
                            match self.config.failure_strategy {
                                FailureStrategy::LogAndContinue => {
                                    warn!(
                                        "Job '{}': Record processing failed (continuing): {:?}",
                                        job_name, e
                                    );
                                }
                                FailureStrategy::FailBatch => {
                                    error!(
                                        "Job '{}': Record processing failed (failing batch): {:?}",
                                        job_name, e
                                    );
                                    return Err(format!(
                                        "Batch failed due to record processing error: {:?}",
                                        e
                                    )
                                    .into());
                                }
                                FailureStrategy::RetryWithBackoff => {
                                    error!(
                                        "Job '{}': Record processing failed (will retry): {:?}",
                                        job_name, e
                                    );
                                    return Err(format!(
                                        "Record processing failed, will retry: {:?}",
                                        e
                                    )
                                    .into());
                                }
                                FailureStrategy::SendToDLQ => {
                                    warn!("Job '{}': Record processing failed, would send to DLQ (not implemented): {:?}", job_name, e);
                                    // TODO: Implement DLQ functionality
                                }
                            }
                        }
                    }
                }

                // Sync state back to context
                // Context state already updated by engine.execute_batch() above
            }
        }

        // Determine if batch should be committed
        let should_commit =
            should_commit_batch(self.config.failure_strategy, total_records_failed, job_name);

        if should_commit {
            // Commit all sources
            for source_name in &source_names {
                if let Err(e) = context.commit_source(source_name).await {
                    warn!(
                        "Job '{}': Failed to commit source '{}': {:?}",
                        job_name, source_name, e
                    );
                    if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                        return Err(
                            format!("Failed to commit source '{}': {:?}", source_name, e).into(),
                        );
                    }
                }
            }

            // Flush all sinks
            if let Err(e) = context.flush_all().await {
                warn!("Job '{}': Failed to flush sinks: {:?}", job_name, e);
                if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                    return Err(format!("Failed to flush sinks: {:?}", e).into());
                }
            }

            // Update stats
            stats.batches_processed += 1;
            stats.records_processed += total_records_processed as u64;
            stats.records_failed += total_records_failed as u64;

            debug!(
                "Job '{}': Successfully processed multi-source batch - {} records processed, {} failed",
                job_name, total_records_processed, total_records_failed
            );
        } else {
            stats.batches_failed += 1;
            warn!(
                "Job '{}': Skipping commit due to {} failures",
                job_name, total_records_failed
            );
        }

        Ok(())
    }
}

/// Create a simple job processor optimized for throughput
pub fn create_simple_processor() -> SimpleJobProcessor {
    SimpleJobProcessor::new(JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue, // Prioritize throughput
        max_batch_size: 1000,                              // Larger batches for throughput
        batch_timeout: Duration::from_millis(100),         // Shorter timeout for lower latency
        max_retries: 1,                                    // Minimal retries for speed
        retry_backoff: Duration::from_millis(100),
        progress_interval: 100, // Less frequent logging
        ..Default::default()
    })
}

/// Create a simple job processor that's more conservative about failures
pub fn create_conservative_simple_processor() -> SimpleJobProcessor {
    SimpleJobProcessor::new(JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::FailBatch, // More conservative
        max_batch_size: 100,                          // Smaller batches to isolate failures
        batch_timeout: Duration::from_millis(1000),
        max_retries: 3,
        retry_backoff: Duration::from_millis(1000),
        progress_interval: 10, // More frequent progress logging
        ..Default::default()
    })
}

/// Create a simple processor optimized for low latency
pub fn create_low_latency_processor() -> SimpleJobProcessor {
    SimpleJobProcessor::new(JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 10,                       // Very small batches
        batch_timeout: Duration::from_millis(10), // Very short timeout
        max_retries: 0,                           // No retries for minimum latency
        retry_backoff: Duration::from_millis(1),
        progress_interval: 1000, // Infrequent logging to avoid overhead
        log_progress: false,     // Disable progress logging for maximum speed
        ..Default::default()
    })
}
