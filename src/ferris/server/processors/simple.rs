//! Simple (non-transactional) streaming job processing
//!
//! This module provides best-effort job processing without transactional semantics.
//! It's optimized for throughput and simplicity, using basic commit/flush operations.

use crate::ferris::datasource::{DataReader, DataWriter, StdoutWriter};
use crate::ferris::server::processors::common::*;
use crate::ferris::sql::{StreamExecutionEngine, StreamingQuery};
use log::{debug, error, info, warn};
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
        if writer.is_none() {
            info!(
                "Job '{}': No sink provided, creating StdoutWriter for output",
                job_name
            );
            writer = Some(Box::new(StdoutWriter::new_pretty()));
        }

        info!(
            "Job '{}' starting simple (non-transactional) processing",
            job_name
        );

        // Log comprehensive configuration details
        info!(
            "Job '{}' configuration: use_transactions={}, failure_strategy={:?}, max_batch_size={}, batch_timeout={}ms, max_retries={}, retry_backoff={}ms, progress_interval={}, log_progress={}",
            job_name,
            self.config.use_transactions,
            self.config.failure_strategy,
            self.config.max_batch_size,
            self.config.batch_timeout.as_millis(),
            self.config.max_retries,
            self.config.retry_backoff.as_millis(),
            self.config.progress_interval,
            self.config.log_progress
        );

        // Check if datasources have transaction capabilities and log detailed info
        let reader_has_tx = reader.supports_transactions();
        let writer_has_tx = writer
            .as_ref()
            .map(|w| w.supports_transactions())
            .unwrap_or(false);

        // Log detailed information about the source and sink types
        let reader_type = std::any::type_name_of_val(reader.as_ref());
        let writer_type = writer
            .as_ref()
            .map(|w| std::any::type_name_of_val(w.as_ref()))
            .unwrap_or("None");

        info!(
            "Job '{}': Source type: '{}' (supports_transactions: {})",
            job_name, reader_type, reader_has_tx
        );

        info!(
            "Job '{}': Sink type: '{}' (supports_transactions: {})",
            job_name, writer_type, writer_has_tx
        );

        if reader_has_tx || writer_has_tx {
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
        let should_commit = match self.config.failure_strategy {
            FailureStrategy::FailBatch => batch_result.records_failed == 0,
            FailureStrategy::LogAndContinue => {
                // Always commit, just log failures
                if batch_result.records_failed > 0 {
                    warn!(
                        "Job '{}': {} records failed in batch, logging and continuing",
                        job_name, batch_result.records_failed
                    );
                }
                true
            }
            FailureStrategy::SendToDLQ => {
                // TODO: Implement DLQ functionality
                if batch_result.records_failed > 0 {
                    warn!(
                        "Job '{}': {} records failed - DLQ not yet implemented, logging instead",
                        job_name, batch_result.records_failed
                    );
                }
                true
            }
            FailureStrategy::RetryWithBackoff => {
                if batch_result.records_failed > 0 {
                    warn!(
                        "Job '{}': {} records failed in batch - will retry with backoff",
                        job_name, batch_result.records_failed
                    );
                    // Don't commit on failures - this will trigger retry at batch level
                    false
                } else {
                    true
                }
            }
        };

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
            self.commit_simple(reader, writer.as_deref_mut(), job_name)
                .await?;

            // Convert to regular BatchProcessingResult for stats update
            let stats_result = BatchProcessingResult {
                records_processed: batch_result.records_processed,
                records_failed: batch_result.records_failed,
                processing_time: batch_result.processing_time,
                batch_size: batch_result.batch_size,
                error_details: batch_result.error_details,
            };
            stats.update_from_batch(&stats_result);

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
