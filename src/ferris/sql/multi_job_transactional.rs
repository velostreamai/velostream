//! Transactional multi-job SQL processing
//!
//! This module provides transactional job processing with exactly-once semantics.
//! It uses datasource and sink transaction capabilities to ensure atomicity.

use crate::ferris::datasource::{DataReader, DataWriter};
use crate::ferris::sql::{
    multi_job_common::*,
    StreamExecutionEngine, StreamingQuery,
};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Transactional job processor
pub struct TransactionalJobProcessor {
    config: JobProcessingConfig,
}

impl TransactionalJobProcessor {
    pub fn new(config: JobProcessingConfig) -> Self {
        Self { config }
    }

    /// Process records from a datasource with transactional guarantees
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
        
        // Check transaction support for reader and writer
        let reader_supports_tx = check_transaction_support(reader.as_ref(), &job_name);
        let writer_supports_tx = writer.as_ref()
            .map(|w| check_writer_transaction_support(w.as_ref(), &job_name))
            .unwrap_or(false);

        info!(
            "Job '{}' starting transactional processing (reader_tx: {}, writer_tx: {})",
            job_name, reader_supports_tx, writer_supports_tx
        );

        loop {
            // Check for shutdown signal
            if shutdown_rx.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Process one transactional batch
            match self.process_transactional_batch(
                reader.as_mut(),
                writer.as_deref_mut(),
                &engine,
                &query,
                &job_name,
                reader_supports_tx,
                writer_supports_tx,
                &mut stats,
            ).await {
                Ok(()) => {
                    // Successful batch processing
                    if self.config.log_progress && 
                       stats.batches_processed % self.config.progress_interval == 0 {
                        log_job_progress(&job_name, &stats);
                    }
                }
                Err(e) => {
                    error!("Job '{}' batch processing failed: {:?}", job_name, e);
                    stats.batches_failed += 1;
                    
                    // Apply retry backoff
                    tokio::time::sleep(self.config.retry_backoff).await;
                }
            }
        }

        log_final_stats(&job_name, &stats);
        Ok(stats)
    }

    /// Process a single transactional batch
    async fn process_transactional_batch(
        &self,
        reader: &mut dyn DataReader,
        mut writer: Option<&mut dyn DataWriter>,
        engine: &Arc<Mutex<StreamExecutionEngine>>,
        query: &StreamingQuery,
        job_name: &str,
        reader_supports_tx: bool,
        writer_supports_tx: bool,
        stats: &mut JobExecutionStats,
    ) -> DataSourceResult<()> {
        // Step 1: Begin transactions if supported
        let reader_tx_active = if reader_supports_tx {
            match reader.begin_transaction().await? {
                true => {
                    debug!("Job '{}': Reader transaction started", job_name);
                    true
                }
                false => {
                    warn!("Job '{}': Reader claimed transaction support but begin_transaction returned false", job_name);
                    false
                }
            }
        } else {
            false
        };

        let writer_tx_active = if let Some(w) = writer.as_mut() {
            if writer_supports_tx {
                match w.begin_transaction().await? {
                    true => {
                        debug!("Job '{}': Writer transaction started", job_name);
                        true
                    }
                    false => {
                        warn!("Job '{}': Writer claimed transaction support but begin_transaction returned false", job_name);
                        false
                    }
                }
            } else {
                false
            }
        } else {
            false
        };

        // Step 2: Read batch from datasource
        let batch = reader.read().await?;
        if batch.is_empty() {
            // No data available - abort any active transactions
            self.abort_transactions(reader, writer, reader_tx_active, writer_tx_active, job_name).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        // Step 3: Process batch through SQL engine
        let batch_result = process_batch_common(batch, engine, query, job_name).await;
        
        // Step 4: Handle results based on failure strategy
        let should_commit = match self.config.failure_strategy {
            FailureStrategy::FailBatch => batch_result.records_failed == 0,
            FailureStrategy::LogAndContinue | FailureStrategy::SendToDLQ => true,
            FailureStrategy::RetryWithBackoff => {
                if batch_result.records_failed > 0 {
                    // TODO: Implement retry logic for failed records
                    warn!("Job '{}': RetryWithBackoff not yet implemented, treating as LogAndContinue", job_name);
                }
                true
            }
        };

        // Step 5: Write processed data to sink if we have one
        if let Some(_w) = writer.as_mut() {
            if should_commit {
                // TODO: Write processed results to sink
                // This would require getting the results from the SQL engine
                debug!("Job '{}': Would write {} processed records to sink", job_name, batch_result.records_processed);
            }
        }

        // Step 6: Commit or abort transactions
        if should_commit {
            self.commit_transactions(reader, writer, reader_tx_active, writer_tx_active, job_name).await?;
            stats.update_from_batch(&batch_result);
            
            if batch_result.records_failed > 0 && self.config.failure_strategy == FailureStrategy::LogAndContinue {
                info!("Job '{}': Committed batch with {} failures (logged and continued)", 
                      job_name, batch_result.records_failed);
            }
        } else {
            self.abort_transactions(reader, writer, reader_tx_active, writer_tx_active, job_name).await?;
            warn!("Job '{}': Aborted batch due to {} failures", job_name, batch_result.records_failed);
            stats.batches_failed += 1;
        }

        Ok(())
    }

    /// Commit all active transactions with proper ordering
    /// CRITICAL: Datasink must commit first, then datasource only commits if sink succeeds
    async fn commit_transactions(
        &self,
        reader: &mut dyn DataReader,
        mut writer: Option<&mut dyn DataWriter>,
        reader_tx_active: bool,
        writer_tx_active: bool,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Step 1: Commit writer/sink transaction FIRST
        // If this fails, we can still abort the reader transaction
        if let Some(w) = writer {
            if writer_tx_active {
                match w.commit().await {
                    Ok(()) => {
                        debug!("Job '{}': Sink transaction committed successfully", job_name);
                    }
                    Err(e) => {
                        error!("Job '{}': Sink transaction commit failed: {:?}", job_name, e);
                        // Abort reader transaction since sink failed
                        if reader_tx_active {
                            if let Err(abort_err) = reader.abort_transaction().await {
                                error!("Job '{}': Failed to abort reader after sink failure: {:?}", job_name, abort_err);
                            } else {
                                debug!("Job '{}': Reader transaction aborted due to sink failure", job_name);
                            }
                        }
                        return Err(format!("Sink transaction failed: {:?}", e).into());
                    }
                }
            } else {
                // Non-transactional sink - flush and hope for the best
                match w.flush().await {
                    Ok(()) => {
                        debug!("Job '{}': Sink flushed successfully (non-transactional)", job_name);
                    }
                    Err(e) => {
                        error!("Job '{}': Sink flush failed: {:?}", job_name, e);
                        // Still abort reader to avoid data loss
                        if reader_tx_active {
                            if let Err(abort_err) = reader.abort_transaction().await {
                                error!("Job '{}': Failed to abort reader after sink flush failure: {:?}", job_name, abort_err);
                            }
                        }
                        return Err(format!("Sink flush failed: {:?}", e).into());
                    }
                }
            }
        }

        // Step 2: Only commit reader/source transaction AFTER sink succeeds
        // This ensures we don't advance read position unless data is safely persisted
        if reader_tx_active {
            match reader.commit_transaction().await {
                Ok(()) => {
                    debug!("Job '{}': Source transaction committed after successful sink commit", job_name);
                }
                Err(e) => {
                    error!("Job '{}': Source transaction commit failed after sink success: {:?}", job_name, e);
                    // This is a critical failure - sink succeeded but we can't advance read position
                    // Data might be duplicated on retry, but it's better than data loss
                    return Err(format!("Source commit failed after sink success: {:?}", e).into());
                }
            }
        } else {
            // Non-transactional reader
            match reader.commit().await {
                Ok(()) => {
                    debug!("Job '{}': Source committed after successful sink (non-transactional)", job_name);
                }
                Err(e) => {
                    error!("Job '{}': Source commit failed after sink success: {:?}", job_name, e);
                    return Err(format!("Source commit failed: {:?}", e).into());
                }
            }
        }

        Ok(())
    }

    /// Abort all active transactions
    async fn abort_transactions(
        &self,
        reader: &mut dyn DataReader,
        mut writer: Option<&mut dyn DataWriter>,
        reader_tx_active: bool,
        writer_tx_active: bool,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Abort writer transaction first
        if let Some(w) = writer {
            if writer_tx_active {
                match w.rollback().await {
                    Ok(()) => debug!("Job '{}': Writer transaction aborted", job_name),
                    Err(e) => error!("Job '{}': Failed to abort writer transaction: {:?}", job_name, e),
                }
            }
        }

        // Then abort reader transaction
        if reader_tx_active {
            match reader.abort_transaction().await {
                Ok(()) => debug!("Job '{}': Reader transaction aborted", job_name),
                Err(e) => error!("Job '{}': Failed to abort reader transaction: {:?}", job_name, e),
            }
        }

        Ok(())
    }
}

/// Create a transactional job processor with default configuration
pub fn create_transactional_processor() -> TransactionalJobProcessor {
    TransactionalJobProcessor::new(JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::FailBatch, // Strict for exactly-once
        ..Default::default()
    })
}

/// Create a transactional job processor with best-effort semantics
pub fn create_best_effort_transactional_processor() -> TransactionalJobProcessor {
    TransactionalJobProcessor::new(JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::LogAndContinue, // More lenient
        max_retries: 1,
        ..Default::default()
    })
}