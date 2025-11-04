//! Transactional multi-job SQL processing
//!
//! This module provides transactional job processing with at-least-once delivery semantics.
//! It uses datasource and sink transaction capabilities to ensure ACID atomicity within
//! each batch, but may deliver duplicates on retry scenarios (at-least-once guarantee).

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::common::*;
use crate::velostream::server::processors::error_tracking_helper::ErrorTracker;
use crate::velostream::server::processors::metrics_helper::ProcessorMetricsHelper;
use crate::velostream::server::processors::observability_helper::ObservabilityHelper;
use crate::velostream::sql::{StreamExecutionEngine, StreamingQuery};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

/// Transactional job processor with at-least-once delivery semantics
///
/// Provides ACID transaction boundaries for batch processing but does not guarantee
/// exactly-once semantics. Records may be reprocessed on retry, making this suitable
/// for idempotent operations or scenarios where occasional duplicates are acceptable.
pub struct TransactionalJobProcessor {
    config: JobProcessingConfig,
    observability: Option<SharedObservabilityManager>,
    metrics_helper: ProcessorMetricsHelper,
}

impl TransactionalJobProcessor {
    pub fn new(config: JobProcessingConfig) -> Self {
        Self {
            config,
            observability: None,
            metrics_helper: ProcessorMetricsHelper::new(),
        }
    }

    /// Create processor with observability support
    pub fn with_observability(
        config: JobProcessingConfig,
        observability: Option<SharedObservabilityManager>,
    ) -> Self {
        Self {
            config,
            observability,
            metrics_helper: ProcessorMetricsHelper::new(),
        }
    }

    /// Get reference to the job processing configuration
    pub fn get_config(&self) -> &JobProcessingConfig {
        &self.config
    }

    /// Process records from multiple datasources with multiple sinks (transactional multi-source processing)
    ///
    /// Provides at-least-once delivery with ACID transaction boundaries. If any part of the
    /// batch fails, all transactions are rolled back. On retry, some records may be reprocessed.
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
            "Job '{}' starting multi-source transactional processing with {} sources and {} sinks",
            job_name,
            readers.len(),
            writers.len()
        );

        // Check transaction support for all readers and writers
        let readers_support_tx: HashMap<String, bool> = readers
            .iter()
            .map(|(name, reader)| (name.clone(), reader.supports_transactions()))
            .collect();
        let writers_support_tx: HashMap<String, bool> = writers
            .iter()
            .map(|(name, writer)| (name.clone(), writer.supports_transactions()))
            .collect();

        info!(
            "Job '{}': Transaction support - Sources: {:?}, Sinks: {:?}",
            job_name, readers_support_tx, writers_support_tx
        );

        // Log comprehensive configuration details
        log_job_configuration(&job_name, &self.config);

        // Register SQL-annotated metrics
        if let Err(e) = self
            .metrics_helper
            .register_counter_metrics(&query, &self.observability, &job_name)
            .await
        {
            warn!(
                "Job '{}': Failed to register counter metrics: {:?}",
                job_name, e
            );
        }
        if let Err(e) = self
            .metrics_helper
            .register_gauge_metrics(&query, &self.observability, &job_name)
            .await
        {
            warn!(
                "Job '{}': Failed to register gauge metrics: {:?}",
                job_name, e
            );
        }
        if let Err(e) = self
            .metrics_helper
            .register_histogram_metrics(&query, &self.observability, &job_name)
            .await
        {
            warn!(
                "Job '{}': Failed to register histogram metrics: {:?}",
                job_name, e
            );
        }

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

            // Check if all sources have finished processing
            let sources_finished = {
                let source_names = context.list_sources();
                let mut all_finished = true;
                for source_name in source_names {
                    match context.has_more_data(&source_name).await {
                        Ok(has_more) => {
                            if has_more {
                                all_finished = false;
                                break;
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Job '{}': Failed to check has_more for source '{}': {:?}",
                                job_name, source_name, e
                            );
                            // On error, assume source has more data to avoid premature exit
                            all_finished = false;
                            break;
                        }
                    }
                }
                all_finished
            };

            if sources_finished {
                info!(
                    "Job '{}': All sources have finished - no more data to process",
                    job_name
                );
                break;
            }

            // Process transactional batch from all sources
            match self
                .process_multi_source_transactional_batch(
                    &mut context,
                    &engine,
                    &query,
                    &job_name,
                    &readers_support_tx,
                    &writers_support_tx,
                    &mut stats,
                )
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
                    let error_msg = format!(
                        "Transactional multi-source batch processing failed: {:?}",
                        e
                    );
                    warn!("Job '{}' {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    stats.batches_failed += 1;

                    // Apply retry backoff
                    tokio::time::sleep(self.config.retry_backoff).await;
                }
            }
        }

        // Final commit all sources and flush all sinks
        info!(
            "Job '{}' shutting down, committing sources and flushing sinks",
            job_name
        );

        for source_name in context.list_sources() {
            if let Err(e) = context.commit_source(&source_name).await {
                let error_msg = format!("Failed to commit source '{}': {:?}", source_name, e);
                warn!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(&self.observability, &job_name, error_msg);
            } else {
                info!(
                    "Job '{}': Successfully committed source '{}'",
                    job_name, source_name
                );
            }
        }

        if let Err(e) = context.flush_all().await {
            let error_msg = format!("Failed to flush all sinks: {:?}", e);
            warn!("Job '{}': {}", job_name, error_msg);
            ErrorTracker::record_error(&self.observability, &job_name, error_msg);
        } else {
            info!("Job '{}': Successfully flushed all sinks", job_name);
        }

        log_final_stats(&job_name, &stats);
        Ok(stats)
    }

    /// Process records from a datasource with transactional guarantees
    ///
    /// Provides at-least-once delivery semantics with ACID transaction boundaries.
    /// Each batch is processed atomically - either all records succeed or the entire
    /// batch is rolled back. On failure with RetryWithBackoff, batches may be reprocessed.
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

        // Check transaction support for reader and writer
        let reader_supports_tx = check_transaction_support(reader.as_ref(), &job_name);
        let writer_supports_tx = writer
            .as_ref()
            .map(|w| check_writer_transaction_support(w.as_ref(), &job_name))
            .unwrap_or(false);

        // Log comprehensive configuration details
        log_job_configuration(&job_name, &self.config);

        // Log detailed information about the source and sink types
        log_datasource_info(&job_name, reader.as_ref(), writer.as_deref());

        info!(
            "Job '{}' starting transactional processing (reader_tx: {}, writer_tx: {})",
            job_name, reader_supports_tx, writer_supports_tx
        );

        // Register SQL-annotated metrics
        if let Err(e) = self
            .metrics_helper
            .register_counter_metrics(&query, &self.observability, &job_name)
            .await
        {
            warn!(
                "Job '{}': Failed to register counter metrics: {:?}",
                job_name, e
            );
        }
        if let Err(e) = self
            .metrics_helper
            .register_gauge_metrics(&query, &self.observability, &job_name)
            .await
        {
            warn!(
                "Job '{}': Failed to register gauge metrics: {:?}",
                job_name, e
            );
        }
        if let Err(e) = self
            .metrics_helper
            .register_histogram_metrics(&query, &self.observability, &job_name)
            .await
        {
            warn!(
                "Job '{}': Failed to register histogram metrics: {:?}",
                job_name, e
            );
        }

        loop {
            // Check for shutdown signal
            if shutdown_rx.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Check if there's more data to process
            match reader.has_more().await {
                Ok(false) => {
                    info!("Job '{}' completed - no more data available", job_name);
                    break;
                }
                Ok(true) => {
                    // Continue processing
                }
                Err(e) => {
                    let error_msg = format!("Error checking for more data: {:?}", e);
                    warn!("Job '{}' {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    stats.batches_failed += 1;
                    tokio::time::sleep(self.config.retry_backoff).await;
                    continue;
                }
            }

            // Process one transactional batch
            match self
                .process_transactional_batch(
                    reader.as_mut(),
                    writer.as_deref_mut(),
                    &engine,
                    &query,
                    &job_name,
                    reader_supports_tx,
                    writer_supports_tx,
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
                    let error_msg = format!("Batch processing failed: {:?}", e);
                    warn!("Job '{}' {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);
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

    /// Process a single transactional batch with at-least-once semantics
    ///
    /// This method processes one batch with ACID transaction boundaries but does not
    /// prevent duplicate processing on retry. If sink commits but source commit fails,
    /// data may be duplicated on the next retry attempt.
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
                    warn!(
                        "Job '{}': Reader claimed transaction support but begin_transaction returned false",
                        job_name
                    );
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
                        warn!(
                            "Job '{}': Writer claimed transaction support but begin_transaction returned false",
                            job_name
                        );
                        false
                    }
                }
            } else {
                false
            }
        } else {
            false
        };

        // Step 2: Read batch from datasource (with deserialization telemetry)
        let batch_start = Instant::now();
        let deser_start = Instant::now();
        let batch = reader.read().await?;
        let deser_duration = deser_start.elapsed().as_millis() as u64;

        if batch.is_empty() {
            // No data available - abort any active transactions and return
            self.abort_transactions(reader, writer, reader_tx_active, writer_tx_active, job_name)
                .await?;
            return Ok(());
        }

        // Create batch span with trace extraction from first record
        let mut batch_span_guard = ObservabilityHelper::start_batch_span(
            &self.observability,
            job_name,
            stats.batches_processed,
            &batch,
        );

        // Record deserialization telemetry
        ObservabilityHelper::record_deserialization(
            &self.observability,
            job_name,
            &batch_span_guard,
            batch.len(),
            deser_duration,
            None,
        );

        // Step 3: Process batch through SQL engine and capture output (with SQL telemetry)
        let sql_start = Instant::now();
        let mut batch_result = process_batch_with_output(batch, engine, query, job_name).await;
        let sql_duration = sql_start.elapsed().as_millis() as u64;

        // Record SQL processing telemetry
        ObservabilityHelper::record_sql_processing(
            &self.observability,
            job_name,
            &batch_span_guard,
            &batch_result,
            sql_duration,
        );

        // Inject trace context into output records for downstream propagation
        // PERF(FR-082 Phase 2): Use Arc records directly - no clone!
        let mut output_owned: Vec<Arc<StreamRecord>> = batch_result.output_records;
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span_guard,
            &mut output_owned,
            job_name,
        );

        // Emit SQL-annotated metrics for output records
        self.metrics_helper
            .emit_counter_metrics(query, &output_owned, &self.observability, job_name)
            .await;
        self.metrics_helper
            .emit_gauge_metrics(query, &output_owned, &self.observability, job_name)
            .await;
        self.metrics_helper
            .emit_histogram_metrics(query, &output_owned, &self.observability, job_name)
            .await;

        // Step 4: Handle results based on failure strategy
        let should_commit = should_commit_batch(
            self.config.failure_strategy,
            batch_result.records_failed,
            job_name,
        );

        // Step 5: Write processed data to sink if we have one (with serialization telemetry)
        if let Some(w) = writer.as_mut() {
            if should_commit && !batch_result.output_records.is_empty() {
                debug!(
                    "Job '{}': Writing {} output records to sink",
                    job_name,
                    batch_result.output_records.len()
                );
                let ser_start = Instant::now();
                let record_count = batch_result.output_records.len();
                // PERF(FR-082 Phase 2): Pass Arc records directly - no clone!
                match w.write_batch(batch_result.output_records).await {
                    Ok(()) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;

                        // Record serialization telemetry on success
                        ObservabilityHelper::record_serialization_success(
                            &self.observability,
                            job_name,
                            &batch_span_guard,
                            record_count,
                            ser_duration,
                            None,
                        );

                        debug!(
                            "Job '{}': Successfully wrote {} records to sink",
                            job_name, record_count
                        );
                    }
                    Err(e) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;
                        let error_msg =
                            format!("Failed to write {} records to sink: {:?}", record_count, e);

                        // Record serialization telemetry on failure
                        ObservabilityHelper::record_serialization_failure(
                            &self.observability,
                            job_name,
                            &batch_span_guard,
                            record_count,
                            ser_duration,
                            &format!("Write failed: {:?}", e),
                            None,
                        );

                        warn!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);

                        // Complete batch span with error
                        ObservabilityHelper::complete_batch_span_error(
                            &mut batch_span_guard,
                            &batch_start,
                            batch_result.records_processed as u64,
                            batch_result.records_failed,
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
                            // This will cause transaction abort in step 6
                            return Err(format!("Sink write failed: {:?}", e).into());
                        }
                    }
                }
            }
        }

        // Step 6: Commit or abort transactions
        if should_commit {
            self.commit_transactions(reader, writer, reader_tx_active, writer_tx_active, job_name)
                .await?;

            // Update stats from batch result
            update_stats_from_batch_result(stats, &batch_result);

            // Complete batch span with success
            ObservabilityHelper::complete_batch_span_success(
                &mut batch_span_guard,
                &batch_start,
                batch_result.records_processed as u64,
            );

            if batch_result.records_failed > 0
                && self.config.failure_strategy == FailureStrategy::LogAndContinue
            {
                info!(
                    "Job '{}': Committed batch with {} failures (logged and continued)",
                    job_name, batch_result.records_failed
                );
            }
        } else {
            self.abort_transactions(reader, writer, reader_tx_active, writer_tx_active, job_name)
                .await?;

            // Complete batch span with error
            ObservabilityHelper::complete_batch_span_error(
                &mut batch_span_guard,
                &batch_start,
                batch_result.records_processed as u64,
                batch_result.records_failed,
            );

            // Handle different failure scenarios with appropriate messaging
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
                        "Job '{}': Aborted batch due to {} record failures",
                        job_name, batch_result.records_failed
                    );
                    stats.batches_failed += 1;
                }
            }
        }

        Ok(())
    }

    /// Commit all active transactions with proper ordering (at-least-once semantics)
    /// CRITICAL: Datasink must commit first, then datasource only commits if sink succeeds.
    /// If sink commits but source commit fails, this results in at-least-once delivery
    /// as the data will be reprocessed on the next batch attempt.
    async fn commit_transactions(
        &self,
        reader: &mut dyn DataReader,
        writer: Option<&mut dyn DataWriter>,
        reader_tx_active: bool,
        writer_tx_active: bool,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Step 1: Commit writer/sink transaction FIRST
        // If this fails, we can still abort the reader transaction
        if let Some(w) = writer {
            if writer_tx_active {
                match w.commit_transaction().await {
                    Ok(()) => {
                        debug!(
                            "Job '{}': Sink transaction committed successfully",
                            job_name
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Sink transaction commit failed: {:?}", e);
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                        // Abort reader transaction since sink failed
                        if reader_tx_active {
                            if let Err(abort_err) = reader.abort_transaction().await {
                                let abort_msg = format!(
                                    "Failed to abort reader after sink failure: {:?}",
                                    abort_err
                                );
                                error!("Job '{}': {}", job_name, abort_msg);
                                ErrorTracker::record_error(
                                    &self.observability,
                                    &job_name,
                                    abort_msg,
                                );
                            } else {
                                debug!(
                                    "Job '{}': Reader transaction aborted due to sink failure",
                                    job_name
                                );
                            }
                        }
                        return Err(format!("Sink transaction failed: {:?}", e).into());
                    }
                }
            } else {
                // Non-transactional sink - flush and hope for the best
                match w.flush().await {
                    Ok(()) => {
                        debug!(
                            "Job '{}': Sink flushed successfully (non-transactional)",
                            job_name
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("Sink flush failed: {:?}", e);
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                        // Still abort reader to avoid data loss
                        if reader_tx_active {
                            if let Err(abort_err) = reader.abort_transaction().await {
                                let abort_msg = format!(
                                    "Failed to abort reader after sink flush failure: {:?}",
                                    abort_err
                                );
                                error!("Job '{}': {}", job_name, abort_msg);
                                ErrorTracker::record_error(
                                    &self.observability,
                                    &job_name,
                                    abort_msg,
                                );
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
                    debug!(
                        "Job '{}': Source transaction committed after successful sink commit",
                        job_name
                    );
                }
                Err(e) => {
                    let error_msg = format!(
                        "Source transaction commit failed after sink success: {:?}",
                        e
                    );
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    // This is a critical failure - sink succeeded but we can't advance read position
                    // Data might be duplicated on retry, but it's better than data loss
                    return Err(format!("Source commit failed after sink success: {:?}", e).into());
                }
            }
        } else {
            // Non-transactional reader
            match reader.commit().await {
                Ok(()) => {
                    debug!(
                        "Job '{}': Source committed after successful sink (non-transactional)",
                        job_name
                    );
                }
                Err(e) => {
                    let error_msg = format!("Source commit failed after sink success: {:?}", e);
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    return Err(format!("Source commit failed: {:?}", e).into());
                }
            }
        }

        Ok(())
    }

    /// Process a batch from multiple sources with full transactional semantics
    async fn process_multi_source_transactional_batch(
        &self,
        context: &mut crate::velostream::sql::execution::processors::ProcessorContext,
        engine: &Arc<Mutex<StreamExecutionEngine>>,
        query: &StreamingQuery,
        job_name: &str,
        readers_support_tx: &HashMap<String, bool>,
        writers_support_tx: &HashMap<String, bool>,
        stats: &mut JobExecutionStats,
    ) -> DataSourceResult<()> {
        debug!(
            "Job '{}': Starting multi-source transactional batch processing cycle",
            job_name
        );

        let source_names = context.list_sources();
        if source_names.is_empty() {
            warn!("Job '{}': No sources available for processing", job_name);
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        // Begin transactions for all supported sources and sinks
        let mut active_reader_transactions = HashMap::new();
        let mut active_writer_transactions = HashMap::new();

        // Begin reader transactions
        for source_name in &source_names {
            if *readers_support_tx.get(source_name).unwrap_or(&false) {
                context.set_active_reader(source_name)?;
                match context.begin_reader_transaction().await {
                    Ok(true) => {
                        debug!(
                            "Job '{}': Started transaction for source '{}'",
                            job_name, source_name
                        );
                        active_reader_transactions.insert(source_name.clone(), true);
                    }
                    Ok(false) => {
                        warn!(
                            "Job '{}': Source '{}' claimed transaction support but begin_transaction returned false",
                            job_name, source_name
                        );
                        active_reader_transactions.insert(source_name.clone(), false);
                    }
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to start transaction for source '{}': {:?}",
                            source_name, e
                        );
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                        // Abort all transactions started so far
                        self.abort_multi_source_transactions(
                            context,
                            &active_reader_transactions,
                            &active_writer_transactions,
                            job_name,
                        )
                        .await?;
                        return Err(format!(
                            "Failed to start transaction for source '{}': {:?}",
                            source_name, e
                        )
                        .into());
                    }
                }
            } else {
                active_reader_transactions.insert(source_name.clone(), false);
            }
        }

        // Begin writer transactions
        let sink_names = context.list_sinks();
        for sink_name in &sink_names {
            if *writers_support_tx.get(sink_name).unwrap_or(&false) {
                context.set_active_writer(sink_name)?;
                match context.begin_writer_transaction().await {
                    Ok(true) => {
                        debug!(
                            "Job '{}': Started transaction for sink '{}'",
                            job_name, sink_name
                        );
                        active_writer_transactions.insert(sink_name.clone(), true);
                    }
                    Ok(false) => {
                        warn!(
                            "Job '{}': Sink '{}' claimed transaction support but begin_transaction returned false",
                            job_name, sink_name
                        );
                        active_writer_transactions.insert(sink_name.clone(), false);
                    }
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to start transaction for sink '{}': {:?}",
                            sink_name, e
                        );
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                        // Abort all transactions
                        self.abort_multi_source_transactions(
                            context,
                            &active_reader_transactions,
                            &active_writer_transactions,
                            job_name,
                        )
                        .await?;
                        return Err(format!(
                            "Failed to start transaction for sink '{}': {:?}",
                            sink_name, e
                        )
                        .into());
                    }
                }
            } else {
                active_writer_transactions.insert(sink_name.clone(), false);
            }
        }

        // Collect all batches first for trace extraction
        let batch_start = Instant::now();
        let mut source_batches = Vec::new();
        for source_name in &source_names {
            context.set_active_reader(source_name)?;
            let deser_start = Instant::now();
            let batch = context.read().await?;
            let deser_duration = deser_start.elapsed().as_millis() as u64;
            source_batches.push((source_name.clone(), batch, deser_start, deser_duration));
        }

        // Create batch span with trace context from first non-empty batch
        let first_batch = source_batches
            .iter()
            .find(|(_, batch, _, _)| !batch.is_empty())
            .map(|(_, batch, _, _)| batch.as_slice())
            .unwrap_or(&[]);

        let mut batch_span_guard = ObservabilityHelper::start_batch_span(
            &self.observability,
            job_name,
            stats.batches_processed,
            first_batch,
        );

        let mut total_records_processed = 0;
        let mut total_records_failed = 0;
        let mut processing_successful = true;
        // PERF: Collect Arc<StreamRecord> for zero-copy multi-source collection
        let mut all_output_records: Vec<
            std::sync::Arc<crate::velostream::sql::execution::StreamRecord>,
        > = Vec::new();

        // Process all collected batches within the transaction
        for (source_name, batch, _deser_start, deser_duration) in source_batches {
            // Record deserialization telemetry
            ObservabilityHelper::record_deserialization(
                &self.observability,
                job_name,
                &batch_span_guard,
                batch.len(),
                deser_duration,
                None,
            );

            if batch.is_empty() {
                debug!(
                    "Job '{}': No data from source '{}', skipping",
                    job_name, source_name
                );
                continue;
            }

            debug!(
                "Job '{}': Read {} records from source '{}' within transaction",
                job_name,
                batch.len(),
                source_name
            );

            // Process batch through SQL engine and capture output records (with SQL telemetry)
            let sql_start = Instant::now();
            let batch_result = process_batch_with_output(batch, engine, query, job_name).await;
            let sql_duration = sql_start.elapsed().as_millis() as u64;

            // Record SQL processing telemetry
            ObservabilityHelper::record_sql_processing(
                &self.observability,
                job_name,
                &batch_span_guard,
                &batch_result,
                sql_duration,
            );

            // PERF: Unwrap Arc to owned records for metrics
            let output_owned: Vec<_> = batch_result
                .output_records
                .iter()
                .map(|arc| (**arc).clone())
                .collect();

            // Emit SQL-annotated metrics for output records from this source
            self.metrics_helper
                .emit_counter_metrics(query, &output_owned, &self.observability, job_name)
                .await;
            self.metrics_helper
                .emit_gauge_metrics(query, &output_owned, &self.observability, job_name)
                .await;
            self.metrics_helper
                .emit_histogram_metrics(query, &output_owned, &self.observability, job_name)
                .await;

            total_records_processed += batch_result.records_processed;
            total_records_failed += batch_result.records_failed;

            debug!(
                "Job '{}': Source '{}' - processed {} records, {} failed, {} output records",
                job_name,
                source_name,
                batch_result.records_processed,
                batch_result.records_failed,
                batch_result.output_records.len()
            );

            // Collect output records for writing to sinks
            all_output_records.extend(batch_result.output_records);

            // Handle failures according to strategy
            if batch_result.records_failed > 0 {
                match self.config.failure_strategy {
                    FailureStrategy::FailBatch => {
                        error!(
                            "Job '{}': Source '{}' had {} failures (failing batch)",
                            job_name, source_name, batch_result.records_failed
                        );
                        processing_successful = false;
                        break; // Exit source processing loop
                    }
                    FailureStrategy::LogAndContinue => {
                        warn!(
                            "Job '{}': Source '{}' had {} failures (continuing within transaction)",
                            job_name, source_name, batch_result.records_failed
                        );
                    }
                    FailureStrategy::RetryWithBackoff => {
                        error!(
                            "Job '{}': Source '{}' had {} failures (will retry)",
                            job_name, source_name, batch_result.records_failed
                        );
                        processing_successful = false;
                        break; // Exit to retry
                    }
                    FailureStrategy::SendToDLQ => {
                        warn!(
                            "Job '{}': Source '{}' had {} failures, would send to DLQ (not implemented)",
                            job_name, source_name, batch_result.records_failed
                        );
                        // TODO: Implement DLQ functionality
                    }
                }
            }
        }

        // Write output records to all sinks within the transaction
        if processing_successful && !all_output_records.is_empty() && !sink_names.is_empty() {
            // PERF(FR-082 Phase 2): Use Arc records directly - no clone!
            let mut output_owned: Vec<Arc<StreamRecord>> = all_output_records;

            // Inject trace context into all output records for downstream propagation
            ObservabilityHelper::inject_trace_context_into_records(
                &batch_span_guard,
                &mut output_owned,
                job_name,
            );

            debug!(
                "Job '{}': Writing {} output records to {} sink(s) within transaction",
                job_name,
                all_output_records.len(),
                sink_names.len()
            );

            // Optimize for multi-sink scenario: use shared slice instead of cloning for each sink
            if sink_names.len() == 1 {
                // Single sink: use move semantics (no clone) with serialization telemetry
                let ser_start = Instant::now();
                let record_count = output_owned.len();
                match context.write_batch_to(&sink_names[0], output_owned).await {
                    Ok(()) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;

                        // Record serialization telemetry on success
                        ObservabilityHelper::record_serialization_success(
                            &self.observability,
                            job_name,
                            &batch_span_guard,
                            record_count,
                            ser_duration,
                            None,
                        );

                        debug!(
                            "Job '{}': Successfully wrote {} records to sink '{}' within transaction",
                            job_name, record_count, &sink_names[0]
                        );
                    }
                    Err(e) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;
                        let error_msg = format!(
                            "Failed to write {} records to sink '{}' within transaction: {:?}",
                            record_count, &sink_names[0], e
                        );

                        // Record serialization telemetry on failure
                        ObservabilityHelper::record_serialization_failure(
                            &self.observability,
                            job_name,
                            &batch_span_guard,
                            record_count,
                            ser_duration,
                            &format!("Write failed: {:?}", e),
                            None,
                        );

                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                        processing_successful = false;
                    }
                }
            } else {
                // Multiple sinks: use shared slice to avoid N clones with serialization telemetry
                // NOTE: output_owned already created above for trace injection
                for sink_name in &sink_names {
                    let ser_start = Instant::now();
                    let record_count = output_owned.len();
                    match context
                        .write_batch_to_shared(sink_name, &output_owned)
                        .await
                    {
                        Ok(()) => {
                            let ser_duration = ser_start.elapsed().as_millis() as u64;

                            // Record serialization telemetry on success
                            ObservabilityHelper::record_serialization_success(
                                &self.observability,
                                job_name,
                                &batch_span_guard,
                                record_count,
                                ser_duration,
                                None,
                            );

                            debug!(
                                "Job '{}': Successfully wrote {} records to sink '{}' within transaction",
                                job_name, record_count, sink_name
                            );
                        }
                        Err(e) => {
                            let ser_duration = ser_start.elapsed().as_millis() as u64;
                            let error_msg = format!(
                                "Failed to write {} records to sink '{}' within transaction: {:?}",
                                record_count, sink_name, e
                            );

                            // Record serialization telemetry on failure
                            ObservabilityHelper::record_serialization_failure(
                                &self.observability,
                                job_name,
                                &batch_span_guard,
                                record_count,
                                ser_duration,
                                &format!("Write failed: {:?}", e),
                                None,
                            );

                            error!("Job '{}': {}", job_name, error_msg);
                            ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                            processing_successful = false;
                            break; // Exit sink write loop - will abort transaction
                        }
                    }
                }
            }
        } else if processing_successful && !all_output_records.is_empty() {
            debug!(
                "Job '{}': Processed {} output records but no sinks configured",
                job_name,
                all_output_records.len()
            );
        }

        // If no records were processed, this is a no-op - abort transactions and return early
        if total_records_processed == 0 {
            debug!(
                "Job '{}': No records processed (all batches empty) - no-op, aborting transactions and returning early",
                job_name
            );

            // Abort transactions cleanly
            self.abort_multi_source_transactions(
                context,
                &active_reader_transactions,
                &active_writer_transactions,
                job_name,
            )
            .await?;

            return Ok(());
        }

        // Determine transaction outcome
        let should_commit = processing_successful
            && should_commit_batch(self.config.failure_strategy, total_records_failed, job_name);

        if should_commit {
            // Commit all transactions
            match self
                .commit_multi_source_transactions(
                    context,
                    &active_reader_transactions,
                    &active_writer_transactions,
                    job_name,
                )
                .await
            {
                Ok(()) => {
                    stats.batches_processed += 1;
                    stats.records_processed += total_records_processed as u64;
                    stats.records_failed += total_records_failed as u64;

                    // Complete batch span with success
                    ObservabilityHelper::complete_batch_span_success(
                        &mut batch_span_guard,
                        &batch_start,
                        total_records_processed as u64,
                    );

                    info!(
                        "Job '{}': Successfully committed multi-source transactional batch - {} records processed, {} failed",
                        job_name, total_records_processed, total_records_failed
                    );
                }
                Err(e) => {
                    let error_msg = format!("Failed to commit transactions: {:?}", e);
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);

                    // Complete batch span with error
                    ObservabilityHelper::complete_batch_span_error(
                        &mut batch_span_guard,
                        &batch_start,
                        total_records_processed as u64,
                        total_records_failed,
                    );

                    // Attempt abort
                    let _ = self
                        .abort_multi_source_transactions(
                            context,
                            &active_reader_transactions,
                            &active_writer_transactions,
                            job_name,
                        )
                        .await;
                    stats.batches_failed += 1;
                    return Err(e);
                }
            }
        } else {
            // Abort all transactions due to processing failures
            warn!(
                "Job '{}': Aborting multi-source transaction due to processing failures - {} records failed",
                job_name, total_records_failed
            );

            // Complete batch span with error
            ObservabilityHelper::complete_batch_span_error(
                &mut batch_span_guard,
                &batch_start,
                total_records_processed as u64,
                total_records_failed,
            );

            self.abort_multi_source_transactions(
                context,
                &active_reader_transactions,
                &active_writer_transactions,
                job_name,
            )
            .await?;

            stats.batches_failed += 1;

            if matches!(
                self.config.failure_strategy,
                FailureStrategy::RetryWithBackoff
            ) {
                return Err(format!(
                    "Transactional batch failed with {} record failures - will retry",
                    total_records_failed
                )
                .into());
            }
        }

        Ok(())
    }

    /// Commit transactions for all active sources and sinks
    async fn commit_multi_source_transactions(
        &self,
        context: &mut crate::velostream::sql::execution::processors::ProcessorContext,
        reader_transactions: &HashMap<String, bool>,
        writer_transactions: &HashMap<String, bool>,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Commit sinks first (write transactions)
        for (sink_name, is_active) in writer_transactions {
            if *is_active {
                context.set_active_writer(sink_name)?;
                context
                    .commit_writer()
                    .await
                    .map_err(|e| format!("Failed to commit sink '{}': {:?}", sink_name, e))?;
                debug!(
                    "Job '{}': Committed transaction for sink '{}'",
                    job_name, sink_name
                );
            }
        }

        // Then commit sources (read transactions)
        for (source_name, is_active) in reader_transactions {
            if *is_active {
                context.set_active_reader(source_name)?;
                context
                    .commit_source(source_name)
                    .await
                    .map_err(|e| format!("Failed to commit source '{}': {:?}", source_name, e))?;
                debug!(
                    "Job '{}': Committed transaction for source '{}'",
                    job_name, source_name
                );
            }
        }

        info!(
            "Job '{}': Successfully committed all multi-source transactions",
            job_name
        );
        Ok(())
    }

    /// Abort transactions for all active sources and sinks
    async fn abort_multi_source_transactions(
        &self,
        context: &mut crate::velostream::sql::execution::processors::ProcessorContext,
        reader_transactions: &HashMap<String, bool>,
        writer_transactions: &HashMap<String, bool>,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Abort sink transactions first
        for (sink_name, is_active) in writer_transactions {
            if *is_active {
                if let Err(e) = context.set_active_writer(sink_name) {
                    warn!(
                        "Job '{}': Failed to set active writer '{}' for abort: {:?}",
                        job_name, sink_name, e
                    );
                    continue;
                }

                match context.abort_writer().await {
                    Ok(()) => debug!(
                        "Job '{}': Aborted transaction for sink '{}'",
                        job_name, sink_name
                    ),
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to abort transaction for sink '{}': {:?}",
                            sink_name, e
                        );
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    }
                }
            }
        }

        // Then abort source transactions
        for (source_name, is_active) in reader_transactions {
            if *is_active {
                if let Err(e) = context.set_active_reader(source_name) {
                    warn!(
                        "Job '{}': Failed to set active reader '{}' for abort: {:?}",
                        job_name, source_name, e
                    );
                    continue;
                }

                match context.abort_reader().await {
                    Ok(()) => debug!(
                        "Job '{}': Aborted transaction for source '{}'",
                        job_name, source_name
                    ),
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to abort transaction for source '{}': {:?}",
                            source_name, e
                        );
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    }
                }
            }
        }

        warn!("Job '{}': Aborted all multi-source transactions", job_name);
        Ok(())
    }

    /// Abort all active transactions
    async fn abort_transactions(
        &self,
        reader: &mut dyn DataReader,
        writer: Option<&mut dyn DataWriter>,
        reader_tx_active: bool,
        writer_tx_active: bool,
        job_name: &str,
    ) -> DataSourceResult<()> {
        // Abort writer transaction first
        if let Some(w) = writer {
            if writer_tx_active {
                match w.abort_transaction().await {
                    Ok(()) => debug!("Job '{}': Writer transaction aborted", job_name),
                    Err(e) => {
                        let error_msg = format!("Failed to abort writer transaction: {:?}", e);
                        error!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                    }
                }
            }
        }

        // Then abort reader transaction
        if reader_tx_active {
            match reader.abort_transaction().await {
                Ok(()) => debug!("Job '{}': Reader transaction aborted", job_name),
                Err(e) => {
                    let error_msg = format!("Failed to abort reader transaction: {:?}", e);
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, &job_name, error_msg);
                }
            }
        }

        Ok(())
    }
}

/// Create a transactional job processor with default configuration (at-least-once semantics)
///
/// Uses FailBatch strategy which provides strong consistency within each batch but
/// allows duplicate processing on retry scenarios.
pub fn create_transactional_processor() -> TransactionalJobProcessor {
    TransactionalJobProcessor::new(JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::FailBatch, // Strict consistency per batch
        ..Default::default()
    })
}

/// Create a transactional job processor with best-effort semantics (at-least-once)
///
/// Uses LogAndContinue strategy which is more lenient with individual record failures
/// but still maintains at-least-once delivery guarantees for the overall batch.
pub fn create_best_effort_transactional_processor() -> TransactionalJobProcessor {
    TransactionalJobProcessor::new(JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::LogAndContinue, // More lenient
        max_retries: 1,
        ..Default::default()
    })
}
