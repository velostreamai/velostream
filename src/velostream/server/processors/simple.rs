//! Simple (non-transactional) streaming job processing
//!
//! This module provides best-effort job processing without transactional semantics.
//! It's optimized for throughput and simplicity, using basic commit/flush operations.

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
use tokio::sync::{mpsc, Mutex};

/// Simple (non-transactional) job processor
pub struct SimpleJobProcessor {
    config: JobProcessingConfig,
    observability: Option<SharedObservabilityManager>,
    /// Shared metrics helper for SQL-annotated metrics
    metrics_helper: ProcessorMetricsHelper,
}

impl SimpleJobProcessor {
    pub fn new(config: JobProcessingConfig) -> Self {
        Self {
            config,
            observability: None,
            metrics_helper: ProcessorMetricsHelper::new(),
        }
    }

    pub fn new_with_observability(
        config: JobProcessingConfig,
        observability: Option<SharedObservabilityManager>,
    ) -> Self {
        Self {
            config,
            observability,
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

    // =========================================================================
    // Metric Helper Delegation (Public Methods for Testing)
    // =========================================================================

    /// Parse a condition string into an SQL expression
    ///
    /// # Visibility
    /// Public for testing purposes. Delegates to ProcessorMetricsHelper.
    pub fn parse_condition_to_expr(
        condition_str: &str,
    ) -> Result<crate::velostream::sql::ast::Expr, String> {
        ProcessorMetricsHelper::parse_condition_to_expr(condition_str)
    }

    /// Evaluate a parsed expression against a record
    ///
    /// # Visibility
    /// Public for testing purposes. Delegates to ProcessorMetricsHelper.
    pub fn evaluate_condition_expr(
        expr: &crate::velostream::sql::ast::Expr,
        record: &crate::velostream::sql::execution::StreamRecord,
        metric_name: &str,
        job_name: &str,
    ) -> bool {
        ProcessorMetricsHelper::evaluate_condition_expr(expr, record, metric_name, job_name)
    }

    /// Register counter metrics from SQL annotations
    async fn register_counter_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics_helper
            .register_counter_metrics(query, &self.observability, job_name)
            .await
    }

    /// Emit counter metrics for processed records
    async fn emit_counter_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        job_name: &str,
    ) {
        self.metrics_helper
            .emit_counter_metrics(query, output_records, &self.observability, job_name)
            .await
    }

    /// Register gauge metrics from SQL annotations
    async fn register_gauge_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics_helper
            .register_gauge_metrics(query, &self.observability, job_name)
            .await
    }

    /// Emit gauge metrics for processed records
    async fn emit_gauge_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        job_name: &str,
    ) {
        self.metrics_helper
            .emit_gauge_metrics(query, output_records, &self.observability, job_name)
            .await
    }

    /// Register histogram metrics from SQL annotations
    async fn register_histogram_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics_helper
            .register_histogram_metrics(query, &self.observability, job_name)
            .await
    }

    /// Emit histogram metrics for processed records
    async fn emit_histogram_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        job_name: &str,
    ) {
        self.metrics_helper
            .emit_histogram_metrics(query, output_records, &self.observability, job_name)
            .await
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

        // FR-073: Register SQL-native metrics from @metric annotations
        info!(
            "Job '{}': ⚡ About to register SQL-native metrics from @metric annotations",
            job_name
        );

        // Register counter metrics from SQL annotations
        if let Err(e) = self.register_counter_metrics(&query, &job_name).await {
            warn!(
                "Job '{}': Failed to register counter metrics: {:?}",
                job_name, e
            );
        }

        // Register gauge metrics from SQL annotations
        if let Err(e) = self.register_gauge_metrics(&query, &job_name).await {
            warn!(
                "Job '{}': Failed to register gauge metrics: {:?}",
                job_name, e
            );
        }

        // Register histogram metrics from SQL annotations
        if let Err(e) = self.register_histogram_metrics(&query, &job_name).await {
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

        // Track if we've seen empty batches from all sources (lazy check)
        let mut consecutive_empty_batches = 0;
        const MAX_EMPTY_BATCHES: usize = 3; // Check has_more() after 3 empty batches

        loop {
            // Check for shutdown signal
            if shutdown_rx.try_recv().is_ok() {
                info!("Job '{}' received shutdown signal", job_name);
                break;
            }

            // Only check has_more() after seeing empty batches (optimization)
            if consecutive_empty_batches >= MAX_EMPTY_BATCHES {
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

                // Reset counter after checking
                consecutive_empty_batches = 0;
            }

            // Track records processed before this batch
            let records_before = stats.records_processed;

            // Process from all sources
            match self
                .process_multi_source_batch(&mut context, &engine, &query, &job_name, &mut stats)
                .await
            {
                Ok(()) => {
                    // Check if we actually processed any records
                    let records_processed = stats.records_processed - records_before;
                    if records_processed > 0 {
                        // Reset empty batch counter on successful processing
                        consecutive_empty_batches = 0;
                    } else {
                        // No records processed - increment empty batch counter
                        consecutive_empty_batches += 1;
                    }

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
                    consecutive_empty_batches += 1; // Increment on failure (likely empty batch)

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
                let error_msg = format!("Failed to commit source '{}': {:?}", source_name, e);
                error!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(&self.observability, error_msg);
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
            ErrorTracker::record_error(&self.observability, error_msg);
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

        // FR-073: Debug - verify we're reaching metric registration code
        info!(
            "Job '{}': ⚡ About to register SQL-native metrics from @metric annotations",
            job_name
        );

        // Register counter metrics from SQL annotations
        if let Err(e) = self.register_counter_metrics(&query, &job_name).await {
            warn!(
                "Job '{}': Failed to register counter metrics: {:?}",
                job_name, e
            );
        }

        // Register gauge metrics from SQL annotations
        if let Err(e) = self.register_gauge_metrics(&query, &job_name).await {
            warn!(
                "Job '{}': Failed to register gauge metrics: {:?}",
                job_name, e
            );
        }

        // Register histogram metrics from SQL annotations
        if let Err(e) = self.register_histogram_metrics(&query, &job_name).await {
            warn!(
                "Job '{}': Failed to register histogram metrics: {:?}",
                job_name, e
            );
        }

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

        // Step 1: Read batch from datasource (with telemetry)
        let deser_start = Instant::now();
        let batch = reader.read().await?;
        let deser_duration = deser_start.elapsed().as_millis() as u64;

        // Create a parent batch span to group all operations
        // Extract upstream trace context from first record's Kafka headers
        let batch_start = Instant::now();
        let mut batch_span_guard = ObservabilityHelper::start_batch_span(
            &self.observability,
            job_name,
            stats.batches_processed,
            &batch, // Pass batch records for trace context extraction
        );

        // Record deserialization telemetry and metrics
        ObservabilityHelper::record_deserialization(
            &self.observability,
            job_name,
            &batch_span_guard,
            batch.len(),
            deser_duration,
        );

        if batch.is_empty() {
            debug!(
                "Job '{}': No data available, checking if more data exists",
                job_name
            );

            // If no data and no more data expected, don't wait - let the main loop check has_more() and exit
            if !reader.has_more().await? {
                debug!(
                    "Job '{}': No data available and no more expected, batch processing complete",
                    job_name
                );
                return Ok(());
            }

            // Otherwise wait briefly and try again
            debug!(
                "Job '{}': No data available but more expected, waiting 100ms",
                job_name
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }

        debug!(
            "Job '{}': Read {} records from datasource",
            job_name,
            batch.len()
        );

        // Step 2: Process batch through SQL engine and capture output (with telemetry)
        let sql_start = Instant::now();
        let mut batch_result = process_batch_with_output(batch, engine, query, job_name).await;
        let sql_duration = sql_start.elapsed().as_millis() as u64;

        // Record SQL processing telemetry and metrics
        ObservabilityHelper::record_sql_processing(
            &self.observability,
            job_name,
            &batch_span_guard,
            &batch_result,
            sql_duration,
        );

        debug!(
            "Job '{}': SQL processing complete - {} records processed, {} failed",
            job_name, batch_result.records_processed, batch_result.records_failed
        );

        // Step 2b: Inject trace context into output records for distributed tracing
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span_guard,
            &mut batch_result.output_records,
            job_name,
        );

        // Emit counter metrics for successfully processed records
        self.emit_counter_metrics(query, &batch_result.output_records, job_name)
            .await;

        // Emit gauge metrics for successfully processed records
        self.emit_gauge_metrics(query, &batch_result.output_records, job_name)
            .await;

        // Emit histogram metrics for successfully processed records
        self.emit_histogram_metrics(query, &batch_result.output_records, job_name)
            .await;

        // Step 3: Handle results based on failure strategy
        let should_commit = should_commit_batch(
            self.config.failure_strategy,
            batch_result.records_failed,
            job_name,
        );

        // Step 4: Write processed data to sink if we have one (with telemetry)
        let mut sink_write_failed = false;
        if let Some(w) = writer.as_mut() {
            if should_commit && !batch_result.output_records.is_empty() {
                debug!(
                    "Job '{}': Writing {} output records to sink",
                    job_name,
                    batch_result.output_records.len()
                );

                // Attempt to write to sink with retry logic (with telemetry)
                let ser_start = Instant::now();
                let record_count = batch_result.output_records.len();
                match w.write_batch(batch_result.output_records.clone()).await {
                    Ok(()) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;

                        // Record serialization success telemetry and metrics
                        ObservabilityHelper::record_serialization_success(
                            &self.observability,
                            job_name,
                            &batch_span_guard,
                            record_count,
                            ser_duration,
                        );

                        debug!(
                            "Job '{}': Successfully wrote {} records to sink",
                            job_name, record_count
                        );
                    }
                    Err(e) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;

                        // Record serialization failure telemetry and metrics
                        ObservabilityHelper::record_serialization_failure(
                            &self.observability,
                            job_name,
                            &batch_span_guard,
                            record_count,
                            ser_duration,
                            &format!("{:?}", e),
                        );

                        let error_msg = format!("Failed to write {} records to sink: {:?}", batch_result.output_records.len(), e);
                        warn!("Job '{}': {}", job_name, error_msg);
                        ErrorTracker::record_error(&self.observability, error_msg);

                        sink_write_failed = true;

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
                        } else if matches!(self.config.failure_strategy, FailureStrategy::FailBatch)
                        {
                            warn!("Job '{}': Sink write failed with FailBatch strategy - batch will fail",
                                  job_name);
                        } else {
                            warn!("Job '{}': Sink write failed but continuing (failure strategy: {:?})",
                                  job_name, self.config.failure_strategy);
                        }
                    }
                }
            }
        }

        // Step 5: Commit with simple semantics (no rollback if sink fails)
        if should_commit && !sink_write_failed {
            self.commit_simple(reader, writer, job_name).await?;

            // Update stats from batch result
            update_stats_from_batch_result(stats, &batch_result);

            if batch_result.records_failed > 0 {
                debug!(
                    "Job '{}': Committed batch with {} failures",
                    job_name, batch_result.records_failed
                );
            }

            // Complete batch span with success
            ObservabilityHelper::complete_batch_span_success(
                &mut batch_span_guard,
                &batch_start,
                batch_result.records_processed as u64,
            );
        } else {
            // Batch failed due to SQL processing errors or sink write failures
            if sink_write_failed {
                warn!(
                    "Job '{}': Batch failed due to sink write failure (strategy: {:?})",
                    job_name, self.config.failure_strategy
                );
                stats.batches_failed += 1;
            } else {
                // Batch failed or RetryWithBackoff strategy triggered
                match self.config.failure_strategy {
                    FailureStrategy::RetryWithBackoff => {
                        warn!(
                            "Job '{}': Batch failed with {} record failures - applying retry backoff and will retry",
                            job_name, batch_result.records_failed
                        );
                        stats.batches_failed += 1;

                        // Complete batch span with error
                        ObservabilityHelper::complete_batch_span_error(
                            &mut batch_span_guard,
                            &batch_start,
                            batch_result.records_processed as u64,
                            batch_result.records_failed,
                        );

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

                        // Complete batch span with error
                        ObservabilityHelper::complete_batch_span_error(
                            &mut batch_span_guard,
                            &batch_start,
                            batch_result.records_processed as u64,
                            batch_result.records_failed,
                        );
                    }
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
                    let error_msg = format!("Sink flush failed (continuing anyway): {:?}", e);
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, error_msg);
                }
            }
        }

        // Step 2: Commit reader/source (always attempt)
        match reader.commit().await {
            Ok(()) => {
                debug!("Job '{}': Source committed", job_name);
            }
            Err(e) => {
                let error_msg = format!("Source commit failed: {:?}", e);
                error!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(&self.observability, error_msg);
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

        let sink_names = context.list_sinks();
        if sink_names.is_empty() {
            debug!(
                "Job '{}': No sinks configured - processed records will not be written",
                job_name
            );
        }

        let mut total_records_processed = 0;
        let mut total_records_failed = 0;
        let mut all_output_records: Vec<crate::velostream::sql::execution::StreamRecord> =
            Vec::new();

        // Start batch timing
        let batch_start = Instant::now();

        // Collect batches from all sources first
        let mut source_batches = Vec::new();
        for source_name in &source_names {
            context.set_active_reader(source_name)?;

            // Read batch from current source
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

        // Now process all collected batches
        for (source_name, batch, deser_start, deser_duration) in source_batches {
            // Record deserialization telemetry and metrics
            ObservabilityHelper::record_deserialization(
                &self.observability,
                job_name,
                &batch_span_guard,
                batch.len(),
                deser_duration,
            );

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

            // Process batch through SQL engine and capture output records (with telemetry)
            let sql_start = Instant::now();
            let batch_result = process_batch_with_output(batch, engine, query, job_name).await;
            let sql_duration = sql_start.elapsed().as_millis() as u64;

            // Record SQL processing telemetry and metrics
            ObservabilityHelper::record_sql_processing(
                &self.observability,
                job_name,
                &batch_span_guard,
                &batch_result,
                sql_duration,
            );

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
            all_output_records.extend(batch_result.output_records.clone());

            // FR-073: Emit SQL-native metrics for processed records from this source
            self.emit_counter_metrics(query, &batch_result.output_records, job_name)
                .await;
            self.emit_gauge_metrics(query, &batch_result.output_records, job_name)
                .await;
            self.emit_histogram_metrics(query, &batch_result.output_records, job_name)
                .await;

            // Handle failures according to strategy
            if batch_result.records_failed > 0 {
                match self.config.failure_strategy {
                    FailureStrategy::LogAndContinue => {
                        warn!(
                            "Job '{}': Source '{}' had {} failures (continuing)",
                            job_name, source_name, batch_result.records_failed
                        );
                    }
                    FailureStrategy::FailBatch => {
                        error!(
                            "Job '{}': Source '{}' had {} failures (failing batch)",
                            job_name, source_name, batch_result.records_failed
                        );
                        return Err(format!(
                            "Batch failed due to {} record processing errors from source '{}'",
                            batch_result.records_failed, source_name
                        )
                        .into());
                    }
                    FailureStrategy::RetryWithBackoff => {
                        error!(
                            "Job '{}': Source '{}' had {} failures (will retry)",
                            job_name, source_name, batch_result.records_failed
                        );
                        return Err(format!(
                            "Record processing failed for source '{}', will retry",
                            source_name
                        )
                        .into());
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

        // If no records were processed, this is a no-op - skip everything
        if total_records_processed == 0 {
            debug!(
                "Job '{}': No records processed (all batches empty) - no-op, returning early",
                job_name
            );
            return Ok(());
        }

        // Determine if batch should be committed
        let should_commit =
            should_commit_batch(self.config.failure_strategy, total_records_failed, job_name);

        if should_commit {
            // Inject trace context into all output records for distributed tracing
            ObservabilityHelper::inject_trace_context_into_records(
                &batch_span_guard,
                &mut all_output_records,
                job_name,
            );

            // Write output records to all sinks
            if !all_output_records.is_empty() && !sink_names.is_empty() {
                debug!(
                    "Job '{}': Writing {} output records to {} sink(s)",
                    job_name,
                    all_output_records.len(),
                    sink_names.len()
                );

                // Optimize for multi-sink scenario: use shared slice instead of cloning for each sink
                if sink_names.len() == 1 {
                    // Single sink: use move semantics (no clone)
                    let ser_start = Instant::now();
                    match context
                        .write_batch_to(&sink_names[0], all_output_records.clone())
                        .await
                    {
                        Ok(()) => {
                            let ser_duration = ser_start.elapsed().as_millis() as u64;

                            // Record serialization telemetry and metrics
                            ObservabilityHelper::record_serialization_success(
                                &self.observability,
                                job_name,
                                &batch_span_guard,
                                all_output_records.len(),
                                ser_duration,
                            );

                            debug!(
                                "Job '{}': Successfully wrote {} records to sink '{}'",
                                job_name,
                                all_output_records.len(),
                                &sink_names[0]
                            );
                        }
                        Err(e) => {
                            let error_msg = format!(
                                "Failed to write {} records to sink '{}': {:?}",
                                all_output_records.len(),
                                &sink_names[0],
                                e
                            );
                            warn!("Job '{}': {}", job_name, error_msg);
                            ErrorTracker::record_error(&self.observability, error_msg);
                            if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                                return Err(format!(
                                    "Failed to write to sink '{}': {:?}",
                                    &sink_names[0], e
                                )
                                .into());
                            }
                        }
                    }
                } else {
                    // Multiple sinks: use shared slice to avoid N clones
                    for sink_name in &sink_names {
                        let ser_start = Instant::now();
                        match context
                            .write_batch_to_shared(sink_name, &all_output_records)
                            .await
                        {
                            Ok(()) => {
                                let ser_duration = ser_start.elapsed().as_millis() as u64;

                                // Record serialization telemetry and metrics
                                ObservabilityHelper::record_serialization_success(
                                    &self.observability,
                                    job_name,
                                    &batch_span_guard,
                                    all_output_records.len(),
                                    ser_duration,
                                );

                                debug!(
                                    "Job '{}': Successfully wrote {} records to sink '{}'",
                                    job_name,
                                    all_output_records.len(),
                                    sink_name
                                );
                            }
                            Err(e) => {
                                let error_msg = format!(
                                    "Failed to write {} records to sink '{}': {:?}",
                                    all_output_records.len(),
                                    sink_name,
                                    e
                                );
                                warn!("Job '{}': {}", job_name, error_msg);
                                ErrorTracker::record_error(&self.observability, error_msg);
                                if matches!(
                                    self.config.failure_strategy,
                                    FailureStrategy::FailBatch
                                ) {
                                    return Err(format!(
                                        "Failed to write to sink '{}': {:?}",
                                        sink_name, e
                                    )
                                    .into());
                                }
                            }
                        }
                    }
                }
            } else if !all_output_records.is_empty() {
                debug!(
                    "Job '{}': Processed {} output records but no sinks configured",
                    job_name,
                    all_output_records.len()
                );
            }

            // Commit all sources
            for source_name in &source_names {
                if let Err(e) = context.commit_source(source_name).await {
                    let error_msg = format!("Failed to commit source '{}': {:?}", source_name, e);
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(&self.observability, error_msg);
                    if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                        return Err(
                            format!("Failed to commit source '{}': {:?}", source_name, e).into(),
                        );
                    }
                }
            }

            // Flush all sinks
            if let Err(e) = context.flush_all().await {
                let error_msg = format!("Failed to flush sinks: {:?}", e);
                warn!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(&self.observability, error_msg);
                if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                    return Err(format!("Failed to flush sinks: {:?}", e).into());
                }
            }

            // Update stats
            stats.batches_processed += 1;
            stats.records_processed += total_records_processed as u64;
            stats.records_failed += total_records_failed as u64;

            debug!(
                "Job '{}': Successfully processed multi-source batch - {} records processed, {} failed, {} written to sinks",
                job_name, total_records_processed, total_records_failed, all_output_records.len()
            );

            // Complete batch span with success
            if let Some(ref mut batch_span) = batch_span_guard {
                let batch_duration = batch_start.elapsed().as_millis() as u64;
                batch_span.set_total_records(total_records_processed as u64);
                batch_span.set_batch_duration(batch_duration);
                batch_span.set_success();
            }
        } else {
            // Batch failed due to processing errors
            stats.batches_failed += 1;
            warn!(
                "Job '{}': Skipping commit due to {} failures",
                job_name, total_records_failed
            );

            // Complete batch span with error
            if let Some(ref mut batch_span) = batch_span_guard {
                let batch_duration = batch_start.elapsed().as_millis() as u64;
                batch_span.set_total_records(total_records_processed as u64);
                batch_span.set_batch_duration(batch_duration);
                batch_span.set_error(&format!("{} records failed", total_records_failed));
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
