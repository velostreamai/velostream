//! Simple (non-transactional) streaming job processing
//!
//! This module provides best-effort job processing without transactional semantics.
//! It's optimized for throughput and simplicity, using basic commit/flush operations.

use super::common::DeadLetterQueue;
use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors;
use crate::velostream::server::processors::common::*;
use crate::velostream::server::processors::error_tracking_helper::ErrorTracker;
use crate::velostream::server::processors::job_processor_trait::{JobProcessor, ProcessorMetrics};
use crate::velostream::server::processors::metrics_collector::MetricsCollector;
use crate::velostream::server::processors::metrics_helper::ProcessorMetricsHelper;
use crate::velostream::server::processors::observability_helper::ObservabilityHelper;
use crate::velostream::server::processors::observability_wrapper::ObservabilityWrapper;
use crate::velostream::server::processors::profiling_helper::{ProfilingHelper, ProfilingMetrics};
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::config::StreamingConfig;
use crate::velostream::sql::execution::processors::ProcessorContext;
use crate::velostream::sql::{StreamExecutionEngine, StreamingQuery};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Simple (non-transactional) job processor
pub struct SimpleJobProcessor {
    config: JobProcessingConfig,
    /// Unified observability, metrics, and DLQ wrapper
    observability_wrapper: ObservabilityWrapper,
    /// Profiling helper for timing instrumentation
    profiling_helper: ProfilingHelper,
    /// Stop flag for graceful shutdown
    stop_flag: Arc<AtomicBool>,
}

impl SimpleJobProcessor {
    /// Create a new Simple processor with optional DLQ based on config
    ///
    /// # Failure Handling Strategy
    /// SimpleJobProcessor uses `FailureStrategy::LogAndContinue` to process records
    /// even if some fail. Failed records can be sent to DLQ for analysis if enabled.
    ///
    /// # DLQ Configuration
    /// DLQ is enabled by default (config.enable_dlq == true) to support error recovery
    /// and debugging in production environments. Disable if overhead is a concern.
    pub fn new(config: JobProcessingConfig) -> Self {
        Self {
            config: config.clone(),
            observability_wrapper: ObservabilityWrapper::builder()
                .with_dlq(config.enable_dlq)
                .build(),
            profiling_helper: ProfilingHelper::new(),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn new_with_observability(
        config: JobProcessingConfig,
        observability: Option<SharedObservabilityManager>,
    ) -> Self {
        Self {
            config: config.clone(),
            observability_wrapper: ObservabilityWrapper::builder()
                .with_observability(observability)
                .with_dlq(config.enable_dlq)
                .build(),
            profiling_helper: ProfilingHelper::new(),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create processor with observability support
    ///
    /// # Failure Handling Strategy
    /// SimpleJobProcessor uses `FailureStrategy::LogAndContinue` which continues
    /// processing even when records fail. With observability enabled, errors are
    /// tracked for monitoring and debugging.
    pub fn with_observability(
        config: JobProcessingConfig,
        observability: Option<SharedObservabilityManager>,
    ) -> Self {
        Self {
            config: config.clone(),
            observability_wrapper: ObservabilityWrapper::builder()
                .with_observability(observability)
                .with_dlq(config.enable_dlq)
                .build(),
            profiling_helper: ProfilingHelper::new(),
            stop_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if Dead Letter Queue is enabled
    pub fn has_dlq(&self) -> bool {
        self.observability_wrapper.has_dlq()
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
        record: &StreamRecord,
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
        let obs = self.observability_wrapper.observability().cloned();
        self.observability_wrapper
            .metrics_helper()
            .register_counter_metrics(query, &obs, job_name)
            .await
    }

    /// Emit counter metrics for processed records
    async fn emit_counter_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[std::sync::Arc<StreamRecord>],
        job_name: &str,
    ) {
        let obs = self.observability_wrapper.observability().cloned();
        self.observability_wrapper
            .metrics_helper()
            .emit_counter_metrics(query, output_records, &obs, job_name)
            .await
    }

    /// Register gauge metrics from SQL annotations
    async fn register_gauge_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let obs = self.observability_wrapper.observability().cloned();
        self.observability_wrapper
            .metrics_helper()
            .register_gauge_metrics(query, &obs, job_name)
            .await
    }

    /// Emit gauge metrics for processed records
    async fn emit_gauge_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[Arc<StreamRecord>],
        job_name: &str,
    ) {
        let obs = self.observability_wrapper.observability().cloned();
        self.observability_wrapper
            .metrics_helper()
            .emit_gauge_metrics(query, output_records, &obs, job_name)
            .await
    }

    /// Register histogram metrics from SQL annotations
    async fn register_histogram_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let obs = self.observability_wrapper.observability().cloned();
        self.observability_wrapper
            .metrics_helper()
            .register_histogram_metrics(query, &obs, job_name)
            .await
    }

    /// Emit histogram metrics for processed records
    async fn emit_histogram_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[std::sync::Arc<StreamRecord>],
        job_name: &str,
    ) {
        let obs = self.observability_wrapper.observability().cloned();
        self.observability_wrapper
            .metrics_helper()
            .emit_histogram_metrics(query, output_records, &obs, job_name)
            .await
    }

    pub async fn process_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        self.process_multi_job(
            HashMap::from([(String::from("default_source"), reader)]),
            match writer {
                Some(w) => HashMap::from([(String::from("default_sink"), w)]),
                None => HashMap::new(),
            },
            engine,
            query,
            job_name,
            shutdown_rx,
        )
        .await
    }

    /// Process records from multiple datasources with multiple sinks (multi-source/sink processing)
    pub async fn process_multi_job(
        &self,
        readers: HashMap<String, Box<dyn DataReader>>,
        writers: HashMap<String, Box<dyn DataWriter>>,
        engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        _shutdown_rx: mpsc::Receiver<()>,
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

        // FR-082 Phase 6.5: Initialize QueryExecution in the engine
        // This creates the persistent ProcessorContext that will hold state across all batches
        {
            let mut engine_lock = engine.write().await;
            engine_lock.init_query_execution(query.clone());
        }

        // Create enhanced context with multiple sources and sinks
        let mut context = ProcessorContext::new_with_sources(&job_name, readers, writers);

        // FR-081 Phase 2A: Enable window_v2 architecture for high-performance window processing
        context.streaming_config = Some(StreamingConfig::default());

        // Copy engine state to context
        {
            let _engine_lock = engine.read().await;
            // Context is already prepared by engine.prepare_context() above
        }

        loop {
            // Check for stop signal from processor
            if self.stop_flag.load(Ordering::Relaxed) {
                info!("Job '{}' received stop signal", job_name);
                break;
            }

            // Check if all sources have finished processing (consistent with transactional)
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

            // Track records processed before this batch
            let records_before = stats.records_processed;

            // Process from all sources
            match self
                .process_data(&mut context, &engine, &query, &job_name, &mut stats)
                .await
            {
                Ok(()) => {
                    if self.config.log_progress
                        && stats
                            .batches_processed
                            .is_multiple_of(self.config.progress_interval)
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
                let error_msg = format!("Failed to commit source '{}': {:?}", source_name, e);
                error!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(
                    &self.observability_wrapper.observability().cloned(),
                    &job_name,
                    error_msg,
                );
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
            ErrorTracker::record_error(
                &self.observability_wrapper.observability().cloned(),
                &job_name,
                error_msg,
            );
        } else {
            info!("Job '{}': Successfully flushed all sinks", job_name);
        }

        log_final_stats(&job_name, &stats);
        Ok(stats)
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
                    ErrorTracker::record_error(
                        &self.observability_wrapper.observability().cloned(),
                        job_name,
                        error_msg,
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
                let error_msg = format!("Source commit failed: {:?}", e);
                error!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(
                    &self.observability_wrapper.observability().cloned(),
                    job_name,
                    error_msg,
                );
                return Err(format!("Source commit failed: {:?}", e).into());
            }
        }

        Ok(())
    }

    /// Process a batch from multiple sources using StreamExecutionEngine's multi-source support
    async fn process_data(
        &self,
        context: &mut ProcessorContext,
        engine: &Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
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
        // PERF: Collect Arc<StreamRecord> for zero-copy multi-source collection
        let mut all_output_records: Vec<Arc<StreamRecord>> = Vec::new();

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

            source_batches.push((source_name.clone(), batch, deser_duration));
        }

        // Create parent batch span for the overall multi-source batch operation
        let first_batch = source_batches
            .iter()
            .find(|(_, batch, _)| !batch.is_empty())
            .map(|(_, batch, _)| batch.as_slice())
            .unwrap_or(&[]);

        let parent_batch_span_guard = ObservabilityHelper::start_batch_span(
            self.observability_wrapper.observability_ref(),
            job_name,
            stats.batches_processed,
            first_batch,
        );

        // Now process all collected batches with per-source batch spans
        for (source_name, batch, deser_duration) in source_batches {
            // Create per-source batch span linked to parent batch span for proper tracing
            // This ensures each source's data is traced independently while maintaining parent-child relationship
            debug!(
                "🔗 Creating per-source batch span for source '{}' linked to parent batch span",
                source_name
            );
            let source_batch_span_guard = ObservabilityHelper::start_batch_span(
                self.observability_wrapper.observability_ref(),
                &format!("{} (source: {})", job_name, source_name),
                stats.batches_processed,
                if !batch.is_empty() { &batch } else { &[] },
            );

            // Record deserialization telemetry and metrics on per-source span
            ObservabilityHelper::record_deserialization(
                self.observability_wrapper.observability_ref(),
                job_name,
                &source_batch_span_guard,
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
                "Job '{}': Read {} records from source '{}'",
                job_name,
                batch.len(),
                source_name
            );

            // Process batch through SQL engine and capture output records (with telemetry)
            let sql_start = Instant::now();
            let batch_result = process_batch(batch, engine, query, job_name).await;
            let sql_duration = sql_start.elapsed().as_millis() as u64;

            // Record SQL processing telemetry and metrics on per-source span
            ObservabilityHelper::record_sql_processing(
                self.observability_wrapper.observability_ref(),
                job_name,
                &source_batch_span_guard,
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
            // PERF: Arc clone is O(1), not full record clone - this is where we win!
            all_output_records.extend(batch_result.output_records.iter().cloned());

            // FR-073: Emit SQL-native metrics for processed records from this source
            // PERF(FR-082 Phase 2): Use Arc records directly for metrics - no clone!
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
                            "Job '{}': Source '{}' had {} failures, sending to DLQ",
                            job_name, source_name, batch_result.records_failed
                        );
                        // Add all failed records to the DLQ
                        for error in &batch_result.error_details {
                            // Note: We don't have the original record here, but error_details has the index
                            // In a full implementation, we'd pass the records through the batch processor
                            info!(
                                "DLQ Entry: Record {} - {}",
                                error.record_index, error.error_message
                            );
                        }
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
            // PERF(FR-082 Phase 2): Use Arc records directly - no clone!
            let mut output_owned: Vec<Arc<StreamRecord>> = all_output_records;
            let output_record_count = output_owned.len(); // Save count before potential move

            // Inject trace context into all output records for distributed tracing
            ObservabilityHelper::inject_trace_context_into_records(
                &parent_batch_span_guard,
                &mut output_owned,
                job_name,
            );

            // Write output records to all sinks
            if !output_owned.is_empty() && !sink_names.is_empty() {
                debug!(
                    "Job '{}': Writing {} output records to {} sink(s)",
                    job_name,
                    output_owned.len(),
                    sink_names.len()
                );

                // Optimize for multi-sink scenario: use shared slice instead of cloning for each sink
                if sink_names.len() == 1 {
                    // Single sink: use move semantics (no clone)
                    let ser_start = Instant::now();
                    let record_count = output_owned.len();
                    match context.write_batch_to(&sink_names[0], output_owned).await {
                        Ok(()) => {
                            let ser_duration = ser_start.elapsed().as_millis() as u64;

                            // Record serialization telemetry and metrics
                            ObservabilityHelper::record_serialization_success(
                                self.observability_wrapper.observability_ref(),
                                job_name,
                                &parent_batch_span_guard,
                                record_count,
                                ser_duration,
                                None,
                            );

                            debug!(
                                "Job '{}': Successfully wrote {} records to sink '{}'",
                                job_name, record_count, &sink_names[0]
                            );
                        }
                        Err(e) => {
                            let error_msg = format!(
                                "Failed to write {} records to sink '{}': {:?}",
                                record_count, &sink_names[0], e
                            );
                            warn!("Job '{}': {}", job_name, error_msg);
                            ErrorTracker::record_error(
                                &self.observability_wrapper.observability().cloned(),
                                job_name,
                                error_msg,
                            );
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
                    // NOTE: output_owned already created above for trace injection
                    for sink_name in &sink_names {
                        let ser_start = Instant::now();
                        match context
                            .write_batch_to_shared(sink_name, &output_owned)
                            .await
                        {
                            Ok(()) => {
                                let ser_duration = ser_start.elapsed().as_millis() as u64;

                                // Record serialization telemetry and metrics
                                ObservabilityHelper::record_serialization_success(
                                    self.observability_wrapper.observability_ref(),
                                    job_name,
                                    &parent_batch_span_guard,
                                    output_owned.len(),
                                    ser_duration,
                                    None,
                                );

                                debug!(
                                    "Job '{}': Successfully wrote {} records to sink '{}'",
                                    job_name,
                                    output_owned.len(),
                                    sink_name
                                );
                            }
                            Err(e) => {
                                let error_msg = format!(
                                    "Failed to write {} records to sink '{}': {:?}",
                                    output_owned.len(),
                                    sink_name,
                                    e
                                );
                                warn!("Job '{}': {}", job_name, error_msg);
                                ErrorTracker::record_error(
                                    &self.observability_wrapper.observability().cloned(),
                                    job_name,
                                    error_msg,
                                );
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
            } else if !output_owned.is_empty() {
                debug!(
                    "Job '{}': Processed {} output records but no sinks configured",
                    job_name,
                    output_owned.len()
                );
            } else if total_records_processed > 0 && output_owned.is_empty() {
                // DIAGNOSTIC: Silent failure case - records processed but no output
                warn!(
                    "Job '{}': ⚠️  CRITICAL DIAGNOSTIC: {} records processed but 0 output records!",
                    job_name, total_records_processed
                );
                warn!(
                    "Job '{}': This indicates that QueryProcessor.process_query() is returning None for result.record",
                    job_name
                );
                warn!("Job '{}': Possible root causes:", job_name);
                warn!(
                    "   1. Window aggregation not emitting (window not yet complete or no EMIT mode)"
                );
                warn!("   2. Query has filtering that removes all records (WHERE/HAVING clauses)");
                warn!(
                    "   3. Stream produces no output by design (passthrough with no transformation)"
                );
                warn!("Job '{}': Recommended debugging steps:", job_name);
                warn!(
                    "   1. Check log output for '⚠️  DIAGNOSTIC: X out of Y processed records produced no output'"
                );
                warn!("   2. Verify window configuration has EMIT CHANGES or EMIT FINAL");
                warn!("   3. Check HAVING/WHERE clauses are not filtering out all records");
                warn!("   4. Review QueryProcessor::process_query() return values");
            }

            // Commit all sources
            for source_name in &source_names {
                if let Err(e) = context.commit_source(source_name).await {
                    let error_msg = format!("Failed to commit source '{}': {:?}", source_name, e);
                    error!("Job '{}': {}", job_name, error_msg);
                    ErrorTracker::record_error(
                        &self.observability_wrapper.observability().cloned(),
                        job_name,
                        error_msg,
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
                let error_msg = format!("Failed to flush sinks: {:?}", e);
                warn!("Job '{}': {}", job_name, error_msg);
                ErrorTracker::record_error(
                    &self.observability_wrapper.observability().cloned(),
                    job_name,
                    error_msg,
                );
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
                job_name, total_records_processed, total_records_failed, output_record_count
            );

            // Complete batch span with success
            if let Some(mut batch_span) = parent_batch_span_guard {
                let batch_duration = batch_start.elapsed().as_millis() as u64;
                batch_span.set_total_records(total_records_processed as u64);
                batch_span.set_batch_duration(batch_duration);
                batch_span.set_success();
            }

            // Sync error metrics to Prometheus after batch completion
            if let Some(obs) = self.observability_wrapper.observability() {
                if let Ok(obs_lock) = obs.try_read() {
                    if let Some(metrics) = obs_lock.metrics() {
                        metrics.sync_error_metrics();
                    }
                }
            }
        } else {
            // Batch failed due to processing errors
            stats.batches_failed += 1;
            warn!(
                "Job '{}': Skipping commit due to {} failures",
                job_name, total_records_failed
            );

            // Complete batch span with error
            if let Some(mut batch_span) = parent_batch_span_guard {
                let batch_duration = batch_start.elapsed().as_millis() as u64;
                batch_span.set_total_records(total_records_processed as u64);
                batch_span.set_batch_duration(batch_duration);
                batch_span.set_error(&format!("{} records failed", total_records_failed));
            }

            // Sync error metrics to Prometheus after batch failure
            if let Some(obs) = self.observability_wrapper.observability() {
                if let Ok(obs_lock) = obs.try_read() {
                    if let Some(metrics) = obs_lock.metrics() {
                        metrics.sync_error_metrics();
                    }
                }
            }
        }

        Ok(())
    }
}

/// Implement JobProcessor trait for SimpleJobProcessor (V1 Architecture)
#[async_trait::async_trait]
impl JobProcessor for SimpleJobProcessor {
    fn num_partitions(&self) -> usize {
        1 // V1 uses single-threaded, single partition
    }

    fn processor_name(&self) -> &str {
        "SimpleJobProcessor"
    }

    fn processor_version(&self) -> &str {
        "V1"
    }

    fn metrics(&self) -> ProcessorMetrics {
        let mc = self.observability_wrapper.metrics_collector();
        ProcessorMetrics {
            version: self.processor_version().to_string(),
            name: self.processor_name().to_string(),
            num_partitions: self.num_partitions(),
            lifecycle_state: mc.lifecycle_state(),
            total_records: mc.total_records(),
            failed_records: mc.failed_records(),
            throughput_rps: mc.throughput_rps(),
            uptime_secs: mc.uptime_secs(),
        }
    }

    async fn process_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        self.process_multi_job(
            HashMap::from([(String::from("default_source"), reader)]),
            match writer {
                Some(w) => HashMap::from([(String::from("default_sink"), w)]),
                None => HashMap::new(),
            },
            engine,
            query,
            job_name,
            shutdown_rx,
        )
        .await
    }

    async fn process_multi_job(
        &self,
        readers: HashMap<String, Box<dyn DataReader>>,
        writers: HashMap<String, Box<dyn DataWriter>>,
        engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        // Delegate to the existing process_multi_job implementation
        self.process_multi_job(readers, writers, engine, query, job_name, shutdown_rx)
            .await
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stop_flag.store(true, Ordering::Relaxed);
        info!("SimpleJobProcessor stop signal set");
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
