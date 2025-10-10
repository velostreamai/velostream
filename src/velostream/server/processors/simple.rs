//! Simple (non-transactional) streaming job processing
//!
//! This module provides best-effort job processing without transactional semantics.
//! It's optimized for throughput and simplicity, using basic commit/flush operations.

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::observability::label_extraction::{
    extract_label_values, LabelExtractionConfig,
};
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::common::*;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::FieldValue;
use crate::velostream::sql::parser::annotations::{MetricAnnotation, MetricType};
use crate::velostream::sql::parser::StreamingSqlParser;
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
    /// Condition strings for conditional metric emission
    /// Key: metric name, Value: condition string
    metric_conditions: Arc<Mutex<HashMap<String, String>>>,
}

impl SimpleJobProcessor {
    pub fn new(config: JobProcessingConfig) -> Self {
        Self {
            config,
            observability: None,
            metric_conditions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn new_with_observability(
        config: JobProcessingConfig,
        observability: Option<SharedObservabilityManager>,
    ) -> Self {
        Self {
            config,
            observability,
            metric_conditions: Arc::new(Mutex::new(HashMap::new())),
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
            metric_conditions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get reference to the job processing configuration
    pub fn get_config(&self) -> &JobProcessingConfig {
        &self.config
    }

    /// Store condition string from annotation
    async fn compile_condition(
        &self,
        annotation: &MetricAnnotation,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(condition_str) = &annotation.condition {
            // Store condition string for evaluation
            let mut conditions = self.metric_conditions.lock().await;
            conditions.insert(annotation.name.clone(), condition_str.clone());
            info!(
                "Job '{}': Registered condition for metric '{}': {}",
                job_name, annotation.name, condition_str
            );
        }
        Ok(())
    }

    /// Parse a condition string into an SQL expression
    ///
    /// Wraps the condition in a dummy SELECT query to leverage the SQL parser.
    ///
    /// # Visibility
    /// Public for testing purposes.
    pub fn parse_condition_to_expr(
        condition_str: &str,
    ) -> Result<crate::velostream::sql::ast::Expr, String> {
        let dummy_sql = format!("SELECT * FROM dummy WHERE {}", condition_str);
        let parser = StreamingSqlParser::new();

        let query = parser
            .parse(&dummy_sql)
            .map_err(|e| format!("Failed to parse condition: {:?}", e))?;

        match query {
            crate::velostream::sql::StreamingQuery::Select { where_clause, .. } => {
                where_clause.ok_or_else(|| "No WHERE clause extracted from condition".to_string())
            }
            _ => Err("Parsed query is not a SELECT statement".to_string()),
        }
    }

    /// Evaluate a parsed expression against a record
    ///
    /// Returns true if the expression evaluates to a boolean true value.
    /// Returns false for non-boolean results or evaluation errors.
    ///
    /// # Visibility
    /// Public for testing purposes.
    pub fn evaluate_condition_expr(
        expr: &crate::velostream::sql::ast::Expr,
        record: &crate::velostream::sql::execution::StreamRecord,
        metric_name: &str,
        job_name: &str,
    ) -> bool {
        match ExpressionEvaluator::evaluate_expression_value(expr, record) {
            Ok(FieldValue::Boolean(result)) => result,
            Ok(other_value) => {
                debug!(
                    "Job '{}': Condition for metric '{}' returned non-boolean value: {:?}. Treating as false.",
                    job_name, metric_name, other_value
                );
                false
            }
            Err(e) => {
                debug!(
                    "Job '{}': Condition evaluation failed for metric '{}': {:?}. Treating as false.",
                    job_name, metric_name, e
                );
                false
            }
        }
    }

    /// Evaluate condition for a record by parsing it on-demand
    ///
    /// # Error Handling Strategy
    ///
    /// - **No condition present**: Returns `true` (always emit metric)
    /// - **Parse errors**: Returns `true` (emit metric, log warning at registration time)
    ///   - Rationale: Invalid syntax should be caught at registration, not silently skip all metrics
    /// - **Evaluation errors**: Returns `false` (don't emit metric, log debug)
    ///   - Rationale: Runtime errors (missing fields, type mismatches) are data-dependent
    /// - **Non-boolean results**: Returns `false` (don't emit metric, log debug)
    ///   - Rationale: Conditions must be boolean expressions
    ///
    /// # Performance Notes
    ///
    /// Conditions are parsed on-demand for each record. For high-throughput scenarios,
    /// consider:
    /// - Using simple conditions (single field comparisons)
    /// - Avoiding complex expressions with multiple operations
    /// - Pre-filtering data with WHERE clauses before metric emission
    async fn evaluate_condition(
        &self,
        metric_name: &str,
        record: &crate::velostream::sql::execution::StreamRecord,
        job_name: &str,
    ) -> bool {
        let conditions = self.metric_conditions.lock().await;
        let condition_str = match conditions.get(metric_name) {
            Some(cond) => cond,
            None => return true, // No condition - always emit
        };

        // Parse condition to expression
        let expr = match Self::parse_condition_to_expr(condition_str) {
            Ok(expr) => expr,
            Err(e) => {
                // Parse error: This should have been caught at registration time
                // Emit metric to avoid silently dropping all metrics due to syntax error
                debug!(
                    "Job '{}': Failed to parse condition for metric '{}': {}. Treating as always true.",
                    job_name, metric_name, e
                );
                return true;
            }
        };

        // Evaluate expression against record
        Self::evaluate_condition_expr(&expr, record, metric_name, job_name)
    }

    /// Check if a metric should be emitted based on its condition
    ///
    /// This is a convenience method that evaluates the condition and logs appropriately.
    /// Returns true if the metric should be emitted, false otherwise.
    async fn should_emit_metric(
        &self,
        annotation: &MetricAnnotation,
        record: &crate::velostream::sql::execution::StreamRecord,
        job_name: &str,
    ) -> bool {
        if !self
            .evaluate_condition(&annotation.name, record, job_name)
            .await
        {
            debug!(
                "Job '{}': Skipping metric '{}' - condition not met",
                job_name, annotation.name
            );
            return false;
        }
        true
    }

    /// Register counter metrics from SQL annotations
    async fn register_counter_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Extract counter annotations from the query
        let counter_annotations = match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == MetricType::Counter)
                .collect::<Vec<_>>(),
            _ => return Ok(()), // Only CreateStream queries have annotations
        };

        if counter_annotations.is_empty() {
            debug!("Job '{}': No counter metrics to register", job_name);
            return Ok(());
        }

        // Register each counter metric with the metrics provider and compile conditions
        if let Some(obs) = &self.observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for annotation in counter_annotations {
                            let help = annotation
                                .help
                                .as_deref()
                                .unwrap_or("SQL-annotated counter metric");

                            metrics.register_counter_metric(
                                &annotation.name,
                                help,
                                &annotation.labels,
                            )?;

                            // Compile condition expression if present
                            self.compile_condition(annotation, job_name).await?;

                            info!(
                                "Job '{}': Registered counter metric '{}' with labels {:?}{}",
                                job_name,
                                annotation.name,
                                annotation.labels,
                                if annotation.condition.is_some() {
                                    " (with condition)"
                                } else {
                                    ""
                                }
                            );
                        }
                    } else {
                        warn!(
                            "Job '{}': No metrics provider available for annotation registration",
                            job_name
                        );
                    }
                }
            }
        } else {
            debug!(
                "Job '{}': No observability manager - skipping metric registration",
                job_name
            );
        }

        Ok(())
    }

    /// Emit counter metrics for processed records
    async fn emit_counter_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        job_name: &str,
    ) {
        // Extract counter annotations from the query
        let counter_annotations = match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == MetricType::Counter)
                .collect::<Vec<_>>(),
            _ => return, // Only CreateStream queries have annotations
        };

        if counter_annotations.is_empty() || output_records.is_empty() {
            return;
        }

        // Emit metrics for each output record
        if let Some(obs) = &self.observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for record in output_records {
                            for annotation in &counter_annotations {
                                // Check if metric should be emitted based on condition
                                if !self.should_emit_metric(annotation, record, job_name).await {
                                    continue;
                                }

                                // Extract label values using enhanced extraction with nested field support
                                let config = LabelExtractionConfig::default();
                                let label_values =
                                    extract_label_values(record, &annotation.labels, &config);

                                // Emit counter (label values always match expected count with enhanced extraction)
                                if !label_values.is_empty() || annotation.labels.is_empty() {
                                    if let Err(e) =
                                        metrics.emit_counter(&annotation.name, &label_values)
                                    {
                                        debug!(
                                            "Job '{}': Failed to emit counter '{}': {:?}",
                                            job_name, annotation.name, e
                                        );
                                    } else {
                                        debug!(
                                            "Job '{}': Emitted counter '{}' with labels {:?}",
                                            job_name, annotation.name, label_values
                                        );
                                    }
                                } else {
                                    debug!(
                                        "Job '{}': Skipping counter '{}' - missing label values (expected {}, got {})",
                                        job_name,
                                        annotation.name,
                                        annotation.labels.len(),
                                        label_values.len()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Register gauge metrics from SQL annotations
    async fn register_gauge_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Extract gauge annotations from the query
        let gauge_annotations = match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == MetricType::Gauge)
                .collect::<Vec<_>>(),
            _ => return Ok(()), // Only CreateStream queries have annotations
        };

        if gauge_annotations.is_empty() {
            debug!("Job '{}': No gauge metrics to register", job_name);
            return Ok(());
        }

        // Register each gauge metric with the metrics provider and compile conditions
        if let Some(obs) = &self.observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for annotation in gauge_annotations {
                            let help = annotation
                                .help
                                .as_deref()
                                .unwrap_or("SQL-annotated gauge metric");

                            metrics.register_gauge_metric(
                                &annotation.name,
                                help,
                                &annotation.labels,
                            )?;

                            // Compile condition expression if present
                            self.compile_condition(annotation, job_name).await?;

                            info!(
                                "Job '{}': Registered gauge metric '{}' with labels {:?}{}",
                                job_name,
                                annotation.name,
                                annotation.labels,
                                if annotation.condition.is_some() {
                                    " (with condition)"
                                } else {
                                    ""
                                }
                            );
                        }
                    } else {
                        warn!(
                            "Job '{}': No metrics provider available for annotation registration",
                            job_name
                        );
                    }
                }
            }
        } else {
            debug!(
                "Job '{}': No observability manager - skipping metric registration",
                job_name
            );
        }

        Ok(())
    }

    /// Emit gauge metrics for processed records
    async fn emit_gauge_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        job_name: &str,
    ) {
        // Extract gauge annotations from the query
        let gauge_annotations = match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == MetricType::Gauge)
                .collect::<Vec<_>>(),
            _ => return, // Only CreateStream queries have annotations
        };

        if gauge_annotations.is_empty() || output_records.is_empty() {
            return;
        }

        // Emit metrics for each output record
        if let Some(obs) = &self.observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for record in output_records {
                            for annotation in &gauge_annotations {
                                // Check if metric should be emitted based on condition
                                if !self.should_emit_metric(annotation, record, job_name).await {
                                    continue;
                                }

                                // Extract label values using enhanced extraction with nested field support
                                let config = LabelExtractionConfig::default();
                                let label_values =
                                    extract_label_values(record, &annotation.labels, &config);

                                // Emit gauge if labels are available
                                if !label_values.is_empty() || annotation.labels.is_empty() {
                                    // Extract the gauge value from the specified field
                                    if let Some(field_name) = &annotation.field {
                                        if let Some(field_value) = record.fields.get(field_name) {
                                            // Convert FieldValue to f64
                                            let value = match field_value {
                                                crate::velostream::sql::execution::FieldValue::Float(v) => *v,
                                                crate::velostream::sql::execution::FieldValue::Integer(v) => *v as f64,
                                                crate::velostream::sql::execution::FieldValue::ScaledInteger(v, scale) => {
                                                    (*v as f64) / 10_f64.powi(*scale as i32)
                                                }
                                                _ => {
                                                    debug!(
                                                        "Job '{}': Gauge '{}' field '{}' is not numeric, skipping",
                                                        job_name, annotation.name, field_name
                                                    );
                                                    continue;
                                                }
                                            };

                                            if let Err(e) = metrics.emit_gauge(
                                                &annotation.name,
                                                &label_values,
                                                value,
                                            ) {
                                                debug!(
                                                    "Job '{}': Failed to emit gauge '{}': {:?}",
                                                    job_name, annotation.name, e
                                                );
                                            } else {
                                                debug!(
                                                    "Job '{}': Emitted gauge '{}' = {} with labels {:?}",
                                                    job_name, annotation.name, value, label_values
                                                );
                                            }
                                        } else {
                                            debug!(
                                                "Job '{}': Gauge '{}' field '{}' not found in record",
                                                job_name, annotation.name, field_name
                                            );
                                        }
                                    } else {
                                        debug!(
                                            "Job '{}': Gauge '{}' has no field specified",
                                            job_name, annotation.name
                                        );
                                    }
                                } else {
                                    debug!(
                                        "Job '{}': Skipping gauge '{}' - missing label values (expected {}, got {})",
                                        job_name,
                                        annotation.name,
                                        annotation.labels.len(),
                                        label_values.len()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Register histogram metrics from SQL annotations
    async fn register_histogram_metrics(
        &self,
        query: &StreamingQuery,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Extract histogram annotations from the query
        let histogram_annotations = match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == MetricType::Histogram)
                .collect::<Vec<_>>(),
            _ => return Ok(()), // Only CreateStream queries have annotations
        };

        if histogram_annotations.is_empty() {
            debug!("Job '{}': No histogram metrics to register", job_name);
            return Ok(());
        }

        // Register each histogram metric with the metrics provider and compile conditions
        if let Some(obs) = &self.observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for annotation in histogram_annotations {
                            let help = annotation
                                .help
                                .as_deref()
                                .unwrap_or("SQL-annotated histogram metric");

                            metrics.register_histogram_metric(
                                &annotation.name,
                                help,
                                &annotation.labels,
                                annotation.buckets.clone(),
                            )?;

                            // Compile condition expression if present
                            self.compile_condition(annotation, job_name).await?;

                            info!(
                                "Job '{}': Registered histogram metric '{}' with labels {:?}{}",
                                job_name,
                                annotation.name,
                                annotation.labels,
                                if annotation.condition.is_some() {
                                    " (with condition)"
                                } else {
                                    ""
                                }
                            );
                        }
                    } else {
                        warn!(
                            "Job '{}': No metrics provider available for annotation registration",
                            job_name
                        );
                    }
                }
            }
        } else {
            debug!(
                "Job '{}': No observability manager - skipping metric registration",
                job_name
            );
        }

        Ok(())
    }

    /// Emit histogram metrics for processed records
    async fn emit_histogram_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        job_name: &str,
    ) {
        // Extract histogram annotations from the query
        let histogram_annotations = match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == MetricType::Histogram)
                .collect::<Vec<_>>(),
            _ => return, // Only CreateStream queries have annotations
        };

        if histogram_annotations.is_empty() || output_records.is_empty() {
            return;
        }

        // Emit metrics for each output record
        if let Some(obs) = &self.observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for record in output_records {
                            for annotation in &histogram_annotations {
                                // Check if metric should be emitted based on condition
                                if !self.should_emit_metric(annotation, record, job_name).await {
                                    continue;
                                }

                                // Extract label values using enhanced extraction with nested field support
                                let config = LabelExtractionConfig::default();
                                let label_values =
                                    extract_label_values(record, &annotation.labels, &config);

                                // Emit histogram if labels are available
                                if !label_values.is_empty() || annotation.labels.is_empty() {
                                    // Extract the histogram value from the specified field
                                    if let Some(field_name) = &annotation.field {
                                        if let Some(field_value) = record.fields.get(field_name) {
                                            // Convert FieldValue to f64
                                            let value = match field_value {
                                                crate::velostream::sql::execution::FieldValue::Float(v) => *v,
                                                crate::velostream::sql::execution::FieldValue::Integer(v) => *v as f64,
                                                crate::velostream::sql::execution::FieldValue::ScaledInteger(v, scale) => {
                                                    (*v as f64) / 10_f64.powi(*scale as i32)
                                                }
                                                _ => {
                                                    debug!(
                                                        "Job '{}': Histogram '{}' field '{}' is not numeric, skipping",
                                                        job_name, annotation.name, field_name
                                                    );
                                                    continue;
                                                }
                                            };

                                            if let Err(e) = metrics.emit_histogram(
                                                &annotation.name,
                                                &label_values,
                                                value,
                                            ) {
                                                debug!(
                                                    "Job '{}': Failed to emit histogram '{}': {:?}",
                                                    job_name, annotation.name, e
                                                );
                                            } else {
                                                debug!(
                                                    "Job '{}': Emitted histogram '{}' observed value {} with labels {:?}",
                                                    job_name, annotation.name, value, label_values
                                                );
                                            }
                                        } else {
                                            debug!(
                                                "Job '{}': Histogram '{}' field '{}' not found in record",
                                                job_name, annotation.name, field_name
                                            );
                                        }
                                    } else {
                                        debug!(
                                            "Job '{}': Histogram '{}' has no field specified",
                                            job_name, annotation.name
                                        );
                                    }
                                } else {
                                    debug!(
                                        "Job '{}': Skipping histogram '{}' - missing label values (expected {}, got {})",
                                        job_name,
                                        annotation.name,
                                        annotation.labels.len(),
                                        label_values.len()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
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

        // Record deserialization telemetry and metrics
        if let Some(obs) = &self.observability {
            info!("Job '{}': Observability manager present, recording deserialization metrics (batch_size={}, duration={}ms)", job_name, batch.len(), deser_duration);
            if let Ok(obs_lock) = obs.try_read() {
                // Record telemetry span
                if let Some(telemetry) = obs_lock.telemetry() {
                    let mut span =
                        telemetry.start_streaming_span("deserialization", batch.len() as u64);
                    span.set_processing_time(deser_duration);
                    span.set_success();
                    info!(
                        "Job '{}': Telemetry span recorded for deserialization",
                        job_name
                    );
                }

                // Record Prometheus metrics
                if let Some(metrics) = obs_lock.metrics() {
                    let throughput = if deser_duration > 0 {
                        (batch.len() as f64 / deser_duration as f64) * 1000.0 // records per second
                    } else {
                        0.0
                    };
                    metrics.record_streaming_operation(
                        "deserialization",
                        std::time::Duration::from_millis(deser_duration),
                        batch.len() as u64,
                        throughput,
                    );
                    info!("Job '{}': Prometheus metrics recorded for deserialization (throughput={:.2} rec/s)", job_name, throughput);
                } else {
                    warn!(
                        "Job '{}': Observability manager present but NO metrics provider",
                        job_name
                    );
                }
            } else {
                warn!(
                    "Job '{}': Observability manager present but could not acquire read lock",
                    job_name
                );
            }
        } else {
            warn!(
                "Job '{}': NO observability manager - metrics will not be recorded",
                job_name
            );
        }

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
        let batch_result = process_batch_with_output(batch, engine, query, job_name).await;
        let sql_duration = sql_start.elapsed().as_millis() as u64;

        // Record SQL processing telemetry and metrics
        if let Some(obs) = &self.observability {
            if let Ok(obs_lock) = obs.try_read() {
                // Record telemetry span
                if let Some(telemetry) = obs_lock.telemetry() {
                    let mut span = telemetry.start_sql_query_span("sql_processing", job_name);
                    span.set_execution_time(sql_duration);
                    span.set_record_count(batch_result.records_processed as u64);
                    if batch_result.records_failed > 0 {
                        span.set_error(&format!("{} records failed", batch_result.records_failed));
                    } else {
                        span.set_success();
                    }
                }

                // Record Prometheus metrics
                if let Some(metrics) = obs_lock.metrics() {
                    let success = batch_result.records_failed == 0;
                    metrics.record_sql_query(
                        "stream_processing",
                        std::time::Duration::from_millis(sql_duration),
                        success,
                        batch_result.records_processed as u64,
                    );
                }
            }
        }

        debug!(
            "Job '{}': SQL processing complete - {} records processed, {} failed",
            job_name, batch_result.records_processed, batch_result.records_failed
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
                        if let Some(obs) = &self.observability {
                            if let Ok(obs_lock) = obs.try_read() {
                                // Record telemetry span
                                if let Some(telemetry) = obs_lock.telemetry() {
                                    let mut span = telemetry
                                        .start_streaming_span("serialization", record_count as u64);
                                    span.set_processing_time(ser_duration);
                                    span.set_success();
                                }

                                // Record Prometheus metrics
                                if let Some(metrics) = obs_lock.metrics() {
                                    let throughput = if ser_duration > 0 {
                                        (record_count as f64 / ser_duration as f64) * 1000.0
                                    } else {
                                        0.0
                                    };
                                    metrics.record_streaming_operation(
                                        "serialization",
                                        std::time::Duration::from_millis(ser_duration),
                                        record_count as u64,
                                        throughput,
                                    );
                                }
                            }
                        }

                        debug!(
                            "Job '{}': Successfully wrote {} records to sink",
                            job_name, record_count
                        );
                    }
                    Err(e) => {
                        let ser_duration = ser_start.elapsed().as_millis() as u64;

                        // Record serialization failure telemetry and metrics
                        if let Some(obs) = &self.observability {
                            if let Ok(obs_lock) = obs.try_read() {
                                // Record telemetry span
                                if let Some(telemetry) = obs_lock.telemetry() {
                                    let mut span = telemetry
                                        .start_streaming_span("serialization", record_count as u64);
                                    span.set_processing_time(ser_duration);
                                    span.set_error(&format!("{:?}", e));
                                }

                                // Record Prometheus metrics (still record the attempt)
                                if let Some(metrics) = obs_lock.metrics() {
                                    let throughput = if ser_duration > 0 {
                                        (record_count as f64 / ser_duration as f64) * 1000.0
                                    } else {
                                        0.0
                                    };
                                    metrics.record_streaming_operation(
                                        "serialization_failed",
                                        std::time::Duration::from_millis(ser_duration),
                                        record_count as u64,
                                        throughput,
                                    );
                                }
                            }
                        }

                        warn!(
                            "Job '{}': Failed to write {} records to sink: {:?}",
                            job_name,
                            batch_result.output_records.len(),
                            e
                        );

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

        // Process records from each source
        for source_name in &source_names {
            context.set_active_reader(source_name)?;

            // Read batch from current source (with telemetry)
            let deser_start = Instant::now();
            let batch = context.read().await?;
            let deser_duration = deser_start.elapsed().as_millis() as u64;

            // Record deserialization metrics
            if let Some(obs) = &self.observability {
                debug!("Job '{}': Recording deserialization metrics for source '{}' (batch_size={}, duration={}ms)", job_name, source_name, batch.len(), deser_duration);
                match obs.read().await {
                    obs_lock => {
                        if let Some(metrics) = obs_lock.metrics() {
                            let throughput = if deser_duration > 0 {
                                (batch.len() as f64 / deser_duration as f64) * 1000.0
                            } else {
                                0.0
                            };
                            metrics.record_streaming_operation(
                                "deserialization",
                                std::time::Duration::from_millis(deser_duration),
                                batch.len() as u64,
                                throughput,
                            );
                            info!("Job '{}': ✅ Metrics recorded: deserialization from '{}' ({} records, {:.2} rec/s)",
                                  job_name, source_name, batch.len(), throughput);
                        } else {
                            warn!(
                                "Job '{}': ❌ Metrics unavailable (no metrics provider)",
                                job_name
                            );
                        }
                    }
                }
            } else {
                warn!(
                    "Job '{}': NO observability manager for multi-source processing",
                    job_name
                );
            }

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
            // Use process_batch_with_output to get actual query results
            let sql_start = Instant::now();
            let batch_result = process_batch_with_output(batch, engine, query, job_name).await;
            let sql_duration = sql_start.elapsed().as_millis() as u64;

            // Record SQL processing metrics
            if let Some(obs) = &self.observability {
                match obs.read().await {
                    obs_lock => {
                        if let Some(metrics) = obs_lock.metrics() {
                            let throughput = if sql_duration > 0 {
                                (batch_result.records_processed as f64 / sql_duration as f64)
                                    * 1000.0
                            } else {
                                0.0
                            };
                            metrics.record_streaming_operation(
                                "sql_processing",
                                std::time::Duration::from_millis(sql_duration),
                                batch_result.records_processed as u64,
                                throughput,
                            );

                            // Also record SQL-specific metrics
                            metrics.record_sql_query(
                                "streaming_query",
                                std::time::Duration::from_millis(sql_duration),
                                batch_result.records_failed == 0,
                                batch_result.records_processed as u64,
                            );

                            info!("Job '{}': ✅ Metrics recorded: sql_processing from '{}' ({} records, {:.2} rec/s)",
                                  job_name, source_name, batch_result.records_processed, throughput);
                        } else {
                            warn!(
                                "Job '{}': ❌ Metrics unavailable (no metrics provider)",
                                job_name
                            );
                        }
                    }
                }
            }

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

        // Determine if batch should be committed
        let should_commit =
            should_commit_batch(self.config.failure_strategy, total_records_failed, job_name);

        if should_commit {
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

                            // Record serialization metrics
                            if let Some(obs) = &self.observability {
                                match obs.read().await {
                                    obs_lock => {
                                        if let Some(metrics) = obs_lock.metrics() {
                                            let throughput = if ser_duration > 0 {
                                                (all_output_records.len() as f64
                                                    / ser_duration as f64)
                                                    * 1000.0
                                            } else {
                                                0.0
                                            };
                                            metrics.record_streaming_operation(
                                                "serialization",
                                                std::time::Duration::from_millis(ser_duration),
                                                all_output_records.len() as u64,
                                                throughput,
                                            );
                                            info!("Job '{}': ✅ Metrics recorded: serialization to '{}' ({} records, {:.2} rec/s)",
                                                  job_name, &sink_names[0], all_output_records.len(), throughput);
                                        } else {
                                            warn!("Job '{}': ❌ Metrics unavailable (no metrics provider)", job_name);
                                        }
                                    }
                                }
                            }

                            debug!(
                                "Job '{}': Successfully wrote {} records to sink '{}'",
                                job_name,
                                all_output_records.len(),
                                &sink_names[0]
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Job '{}': Failed to write {} records to sink '{}': {:?}",
                                job_name,
                                all_output_records.len(),
                                &sink_names[0],
                                e
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
                    for sink_name in &sink_names {
                        let ser_start = Instant::now();
                        match context
                            .write_batch_to_shared(sink_name, &all_output_records)
                            .await
                        {
                            Ok(()) => {
                                let ser_duration = ser_start.elapsed().as_millis() as u64;

                                // Record serialization metrics
                                if let Some(obs) = &self.observability {
                                    match obs.read().await {
                                        obs_lock => {
                                            if let Some(metrics) = obs_lock.metrics() {
                                                let throughput = if ser_duration > 0 {
                                                    (all_output_records.len() as f64
                                                        / ser_duration as f64)
                                                        * 1000.0
                                                } else {
                                                    0.0
                                                };
                                                metrics.record_streaming_operation(
                                                    "serialization",
                                                    std::time::Duration::from_millis(ser_duration),
                                                    all_output_records.len() as u64,
                                                    throughput,
                                                );
                                                info!("Job '{}': ✅ Metrics recorded: serialization to '{}' ({} records, {:.2} rec/s)",
                                                      job_name, sink_name, all_output_records.len(), throughput);
                                            } else {
                                                warn!("Job '{}': ❌ Metrics unavailable (no metrics provider)", job_name);
                                            }
                                        }
                                    }
                                }

                                debug!(
                                    "Job '{}': Successfully wrote {} records to sink '{}'",
                                    job_name,
                                    all_output_records.len(),
                                    sink_name
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Job '{}': Failed to write {} records to sink '{}': {:?}",
                                    job_name,
                                    all_output_records.len(),
                                    sink_name,
                                    e
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
                "Job '{}': Successfully processed multi-source batch - {} records processed, {} failed, {} written to sinks",
                job_name, total_records_processed, total_records_failed, all_output_records.len()
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
