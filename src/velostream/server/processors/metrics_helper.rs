//! Shared metrics helper for job processors
//!
//! This module provides shared functionality for SQL-annotated metrics that can be used
//! by both SimpleJobProcessor and TransactionalJobProcessor.
//!
//! # Performance Instrumentation
//!
//! This module tracks performance metrics for condition evaluation and label extraction:
//! - `condition_eval_times`: Time spent evaluating conditions per metric
//! - `label_extract_times`: Time spent extracting labels per record
//! - `emission_overhead`: Total overhead for metric emission per record

use crate::velostream::observability::label_extraction::{
    extract_label_values, LabelExtractionConfig,
};
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::observability_utils::{
    extract_and_validate_labels, with_observability_lock,
};
use crate::velostream::sql::ast::Expr;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::FieldValue;
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::parser::annotations::{MetricAnnotation, MetricType};
use crate::velostream::sql::parser::StreamingSqlParser;
use crate::velostream::sql::StreamingQuery;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Configuration for label handling behavior
#[derive(Debug, Clone)]
pub struct LabelHandlingConfig {
    /// If true, emit metrics even if label extraction fails (use defaults)
    /// If false, skip metric if label extraction produces fewer labels than expected
    pub strict_mode: bool,
}

impl Default for LabelHandlingConfig {
    fn default() -> Self {
        Self { strict_mode: false } // Default: permissive (emit with defaults)
    }
}

/// Performance telemetry for metrics operations
#[derive(Debug, Clone)]
pub struct MetricsPerformanceTelemetry {
    /// Time spent in condition evaluation (microseconds)
    pub condition_eval_time_us: u64,
    /// Time spent in label extraction (microseconds)
    pub label_extract_time_us: u64,
    /// Total emission overhead per record (microseconds)
    pub total_emission_overhead_us: u64,
}

impl Default for MetricsPerformanceTelemetry {
    fn default() -> Self {
        Self {
            condition_eval_time_us: 0,
            label_extract_time_us: 0,
            total_emission_overhead_us: 0,
        }
    }
}

/// Helper for managing SQL-annotated metrics across different processor types
///
/// Provides:
/// - Condition parsing and evaluation (Phase 4)
/// - Metric registration from SQL annotations
/// - Metric emission for processed records
/// - Performance telemetry for condition evaluation and label extraction
///
/// # Performance Optimizations
/// - Conditions are parsed once at registration time and cached
/// - Label extraction config is cached at initialization time (no per-record recreation)
/// - Uses RwLock for efficient concurrent read access
/// - Minimal lock contention on hot paths
/// - Performance telemetry tracks overhead without measurable impact
pub struct ProcessorMetricsHelper {
    /// Parsed condition expressions for conditional metric emission
    /// Key: metric name, Value: parsed SQL expression (Arc for cheap cloning)
    metric_conditions: Arc<RwLock<HashMap<String, Arc<Expr>>>>,
    /// Label handling configuration (strict vs permissive mode)
    pub label_config: LabelHandlingConfig,
    /// Cached label extraction config (initialized once, reused for all records)
    label_extraction_config: LabelExtractionConfig,
    /// Performance telemetry (thread-local accumulated data)
    telemetry: Arc<RwLock<MetricsPerformanceTelemetry>>,
}

impl ProcessorMetricsHelper {
    /// Create a new metrics helper with default (permissive) label handling
    pub fn new() -> Self {
        Self::with_config(LabelHandlingConfig::default())
    }

    /// Create a new metrics helper with custom label handling configuration
    pub fn with_config(label_config: LabelHandlingConfig) -> Self {
        Self {
            metric_conditions: Arc::new(RwLock::new(HashMap::new())),
            label_config,
            label_extraction_config: LabelExtractionConfig::default(),
            telemetry: Arc::new(RwLock::new(MetricsPerformanceTelemetry::default())),
        }
    }

    /// Enable strict mode (skip metrics if labels cannot be extracted)
    pub fn with_strict_mode(mut self) -> Self {
        self.label_config.strict_mode = true;
        self
    }

    /// Get current performance telemetry
    pub async fn get_telemetry(&self) -> MetricsPerformanceTelemetry {
        self.telemetry.read().await.clone()
    }

    /// Reset performance telemetry
    pub async fn reset_telemetry(&self) {
        let mut telemetry = self.telemetry.write().await;
        *telemetry = MetricsPerformanceTelemetry::default();
    }

    /// Parse and store condition expression from annotation
    ///
    /// This method parses the condition once and stores the parsed expression
    /// for efficient evaluation on every record. Parse errors are logged but
    /// don't prevent metric registration.
    pub async fn compile_condition(
        &self,
        annotation: &MetricAnnotation,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(condition_str) = &annotation.condition {
            // Parse condition to expression (only once, at registration time)
            match Self::parse_condition_to_expr(condition_str) {
                Ok(expr) => {
                    // Store parsed expression for fast evaluation
                    let mut conditions = self.metric_conditions.write().await;
                    conditions.insert(annotation.name.clone(), Arc::new(expr));
                    info!(
                        "Job '{}': Compiled condition for metric '{}': {}",
                        job_name, annotation.name, condition_str
                    );
                }
                Err(e) => {
                    // Log parse error but don't fail registration
                    // Metric will emit unconditionally if condition fails to parse
                    warn!(
                        "Job '{}': Failed to parse condition for metric '{}' (will emit unconditionally): {} - Error: {}",
                        job_name, annotation.name, condition_str, e
                    );
                }
            }
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

    /// Evaluate condition for a record using cached parsed expression
    ///
    /// # Error Handling Strategy
    ///
    /// - **No condition present**: Returns `true` (always emit metric)
    /// - **Parse errors**: Returns `true` (emit metric, warning logged at registration time)
    ///   - Rationale: Invalid syntax should be caught at registration, not silently skip all metrics
    /// - **Evaluation errors**: Returns `false` (don't emit metric, log debug)
    ///   - Rationale: Runtime errors (missing fields, type mismatches) are data-dependent
    /// - **Non-boolean results**: Returns `false` (don't emit metric, log debug)
    ///   - Rationale: Conditions must be boolean expressions
    ///
    /// # Performance
    ///
    /// This method uses a cached parsed expression for O(1) lookup with shared read lock.
    /// No parsing overhead on the hot path - expressions are parsed once at registration time.
    async fn evaluate_condition(
        &self,
        metric_name: &str,
        record: &crate::velostream::sql::execution::StreamRecord,
        job_name: &str,
    ) -> bool {
        // Use read lock for concurrent access (no contention)
        let conditions = self.metric_conditions.read().await;

        match conditions.get(metric_name) {
            Some(expr) => {
                // Evaluate cached expression (no parsing!)
                Self::evaluate_condition_expr(expr, record, metric_name, job_name)
            }
            None => {
                // No condition - always emit
                true
            }
        }
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

    /// Extract annotations of a specific type from a query
    fn extract_annotations_by_type<'a>(
        query: &'a StreamingQuery,
        metric_type: MetricType,
    ) -> Vec<&'a MetricAnnotation> {
        match query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations
                .iter()
                .filter(|a| a.metric_type == metric_type)
                .collect(),
            _ => vec![],
        }
    }

    /// Record condition evaluation time in telemetry
    pub async fn record_condition_eval_time(&self, duration_us: u64) {
        let mut telemetry = self.telemetry.write().await;
        telemetry.condition_eval_time_us =
            telemetry.condition_eval_time_us.saturating_add(duration_us);
    }

    /// Record label extraction time in telemetry
    pub async fn record_label_extract_time(&self, duration_us: u64) {
        let mut telemetry = self.telemetry.write().await;
        telemetry.label_extract_time_us =
            telemetry.label_extract_time_us.saturating_add(duration_us);
    }

    /// Record total emission overhead in telemetry
    pub async fn record_emission_overhead(&self, duration_us: u64) {
        let mut telemetry = self.telemetry.write().await;
        telemetry.total_emission_overhead_us = telemetry
            .total_emission_overhead_us
            .saturating_add(duration_us);
    }

    /// Check if labels are valid (all extracted or strict mode disabled)
    pub fn validate_labels(&self, annotation: &MetricAnnotation, extracted_count: usize) -> bool {
        if self.label_config.strict_mode {
            // Strict mode: all expected labels must be extracted
            extracted_count == annotation.labels.len()
        } else {
            // Permissive mode: allow any number of extracted labels
            true
        }
    }

    /// Common logic for registering metrics with the metrics provider
    async fn register_metrics_common<F>(
        &self,
        annotations: Vec<&MetricAnnotation>,
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        metric_type_name: &str,
        register_fn: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(
            &crate::velostream::observability::metrics::MetricsProvider,
            &MetricAnnotation,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        if annotations.is_empty() {
            debug!(
                "Job '{}': No {} metrics to register",
                job_name, metric_type_name
            );
            return Ok(());
        }

        if let Some(obs) = observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for annotation in annotations {
                            // Register the metric using the provided function
                            register_fn(metrics, annotation)?;

                            // Compile condition expression if present
                            self.compile_condition(annotation, job_name).await?;

                            info!(
                                "Job '{}': Registered {} metric '{}' with labels {:?}{}",
                                job_name,
                                metric_type_name,
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

    /// Register counter metrics from SQL annotations
    pub async fn register_counter_metrics(
        &self,
        query: &StreamingQuery,
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let annotations = Self::extract_annotations_by_type(query, MetricType::Counter);

        // FR-073: Debug logging for registration attempts
        info!(
            "Job '{}': Attempting to register counter metrics (found {} annotations)",
            job_name,
            annotations.len()
        );

        self.register_metrics_common(
            annotations,
            observability,
            job_name,
            "counter",
            |metrics, annotation| {
                let help = annotation
                    .help
                    .as_deref()
                    .unwrap_or("SQL-annotated counter metric");
                metrics
                    .register_counter_metric(&annotation.name, help, &annotation.labels)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        )
        .await
    }

    /// Emit counter metrics for processed records
    pub async fn emit_counter_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        observability: &Option<SharedObservabilityManager>,
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
        if let Some(obs) = observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for record in output_records {
                            let record_start = Instant::now();
                            for annotation in &counter_annotations {
                                // Check if metric should be emitted based on condition
                                let cond_start = Instant::now();
                                if !self.should_emit_metric(annotation, record, job_name).await {
                                    self.record_condition_eval_time(
                                        cond_start.elapsed().as_micros() as u64,
                                    )
                                    .await;
                                    continue;
                                }
                                self.record_condition_eval_time(
                                    cond_start.elapsed().as_micros() as u64
                                )
                                .await;

                                // Extract label values using enhanced extraction with nested field support
                                let extract_start = Instant::now();
                                let label_values = extract_label_values(
                                    record,
                                    &annotation.labels,
                                    &self.label_extraction_config,
                                );
                                self.record_label_extract_time(
                                    extract_start.elapsed().as_micros() as u64
                                )
                                .await;

                                // Validate labels based on configuration
                                if !self.validate_labels(annotation, label_values.len()) {
                                    debug!(
                                        "Job '{}': Skipping counter '{}' - strict mode: missing label values (expected {}, got {})",
                                        job_name,
                                        annotation.name,
                                        annotation.labels.len(),
                                        label_values.len()
                                    );
                                    continue;
                                }

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
                            self.record_emission_overhead(record_start.elapsed().as_micros() as u64).await;
                        }
                    }
                }
            }
        }
    }

    /// Register gauge metrics from SQL annotations
    pub async fn register_gauge_metrics(
        &self,
        query: &StreamingQuery,
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let annotations = Self::extract_annotations_by_type(query, MetricType::Gauge);

        // FR-073: Debug logging for registration attempts
        info!(
            "Job '{}': Attempting to register gauge metrics (found {} annotations)",
            job_name,
            annotations.len()
        );

        self.register_metrics_common(
            annotations,
            observability,
            job_name,
            "gauge",
            |metrics, annotation| {
                let help = annotation
                    .help
                    .as_deref()
                    .unwrap_or("SQL-annotated gauge metric");
                metrics
                    .register_gauge_metric(&annotation.name, help, &annotation.labels)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        )
        .await
    }

    /// Emit gauge metrics for processed records
    pub async fn emit_gauge_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        observability: &Option<SharedObservabilityManager>,
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
        if let Some(obs) = observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for record in output_records {
                            let record_start = Instant::now();
                            for annotation in &gauge_annotations {
                                // Check if metric should be emitted based on condition
                                let cond_start = Instant::now();
                                if !self.should_emit_metric(annotation, record, job_name).await {
                                    self.record_condition_eval_time(
                                        cond_start.elapsed().as_micros() as u64,
                                    )
                                    .await;
                                    continue;
                                }
                                self.record_condition_eval_time(
                                    cond_start.elapsed().as_micros() as u64
                                )
                                .await;

                                // Extract label values using enhanced extraction with nested field support
                                let extract_start = Instant::now();
                                let label_values = extract_label_values(
                                    record,
                                    &annotation.labels,
                                    &self.label_extraction_config,
                                );
                                self.record_label_extract_time(
                                    extract_start.elapsed().as_micros() as u64
                                )
                                .await;

                                // Validate labels based on configuration
                                if !self.validate_labels(annotation, label_values.len()) {
                                    debug!(
                                        "Job '{}': Skipping gauge '{}' - strict mode: missing label values (expected {}, got {})",
                                        job_name,
                                        annotation.name,
                                        annotation.labels.len(),
                                        label_values.len()
                                    );
                                    continue;
                                }

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
                            self.record_emission_overhead(record_start.elapsed().as_micros() as u64).await;
                        }
                    }
                }
            }
        }
    }

    /// Register histogram metrics from SQL annotations
    pub async fn register_histogram_metrics(
        &self,
        query: &StreamingQuery,
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let annotations = Self::extract_annotations_by_type(query, MetricType::Histogram);

        // FR-073: Debug logging for registration attempts
        info!(
            "Job '{}': Attempting to register histogram metrics (found {} annotations)",
            job_name,
            annotations.len()
        );

        self.register_metrics_common(
            annotations,
            observability,
            job_name,
            "histogram",
            |metrics, annotation| {
                let help = annotation
                    .help
                    .as_deref()
                    .unwrap_or("SQL-annotated histogram metric");
                metrics
                    .register_histogram_metric(
                        &annotation.name,
                        help,
                        &annotation.labels,
                        annotation.buckets.clone(),
                    )
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            },
        )
        .await
    }

    /// Emit histogram metrics for processed records
    pub async fn emit_histogram_metrics(
        &self,
        query: &StreamingQuery,
        output_records: &[crate::velostream::sql::execution::StreamRecord],
        observability: &Option<SharedObservabilityManager>,
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
        if let Some(obs) = observability {
            match obs.read().await {
                obs_lock => {
                    if let Some(metrics) = obs_lock.metrics() {
                        for record in output_records {
                            let record_start = Instant::now();
                            for annotation in &histogram_annotations {
                                // Check if metric should be emitted based on condition
                                let cond_start = Instant::now();
                                if !self.should_emit_metric(annotation, record, job_name).await {
                                    self.record_condition_eval_time(
                                        cond_start.elapsed().as_micros() as u64,
                                    )
                                    .await;
                                    continue;
                                }
                                self.record_condition_eval_time(
                                    cond_start.elapsed().as_micros() as u64
                                )
                                .await;

                                // Extract label values using enhanced extraction with nested field support
                                let extract_start = Instant::now();
                                let label_values = extract_label_values(
                                    record,
                                    &annotation.labels,
                                    &self.label_extraction_config,
                                );
                                self.record_label_extract_time(
                                    extract_start.elapsed().as_micros() as u64
                                )
                                .await;

                                // Validate labels based on configuration
                                if !self.validate_labels(annotation, label_values.len()) {
                                    debug!(
                                        "Job '{}': Skipping histogram '{}' - strict mode: missing label values (expected {}, got {})",
                                        job_name,
                                        annotation.name,
                                        annotation.labels.len(),
                                        label_values.len()
                                    );
                                    continue;
                                }

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
                            self.record_emission_overhead(record_start.elapsed().as_micros() as u64).await;
                        }
                    }
                }
            }
        }
    }
}

impl Default for ProcessorMetricsHelper {
    fn default() -> Self {
        Self::new()
    }
}
