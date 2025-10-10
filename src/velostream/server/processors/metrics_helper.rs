//! Shared metrics helper for job processors
//!
//! This module provides shared functionality for SQL-annotated metrics that can be used
//! by both SimpleJobProcessor and TransactionalJobProcessor.

use crate::velostream::observability::label_extraction::{
    extract_label_values, LabelExtractionConfig,
};
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::sql::execution::expression::ExpressionEvaluator;
use crate::velostream::sql::execution::FieldValue;
use crate::velostream::sql::parser::annotations::{MetricAnnotation, MetricType};
use crate::velostream::sql::parser::StreamingSqlParser;
use crate::velostream::sql::StreamingQuery;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Helper for managing SQL-annotated metrics across different processor types
///
/// Provides:
/// - Condition parsing and evaluation (Phase 4)
/// - Metric registration from SQL annotations
/// - Metric emission for processed records
pub struct ProcessorMetricsHelper {
    /// Condition strings for conditional metric emission
    /// Key: metric name, Value: condition string
    metric_conditions: Arc<Mutex<HashMap<String, String>>>,
}

impl ProcessorMetricsHelper {
    /// Create a new metrics helper
    pub fn new() -> Self {
        Self {
            metric_conditions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Store condition string from annotation
    pub async fn compile_condition(
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
    pub async fn register_counter_metrics(
        &self,
        query: &StreamingQuery,
        observability: &Option<SharedObservabilityManager>,
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
        if let Some(obs) = observability {
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
    pub async fn register_gauge_metrics(
        &self,
        query: &StreamingQuery,
        observability: &Option<SharedObservabilityManager>,
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
        if let Some(obs) = observability {
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
    pub async fn register_histogram_metrics(
        &self,
        query: &StreamingQuery,
        observability: &Option<SharedObservabilityManager>,
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
        if let Some(obs) = observability {
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
}

impl Default for ProcessorMetricsHelper {
    fn default() -> Self {
        Self::new()
    }
}
