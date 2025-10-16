//! Observability helper for job processors
//!
//! This module centralizes telemetry (tracing) and metrics recording logic
//! to eliminate duplication across SimpleJobProcessor and TransactionalJobProcessor.

use crate::velostream::observability::telemetry::BatchSpan;
use crate::velostream::observability::trace_propagation;
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::common::BatchProcessingResultWithOutput;
use crate::velostream::server::processors::observability_utils::{
    calculate_throughput, with_observability_try_lock,
};
use crate::velostream::sql::execution::StreamRecord;
use log::{info, warn};
use std::time::Instant;

/// Helper for recording batch processing observability data
pub struct ObservabilityHelper;

impl ObservabilityHelper {
    /// Create a batch span to track overall batch processing
    ///
    /// Extracts trace context from incoming Kafka message headers for distributed tracing.
    /// If upstream trace context is found, the batch span becomes a child of the upstream trace.
    ///
    /// # Arguments
    /// * `observability` - Observability manager
    /// * `job_name` - Name of the job/query
    /// * `batch_number` - Batch sequence number
    /// * `batch_records` - Input records (used to extract upstream trace context)
    ///
    /// # Returns
    /// - `Some(BatchSpan)` if tracing is enabled
    /// - `None` if no observability manager or tracing disabled
    pub fn start_batch_span(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_number: u64,
        batch_records: &[StreamRecord],
    ) -> Option<BatchSpan> {
        if let Some(obs) = observability {
            if let Ok(obs_lock) = obs.try_read() {
                if let Some(telemetry) = obs_lock.telemetry() {
                    // Extract trace context from first record's Kafka headers
                    let upstream_context = batch_records.first().and_then(|record| {
                        let ctx = trace_propagation::extract_trace_context(&record.headers);
                        if ctx.is_some() {
                            info!(
                                "Job '{}': 🔗 Extracted upstream trace context from Kafka headers",
                                job_name
                            );
                        } else {
                            info!(
                                "Job '{}': 🆕 No upstream trace context - starting new trace",
                                job_name
                            );
                        }
                        ctx
                    });

                    return Some(telemetry.start_batch_span(
                        job_name,
                        batch_number,
                        upstream_context,
                    ));
                }
            }
        }
        None
    }

    /// Inject trace context into output records for downstream distributed tracing
    ///
    /// Propagates the current batch span context into Kafka message headers
    /// so downstream consumers can link their traces.
    ///
    /// # Arguments
    /// * `batch_span` - Current batch span (contains trace context)
    /// * `output_records` - Output records to inject headers into
    /// * `job_name` - Name of the job (for logging)
    pub fn inject_trace_context_into_records(
        batch_span: &Option<BatchSpan>,
        output_records: &mut [StreamRecord],
        job_name: &str,
    ) {
        if let Some(span) = batch_span {
            if let Some(span_ctx) = span.span_context() {
                if span_ctx.is_valid() {
                    info!(
                        "Job '{}': 📤 Injecting trace context into {} output records",
                        job_name,
                        output_records.len()
                    );

                    for record in output_records.iter_mut() {
                        trace_propagation::inject_trace_context(&span_ctx, &mut record.headers);
                    }
                } else {
                    warn!(
                        "Job '{}': ⚠️  Batch span context is invalid, skipping trace injection",
                        job_name
                    );
                }
            }
        }
    }

    /// Record deserialization telemetry and metrics
    pub fn record_deserialization(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        record_count: usize,
        duration_ms: u64,
    ) {
        if let Some(obs) = observability {
            info!(
                "Job '{}': Recording deserialization metrics (batch_size={}, duration={}ms)",
                job_name, record_count, duration_ms
            );

            with_observability_try_lock(observability, |obs_lock| {
                // Record telemetry span
                if let Some(telemetry) = obs_lock.telemetry() {
                    let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                    let mut span = telemetry.start_streaming_span(
                        "deserialization",
                        record_count as u64,
                        parent_ctx,
                    );
                    span.set_processing_time(duration_ms);
                    span.set_success();
                    info!(
                        "Job '{}': Telemetry span recorded for deserialization",
                        job_name
                    );
                }

                // Record Prometheus metrics
                if let Some(metrics) = obs_lock.metrics() {
                    let throughput = calculate_throughput(record_count, duration_ms);
                    metrics.record_streaming_operation(
                        "deserialization",
                        std::time::Duration::from_millis(duration_ms),
                        record_count as u64,
                        throughput,
                    );
                    info!(
                        "Job '{}': Metrics recorded for deserialization (throughput={:.2} rec/s)",
                        job_name, throughput
                    );
                } else {
                    warn!(
                        "Job '{}': Observability manager present but NO metrics provider",
                        job_name
                    );
                }
                None::<()>
            });
        }
    }

    /// Record SQL processing telemetry and metrics
    pub fn record_sql_processing(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        batch_result: &BatchProcessingResultWithOutput,
        duration_ms: u64,
    ) {
        if let Some(obs) = observability {
            if let Ok(obs_lock) = obs.try_read() {
                // Record telemetry span
                if let Some(telemetry) = obs_lock.telemetry() {
                    let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                    let mut span =
                        telemetry.start_sql_query_span("sql_processing", job_name, parent_ctx);
                    span.set_execution_time(duration_ms);
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
                        std::time::Duration::from_millis(duration_ms),
                        success,
                        batch_result.records_processed as u64,
                    );
                }
            }
        }
    }

    /// Record serialization success telemetry and metrics
    pub fn record_serialization_success(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        record_count: usize,
        duration_ms: u64,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            // Record telemetry span
            if let Some(telemetry) = obs_lock.telemetry() {
                let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                let mut span = telemetry.start_streaming_span(
                    "serialization",
                    record_count as u64,
                    parent_ctx,
                );
                span.set_processing_time(duration_ms);
                span.set_success();
            }

            // Record Prometheus metrics
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);
                metrics.record_streaming_operation(
                    "serialization",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );
            }
            None::<()>
        });
    }

    /// Record serialization failure telemetry and metrics
    pub fn record_serialization_failure(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        record_count: usize,
        duration_ms: u64,
        error: &str,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            // Record telemetry span
            if let Some(telemetry) = obs_lock.telemetry() {
                let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                let mut span = telemetry.start_streaming_span(
                    "serialization",
                    record_count as u64,
                    parent_ctx,
                );
                span.set_processing_time(duration_ms);
                span.set_error(error);
            }

            // Record Prometheus metrics
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);
                metrics.record_streaming_operation(
                    "serialization_failed",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );
            }
            None::<()>
        });
    }

    /// Complete a batch span with success
    pub fn complete_batch_span_success(
        batch_span: &mut Option<BatchSpan>,
        batch_start: &Instant,
        records_processed: u64,
    ) {
        if let Some(ref mut span) = batch_span {
            let batch_duration = batch_start.elapsed().as_millis() as u64;
            span.set_total_records(records_processed);
            span.set_batch_duration(batch_duration);
            span.set_success();
        }
    }

    /// Complete a batch span with error
    pub fn complete_batch_span_error(
        batch_span: &mut Option<BatchSpan>,
        batch_start: &Instant,
        records_processed: u64,
        records_failed: usize,
    ) {
        if let Some(ref mut span) = batch_span {
            let batch_duration = batch_start.elapsed().as_millis() as u64;
            span.set_total_records(records_processed);
            span.set_batch_duration(batch_duration);
            span.set_error(&format!("{} records failed", records_failed));
        }
    }
}
