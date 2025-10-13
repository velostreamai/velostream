//! Observability helper for job processors
//!
//! This module centralizes telemetry (tracing) and metrics recording logic
//! to eliminate duplication across SimpleJobProcessor and TransactionalJobProcessor.

use crate::velostream::observability::telemetry::BatchSpan;
use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::common::BatchProcessingResultWithOutput;
use log::{info, warn};
use std::time::Instant;

/// Helper for recording batch processing observability data
pub struct ObservabilityHelper;

impl ObservabilityHelper {
    /// Create a batch span to track overall batch processing
    ///
    /// # Returns
    /// - `Some(BatchSpan)` if tracing is enabled
    /// - `None` if no observability manager or tracing disabled
    pub fn start_batch_span(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_number: u64,
    ) -> Option<BatchSpan> {
        if let Some(obs) = observability {
            if let Ok(obs_lock) = obs.try_read() {
                if let Some(telemetry) = obs_lock.telemetry() {
                    return Some(telemetry.start_batch_span(job_name, batch_number));
                }
            }
        }
        None
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

            if let Ok(obs_lock) = obs.try_read() {
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
                    let throughput = if duration_ms > 0 {
                        (record_count as f64 / duration_ms as f64) * 1000.0
                    } else {
                        0.0
                    };
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
            } else {
                warn!(
                    "Job '{}': Observability manager present but could not acquire read lock",
                    job_name
                );
            }
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
        if let Some(obs) = observability {
            if let Ok(obs_lock) = obs.try_read() {
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
                    let throughput = if duration_ms > 0 {
                        (record_count as f64 / duration_ms as f64) * 1000.0
                    } else {
                        0.0
                    };
                    metrics.record_streaming_operation(
                        "serialization",
                        std::time::Duration::from_millis(duration_ms),
                        record_count as u64,
                        throughput,
                    );
                }
            }
        }
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
        if let Some(obs) = observability {
            if let Ok(obs_lock) = obs.try_read() {
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
                    let throughput = if duration_ms > 0 {
                        (record_count as f64 / duration_ms as f64) * 1000.0
                    } else {
                        0.0
                    };
                    metrics.record_streaming_operation(
                        "serialization_failed",
                        std::time::Duration::from_millis(duration_ms),
                        record_count as u64,
                        throughput,
                    );
                }
            }
        }
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
