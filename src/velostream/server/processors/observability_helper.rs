//! Observability helper for job processors
//!
//! This module centralizes telemetry (tracing) and metrics recording logic
//! to eliminate duplication across SimpleJobProcessor and TransactionalJobProcessor.
//!
//! Uses per-record head-based sampling instead of batch-level spans:
//! - Sampling decisions propagate via W3C traceparent flags
//! - Non-sampled records have zero tracing overhead
//! - Prometheus metrics provide operational visibility regardless of sampling

use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::observability::telemetry::RecordSpan;
use crate::velostream::observability::trace_propagation;
use crate::velostream::server::processors::common::BatchProcessingResultWithOutput;
use crate::velostream::server::processors::observability_utils::{
    calculate_throughput, with_observability_try_lock,
};
use crate::velostream::sql::execution::StreamRecord;
use log::debug;
use std::sync::Arc;

/// Helper for recording batch processing observability data
pub struct ObservabilityHelper;

impl ObservabilityHelper {
    /// Get the configured sampling ratio from the observability manager.
    ///
    /// Returns 0.0 if no observability manager is configured (no records sampled).
    pub fn sampling_ratio(observability: &Option<SharedObservabilityManager>) -> f64 {
        with_observability_try_lock(observability, |obs_lock| Some(obs_lock.sampling_ratio()))
            .unwrap_or(0.0)
    }

    /// Get the effective sampling ratio, reduced under queue pressure.
    ///
    /// When the span export queue exceeds 80% capacity, the sampling ratio is
    /// linearly reduced (floored at 5% of base) to prevent span drops.
    /// This provides adaptive backpressure without losing all observability.
    pub fn effective_sampling_ratio(observability: &Option<SharedObservabilityManager>) -> f64 {
        with_observability_try_lock(observability, |obs_lock| {
            let base = obs_lock.sampling_ratio();
            let pressure = obs_lock.queue_pressure();
            if pressure > 0.8 {
                // Linearly reduce when queue >80% full; floor at 5% of base
                let reduced = base * (1.0 - pressure).max(0.05);
                log::debug!(
                    "Adaptive sampling: pressure={:.2}, base={:.2}, effective={:.4}",
                    pressure,
                    base,
                    reduced
                );
                Some(reduced)
            } else {
                Some(base)
            }
        })
        .unwrap_or(0.0)
    }

    /// Create a per-record processing span for a sampled record.
    ///
    /// Only call this for records that passed the sampling decision.
    /// Returns `None` if telemetry is not enabled.
    pub fn start_record_span(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        upstream_context: Option<&opentelemetry::trace::SpanContext>,
    ) -> Option<RecordSpan> {
        with_observability_try_lock(observability, |obs_lock| {
            let telemetry = obs_lock.telemetry()?;
            Some(telemetry.start_record_span(job_name, upstream_context))
        })
    }

    /// Inject the sampled record span's trace context into a single output record.
    ///
    /// This propagates the trace chain to downstream consumers via W3C traceparent headers.
    pub fn inject_record_trace_context(
        record_span: &RecordSpan,
        output_record: &mut Arc<StreamRecord>,
    ) {
        match record_span.span_context() {
            Some(span_ctx) if span_ctx.is_valid() => {
                let record = Arc::make_mut(output_record);
                trace_propagation::inject_trace_context(&span_ctx, &mut record.headers);
            }
            Some(_) => {
                log::warn!(
                    "RecordSpan has invalid span context — output record will not carry traceparent. \
                     This may break the trace chain for downstream consumers."
                );
            }
            None => {
                // No span context available (telemetry inactive) — expected, no action needed.
            }
        }
    }

    /// Inject a "not sampled" traceparent into a single output record.
    ///
    /// This tells downstream consumers that the sampling decision was already made
    /// (flag=00), preventing them from re-rolling the dice. Creates a random trace-id
    /// for log correlation even though no spans are created.
    pub fn inject_not_sampled_context(output_record: &mut Arc<StreamRecord>) {
        let record = Arc::make_mut(output_record);
        trace_propagation::inject_not_sampled_context(&mut record.headers);
    }

    /// Record deserialization metrics (Prometheus only, no tracing spans).
    pub fn record_deserialization(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        record_count: usize,
        duration_ms: u64,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);

                metrics.record_streaming_operation(
                    "deserialization",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );

                metrics.record_profiling_phase(
                    job_name,
                    "deserialization",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );
            }
            None::<()>
        });
    }

    /// Record SQL processing metrics (Prometheus only, no tracing spans).
    pub fn record_sql_processing(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_result: &BatchProcessingResultWithOutput,
        duration_ms: u64,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            if let Some(metrics) = obs_lock.metrics() {
                let success = batch_result.records_failed == 0;

                metrics.record_sql_query(
                    "stream_processing",
                    std::time::Duration::from_millis(duration_ms),
                    success,
                    batch_result.records_processed as u64,
                );

                metrics.record_pipeline_operation(
                    job_name,
                    "sql_processing",
                    std::time::Duration::from_millis(duration_ms),
                    batch_result.records_processed as u64,
                );
            }
            None::<()>
        });
    }

    /// Record serialization success metrics (Prometheus only, no tracing spans).
    pub fn record_serialization_success(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        record_count: usize,
        duration_ms: u64,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);

                metrics.record_streaming_operation(
                    "serialization",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );

                metrics.record_profiling_phase(
                    job_name,
                    "serialization",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );
            }
            None::<()>
        });
    }

    /// Record serialization failure metrics (Prometheus only, no tracing spans).
    pub fn record_serialization_failure(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        record_count: usize,
        duration_ms: u64,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);

                metrics.record_streaming_operation(
                    "serialization_failed",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );

                metrics.record_profiling_phase(
                    job_name,
                    "serialization_failed",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );
            }
            None::<()>
        });
    }
}
