//! Observability helper for job processors
//!
//! This module centralizes telemetry (tracing) and metrics recording logic
//! to eliminate duplication across SimpleJobProcessor and TransactionalJobProcessor.

use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::observability::query_metadata::QuerySpanMetadata;
use crate::velostream::observability::telemetry::{BatchSpan, PipelineStageSpan};
use crate::velostream::observability::trace_propagation;
use crate::velostream::server::processors::common::BatchProcessingResultWithOutput;
use crate::velostream::server::processors::observability_utils::{
    calculate_throughput, with_observability_try_lock,
};
use crate::velostream::sql::execution::StreamRecord;
use crate::velostream::sql::execution::types::FieldValue;
use log::{debug, warn};
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
            // Retry try_read() with brief sleeps as a defensive measure against
            // transient lock contention (e.g., during initialization or shutdown).
            let obs_lock = {
                let mut lock = None;
                for attempt in 0..5 {
                    match obs.try_read() {
                        Ok(l) => {
                            lock = Some(l);
                            break;
                        }
                        Err(_) => {
                            if attempt < 4 {
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                        }
                    }
                }
                match lock {
                    Some(l) => l,
                    None => {
                        warn!(
                            "Job '{}': Could not acquire observability read lock for batch span after 5 attempts, skipping trace for batch #{}",
                            job_name, batch_number
                        );
                        return None;
                    }
                }
            };
            if let Some(telemetry) = obs_lock.telemetry() {
                // Extract trace context from first record's Kafka headers
                let upstream_context = batch_records.first().and_then(|record| {
                    let ctx = trace_propagation::extract_trace_context(&record.headers);
                    if ctx.is_some() {
                        debug!(
                            "Job '{}': 🔗 Extracted upstream trace context from Kafka headers",
                            job_name
                        );
                    } else {
                        debug!(
                            "Job '{}': 🆕 No upstream trace context - starting new trace",
                            job_name
                        );
                    }
                    ctx
                });

                return Some(telemetry.start_batch_span(job_name, batch_number, upstream_context));
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
        output_records: &mut [std::sync::Arc<StreamRecord>],
        job_name: &str,
    ) {
        if let Some(span) = batch_span {
            if let Some(span_ctx) = span.span_context() {
                if span_ctx.is_valid() {
                    debug!(
                        "Job '{}': Injecting trace context into {} output records",
                        job_name,
                        output_records.len()
                    );

                    // Pre-compute trace headers once per batch to avoid
                    // redundant format!/to_string() allocations per record
                    let precomputed = trace_propagation::precompute_trace_headers(&span_ctx);

                    // Use Arc::make_mut for copy-on-write mutation
                    // Clones only if refcount > 1 (shared ownership)
                    for record_arc in output_records.iter_mut() {
                        let record = std::sync::Arc::make_mut(record_arc);
                        trace_propagation::inject_precomputed_trace_context(
                            &precomputed,
                            &mut record.headers,
                        );
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

    /// Enrich a batch span with pre-computed query metadata
    pub fn enrich_batch_span_with_query_metadata(
        batch_span: &mut Option<BatchSpan>,
        metadata: &QuerySpanMetadata,
    ) {
        if let Some(span) = batch_span.as_mut() {
            span.set_query_metadata(metadata);
        }
    }

    /// Enrich a batch span with Kafka record metadata from the first record in the batch
    pub fn enrich_batch_span_with_record_metadata(
        batch_span: &mut Option<BatchSpan>,
        batch_records: &[StreamRecord],
    ) {
        if let Some(span) = batch_span.as_mut() {
            if let Some(first) = batch_records.first() {
                if let Some(FieldValue::String(topic)) = &first.topic {
                    span.set_input_topic(topic);
                }
                span.set_input_partition(first.partition);
                if let Some(FieldValue::String(key)) = &first.key {
                    span.set_message_key(key);
                }
            }
        }
    }

    /// Record deserialization telemetry and metrics
    ///
    /// # Arguments
    /// * `observability` - Observability manager
    /// * `job_name` - Name of the job/query
    /// * `batch_span` - Current batch span (parent span for child operations)
    /// * `record_count` - Number of records deserialized
    /// * `duration_ms` - Duration of deserialization in milliseconds
    /// * `metadata` - Optional Kafka metadata (topic, partition, offset)
    pub fn record_deserialization(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        record_count: usize,
        duration_ms: u64,
        metadata: Option<(&str, i32, i64)>, // (topic, partition, offset)
    ) {
        if observability.is_some() {
            debug!(
                "Job '{}': Recording deserialization metrics (batch_size={}, duration={}ms)",
                job_name, record_count, duration_ms
            );

            with_observability_try_lock(observability, |obs_lock| {
                // Record telemetry span with Kafka metadata enrichment
                if let Some(telemetry) = obs_lock.telemetry() {
                    let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                    let mut span = telemetry.start_streaming_span(
                        job_name,
                        "deserialization",
                        record_count as u64,
                        parent_ctx,
                    );
                    span.set_processing_time(duration_ms);

                    // Attach Kafka metadata to the span if available
                    if let Some((topic, partition, offset)) = metadata {
                        span.set_kafka_metadata(topic, partition, offset);
                    }

                    span.set_success();
                    debug!(
                        "Job '{}': Telemetry span recorded for deserialization",
                        job_name
                    );
                }

                // Record Prometheus metrics
                if let Some(metrics) = obs_lock.metrics() {
                    let throughput = calculate_throughput(record_count, duration_ms);

                    // Phase 2.1: Global streaming operation metrics
                    metrics.record_streaming_operation(
                        "deserialization",
                        std::time::Duration::from_millis(duration_ms),
                        record_count as u64,
                        throughput,
                    );

                    // Phase 2.2: Job-aware profiling phase metrics
                    metrics.record_profiling_phase(
                        job_name,
                        "deserialization",
                        std::time::Duration::from_millis(duration_ms),
                        record_count as u64,
                        throughput,
                    );

                    debug!(
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
        query_metadata: Option<&QuerySpanMetadata>,
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            // Record telemetry span
            if let Some(telemetry) = obs_lock.telemetry() {
                let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                let mut span = telemetry.start_sql_query_span(
                    job_name,
                    "sql_processing",
                    "stream_processor",
                    parent_ctx,
                );
                if let Some(metadata) = query_metadata {
                    span.set_query_metadata(metadata);
                }
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

                // Phase 2.1: Global SQL query metrics
                metrics.record_sql_query(
                    "stream_processing",
                    std::time::Duration::from_millis(duration_ms),
                    success,
                    batch_result.records_processed as u64,
                );

                // Phase 2.3: Job-aware pipeline operation metrics
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

    /// Record serialization success telemetry and metrics
    pub fn record_serialization_success(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        record_count: usize,
        duration_ms: u64,
        metadata: Option<(&str, i32, i64)>, // (topic, partition, offset)
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            // Record telemetry span with Kafka metadata enrichment
            if let Some(telemetry) = obs_lock.telemetry() {
                let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                let mut span = telemetry.start_streaming_span(
                    job_name,
                    "serialization",
                    record_count as u64,
                    parent_ctx,
                );
                span.set_processing_time(duration_ms);

                // Attach Kafka metadata to the span if available
                if let Some((topic, partition, offset)) = metadata {
                    span.set_kafka_metadata(topic, partition, offset);
                }

                span.set_success();
            }

            // Record Prometheus metrics
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);

                // Phase 2.1: Global streaming operation metrics
                metrics.record_streaming_operation(
                    "serialization",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );

                // Phase 2.2: Job-aware profiling phase metrics
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

    /// Record serialization failure telemetry and metrics
    pub fn record_serialization_failure(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_span: &Option<BatchSpan>,
        record_count: usize,
        duration_ms: u64,
        error: &str,
        metadata: Option<(&str, i32, i64)>, // (topic, partition, offset)
    ) {
        with_observability_try_lock(observability, |obs_lock| {
            // Record telemetry span with Kafka metadata enrichment
            if let Some(telemetry) = obs_lock.telemetry() {
                let parent_ctx = batch_span.as_ref().and_then(|s| s.span_context());
                let mut span = telemetry.start_streaming_span(
                    job_name,
                    "serialization",
                    record_count as u64,
                    parent_ctx,
                );
                span.set_processing_time(duration_ms);

                // Attach Kafka metadata to the span if available
                if let Some((topic, partition, offset)) = metadata {
                    span.set_kafka_metadata(topic, partition, offset);
                }

                span.set_error(error);
            }

            // Record Prometheus metrics
            if let Some(metrics) = obs_lock.metrics() {
                let throughput = calculate_throughput(record_count, duration_ms);

                // Phase 2.1: Global streaming operation metrics
                metrics.record_streaming_operation(
                    "serialization_failed",
                    std::time::Duration::from_millis(duration_ms),
                    record_count as u64,
                    throughput,
                );

                // Phase 2.2: Job-aware profiling phase metrics (failure case)
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

    /// Complete a batch span with success and record Phase 2.4 job throughput
    pub fn complete_batch_span_success(
        batch_span: &mut Option<BatchSpan>,
        batch_start: &Instant,
        records_processed: u64,
    ) {
        if let Some(span) = batch_span {
            let batch_duration = batch_start.elapsed().as_millis() as u64;
            span.set_total_records(records_processed);
            span.set_batch_duration(batch_duration);
            span.set_success();
        }
    }

    /// Record job-specific throughput metrics (Phase 2.4)
    ///
    /// # Arguments
    /// * `observability` - Observability manager
    /// * `job_name` - Name of the job/query
    /// * `batch_duration_ms` - Duration of the batch in milliseconds
    /// * `records_processed` - Number of records processed in the batch
    pub fn record_batch_throughput(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        batch_duration_ms: u64,
        records_processed: u64,
    ) {
        if observability.is_some() {
            with_observability_try_lock(observability, |obs_lock| {
                if let Some(metrics) = obs_lock.metrics() {
                    let throughput_rps =
                        calculate_throughput(records_processed as usize, batch_duration_ms);

                    // Phase 2.4: Job-specific throughput gauge
                    metrics.record_throughput_by_job(job_name, throughput_rps);

                    debug!(
                        "Job '{}': Batch throughput recorded: {:.2} rec/s",
                        job_name, throughput_rps
                    );
                }
                None::<()>
            });
        }
    }

    /// Complete a batch span with error
    pub fn complete_batch_span_error(
        batch_span: &mut Option<BatchSpan>,
        batch_start: &Instant,
        records_processed: u64,
        records_failed: usize,
    ) {
        if let Some(span) = batch_span {
            let batch_duration = batch_start.elapsed().as_millis() as u64;
            span.set_total_records(records_processed);
            span.set_batch_duration(batch_duration);
            span.set_error(&format!("{} records failed", records_failed));
        }
    }

    /// Enrich batch span with OpenTelemetry messaging semantic conventions
    ///
    /// Sets standard messaging.* attributes for Kafka processing spans to ensure
    /// compatibility with standard OTel dashboards and trace analysis tools.
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `topic` - Kafka topic name
    /// * `partition` - Kafka partition number
    /// * `offset` - Kafka message offset
    /// * `consumer_group` - Optional consumer group ID
    /// * `message_key` - Optional Kafka message key
    pub fn set_messaging_attributes(
        batch_span: &mut Option<BatchSpan>,
        topic: &str,
        partition: i32,
        offset: i64,
        consumer_group: Option<&str>,
        message_key: Option<&str>,
    ) {
        if let Some(span) = batch_span {
            span.set_messaging_attributes(topic, partition, offset, consumer_group, message_key);
        }
    }

    /// Enrich batch span with application-specific attributes
    ///
    /// Sets velostream.* namespace attributes for filtering and grouping traces
    /// by job name, application, version, and processing mode.
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `job_name` - Name of the streaming job/query
    /// * `app_name` - Optional application name (from @app annotation)
    /// * `app_version` - Optional application version (from @version annotation)
    /// * `job_mode` - Optional job mode (e.g., "simple", "adaptive", "transactional")
    pub fn set_application_attributes(
        batch_span: &mut Option<BatchSpan>,
        job_name: &str,
        app_name: Option<&str>,
        app_version: Option<&str>,
        job_mode: Option<&str>,
    ) {
        if let Some(span) = batch_span {
            span.set_application_attributes(job_name, app_name, app_version, job_mode);
        }
    }

    /// Enrich batch span with performance timing breakdown
    ///
    /// Sets timing attributes for each stage of the processing pipeline to identify
    /// performance bottlenecks (e.g., is it deserialization? execution? serialization?).
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `deserialization_ms` - Time spent deserializing input records
    /// * `execution_ms` - Time spent executing SQL query
    /// * `serialization_ms` - Time spent serializing output records
    pub fn set_performance_timings(
        batch_span: &mut Option<BatchSpan>,
        deserialization_ms: u64,
        execution_ms: u64,
        serialization_ms: u64,
    ) {
        if let Some(span) = batch_span {
            span.set_performance_timings(deserialization_ms, execution_ms, serialization_ms);
        }
    }

    /// Enrich batch span with throughput metrics
    ///
    /// Sets throughput attributes for monitoring processing rate and identifying
    /// performance degradation.
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `records_per_second` - Processing rate in records/second
    /// * `bytes_per_second` - Optional bytes processed per second
    pub fn set_throughput_metrics(
        batch_span: &mut Option<BatchSpan>,
        records_per_second: f64,
        bytes_per_second: Option<f64>,
    ) {
        if let Some(span) = batch_span {
            span.set_throughput_metrics(records_per_second, bytes_per_second);
        }
    }

    /// Enrich batch span with error rate metrics
    ///
    /// Sets error count and percentage attributes for monitoring data quality
    /// and processing reliability.
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `errors` - Number of records that failed processing
    /// * `total` - Total number of records processed
    pub fn set_error_rate(batch_span: &mut Option<BatchSpan>, errors: u64, total: u64) {
        if let Some(span) = batch_span {
            span.set_error_rate(errors, total);
        }
    }

    /// Enrich batch span with window-specific metrics (for windowed queries)
    ///
    /// Sets window timing and record count attributes for analyzing windowing
    /// performance and late data handling.
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `window_start_ms` - Window start timestamp (epoch milliseconds)
    /// * `window_end_ms` - Window end timestamp (epoch milliseconds)
    /// * `records_in_window` - Number of records in the window
    /// * `windows_emitted` - Number of windows emitted in this batch
    pub fn set_window_metrics(
        batch_span: &mut Option<BatchSpan>,
        window_start_ms: i64,
        window_end_ms: i64,
        records_in_window: u64,
        windows_emitted: u64,
    ) {
        if let Some(span) = batch_span {
            span.set_window_metrics(
                window_start_ms,
                window_end_ms,
                records_in_window,
                windows_emitted,
            );
        }
    }

    /// Enrich batch span with join-specific metrics
    ///
    /// Sets join performance and matching statistics for analyzing join
    /// effectiveness and cache hit rates.
    ///
    /// # Arguments
    /// * `batch_span` - Batch span to enrich
    /// * `left_records` - Number of records from left source
    /// * `right_records` - Number of records from right source
    /// * `joined_records` - Number of successfully joined records
    /// * `join_hit_rate` - Percentage of successful joins (0.0-1.0)
    pub fn set_join_metrics(
        batch_span: &mut Option<BatchSpan>,
        left_records: u64,
        right_records: u64,
        joined_records: u64,
        join_hit_rate: f64,
    ) {
        if let Some(span) = batch_span {
            span.set_join_metrics(left_records, right_records, joined_records, join_hit_rate);
        }
    }
}
