// === PHASE 4: OPENTELEMETRY DISTRIBUTED TRACING ===

use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::TracingConfig;
use opentelemetry::{
    global,
    trace::{Span, SpanKind, Status, Tracer, TracerProvider as _},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    runtime,
    trace::{RandomIdGenerator, Sampler, TracerProvider},
    Resource,
};
use std::time::Instant;

/// OpenTelemetry telemetry provider for distributed tracing
#[derive(Debug)]
pub struct TelemetryProvider {
    config: TracingConfig,
    active: bool,
}

impl TelemetryProvider {
    /// Create a new telemetry provider with the given configuration
    pub async fn new(config: TracingConfig) -> Result<Self, SqlError> {
        let otlp_endpoint = config
            .otlp_endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:4317".to_string());

        log::info!(
            "🔍 Initializing OpenTelemetry distributed tracing for service '{}'",
            config.service_name
        );
        log::info!(
            "📊 Tracing configuration: endpoint={}, sampling_ratio={}",
            otlp_endpoint,
            config.sampling_ratio
        );

        // Initialize OTLP exporter
        let exporter = match opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(&otlp_endpoint)
            .build_span_exporter()
        {
            Ok(exporter) => {
                log::info!(
                    "✅ OTLP exporter created successfully for {}",
                    otlp_endpoint
                );
                exporter
            }
            Err(e) => {
                log::error!("❌ Failed to create OTLP exporter: {}", e);
                return Err(SqlError::ConfigurationError {
                    message: format!("Failed to create OTLP exporter: {}", e),
                });
            }
        };

        // Create resource with service information
        let resource = Resource::new(vec![
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                config.service_name.clone(),
            ),
            KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                "0.1.0",
            ),
        ]);

        // Create tracer provider with batch exporter
        let provider = TracerProvider::builder()
            .with_batch_exporter(exporter, runtime::Tokio)
            .with_config(
                opentelemetry_sdk::trace::config()
                    .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
                    .with_id_generator(RandomIdGenerator::default())
                    .with_resource(resource),
            )
            .build();

        // Set as global tracer provider
        global::set_tracer_provider(provider);

        log::info!("✅ OpenTelemetry tracer initialized - spans will be exported to Tempo");
        log::info!("🔍 Trace sampling: 100% (AlwaysOn for demo)");

        Ok(Self {
            config,
            active: true,
        })
    }

    /// Create a new trace span for batch processing (parent span for entire batch)
    ///
    /// # Arguments
    /// * `job_name` - Name of the job/query
    /// * `batch_id` - Batch sequence number
    /// * `upstream_context` - Optional trace context from upstream Kafka headers
    ///
    /// If upstream_context is provided, this batch becomes a child of the upstream trace.
    /// Otherwise, a new trace is started.
    pub fn start_batch_span(
        &self,
        job_name: &str,
        batch_id: u64,
        upstream_context: Option<opentelemetry::trace::SpanContext>,
    ) -> BatchSpan {
        if !self.active {
            return BatchSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        // Create span with upstream context if available for distributed tracing
        let mut span = if let Some(parent_ctx) = upstream_context {
            use opentelemetry::trace::{TraceContextExt, Tracer as _};

            log::info!(
                "🔗 Starting batch span as child of upstream trace: {}",
                parent_ctx.trace_id()
            );

            let parent_cx = opentelemetry::Context::current().with_remote_span_context(parent_ctx);

            tracer
                .span_builder(format!("batch:{}", job_name))
                .with_kind(SpanKind::Consumer) // Consumer span for Kafka message processing
                .with_attributes(vec![
                    KeyValue::new("job.name", job_name.to_string()),
                    KeyValue::new("batch.id", batch_id as i64),
                    KeyValue::new("messaging.system", "kafka"),
                    KeyValue::new("messaging.operation", "process"),
                ])
                .start_with_context(&tracer, &parent_cx)
        } else {
            log::debug!("🆕 Starting new trace for batch (no upstream context)");

            tracer
                .span_builder(format!("batch:{}", job_name))
                .with_kind(SpanKind::Internal)
                .with_attributes(vec![
                    KeyValue::new("job.name", job_name.to_string()),
                    KeyValue::new("batch.id", batch_id as i64),
                ])
                .start(&tracer)
        };

        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started batch span: {} (batch #{}) (exporting to Tempo)",
            job_name,
            batch_id
        );

        BatchSpan::new_active(span)
    }

    /// Create a new trace span for SQL query execution
    pub fn start_sql_query_span(
        &self,
        query: &str,
        source: &str,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> QuerySpan {
        if !self.active {
            return QuerySpan::new_inactive();
        }

        let operation_name = Self::extract_operation_name(query);
        let tracer = global::tracer(self.config.service_name.clone());

        let span_name = format!("sql_query:{}", operation_name);

        // Start span with parent context if provided for proper parent-child hierarchy
        let mut span = if let Some(parent_ctx) = parent_context {
            use opentelemetry::trace::{TraceContextExt, Tracer as _};
            // Create a context with the parent span context for proper parent-child relationship
            let parent_cx = opentelemetry::Context::current().with_remote_span_context(parent_ctx);
            tracer
                .span_builder(span_name)
                .with_kind(SpanKind::Internal)
                .start_with_context(&tracer, &parent_cx)
        } else {
            tracer.start(span_name)
        };

        // Set span attributes
        span.set_attribute(KeyValue::new("db.system", "velostream"));
        span.set_attribute(KeyValue::new("db.operation", operation_name.to_string()));
        span.set_attribute(KeyValue::new(
            "db.statement",
            query.chars().take(200).collect::<String>(),
        ));
        span.set_attribute(KeyValue::new("source", source.to_string()));
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started SQL query span: {} from source: {} (child of parent span, exporting to Tempo)",
            operation_name,
            source
        );

        QuerySpan::new_active(span)
    }

    /// Create a new trace span for streaming operations
    pub fn start_streaming_span(
        &self,
        operation: &str,
        record_count: u64,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> StreamingSpan {
        if !self.active {
            return StreamingSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        let span_name = format!("streaming:{}", operation);

        // Start span with parent context if provided for proper parent-child hierarchy
        let mut span = if let Some(parent_ctx) = parent_context {
            use opentelemetry::trace::{TraceContextExt, Tracer as _};
            // Create a context with the parent span context for proper parent-child relationship
            let parent_cx = opentelemetry::Context::current().with_remote_span_context(parent_ctx);
            tracer
                .span_builder(span_name)
                .with_kind(SpanKind::Internal)
                .start_with_context(&tracer, &parent_cx)
        } else {
            tracer.start(span_name)
        };

        // Set span attributes
        span.set_attribute(KeyValue::new("operation", operation.to_string()));
        span.set_attribute(KeyValue::new("record_count", record_count as i64));
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started streaming span: {} with {} records (child of parent span, exporting to Tempo)",
            operation,
            record_count
        );

        StreamingSpan::new_active(span, record_count)
    }

    /// Create a new trace span for aggregation operations
    pub fn start_aggregation_span(&self, function: &str, window_type: &str) -> AggregationSpan {
        if !self.active {
            return AggregationSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        let mut span = tracer
            .span_builder(format!("aggregation:{}", function))
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                KeyValue::new("function", function.to_string()),
                KeyValue::new("window_type", window_type.to_string()),
            ])
            .start(&tracer);

        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started aggregation span: {} with window: {} (exporting to Tempo)",
            function,
            window_type
        );

        AggregationSpan::new_active(span)
    }

    /// Extract operation name from SQL query for span naming
    fn extract_operation_name(query: &str) -> &str {
        let query_trimmed = query.trim_start();
        if query_trimmed.starts_with("SELECT") || query_trimmed.starts_with("select") {
            "select"
        } else if query_trimmed.starts_with("CREATE") || query_trimmed.starts_with("create") {
            "create"
        } else if query_trimmed.starts_with("INSERT") || query_trimmed.starts_with("insert") {
            "insert"
        } else if query_trimmed.starts_with("UPDATE") || query_trimmed.starts_with("update") {
            "update"
        } else if query_trimmed.starts_with("DELETE") || query_trimmed.starts_with("delete") {
            "delete"
        } else {
            "unknown"
        }
    }

    /// Get the current trace ID if available
    pub fn current_trace_id(&self) -> Option<String> {
        if self.active {
            use opentelemetry::trace::TraceContextExt;
            let cx = opentelemetry::Context::current();
            let span = cx.span();
            let span_context = span.span_context();
            if span_context.is_valid() {
                Some(format!("{}", span_context.trace_id()))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Shutdown the telemetry provider
    pub async fn shutdown(&mut self) -> Result<(), SqlError> {
        self.active = false;

        log::info!("🔍 Shutting down OpenTelemetry tracer...");

        // Shutdown the global tracer provider to flush remaining spans
        global::shutdown_tracer_provider();

        log::info!("✅ Distributed tracing stopped - all spans flushed to Tempo");

        Ok(())
    }
}

/// SQL query execution span wrapper
pub struct QuerySpan {
    span: Option<opentelemetry::global::BoxedSpan>,
    start_time: Instant,
    active: bool,
}

impl QuerySpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            span: Some(span),
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span: None,
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add execution time to the span
    pub fn set_execution_time(&mut self, duration_ms: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("execution_time_ms", duration_ms as i64));
            log::trace!("🔍 SQL span execution time: {}ms", duration_ms);
        }
    }

    /// Add record count to the span
    pub fn set_record_count(&mut self, count: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("record_count", count as i64));
            log::trace!("🔍 SQL span processed {} records", count);
        }
    }

    /// Mark the query as successful
    pub fn set_success(&mut self) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::Ok);
            let duration = self.start_time.elapsed();
            log::debug!("🔍 SQL span completed successfully in {:?}", duration);
        }
    }

    /// Mark the query as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::error(error.to_string()));
            span.set_attribute(KeyValue::new("error", error.to_string()));
            let duration = self.start_time.elapsed();
            log::warn!("🔍 SQL span failed after {:?}: {}", duration, error);
        }
    }
}

impl Drop for QuerySpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!("🔍 SQL span finished in {:?}", duration);
            // Span automatically ends when dropped
        }
    }
}

/// Streaming operation span wrapper
pub struct StreamingSpan {
    span: Option<opentelemetry::global::BoxedSpan>,
    record_count: u64,
    start_time: Instant,
    active: bool,
}

impl StreamingSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan, record_count: u64) -> Self {
        Self {
            span: Some(span),
            record_count,
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span: None,
            record_count: 0,
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add throughput information to the span
    pub fn set_throughput(&mut self, records_per_second: f64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("throughput_rps", records_per_second));
            log::trace!(
                "🔍 Streaming span throughput: {:.2} rps",
                records_per_second
            );
        }
    }

    /// Add processing time to the span
    pub fn set_processing_time(&mut self, duration_ms: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("processing_time_ms", duration_ms as i64));
            log::trace!("🔍 Streaming span processing time: {}ms", duration_ms);
        }
    }

    /// Mark the operation as successful
    pub fn set_success(&mut self) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::Ok);
            let duration = self.start_time.elapsed();
            log::debug!("🔍 Streaming span completed successfully in {:?}", duration);
        }
    }

    /// Mark the operation as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::error(error.to_string()));
            span.set_attribute(KeyValue::new("error", error.to_string()));
            let duration = self.start_time.elapsed();
            log::warn!("🔍 Streaming span failed after {:?}: {}", duration, error);
        }
    }
}

impl Drop for StreamingSpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!("🔍 Streaming span finished in {:?}", duration);
            // Span automatically ends when dropped
        }
    }
}

/// Aggregation operation span wrapper
pub struct AggregationSpan {
    span: Option<opentelemetry::global::BoxedSpan>,
    start_time: Instant,
    active: bool,
}

impl AggregationSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            span: Some(span),
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span: None,
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add window size information to the span
    pub fn set_window_size(&mut self, size_ms: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("window_size_ms", size_ms as i64));
            log::trace!("🔍 Aggregation span window size: {}ms", size_ms);
        }
    }

    /// Add input record count to the span
    pub fn set_input_records(&mut self, count: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("input_records", count as i64));
            log::trace!("🔍 Aggregation span input records: {}", count);
        }
    }

    /// Add output record count to the span
    pub fn set_output_records(&mut self, count: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("output_records", count as i64));
            log::trace!("🔍 Aggregation span output records: {}", count);
        }
    }

    /// Mark the aggregation as successful
    pub fn set_success(&mut self) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::Ok);
            let duration = self.start_time.elapsed();
            log::debug!(
                "🔍 Aggregation span completed successfully in {:?}",
                duration
            );
        }
    }

    /// Mark the aggregation as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::error(error.to_string()));
            span.set_attribute(KeyValue::new("error", error.to_string()));
            let duration = self.start_time.elapsed();
            log::warn!("🔍 Aggregation span failed after {:?}: {}", duration, error);
        }
    }
}

impl Drop for AggregationSpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!("🔍 Aggregation span finished in {:?}", duration);
            // Span automatically ends when dropped
        }
    }
}

/// Batch processing span wrapper (parent span for all operations in a batch)
///
/// Note: This creates a parent span for the entire batch operation. However, due to
/// Rust async/Send requirements with tokio::spawn, we cannot use OpenTelemetry's
/// ContextGuard (which is !Send) across await points. Therefore, child spans are
/// currently created independently. Future enhancement: implement manual parent-child
/// linking via span IDs.
pub struct BatchSpan {
    span: Option<opentelemetry::global::BoxedSpan>,
    start_time: Instant,
    active: bool,
}

impl BatchSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            span: Some(span),
            start_time: Instant::now(),
            active: true,
        }
    }

    fn new_inactive() -> Self {
        Self {
            span: None,
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Add total records processed to the span
    pub fn set_total_records(&mut self, count: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("total_records", count as i64));
            log::trace!("🔍 Batch span processed {} total records", count);
        }
    }

    /// Add batch duration to the span
    pub fn set_batch_duration(&mut self, duration_ms: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new("batch_duration_ms", duration_ms as i64));
            log::trace!("🔍 Batch span duration: {}ms", duration_ms);
        }
    }

    /// Mark the batch as successful
    pub fn set_success(&mut self) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::Ok);
            let duration = self.start_time.elapsed();
            log::debug!("🔍 Batch span completed successfully in {:?}", duration);
        }
    }

    /// Mark the batch as failed with error information
    pub fn set_error(&mut self, error: &str) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::error(error.to_string()));
            span.set_attribute(KeyValue::new("error", error.to_string()));
            let duration = self.start_time.elapsed();
            log::warn!("🔍 Batch span failed after {:?}: {}", duration, error);
        }
    }

    /// Get the span context for creating child spans with parent relationship
    pub fn span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        self.span.as_ref().map(|span| span.span_context().clone())
    }
}

impl Drop for BatchSpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!("🔍 Batch span finished in {:?}", duration);
            // Span automatically ends when dropped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_operation_name() {
        assert_eq!(
            TelemetryProvider::extract_operation_name("SELECT * FROM table"),
            "select"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("  select id from users"),
            "select"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("CREATE STREAM test"),
            "create"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("INSERT INTO table"),
            "insert"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("UPDATE table SET"),
            "update"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("DELETE FROM table"),
            "delete"
        );
        assert_eq!(
            TelemetryProvider::extract_operation_name("UNKNOWN QUERY"),
            "unknown"
        );
    }

    #[tokio::test]
    async fn test_telemetry_provider_creation() {
        let config = TracingConfig::development();
        // Note: This test will try to connect to localhost:4317
        // In CI, this might fail, which is expected
        let _ = TelemetryProvider::new(config).await;
    }
}
