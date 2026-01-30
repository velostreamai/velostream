// === PHASE 4: OPENTELEMETRY DISTRIBUTED TRACING ===

use crate::velostream::observability::span_collector::CollectingSpanProcessor;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::config::TracingConfig;
use opentelemetry::{
    KeyValue, global,
    trace::{Span, SpanKind, Status, Tracer, TracerProvider as _},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    export::trace::SpanData,
    runtime,
    trace::{RandomIdGenerator, Sampler, SpanProcessor, TracerProvider},
};
use std::sync::Arc;
use std::time::Instant;

/// Base span wrapper with common timing and status functionality
///
/// This struct consolidates the common logic shared by all span types:
/// - Automatic timing via `Instant::now()`
/// - Unified success/error status handling
/// - Active/inactive span management
/// - Drop handler for automatic logging
pub struct BaseSpan {
    span: Option<opentelemetry::global::BoxedSpan>,
    start_time: Instant,
    active: bool,
}

impl BaseSpan {
    /// Create a new active span with the provided span handle
    pub(crate) fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            span: Some(span),
            start_time: Instant::now(),
            active: true,
        }
    }

    /// Create a new inactive span (used when telemetry is disabled)
    pub(crate) fn new_inactive() -> Self {
        Self {
            span: None,
            start_time: Instant::now(),
            active: false,
        }
    }

    /// Get immutable reference to the underlying span (if active)
    pub(crate) fn span(&self) -> Option<&opentelemetry::global::BoxedSpan> {
        self.span.as_ref()
    }

    /// Get mutable reference to the underlying span (if active)
    pub(crate) fn span_mut(&mut self) -> Option<&mut opentelemetry::global::BoxedSpan> {
        self.span.as_mut()
    }

    /// Check if this span is active
    pub(crate) fn is_active(&self) -> bool {
        self.active
    }

    /// Get elapsed time since span creation
    pub(crate) fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Mark the span as successful
    pub(crate) fn set_success(&mut self) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::Ok);
            let duration = self.start_time.elapsed();
            log::debug!("🔍 Span completed successfully in {:?}", duration);
        }
    }

    /// Mark the span as failed with error information
    pub(crate) fn set_error(&mut self, error: &str) {
        if let Some(span) = &mut self.span {
            span.set_status(Status::error(error.to_string()));
            span.set_attribute(KeyValue::new("error", error.to_string()));
            let duration = self.start_time.elapsed();
            log::warn!("🔍 Span failed after {:?}: {}", duration, error);
        }
    }
}

impl Drop for BaseSpan {
    fn drop(&mut self) {
        if self.active {
            let duration = self.start_time.elapsed();
            log::trace!("🔍 Span finished in {:?}", duration);
            // Span automatically ends when dropped
        }
    }
}

/// OpenTelemetry telemetry provider for distributed tracing
#[derive(Clone, Debug)]
pub struct TelemetryProvider {
    config: TracingConfig,
    active: bool,
    deployment_node_id: Option<String>,
    deployment_node_name: Option<String>,
    deployment_region: Option<String>,
    /// Span collector for testing (when no OTLP endpoint is configured)
    span_collector: Option<CollectingSpanProcessor>,
}

impl TelemetryProvider {
    /// Create a new telemetry provider with the given configuration
    pub async fn new(config: TracingConfig) -> Result<Self, SqlError> {
        // If no OTLP endpoint is specified, use a no-op mode (useful for testing)
        let otlp_endpoint = config.otlp_endpoint.clone();

        log::info!(
            "🔍 Initializing OpenTelemetry distributed tracing for service '{}'",
            config.service_name
        );

        if let Some(ref endpoint) = otlp_endpoint {
            log::info!(
                "📊 Tracing configuration: endpoint={}, sampling_ratio={}",
                endpoint,
                config.sampling_ratio
            );
        } else {
            log::info!(
                "📊 Tracing configuration: no-op mode (no OTLP endpoint), sampling_ratio={}",
                config.sampling_ratio
            );
        }

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

        // Create tracer provider with batch exporter (only if endpoint is specified)
        // Use AlwaysOn sampler for test mode to collect all spans
        // Use config sampling ratio for production mode
        let sampler = if otlp_endpoint.is_none() {
            // Test mode: collect all spans
            Sampler::AlwaysOn
        } else if config.sampling_ratio >= 0.99 {
            Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
        } else if config.sampling_ratio <= 0.01 {
            Sampler::ParentBased(Box::new(Sampler::AlwaysOff))
        } else {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(config.sampling_ratio)))
        };

        let mut span_collector_ref: Option<CollectingSpanProcessor> = None;

        let provider = if let Some(endpoint) = otlp_endpoint {
            // Create OTLP exporter and tracer provider
            match opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&endpoint)
                .build_span_exporter()
            {
                Ok(exporter) => {
                    log::info!("✅ OTLP exporter created successfully for {}", endpoint);

                    // Configure batch processor with larger queue for high-throughput streaming
                    let batch_processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(
                        exporter,
                        runtime::Tokio,
                    )
                    .with_max_queue_size(16384) // Increased from default 2048
                    .with_max_export_batch_size(512) // Default is 512
                    .with_scheduled_delay(std::time::Duration::from_millis(5000)) // Default is 5s
                    .build();

                    TracerProvider::builder()
                        .with_span_processor(batch_processor)
                        .with_config(
                            opentelemetry_sdk::trace::config()
                                .with_sampler(sampler)
                                .with_id_generator(RandomIdGenerator::default())
                                .with_resource(resource),
                        )
                        .build()
                }
                Err(e) => {
                    log::error!("❌ Failed to create OTLP exporter: {}", e);
                    return Err(SqlError::ConfigurationError {
                        message: format!("Failed to create OTLP exporter: {}", e),
                    });
                }
            }
        } else {
            // Create span collector for in-memory span collection (test mode)
            let collector = CollectingSpanProcessor::new();
            span_collector_ref = Some(collector.clone());

            // Create tracer provider with span collector (no OTLP exporter for testing)
            log::info!("ℹ️  Using test mode with in-memory span collection (no OTLP endpoint)");
            TracerProvider::builder()
                .with_span_processor(collector)
                .with_config(
                    opentelemetry_sdk::trace::config()
                        .with_sampler(sampler)
                        .with_id_generator(RandomIdGenerator::default())
                        .with_resource(resource),
                )
                .build()
        };

        // Set as global tracer provider
        global::set_tracer_provider(provider);

        if config.otlp_endpoint.is_some() {
            log::info!("✅ OpenTelemetry tracer initialized - spans will be exported to Tempo");
        } else {
            log::info!("✅ OpenTelemetry tracer initialized in no-op mode (no spans exported)");
        }
        log::info!(
            "🔍 Trace sampling: {:.1}% (using config sampling_ratio)",
            config.sampling_ratio * 100.0
        );

        Ok(Self {
            config,
            active: true,
            deployment_node_id: None,
            deployment_node_name: None,
            deployment_region: None,
            span_collector: span_collector_ref,
        })
    }

    /// Set deployment context (node ID, name, and region) for all traces
    ///
    /// This adds OpenTelemetry semantic convention attributes to traces:
    /// - `service.instance.id`: Unique identifier for this service instance
    /// - `host.name`: Name of the deployment node
    /// - `cloud.region`: Cloud region if applicable
    pub fn set_deployment_context(
        &mut self,
        node_id: Option<String>,
        node_name: Option<String>,
        region: Option<String>,
    ) -> Result<(), SqlError> {
        self.deployment_node_id = node_id.clone();
        self.deployment_node_name = node_name.clone();
        self.deployment_region = region.clone();

        if let Some(id) = node_id {
            log::info!("🔍 Telemetry deployment context set - Instance: {}", id);
        }
        if let Some(name) = node_name {
            log::info!("🔍 Telemetry deployment context set - Node: {}", name);
        }
        if let Some(r) = region {
            log::info!("🔍 Telemetry deployment context set - Region: {}", r);
        }

        Ok(())
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

        // Build attributes with deployment context
        let mut attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("batch.id", batch_id as i64),
            KeyValue::new("messaging.system", "kafka"),
            KeyValue::new("messaging.operation", "process"),
        ];

        // Add deployment context attributes if set
        if let Some(node_id) = &self.deployment_node_id {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }
        if let Some(node_name) = &self.deployment_node_name {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::HOST_NAME,
                node_name.clone(),
            ));
        }
        if let Some(region) = &self.deployment_region {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::CLOUD_REGION,
                region.clone(),
            ));
        }

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
                .with_attributes(attributes)
                .start_with_context(&tracer, &parent_cx)
        } else {
            log::debug!("🆕 Starting new trace for batch (no upstream context)");

            tracer
                .span_builder(format!("batch:{}", job_name))
                .with_kind(SpanKind::Internal)
                .with_attributes(attributes)
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
        job_name: &str,
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

        // Build attributes with deployment context
        let mut attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("db.system", "velostream"),
            KeyValue::new("db.operation", operation_name.to_string()),
            KeyValue::new("db.statement", query.chars().take(200).collect::<String>()),
            KeyValue::new("source", source.to_string()),
        ];

        // Add deployment context attributes if set
        if let Some(node_id) = &self.deployment_node_id {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }
        if let Some(node_name) = &self.deployment_node_name {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::HOST_NAME,
                node_name.clone(),
            ));
        }
        if let Some(region) = &self.deployment_region {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::CLOUD_REGION,
                region.clone(),
            ));
        }

        // Start span with parent context using Span Links (Send-safe across async boundaries)
        // Span Links preserve parent-child relationships without requiring ContextGuard (!Send)
        let mut span_builder = tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes);

        // Add parent span as a link if provided (async-boundary safe alternative to ContextGuard)
        if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                log::debug!(
                    "🔗 Linking SQL query span to parent span: {}",
                    parent_ctx.trace_id()
                );
                span_builder = span_builder
                    .with_links(vec![opentelemetry::trace::Link::new(parent_ctx, vec![])]);
            }
        }

        let mut span = span_builder.start(&tracer);
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started SQL query span: {} from source: {} (linked to parent, exporting to Tempo)",
            operation_name,
            source
        );

        QuerySpan::new_active(span)
    }

    /// Create a new trace span for streaming operations
    pub fn start_streaming_span(
        &self,
        job_name: &str,
        operation: &str,
        record_count: u64,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> StreamingSpan {
        if !self.active {
            return StreamingSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        let span_name = format!("streaming:{}", operation);

        // Build attributes with deployment context
        let mut attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("record_count", record_count as i64),
        ];

        // Add deployment context attributes if set
        if let Some(node_id) = &self.deployment_node_id {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }
        if let Some(node_name) = &self.deployment_node_name {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::HOST_NAME,
                node_name.clone(),
            ));
        }
        if let Some(region) = &self.deployment_region {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::CLOUD_REGION,
                region.clone(),
            ));
        }

        // Start span with parent context using Span Links (Send-safe across async boundaries)
        // Span Links preserve parent-child relationships without requiring ContextGuard (!Send)
        let mut span_builder = tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes);

        // Add parent span as a link if provided (async-boundary safe alternative to ContextGuard)
        if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                log::debug!(
                    "🔗 Linking streaming span to parent span: {}",
                    parent_ctx.trace_id()
                );
                span_builder = span_builder
                    .with_links(vec![opentelemetry::trace::Link::new(parent_ctx, vec![])]);
            }
        }

        let mut span = span_builder.start(&tracer);
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started streaming span: {} with {} records (linked to parent, exporting to Tempo)",
            operation,
            record_count
        );

        StreamingSpan::new_active(span, record_count)
    }

    /// Create a new trace span for aggregation operations
    pub fn start_aggregation_span(
        &self,
        job_name: &str,
        function: &str,
        window_type: &str,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> AggregationSpan {
        if !self.active {
            return AggregationSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        // Build attributes with deployment context
        let mut attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("function", function.to_string()),
            KeyValue::new("window_type", window_type.to_string()),
        ];

        // Add deployment context attributes if set
        if let Some(node_id) = &self.deployment_node_id {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }
        if let Some(node_name) = &self.deployment_node_name {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::HOST_NAME,
                node_name.clone(),
            ));
        }
        if let Some(region) = &self.deployment_region {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::CLOUD_REGION,
                region.clone(),
            ));
        }

        // Start span with parent context using Span Links (Send-safe across async boundaries)
        // Span Links preserve parent-child relationships without requiring ContextGuard (!Send)
        let mut span_builder = tracer
            .span_builder(format!("aggregation:{}", function))
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes);

        // Add parent span as a link if provided (async-boundary safe alternative to ContextGuard)
        if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                log::debug!(
                    "🔗 Linking aggregation span to parent span: {}",
                    parent_ctx.trace_id()
                );
                span_builder = span_builder
                    .with_links(vec![opentelemetry::trace::Link::new(parent_ctx, vec![])]);
            }
        }

        let mut span = span_builder.start(&tracer);
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started aggregation span: {} with window: {} (linked to parent, exporting to Tempo)",
            function,
            window_type
        );

        AggregationSpan::new_active(span)
    }

    /// Create a new trace span for profiling phases (deserialization, processing, serialization)
    ///
    /// # Arguments
    /// * `job_name` - Name of the job/query
    /// * `phase` - Phase being profiled: "deserialization", "processing", or "serialization"
    /// * `record_count` - Number of records being processed in this phase
    /// * `latency_ms` - Latency of the phase in milliseconds
    /// * `parent_context` - Optional parent span context for linking
    pub fn start_profiling_phase_span(
        &self,
        job_name: &str,
        phase: &str,
        record_count: u64,
        latency_ms: u64,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> StreamingSpan {
        if !self.active {
            return StreamingSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        let span_name = format!("profiling_phase:{}", phase);

        // Build attributes with profiling phase information
        // Calculate throughput: records per second
        let throughput_rps = if latency_ms > 0 {
            (record_count as f64 / latency_ms as f64) * 1000.0
        } else {
            0.0
        };

        let mut attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("profiling.phase", phase.to_string()),
            KeyValue::new("record_count", record_count as i64),
            KeyValue::new("latency_ms", latency_ms as i64),
            KeyValue::new("throughput_rps", throughput_rps),
        ];

        // Add deployment context attributes if set
        if let Some(node_id) = &self.deployment_node_id {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }
        if let Some(node_name) = &self.deployment_node_name {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::HOST_NAME,
                node_name.clone(),
            ));
        }
        if let Some(region) = &self.deployment_region {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::CLOUD_REGION,
                region.clone(),
            ));
        }

        // Start span with parent context using Span Links (Send-safe across async boundaries)
        // Span Links preserve parent-child relationships without requiring ContextGuard (!Send)
        let mut span_builder = tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes);

        // Add parent span as a link if provided (async-boundary safe alternative to ContextGuard)
        if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                log::debug!(
                    "🔗 Linking profiling phase span to parent span: {}",
                    parent_ctx.trace_id()
                );
                span_builder = span_builder
                    .with_links(vec![opentelemetry::trace::Link::new(parent_ctx, vec![])]);
            }
        }

        let mut span = span_builder.start(&tracer);
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started profiling phase span: {} (phase: {}, records: {}, latency: {}ms, linked to parent)",
            job_name,
            phase,
            record_count,
            latency_ms
        );

        StreamingSpan::new_active(span, record_count)
    }

    /// Create a new trace span for job lifecycle events (submit, queue, execute, complete)
    ///
    /// # Arguments
    /// * `job_name` - Name of the job
    /// * `lifecycle_event` - Event type: "submit", "queue", "execute", or "complete"
    /// * `parent_context` - Optional parent span context for linking
    pub fn start_job_lifecycle_span(
        &self,
        job_name: &str,
        lifecycle_event: &str,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> StreamingSpan {
        if !self.active {
            return StreamingSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());
        let span_name = format!("job.lifecycle:{}", lifecycle_event);

        // Build attributes with deployment context
        let mut attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("job.lifecycle_event", lifecycle_event.to_string()),
            KeyValue::new("span.kind", "internal"),
        ];

        // Add deployment context attributes if set
        if let Some(node_id) = &self.deployment_node_id {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
                node_id.clone(),
            ));
        }
        if let Some(node_name) = &self.deployment_node_name {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::HOST_NAME,
                node_name.clone(),
            ));
        }
        if let Some(region) = &self.deployment_region {
            attributes.push(KeyValue::new(
                opentelemetry_semantic_conventions::resource::CLOUD_REGION,
                region.clone(),
            ));
        }

        // Start span with parent context using Span Links (Send-safe across async boundaries)
        let mut span_builder = tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes);

        // Link to parent span if provided
        if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                log::debug!(
                    "🔗 Linking job lifecycle span ({}) to parent span: {}",
                    lifecycle_event,
                    parent_ctx.trace_id()
                );
                span_builder = span_builder
                    .with_links(vec![opentelemetry::trace::Link::new(parent_ctx, vec![])]);
            }
        }

        let mut span = span_builder.start(&tracer);
        span.set_status(Status::Ok);

        log::info!(
            "📍 Job lifecycle event: {} -> {} (exporting to Tempo)",
            job_name,
            lifecycle_event
        );

        StreamingSpan::new_active(span, 0)
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

    /// Get all collected spans (for testing with in-memory span collection)
    ///
    /// This method is only available when using no-op mode (no OTLP endpoint).
    /// Returns empty vec if using OTLP exporter mode or if no spans were collected.
    pub fn collected_spans(&self) -> Vec<SpanData> {
        if let Some(collector) = &self.span_collector {
            collector.spans()
        } else {
            vec![]
        }
    }

    /// Get count of collected spans
    pub fn span_count(&self) -> usize {
        if let Some(collector) = &self.span_collector {
            collector.span_count()
        } else {
            0
        }
    }
}

/// SQL query execution span wrapper
pub struct QuerySpan {
    base: BaseSpan,
}

impl QuerySpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            base: BaseSpan::new_active(span),
        }
    }

    fn new_inactive() -> Self {
        Self {
            base: BaseSpan::new_inactive(),
        }
    }

    /// Add execution time to the span
    pub fn set_execution_time(&mut self, duration_ms: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("execution_time_ms", duration_ms as i64));
            log::trace!("🔍 SQL span execution time: {}ms", duration_ms);
        }
    }

    /// Add record count to the span
    pub fn set_record_count(&mut self, count: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("record_count", count as i64));
            log::trace!("🔍 SQL span processed {} records", count);
        }
    }

    /// Mark the query as successful
    pub fn set_success(&mut self) {
        self.base.set_success();
    }

    /// Mark the query as failed with error information
    pub fn set_error(&mut self, error: &str) {
        self.base.set_error(error);
    }
}

/// Streaming operation span wrapper
pub struct StreamingSpan {
    base: BaseSpan,
    record_count: u64,
}

impl StreamingSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan, record_count: u64) -> Self {
        Self {
            base: BaseSpan::new_active(span),
            record_count,
        }
    }

    fn new_inactive() -> Self {
        Self {
            base: BaseSpan::new_inactive(),
            record_count: 0,
        }
    }

    /// Add throughput information to the span
    pub fn set_throughput(&mut self, records_per_second: f64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("throughput_rps", records_per_second));
            log::trace!(
                "🔍 Streaming span throughput: {:.2} rps",
                records_per_second
            );
        }
    }

    /// Add processing time to the span
    pub fn set_processing_time(&mut self, duration_ms: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("processing_time_ms", duration_ms as i64));
            log::trace!("🔍 Streaming span processing time: {}ms", duration_ms);
        }
    }

    /// Mark the operation as successful
    pub fn set_success(&mut self) {
        self.base.set_success();
    }

    /// Mark the operation as failed with error information
    pub fn set_error(&mut self, error: &str) {
        self.base.set_error(error);
    }

    /// Add Kafka metadata (topic, partition, offset) to the span
    pub fn set_kafka_metadata(&mut self, topic: &str, partition: i32, offset: i64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("kafka.topic", topic.to_string()));
            span.set_attribute(KeyValue::new("kafka.partition", partition as i64));
            span.set_attribute(KeyValue::new("kafka.offset", offset));
            log::trace!(
                "🔍 Streaming span enriched with Kafka metadata: topic={}, partition={}, offset={}",
                topic,
                partition,
                offset
            );
        }
    }
}

/// Aggregation operation span wrapper
pub struct AggregationSpan {
    base: BaseSpan,
}

impl AggregationSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            base: BaseSpan::new_active(span),
        }
    }

    fn new_inactive() -> Self {
        Self {
            base: BaseSpan::new_inactive(),
        }
    }

    /// Add window size information to the span
    pub fn set_window_size(&mut self, size_ms: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("window_size_ms", size_ms as i64));
            log::trace!("🔍 Aggregation span window size: {}ms", size_ms);
        }
    }

    /// Add input record count to the span
    pub fn set_input_records(&mut self, count: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("input_records", count as i64));
            log::trace!("🔍 Aggregation span input records: {}", count);
        }
    }

    /// Add output record count to the span
    pub fn set_output_records(&mut self, count: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("output_records", count as i64));
            log::trace!("🔍 Aggregation span output records: {}", count);
        }
    }

    /// Mark the aggregation as successful
    pub fn set_success(&mut self) {
        self.base.set_success();
    }

    /// Mark the aggregation as failed with error information
    pub fn set_error(&mut self, error: &str) {
        self.base.set_error(error);
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
    base: BaseSpan,
}

impl BatchSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            base: BaseSpan::new_active(span),
        }
    }

    fn new_inactive() -> Self {
        Self {
            base: BaseSpan::new_inactive(),
        }
    }

    /// Add total records processed to the span
    pub fn set_total_records(&mut self, count: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("total_records", count as i64));
            log::trace!("🔍 Batch span processed {} total records", count);
        }
    }

    /// Add batch duration to the span
    pub fn set_batch_duration(&mut self, duration_ms: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("batch_duration_ms", duration_ms as i64));
            log::trace!("🔍 Batch span duration: {}ms", duration_ms);
        }
    }

    /// Mark the batch as successful
    pub fn set_success(&mut self) {
        self.base.set_success();
    }

    /// Mark the batch as failed with error information
    pub fn set_error(&mut self, error: &str) {
        self.base.set_error(error);
    }

    /// Get the span context for creating child spans with parent relationship
    pub fn span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        self.base.span().map(|span| span.span_context().clone())
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
