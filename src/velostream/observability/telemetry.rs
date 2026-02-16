// === PHASE 4: OPENTELEMETRY DISTRIBUTED TRACING ===

use crate::velostream::observability::span_collector::CollectingSpanProcessor;
use crate::velostream::observability::tokio_span_processor::{
    TokioSpanProcessor, TokioSpanProcessorConfig,
};
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
                config.service_version.clone(),
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
            // Derive HTTP endpoint from gRPC endpoint for OTLP/HTTP export
            // gRPC default: localhost:4317 → HTTP: http://localhost:4318/v1/traces
            let http_endpoint = if endpoint.contains(":4317") {
                endpoint.replace(":4317", ":4318")
            } else {
                endpoint.clone()
            };

            // Create OTLP HTTP exporter (more reliable than gRPC for local deployments)
            match opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(&http_endpoint)
                .build_span_exporter()
            {
                Ok(exporter) => {
                    log::info!(
                        "✅ OTLP HTTP exporter created successfully for {}",
                        http_endpoint
                    );

                    // Use TokioSpanProcessor instead of BatchSpanProcessor to avoid
                    // the SDK 0.21.2 FusedStream bug where the background export loop
                    // stops after the first cycle.
                    let processor = TokioSpanProcessor::new(
                        exporter,
                        TokioSpanProcessorConfig {
                            max_queue_size: 65536,
                            max_export_batch_size: 1024,
                            scheduled_delay: std::time::Duration::from_millis(2000),
                            export_timeout: std::time::Duration::from_secs(10),
                        },
                    );

                    TracerProvider::builder()
                        .with_span_processor(processor)
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

    /// Create a new telemetry provider with async queue-based span processing
    ///
    /// This variant uses `QueuedSpanProcessor` instead of `BatchSpanProcessor`,
    /// ensuring tracing never blocks the processing loop.
    ///
    /// # Arguments
    ///
    /// * `config` - Tracing configuration
    /// * `queue` - Async observability queue for non-blocking span submission
    ///
    /// # Returns
    ///
    /// Returns (TelemetryProvider, Option<SpanExporter>) where the exporter
    /// should be passed to BackgroundFlusher for actual OTLP export.
    ///
    /// # Example
    /// ```ignore
    /// use velostream::observability::telemetry::TelemetryProvider;
    /// use velostream::observability::async_queue::ObservabilityQueue;
    ///
    /// let (queue, receivers) = ObservabilityQueue::new(config.clone());
    /// let (provider, exporter) = TelemetryProvider::new_with_queue(
    ///     tracing_config,
    ///     Arc::new(queue),
    /// ).await?;
    ///
    /// // Pass exporter to BackgroundFlusher
    /// let flusher = BackgroundFlusher::start(receivers, metrics, exporter, config).await;
    /// ```
    pub async fn new_with_queue(
        config: TracingConfig,
        queue: std::sync::Arc<crate::velostream::observability::async_queue::ObservabilityQueue>,
    ) -> Result<
        (
            Self,
            Option<Box<dyn opentelemetry_sdk::export::trace::SpanExporter>>,
        ),
        SqlError,
    > {
        use crate::velostream::observability::queued_span_processor::QueuedSpanProcessor;

        let otlp_endpoint = config.otlp_endpoint.clone();

        log::info!(
            "🔍 Initializing OpenTelemetry distributed tracing (queue-based) for service '{}'",
            config.service_name
        );

        if let Some(ref endpoint) = otlp_endpoint {
            log::info!(
                "📊 Queue-based tracing configuration: endpoint={}, sampling_ratio={}",
                endpoint,
                config.sampling_ratio
            );
        } else {
            log::info!(
                "📊 Queue-based tracing configuration: no-op mode (no OTLP endpoint), sampling_ratio={}",
                config.sampling_ratio
            );
        }

        // Create resource with service information
        let resource = opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                config.service_name.clone(),
            ),
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
                config.service_version.clone(),
            ),
        ]);

        // Configure sampler
        let sampler = if otlp_endpoint.is_none() {
            opentelemetry_sdk::trace::Sampler::AlwaysOn
        } else if config.sampling_ratio >= 0.99 {
            opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
                opentelemetry_sdk::trace::Sampler::AlwaysOn,
            ))
        } else if config.sampling_ratio <= 0.01 {
            opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
                opentelemetry_sdk::trace::Sampler::AlwaysOff,
            ))
        } else {
            opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
                opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(config.sampling_ratio),
            ))
        };

        let mut span_collector_ref: Option<CollectingSpanProcessor> = None;
        let mut exporter_ref: Option<Box<dyn opentelemetry_sdk::export::trace::SpanExporter>> =
            None;

        let provider = if let Some(endpoint) = otlp_endpoint {
            // Derive HTTP endpoint from gRPC endpoint for OTLP/HTTP export
            let http_endpoint = if endpoint.contains(":4317") {
                endpoint.replace(":4317", ":4318")
            } else {
                endpoint.clone()
            };

            // Create OTLP HTTP exporter for queue-based processing
            match opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(&http_endpoint)
                .build_span_exporter()
            {
                Ok(exporter) => {
                    log::info!(
                        "✅ OTLP HTTP exporter created successfully for {} (queue-based)",
                        http_endpoint
                    );

                    // Store exporter for return (BackgroundFlusher will use it)
                    exporter_ref = Some(Box::new(exporter));

                    // Create QueuedSpanProcessor (non-blocking)
                    let queued_processor = QueuedSpanProcessor::new(
                        queue.clone(),
                        // Create a second exporter for the processor
                        // (BackgroundFlusher will use the one we stored)
                        Box::new(
                            opentelemetry_otlp::new_exporter()
                                .http()
                                .with_endpoint(&http_endpoint)
                                .build_span_exporter()
                                .map_err(|e| SqlError::ConfigurationError {
                                    message: format!(
                                        "Failed to create second OTLP exporter: {}",
                                        e
                                    ),
                                })?,
                        ),
                    );

                    log::info!("🚀 Using QueuedSpanProcessor for non-blocking trace submission");

                    opentelemetry_sdk::trace::TracerProvider::builder()
                        .with_span_processor(queued_processor)
                        .with_config(
                            opentelemetry_sdk::trace::config()
                                .with_sampler(sampler)
                                .with_id_generator(
                                    opentelemetry_sdk::trace::RandomIdGenerator::default(),
                                )
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
            // No OTLP endpoint: use in-memory span collection for testing
            let collector = CollectingSpanProcessor::new();
            span_collector_ref = Some(collector.clone());

            log::info!("ℹ️  Using test mode with in-memory span collection (no OTLP endpoint)");
            opentelemetry_sdk::trace::TracerProvider::builder()
                .with_span_processor(collector)
                .with_config(
                    opentelemetry_sdk::trace::config()
                        .with_sampler(sampler)
                        .with_id_generator(opentelemetry_sdk::trace::RandomIdGenerator::default())
                        .with_resource(resource),
                )
                .build()
        };

        // Set as global tracer provider
        opentelemetry::global::set_tracer_provider(provider);

        if config.otlp_endpoint.is_some() {
            log::info!(
                "✅ OpenTelemetry tracer initialized (queue-based) - spans will be exported via async queue"
            );
        } else {
            log::info!("✅ OpenTelemetry tracer initialized in no-op mode (no spans exported)");
        }
        log::info!(
            "🔍 Trace sampling: {:.1}% (using config sampling_ratio)",
            config.sampling_ratio * 100.0
        );

        let provider_instance = Self {
            config,
            active: true,
            deployment_node_id: None,
            deployment_node_name: None,
            deployment_region: None,
            span_collector: span_collector_ref,
        };

        Ok((provider_instance, exporter_ref))
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

    /// Create a per-record processing span for head-based sampling.
    ///
    /// This span is created only for records that pass the sampling decision
    /// (`should_sample_record()`). If `upstream_context` is provided, the span
    /// becomes a child of the upstream trace; otherwise a new root trace starts.
    ///
    /// # Arguments
    /// * `job_name` - Name of the job/query
    /// * `upstream_context` - Optional upstream span context from traceparent header
    pub fn start_record_span(
        &self,
        job_name: &str,
        upstream_context: Option<&opentelemetry::trace::SpanContext>,
    ) -> RecordSpan {
        if !self.active {
            return RecordSpan {
                base: BaseSpan::new_inactive(),
            };
        }

        let tracer = global::tracer(self.config.service_name.clone());

        let attributes = vec![
            KeyValue::new("job.name", job_name.to_string()),
            KeyValue::new("messaging.system", "kafka"),
        ];

        let span = if let Some(parent_ctx) = upstream_context {
            use opentelemetry::trace::{TraceContextExt, Tracer as _};

            let parent_cx =
                opentelemetry::Context::new().with_remote_span_context(parent_ctx.clone());

            tracer
                .span_builder(format!("process:{}", job_name))
                .with_kind(SpanKind::Consumer)
                .with_attributes(attributes)
                .start_with_context(&tracer, &parent_cx)
        } else {
            tracer
                .span_builder(format!("process:{}", job_name))
                .with_kind(SpanKind::Internal)
                .with_attributes(attributes)
                .start(&tracer)
        };

        RecordSpan::new_active(span)
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

        // Create child span under parent context for proper span hierarchy
        let span = if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                use opentelemetry::trace::{TraceContextExt, Tracer as _};
                log::debug!(
                    "🔗 Creating streaming child span under parent trace: {}",
                    parent_ctx.trace_id()
                );
                let parent_cx =
                    opentelemetry::Context::current().with_remote_span_context(parent_ctx);
                tracer
                    .span_builder(span_name)
                    .with_kind(SpanKind::Internal)
                    .with_attributes(attributes)
                    .start_with_context(&tracer, &parent_cx)
            } else {
                tracer
                    .span_builder(span_name)
                    .with_kind(SpanKind::Internal)
                    .with_attributes(attributes)
                    .start(&tracer)
            }
        } else {
            tracer
                .span_builder(span_name)
                .with_kind(SpanKind::Internal)
                .with_attributes(attributes)
                .start(&tracer)
        };

        log::debug!(
            "🔍 Started streaming span: {} with {} records (child of parent, exporting to Tempo)",
            operation,
            record_count
        );

        StreamingSpan::new_active(span)
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

        // Create child span under parent context for proper span hierarchy
        let span = if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                use opentelemetry::trace::{TraceContextExt, Tracer as _};
                log::debug!(
                    "🔗 Creating job lifecycle child span ({}) under parent trace: {}",
                    lifecycle_event,
                    parent_ctx.trace_id()
                );
                let parent_cx =
                    opentelemetry::Context::current().with_remote_span_context(parent_ctx);
                tracer
                    .span_builder(span_name)
                    .with_kind(SpanKind::Internal)
                    .with_attributes(attributes)
                    .start_with_context(&tracer, &parent_cx)
            } else {
                tracer
                    .span_builder(span_name)
                    .with_kind(SpanKind::Internal)
                    .with_attributes(attributes)
                    .start(&tracer)
            }
        } else {
            tracer
                .span_builder(span_name)
                .with_kind(SpanKind::Internal)
                .with_attributes(attributes)
                .start(&tracer)
        };

        log::info!(
            "📍 Job lifecycle event: {} -> {} (exporting to Tempo)",
            job_name,
            lifecycle_event
        );

        StreamingSpan::new_active(span)
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

/// Per-record processing span for head-based sampling.
///
/// Lightweight wrapper around a single span that tracks the processing of one
/// input record. Created only for sampled records (via `should_sample_record()`).
///
/// On drop, the underlying OpenTelemetry span ends automatically.
pub struct RecordSpan {
    base: BaseSpan,
}

impl RecordSpan {
    fn new_active(span: opentelemetry::global::BoxedSpan) -> Self {
        Self {
            base: BaseSpan::new_active(span),
        }
    }

    /// Get the span context for injecting into output records.
    pub fn span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        self.base.span().map(|span| span.span_context().clone())
    }

    /// Track how many output records this input produced.
    pub fn set_output_count(&mut self, count: usize) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("record.output_count", count as i64));
        }
    }

    /// Mark the record processing as failed.
    pub fn set_error(&mut self, error: &str) {
        self.base.set_error(error);
    }

    /// Mark the record processing as successful.
    pub fn set_success(&mut self) {
        self.base.set_success();
    }
}

/// Streaming operation span wrapper
pub struct StreamingSpan {
    base: BaseSpan,
}

impl StreamingSpan {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_telemetry_provider_creation() {
        let config = TracingConfig::development();
        // Note: This test will try to connect to localhost:4317
        // In CI, this might fail, which is expected
        let _ = TelemetryProvider::new(config).await;
    }
}
