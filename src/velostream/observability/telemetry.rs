// === PHASE 4: OPENTELEMETRY DISTRIBUTED TRACING ===

use crate::velostream::observability::query_metadata::QuerySpanMetadata;
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

    /// Set pre-computed query metadata as span attributes for streaming intelligence.
    /// Shared implementation used by both BatchSpan and QuerySpan.
    pub(crate) fn set_query_metadata(&mut self, metadata: &QuerySpanMetadata) {
        if let Some(span) = self.span.as_mut() {
            span.set_attribute(KeyValue::new("sql.has_join", metadata.has_join));
            if let Some(jt) = &metadata.join_type {
                span.set_attribute(KeyValue::new("sql.join_type", jt.clone()));
            }
            if let Some(js) = &metadata.join_sources {
                span.set_attribute(KeyValue::new("sql.join_sources", js.clone()));
            }
            if let Some(jk) = &metadata.join_key_fields {
                span.set_attribute(KeyValue::new("sql.join_key_fields", jk.clone()));
            }
            span.set_attribute(KeyValue::new("sql.has_window", metadata.has_window));
            if let Some(wt) = &metadata.window_type {
                span.set_attribute(KeyValue::new("sql.window_type", wt.clone()));
            }
            if let Some(ws) = &metadata.window_size_ms {
                span.set_attribute(KeyValue::new("sql.window_size_ms", *ws));
            }
            span.set_attribute(KeyValue::new("sql.has_group_by", metadata.has_group_by));
            if let Some(gf) = &metadata.group_by_fields {
                span.set_attribute(KeyValue::new("sql.group_by_fields", gf.clone()));
            }
            if let Some(em) = &metadata.emit_mode {
                span.set_attribute(KeyValue::new("sql.emit_mode", em.clone()));
            }
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
                "0.1.0",
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

            log::debug!(
                "🔗 Starting batch span as child of upstream trace: {} (parent_span={})",
                parent_ctx.trace_id(),
                parent_ctx.span_id()
            );

            // Use Context::new() instead of Context::current() to avoid interference
            // from any existing spans in the thread-local context. This ensures the
            // remote span context is the sole parent for the new child span.
            let parent_cx =
                opentelemetry::Context::new().with_remote_span_context(parent_ctx.clone());

            let child_span = tracer
                .span_builder(format!("batch:{}", job_name))
                .with_kind(SpanKind::Consumer) // Consumer span for Kafka message processing
                .with_attributes(attributes)
                .start_with_context(&tracer, &parent_cx);

            // Verify child span was properly linked to parent
            let child_ctx = child_span.span_context();
            if child_ctx.trace_id() != parent_ctx.trace_id() {
                log::warn!(
                    "⚠️  Child span NOT linked to parent: child_trace={} != parent_trace={}",
                    child_ctx.trace_id(),
                    parent_ctx.trace_id()
                );
            } else {
                log::debug!(
                    "Child span linked: trace_id={}, child_span_id={}, parent_span_id={}",
                    child_ctx.trace_id(),
                    child_ctx.span_id(),
                    parent_ctx.span_id(),
                );
            }

            child_span
        } else {
            log::debug!("🆕 Starting new trace for batch (no upstream context)");

            tracer
                .span_builder(format!("batch:{}", job_name))
                .with_kind(SpanKind::Internal)
                .with_attributes(attributes)
                .start(&tracer)
        };

        span.set_status(Status::Ok);

        log::debug!("Batch span created: {} (batch #{})", job_name, batch_id,);

        BatchSpan::new_active(span)
    }

    /// Create a child span for tracking a specific pipeline processing stage
    ///
    /// This method creates a child span with proper parent-child relationships for tracking
    /// individual stages of the processing pipeline (e.g., deserialization, execution, serialization).
    ///
    /// # Arguments
    /// * `stage_name` - Name of the pipeline stage (e.g., "deserialization", "sql_execution", "serialization")
    /// * `parent_context` - Optional parent span context (typically from a BatchSpan)
    ///
    /// # Example
    /// ```rust,ignore
    /// let mut batch_span = telemetry.start_batch_span("job", 123, None);
    /// let mut deser_span = telemetry.start_pipeline_stage_span(
    ///     "deserialization",
    ///     batch_span.span_context()
    /// );
    /// // ... perform deserialization
    /// deser_span.set_stage_attributes("deserialization", 1000, 15);
    /// deser_span.set_success();
    /// ```
    pub fn start_pipeline_stage_span(
        &self,
        stage_name: &str,
        parent_context: Option<opentelemetry::trace::SpanContext>,
    ) -> PipelineStageSpan {
        if !self.active {
            return PipelineStageSpan::new_inactive();
        }

        let tracer = global::tracer(self.config.service_name.clone());

        // Build attributes for the pipeline stage
        let attributes = vec![KeyValue::new(
            "velostream.pipeline.stage",
            stage_name.to_string(),
        )];

        // Create span as child of parent if parent_context provided
        let span = if let Some(parent_ctx) = parent_context {
            use opentelemetry::trace::{TraceContextExt, Tracer as _};

            log::debug!(
                "🔗 Starting pipeline stage '{}' as child of parent trace: {}",
                stage_name,
                parent_ctx.trace_id()
            );

            let parent_cx = opentelemetry::Context::current().with_remote_span_context(parent_ctx);

            tracer
                .span_builder(format!("pipeline:{}", stage_name))
                .with_kind(SpanKind::Internal) // Internal span for processing stages
                .with_attributes(attributes)
                .start_with_context(&tracer, &parent_cx)
        } else {
            log::debug!(
                "🆕 Starting pipeline stage '{}' (no parent context)",
                stage_name
            );

            tracer
                .span_builder(format!("pipeline:{}", stage_name))
                .with_kind(SpanKind::Internal)
                .with_attributes(attributes)
                .start(&tracer)
        };

        log::debug!("🔍 Started pipeline stage span: {}", stage_name);

        PipelineStageSpan::new_active(span)
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

        // Create child span under parent context for proper span hierarchy
        let mut span = if let Some(parent_ctx) = parent_context {
            if parent_ctx.is_valid() {
                use opentelemetry::trace::{TraceContextExt, Tracer as _};
                log::debug!(
                    "🔗 Creating SQL query child span under parent trace: {}",
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
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started SQL query span: {} from source: {} (child of parent, exporting to Tempo)",
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

        // Create child span under parent context for proper span hierarchy
        let mut span = if let Some(parent_ctx) = parent_context {
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
        span.set_status(Status::Ok);

        log::debug!(
            "🔍 Started streaming span: {} with {} records (child of parent, exporting to Tempo)",
            operation,
            record_count
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

        // Create child span under parent context for proper span hierarchy
        let mut span = if let Some(parent_ctx) = parent_context {
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

    /// Set pre-computed query metadata as span attributes for streaming intelligence
    pub fn set_query_metadata(&mut self, metadata: &QuerySpanMetadata) {
        self.base.set_query_metadata(metadata);
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

/// Batch processing span wrapper (parent span for all operations in a batch)
///
/// Child spans (deserialization, SQL processing, serialization) are created as true
/// parent-child relationships using `start_with_context()`. The batch span context
/// is extracted via `span_context()` and passed to child span creation methods.
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

    /// Set pre-computed query metadata as span attributes for streaming intelligence
    pub fn set_query_metadata(&mut self, metadata: &QuerySpanMetadata) {
        self.base.set_query_metadata(metadata);
    }

    /// Set input Kafka topic on the span
    pub fn set_input_topic(&mut self, topic: &str) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("kafka.input_topic", topic.to_string()));
        }
    }

    /// Set input Kafka partition on the span
    pub fn set_input_partition(&mut self, partition: i32) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("kafka.input_partition", partition as i64));
        }
    }

    /// Set Kafka message key on the span
    pub fn set_message_key(&mut self, key: &str) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("kafka.message_key", key.to_string()));
        }
    }

    /// Set OpenTelemetry messaging semantic conventions for Kafka operations
    pub fn set_messaging_attributes(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        consumer_group: Option<&str>,
        message_key: Option<&str>,
    ) {
        if let Some(span) = self.base.span_mut() {
            // OpenTelemetry messaging semantic conventions
            span.set_attribute(KeyValue::new("messaging.system", "kafka"));
            span.set_attribute(KeyValue::new("messaging.operation", "process"));
            span.set_attribute(KeyValue::new(
                "messaging.destination.name",
                topic.to_string(),
            ));
            span.set_attribute(KeyValue::new("messaging.kafka.partition", partition as i64));
            span.set_attribute(KeyValue::new("messaging.kafka.offset", offset));

            if let Some(group) = consumer_group {
                span.set_attribute(KeyValue::new(
                    "messaging.kafka.consumer.group",
                    group.to_string(),
                ));
            }

            if let Some(key) = message_key {
                span.set_attribute(KeyValue::new(
                    "messaging.kafka.message.key",
                    key.to_string(),
                ));
            }
        }
    }

    /// Set application-specific attributes (velostream namespace)
    pub fn set_application_attributes(
        &mut self,
        job_name: &str,
        app_name: Option<&str>,
        app_version: Option<&str>,
        job_mode: Option<&str>,
    ) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("velostream.job_name", job_name.to_string()));

            if let Some(name) = app_name {
                span.set_attribute(KeyValue::new("velostream.app_name", name.to_string()));
            }

            if let Some(version) = app_version {
                span.set_attribute(KeyValue::new("velostream.app_version", version.to_string()));
            }

            if let Some(mode) = job_mode {
                span.set_attribute(KeyValue::new("velostream.job_mode", mode.to_string()));
            }
        }
    }

    /// Set performance timing breakdown (in milliseconds)
    pub fn set_performance_timings(
        &mut self,
        deserialization_ms: u64,
        execution_ms: u64,
        serialization_ms: u64,
    ) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new(
                "velostream.timing.deserialization_ms",
                deserialization_ms as i64,
            ));
            span.set_attribute(KeyValue::new(
                "velostream.timing.execution_ms",
                execution_ms as i64,
            ));
            span.set_attribute(KeyValue::new(
                "velostream.timing.serialization_ms",
                serialization_ms as i64,
            ));

            // Calculate percentage breakdown
            let total = deserialization_ms + execution_ms + serialization_ms;
            if total > 0 {
                let deser_pct = (deserialization_ms as f64 / total as f64) * 100.0;
                let exec_pct = (execution_ms as f64 / total as f64) * 100.0;
                let ser_pct = (serialization_ms as f64 / total as f64) * 100.0;

                span.set_attribute(KeyValue::new(
                    "velostream.timing.deserialization_pct",
                    deser_pct,
                ));
                span.set_attribute(KeyValue::new("velostream.timing.execution_pct", exec_pct));
                span.set_attribute(KeyValue::new(
                    "velostream.timing.serialization_pct",
                    ser_pct,
                ));
            }
        }
    }

    /// Set throughput metrics
    pub fn set_throughput_metrics(
        &mut self,
        records_per_second: f64,
        bytes_per_second: Option<f64>,
    ) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new(
                "velostream.throughput.records_per_sec",
                records_per_second,
            ));

            if let Some(bps) = bytes_per_second {
                span.set_attribute(KeyValue::new("velostream.throughput.bytes_per_sec", bps));
            }
        }
    }

    /// Set error rate metrics
    pub fn set_error_rate(&mut self, errors: u64, total: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("velostream.errors.count", errors as i64));

            if total > 0 {
                let error_rate = (errors as f64 / total as f64) * 100.0;
                span.set_attribute(KeyValue::new("velostream.errors.rate_pct", error_rate));
            }
        }
    }

    /// Set window-specific metrics (for windowed queries)
    pub fn set_window_metrics(
        &mut self,
        window_start_ms: i64,
        window_end_ms: i64,
        records_in_window: u64,
        windows_emitted: u64,
    ) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("velostream.window.start_ms", window_start_ms));
            span.set_attribute(KeyValue::new("velostream.window.end_ms", window_end_ms));
            span.set_attribute(KeyValue::new(
                "velostream.window.records_in_window",
                records_in_window as i64,
            ));
            span.set_attribute(KeyValue::new(
                "velostream.window.windows_emitted",
                windows_emitted as i64,
            ));
        }
    }

    /// Set join-specific metrics
    pub fn set_join_metrics(
        &mut self,
        left_records: u64,
        right_records: u64,
        joined_records: u64,
        join_hit_rate: f64,
    ) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new(
                "velostream.join.left_records",
                left_records as i64,
            ));
            span.set_attribute(KeyValue::new(
                "velostream.join.right_records",
                right_records as i64,
            ));
            span.set_attribute(KeyValue::new(
                "velostream.join.joined_records",
                joined_records as i64,
            ));
            span.set_attribute(KeyValue::new("velostream.join.hit_rate", join_hit_rate));
        }
    }

    /// Get the span context for creating child spans with parent relationship
    pub fn span_context(&self) -> Option<opentelemetry::trace::SpanContext> {
        self.base.span().map(|span| span.span_context().clone())
    }
}

/// Pipeline stage span for tracking individual processing stages with parent-child relationships
///
/// This span type is used to create child spans that track specific stages of the processing
/// pipeline (e.g., deserialization, execution, serialization, kafka produce).
///
/// # Example
/// ```rust,ignore
/// // Create parent batch span
/// let mut batch_span = telemetry.start_batch_span("job_name", 123, None);
///
/// // Create child span for deserialization stage
/// let mut deser_span = telemetry.start_pipeline_stage_span(
///     "deserialization",
///     batch_span.span_context()
/// );
/// // ... perform deserialization
/// deser_span.set_stage_attributes("deserialization", 1000, 15);
/// deser_span.set_success();
/// ```
pub struct PipelineStageSpan {
    base: BaseSpan,
}

impl PipelineStageSpan {
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

    /// Set attributes specific to a pipeline processing stage
    ///
    /// # Arguments
    /// * `stage_name` - Name of the pipeline stage (e.g., "deserialization", "execution", "serialization")
    /// * `record_count` - Number of records processed in this stage
    /// * `duration_ms` - Duration of this stage in milliseconds
    pub fn set_stage_attributes(
        &mut self,
        stage_name: &str,
        record_count: usize,
        duration_ms: u64,
    ) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new(
                "velostream.pipeline.stage",
                stage_name.to_string(),
            ));
            span.set_attribute(KeyValue::new(
                "velostream.pipeline.records",
                record_count as i64,
            ));
            span.set_attribute(KeyValue::new(
                "velostream.pipeline.duration_ms",
                duration_ms as i64,
            ));

            // Calculate throughput for this stage
            if duration_ms > 0 {
                let throughput = (record_count as f64) / (duration_ms as f64 / 1000.0);
                span.set_attribute(KeyValue::new(
                    "velostream.pipeline.throughput_records_per_sec",
                    throughput,
                ));
            }
        }
    }

    /// Set additional timing breakdown for the stage
    ///
    /// # Arguments
    /// * `cpu_ms` - CPU time spent in this stage
    /// * `wait_ms` - Wait/idle time in this stage
    pub fn set_timing_breakdown(&mut self, cpu_ms: u64, wait_ms: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new("velostream.pipeline.cpu_ms", cpu_ms as i64));
            span.set_attribute(KeyValue::new("velostream.pipeline.wait_ms", wait_ms as i64));
        }
    }

    /// Set bytes processed in this stage (for serialization/deserialization stages)
    pub fn set_bytes_processed(&mut self, bytes: u64) {
        if let Some(span) = self.base.span_mut() {
            span.set_attribute(KeyValue::new(
                "velostream.pipeline.bytes_processed",
                bytes as i64,
            ));
        }
    }

    /// Mark the stage as successfully completed
    pub fn set_success(&mut self) {
        self.base.set_success();
    }

    /// Mark the stage as failed with an error message
    pub fn set_error(&mut self, error: &str) {
        self.base.set_error(error);
    }

    /// Get the span context for creating nested child spans
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
