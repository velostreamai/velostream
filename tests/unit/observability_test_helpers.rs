//! Shared observability test helpers
//!
//! Consolidated helpers for distributed tracing tests to eliminate code duplication.
//! Used by:
//! - tests/integration/processor_trace_patterns_test.rs
//! - tests/unit/observability/distributed_tracing_test.rs
//! - tests/integration/trace_chain_kafka_test.rs
//! - tests/unit/observability/async_queue_test.rs
//! - tests/unit/observability/background_flusher_test.rs
//! - tests/integration/observability_queue_integration_test.rs

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use velostream::velostream::observability::{ObservabilityManager, SharedObservabilityManager};
use velostream::velostream::sql::execution::config::{StreamingConfig, TracingConfig};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

// OpenTelemetry imports for mock exporters
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use opentelemetry_sdk::InstrumentationLibrary;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use std::borrow::Cow;

/// Well-known upstream traceparent for testing
pub const UPSTREAM_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// Create a basic test record with no trace headers
pub fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(150.0));
    StreamRecord {
        fields,
        timestamp: 1700000000000,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

/// Create a test record with an upstream traceparent header
pub fn create_test_record_with_traceparent(traceparent: &str) -> StreamRecord {
    let mut record = create_test_record();
    record
        .headers
        .insert("traceparent".to_string(), traceparent.to_string());
    record
}

/// Create a test record with arbitrary headers
pub fn create_test_record_with_headers(headers: HashMap<String, String>) -> StreamRecord {
    let mut record = create_test_record();
    record.headers = headers;
    record
}

/// Create a test observability manager with in-memory span collection
pub async fn create_test_observability_manager(service_name: &str) -> SharedObservabilityManager {
    let tracing_config = TracingConfig {
        service_name: service_name.to_string(),
        otlp_endpoint: None,
        ..Default::default()
    };

    let streaming_config = StreamingConfig::default().with_tracing_config(tracing_config);

    let mut manager = ObservabilityManager::from_streaming_config(streaming_config);
    manager
        .initialize()
        .await
        .expect("Failed to initialize observability manager");

    Arc::new(tokio::sync::RwLock::new(manager))
}

/// Extract the trace_id portion from a traceparent header
pub fn extract_trace_id(traceparent: &str) -> &str {
    traceparent.split('-').nth(1).expect("invalid traceparent")
}

/// Extract the span_id portion from a traceparent header
pub fn extract_span_id(traceparent: &str) -> &str {
    traceparent.split('-').nth(2).expect("invalid traceparent")
}

/// Assert that a record has a valid traceparent header injected
pub fn assert_traceparent_injected(record: &StreamRecord, msg: &str) {
    let traceparent = record
        .headers
        .get("traceparent")
        .unwrap_or_else(|| panic!("{}: record should have traceparent header", msg));
    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4, "{}: traceparent should have 4 parts", msg);
    assert_eq!(parts[0], "00", "{}: version should be 00", msg);
    assert_eq!(
        parts[1].len(),
        32,
        "{}: trace_id should be 32 hex chars",
        msg
    );
    assert_eq!(
        parts[2].len(),
        16,
        "{}: span_id should be 16 hex chars",
        msg
    );
}

// ===== Mock Exporters for Testing =====

/// Mock SpanExporter that counts exported spans
///
/// Used for verifying that spans are actually exported via the queue.
/// Thread-safe via Arc<AtomicUsize> for concurrent access.
///
/// # Example
/// ```ignore
/// let export_count = Arc::new(AtomicUsize::new(0));
/// let exporter = Box::new(CountingExporter::new(export_count.clone()));
/// // ... export spans ...
/// assert_eq!(export_count.load(Ordering::Relaxed), 5);
/// ```
#[derive(Debug)]
pub struct CountingExporter {
    pub count: Arc<AtomicUsize>,
}

impl CountingExporter {
    /// Create a new CountingExporter with a shared counter
    pub fn new(count: Arc<AtomicUsize>) -> Self {
        Self { count }
    }
}

impl SpanExporter for CountingExporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = opentelemetry::trace::TraceResult<()>> + Send>,
    > {
        let count = batch.len();
        let counter = self.count.clone();
        Box::pin(async move {
            counter.fetch_add(count, Ordering::Relaxed);
            Ok(())
        })
    }
}

/// Mock SpanExporter that does nothing (no-op)
///
/// Used for testing queue behavior without actual span export.
/// Useful when you only care about queue mechanics, not span content.
///
/// # Example
/// ```ignore
/// let exporter = Box::new(NoOpExporter);
/// // Spans are "exported" but discarded
/// ```
#[derive(Debug)]
pub struct NoOpExporter;

impl SpanExporter for NoOpExporter {
    fn export(
        &mut self,
        _batch: Vec<SpanData>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = opentelemetry::trace::TraceResult<()>> + Send>,
    > {
        Box::pin(async { Ok(()) })
    }
}

// ===== Test Data Factories =====

/// Create a test SpanData with default values
///
/// Generates a valid SpanData structure with:
/// - Trace ID: [0u8; 16]
/// - Span ID: [1u8; 8]
/// - Parent Span ID: [0u8; 8] (no parent)
/// - Name: "test_span"
/// - Kind: Internal
/// - No attributes, events, or links
///
/// # Example
/// ```ignore
/// let span = create_test_span();
/// processor.on_end(span);
/// ```
pub fn create_test_span() -> SpanData {
    let trace_id_bytes = [0u8; 16];
    let span_id_bytes = [1u8; 8];
    let parent_span_id_bytes = [0u8; 8];

    SpanData {
        span_context: SpanContext::new(
            TraceId::from_bytes(trace_id_bytes),
            SpanId::from_bytes(span_id_bytes),
            TraceFlags::default(),
            false,
            TraceState::default(),
        ),
        parent_span_id: SpanId::from_bytes(parent_span_id_bytes),
        span_kind: opentelemetry::trace::SpanKind::Internal,
        name: Cow::Borrowed("test_span"),
        start_time: std::time::SystemTime::now(),
        end_time: std::time::SystemTime::now(),
        attributes: vec![],
        dropped_attributes_count: 0,
        events: opentelemetry_sdk::trace::EvictedQueue::new(128),
        links: opentelemetry_sdk::trace::EvictedQueue::new(128),
        status: opentelemetry::trace::Status::Unset,
        resource: Cow::Owned(Resource::empty()),
        instrumentation_lib: InstrumentationLibrary::default(),
    }
}

/// Create a test SpanData with custom trace and span IDs
///
/// Useful for creating unique spans in loops or when testing trace propagation.
///
/// # Example
/// ```ignore
/// for i in 0..10 {
///     let span = create_test_span_with_ids(i, i);
///     queue.try_send_trace(TraceEvent::Span { span_data: span, timestamp: Instant::now() });
/// }
/// ```
pub fn create_test_span_with_ids(trace_id: u8, span_id: u8) -> SpanData {
    let trace_id_bytes = [trace_id; 16];
    let span_id_bytes = [span_id; 8];
    let parent_span_id_bytes = [0u8; 8];

    SpanData {
        span_context: SpanContext::new(
            TraceId::from_bytes(trace_id_bytes),
            SpanId::from_bytes(span_id_bytes),
            TraceFlags::default(),
            false,
            TraceState::default(),
        ),
        parent_span_id: SpanId::from_bytes(parent_span_id_bytes),
        span_kind: opentelemetry::trace::SpanKind::Internal,
        name: Cow::Borrowed("test_span"),
        start_time: std::time::SystemTime::now(),
        end_time: std::time::SystemTime::now(),
        attributes: vec![],
        dropped_attributes_count: 0,
        events: opentelemetry_sdk::trace::EvictedQueue::new(128),
        links: opentelemetry_sdk::trace::EvictedQueue::new(128),
        status: opentelemetry::trace::Status::Unset,
        resource: Cow::Owned(Resource::empty()),
        instrumentation_lib: InstrumentationLibrary::default(),
    }
}

/// Create a standard test TracingConfig for consistent test setup
///
/// Returns a TracingConfig with:
/// - Service name: "test_service"
/// - Service version: "1.0.0"
/// - No OTLP endpoint (test mode - in-memory spans)
/// - Sampling ratio: 1.0 (sample everything)
/// - Console output disabled
/// - Standard timeouts
///
/// # Example
/// ```ignore
/// let tracing_config = create_test_tracing_config();
/// let wrapper = ObservabilityWrapper::with_observability_and_async_queue(
///     None,
///     Some(tracing_config),
///     ObservabilityQueueConfig::default(),
/// ).await;
/// ```
pub fn create_test_tracing_config() -> TracingConfig {
    TracingConfig {
        service_name: "test_service".to_string(),
        service_version: "1.0.0".to_string(),
        otlp_endpoint: None, // Test mode - no actual export
        sampling_ratio: 1.0,
        enable_console_output: false,
        max_span_duration_seconds: 300,
        batch_export_timeout_ms: 30000,
    }
}

/// Create a custom test TracingConfig with specified service name
///
/// Useful for creating multiple configs with different service names
/// in integration tests.
///
/// # Example
/// ```ignore
/// let config1 = create_test_tracing_config_with_name("service_a");
/// let config2 = create_test_tracing_config_with_name("service_b");
/// ```
pub fn create_test_tracing_config_with_name(service_name: &str) -> TracingConfig {
    TracingConfig {
        service_name: service_name.to_string(),
        service_version: "1.0.0".to_string(),
        otlp_endpoint: None,
        sampling_ratio: 1.0,
        enable_console_output: false,
        max_span_duration_seconds: 300,
        batch_export_timeout_ms: 30000,
    }
}
