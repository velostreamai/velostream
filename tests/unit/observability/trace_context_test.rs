// Unit tests for OpenTelemetry trace context propagation
//
// These tests verify that span context extraction and parent-child linking work correctly.
// Tests run serially to avoid conflicts with OpenTelemetry's global tracer provider singleton.
//
// NOTE: Tests are marked with #[serial] to run one at a time, preventing race conditions
// when initializing/using the global tracer provider.

use opentelemetry::trace::{SpanId, TraceId};
use serial_test::serial;
use std::sync::{Mutex, OnceLock};
use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::sql::execution::config::TracingConfig;

// Global telemetry provider that's initialized once and never dropped
static TELEMETRY_PROVIDER: OnceLock<TelemetryProvider> = OnceLock::new();
// Mutex to ensure initialization happens only once across threads
static INIT_LOCK: Mutex<()> = Mutex::new(());

/// Get or create the shared TelemetryProvider for all tests
/// This ensures the global tracer provider is only initialized once
/// Uses a separate thread to avoid "runtime within runtime" errors
fn get_telemetry() -> &'static TelemetryProvider {
    TELEMETRY_PROVIDER.get_or_init(|| {
        let _guard = INIT_LOCK.lock().unwrap();

        // Spawn a separate thread to initialize the provider
        // This avoids the "Cannot start a runtime from within a runtime" error
        let handle = std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let mut config = TracingConfig::default();
                config.service_name = "test-service".to_string();
                config.otlp_endpoint = None; // No actual export in tests

                TelemetryProvider::new(config)
                    .await
                    .expect("Failed to create telemetry provider")
            })
        });

        handle
            .join()
            .expect("Failed to initialize telemetry provider")
    })
}

#[tokio::test]
#[serial]
async fn test_record_span_provides_context_when_active() {
    // Get shared telemetry provider (initialized once)
    let telemetry = get_telemetry();

    // Create record span
    let record_span = telemetry.start_record_span("test-job", None);

    // Verify span_context() returns valid SpanContext when active
    let span_context = record_span.span_context();
    assert!(
        span_context.is_some(),
        "Active RecordSpan should provide a valid SpanContext"
    );

    let ctx = span_context.unwrap();

    // Verify trace ID and span ID are set (not default/zero values)
    assert_ne!(ctx.trace_id(), TraceId::INVALID, "Trace ID should be set");
    assert_ne!(ctx.span_id(), SpanId::INVALID, "Span ID should be set");

    // Verify span context is valid
    assert!(ctx.is_valid(), "SpanContext should be valid");
}

#[tokio::test]
#[serial]
async fn test_streaming_span_accepts_parent_context() {
    // Get shared telemetry provider
    let telemetry = get_telemetry();

    // Create record span (parent)
    let record_span = telemetry.start_record_span("test-job", None);

    // Extract parent context
    let parent_context = record_span.span_context();
    assert!(
        parent_context.is_some(),
        "Parent span should provide context"
    );

    let parent_ctx = parent_context.clone().unwrap();
    let parent_trace_id = parent_ctx.trace_id();

    // Create streaming span with parent context - should not panic
    let streaming_span =
        telemetry.start_streaming_span("test-job", "deserialization", 100, parent_context);

    // The test passes if we can create the span without errors
    // In a real scenario, the span would inherit the parent's trace ID
    drop(streaming_span);
    drop(record_span);

    // Verify we captured the parent trace ID for potential linking
    assert_ne!(
        parent_trace_id,
        TraceId::INVALID,
        "Parent trace ID should be valid"
    );
}

#[tokio::test]
#[serial]
async fn test_job_lifecycle_span_accepts_parent_context() {
    // Get shared telemetry provider
    let telemetry = get_telemetry();

    // Create record span (parent)
    let record_span = telemetry.start_record_span("test-job", None);

    // Extract parent context
    let parent_context = record_span.span_context();
    assert!(
        parent_context.is_some(),
        "Parent span should provide context"
    );

    // Create job lifecycle span with parent context - should not panic
    let lifecycle_span = telemetry.start_job_lifecycle_span("test-job", "execute", parent_context);

    // The test passes if we can create the span without errors
    drop(lifecycle_span);
    drop(record_span);
}

#[tokio::test]
#[serial]
async fn test_streaming_span_without_parent_context() {
    // Get shared telemetry provider
    let telemetry = get_telemetry();

    // Create streaming span without parent context (None) - should not panic
    let span = telemetry.start_streaming_span("test-job", "operation", 50, None);

    // The test passes if we can create the span without errors
    drop(span);
}

#[tokio::test]
#[serial]
async fn test_job_lifecycle_span_without_parent_context() {
    // Get shared telemetry provider
    let telemetry = get_telemetry();

    // Create job lifecycle span without parent context (None) - should not panic
    let span = telemetry.start_job_lifecycle_span("test-job", "submit", None);

    // The test passes if we can create the span without errors
    drop(span);
}

#[tokio::test]
#[serial]
async fn test_multiple_children_can_extract_same_parent_context() {
    // Get shared telemetry provider
    let telemetry = get_telemetry();

    // Create record span (parent)
    let record_span = telemetry.start_record_span("test-job", None);

    // Extract parent context
    let parent_context = record_span.span_context();
    assert!(
        parent_context.is_some(),
        "Parent span should provide context"
    );

    let parent_trace_id = parent_context.as_ref().unwrap().trace_id();

    // Create multiple child spans with the same parent context
    let deser_span =
        telemetry.start_streaming_span("test-job", "deserialization", 100, parent_context.clone());

    let lifecycle_span =
        telemetry.start_job_lifecycle_span("test-job", "execute", parent_context.clone());

    let ser_span =
        telemetry.start_streaming_span("test-job", "serialization", 100, parent_context.clone());

    // All spans should be created successfully without panics
    drop(deser_span);
    drop(lifecycle_span);
    drop(ser_span);
    drop(record_span);

    // Verify parent trace ID was valid and could be shared
    assert_ne!(
        parent_trace_id,
        TraceId::INVALID,
        "Parent trace ID should be valid"
    );
}

#[tokio::test]
#[serial]
async fn test_record_span_context_method_exists_and_is_callable() {
    // This test verifies that the span_context() method exists and is callable
    // on RecordSpan, which is the key requirement for parent-child linking
    let telemetry = get_telemetry();
    let record_span = telemetry.start_record_span("test-job", None);

    // The fact that this compiles proves the method exists
    let _ = record_span.span_context();

    // Test passes if the method is callable
}

#[tokio::test]
#[serial]
async fn test_record_span_with_upstream_context() {
    // Get shared telemetry provider
    let telemetry = get_telemetry();

    // Create a parent span and extract its context
    let parent_span = telemetry.start_record_span("parent-job", None);
    let parent_ctx = parent_span.span_context().expect("Should have context");
    let parent_trace_id = parent_ctx.trace_id();

    // Create a child record span using the parent context
    let child_span = telemetry.start_record_span("child-job", Some(&parent_ctx));
    let child_ctx = child_span.span_context().expect("Should have context");

    // Child should inherit the parent's trace ID
    assert_eq!(
        child_ctx.trace_id(),
        parent_trace_id,
        "Child record span should inherit parent's trace ID"
    );

    // But have its own unique span ID
    assert_ne!(
        child_ctx.span_id(),
        parent_ctx.span_id(),
        "Child record span should have unique span ID"
    );

    drop(child_span);
    drop(parent_span);
}
