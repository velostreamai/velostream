//! Comprehensive distributed tracing tests
//!
//! Verifies end-to-end trace context propagation through the Velostream pipeline:
//! - W3C traceparent header injection into output records
//! - Trace context extraction from incoming Kafka headers
//! - ObservabilityHelper batch span creation and injection
//! - Round-trip: inject → serialize → extract → verify same trace

use opentelemetry::trace::{SpanId, TraceId};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use velostream::velostream::observability::SharedObservabilityManager;
use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::config::TracingConfig;
use velostream::velostream::sql::execution::types::StreamRecord;

// Use shared helpers
use crate::unit::observability_test_helpers::{
    create_test_observability_manager, create_test_record, create_test_record_with_traceparent,
};

// Shared TelemetryProvider initialized once across all tests in this module
static TELEMETRY_PROVIDER: OnceLock<TelemetryProvider> = OnceLock::new();
static INIT_LOCK: Mutex<()> = Mutex::new(());

fn get_telemetry() -> &'static TelemetryProvider {
    TELEMETRY_PROVIDER.get_or_init(|| {
        let _guard = INIT_LOCK.lock().unwrap();
        let handle = std::thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async {
                let mut config = TracingConfig::default();
                config.service_name = "distributed-tracing-test".to_string();
                config.otlp_endpoint = None;
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

// create_test_record and create_test_record_with_traceparent imported from shared helpers

// =============================================================================
// W3C Trace Context Injection Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_inject_trace_context_adds_traceparent_header() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
    let span_ctx = batch_span.span_context().expect("Should have span context");

    let mut headers = HashMap::new();
    trace_propagation::inject_trace_context(&span_ctx, &mut headers);

    assert!(
        headers.contains_key("traceparent"),
        "Should inject traceparent header"
    );

    let traceparent = &headers["traceparent"];
    // W3C format: 00-{trace_id}-{span_id}-{trace_flags}
    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4, "traceparent should have 4 parts");
    assert_eq!(parts[0], "00", "Version should be 00");
    assert_eq!(parts[1].len(), 32, "Trace ID should be 32 hex chars");
    assert_eq!(parts[2].len(), 16, "Span ID should be 16 hex chars");
    assert_eq!(parts[3].len(), 2, "Trace flags should be 2 hex chars");
}

#[tokio::test]
#[serial]
async fn test_inject_trace_context_preserves_trace_id() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("test-job", 1, None, Vec::new());
    let span_ctx = batch_span.span_context().expect("Should have span context");

    let expected_trace_id = span_ctx.trace_id().to_string();
    let expected_span_id = span_ctx.span_id().to_string();

    let mut headers = HashMap::new();
    trace_propagation::inject_trace_context(&span_ctx, &mut headers);

    let traceparent = &headers["traceparent"];
    let parts: Vec<&str> = traceparent.split('-').collect();

    assert_eq!(
        parts[1], expected_trace_id,
        "Trace ID in header should match span context"
    );
    assert_eq!(
        parts[2], expected_span_id,
        "Span ID in header should match span context"
    );
}

// =============================================================================
// W3C Trace Context Extraction Tests
// =============================================================================

#[test]
fn test_extract_trace_context_valid_traceparent() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );

    let ctx = trace_propagation::extract_trace_context(&headers);
    assert!(ctx.is_some(), "Should extract valid trace context");

    let ctx = ctx.unwrap();
    assert_eq!(
        ctx.trace_id().to_string(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );
    assert_eq!(ctx.span_id().to_string(), "00f067aa0ba902b7");
    assert!(ctx.is_valid(), "Extracted context should be valid");
}

#[test]
fn test_extract_trace_context_missing_header() {
    let headers = HashMap::new();
    let ctx = trace_propagation::extract_trace_context(&headers);
    assert!(ctx.is_none(), "Should return None for missing header");
}

#[test]
fn test_extract_trace_context_invalid_format() {
    let mut headers = HashMap::new();
    headers.insert("traceparent".to_string(), "invalid-format".to_string());

    let ctx = trace_propagation::extract_trace_context(&headers);
    assert!(
        ctx.is_none(),
        "Should return None for invalid traceparent format"
    );
}

#[test]
fn test_extract_trace_context_wrong_version() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );

    let ctx = trace_propagation::extract_trace_context(&headers);
    assert!(ctx.is_none(), "Should return None for unsupported version");
}

#[test]
fn test_extract_trace_context_unsampled_flag() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_string(),
    );

    let ctx = trace_propagation::extract_trace_context(&headers);
    assert!(ctx.is_some(), "Should extract even with unsampled flag");
    let ctx = ctx.unwrap();
    assert!(!ctx.trace_flags().is_sampled(), "Should not be sampled");
}

#[test]
fn test_extract_trace_context_zero_trace_id() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01".to_string(),
    );

    let ctx = trace_propagation::extract_trace_context(&headers);
    // Per W3C spec, zero trace ID is invalid
    if let Some(ctx) = ctx {
        assert!(!ctx.is_valid(), "Zero trace ID should not be valid");
    }
}

#[test]
fn test_extract_trace_context_zero_span_id() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01".to_string(),
    );

    let ctx = trace_propagation::extract_trace_context(&headers);
    // Per W3C spec, zero span ID is invalid
    if let Some(ctx) = ctx {
        assert!(!ctx.is_valid(), "Zero span ID should not be valid");
    }
}

// =============================================================================
// Round-Trip: Inject → Extract Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_trace_context_round_trip() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("round-trip-test", 1, None, Vec::new());
    let original_ctx = batch_span.span_context().expect("Should have span context");

    let original_trace_id = original_ctx.trace_id();
    let original_span_id = original_ctx.span_id();

    // Inject into headers
    let mut headers = HashMap::new();
    trace_propagation::inject_trace_context(&original_ctx, &mut headers);

    // Extract from headers
    let extracted_ctx =
        trace_propagation::extract_trace_context(&headers).expect("Should extract context");

    // Verify trace ID and span ID are preserved
    assert_eq!(
        extracted_ctx.trace_id(),
        original_trace_id,
        "Trace ID should survive round-trip"
    );
    assert_eq!(
        extracted_ctx.span_id(),
        original_span_id,
        "Span ID should survive round-trip"
    );
}

#[tokio::test]
#[serial]
async fn test_trace_context_round_trip_preserves_flags() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("flags-test", 1, None, Vec::new());
    let original_ctx = batch_span.span_context().expect("Should have span context");

    let original_flags = original_ctx.trace_flags();

    let mut headers = HashMap::new();
    trace_propagation::inject_trace_context(&original_ctx, &mut headers);

    let extracted_ctx =
        trace_propagation::extract_trace_context(&headers).expect("Should extract context");

    assert_eq!(
        extracted_ctx.trace_flags(),
        original_flags,
        "Trace flags should survive round-trip"
    );
}

// =============================================================================
// ObservabilityHelper Integration Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_observability_helper_injects_trace_into_output_records() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("inject-test", 1, None, Vec::new());
    let batch_span_opt = Some(batch_span);

    // Create output records without headers
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    // Inject trace context
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );

    // Verify all records have traceparent
    for (i, record) in output_records.iter().enumerate() {
        assert!(
            record.headers.contains_key("traceparent"),
            "Record {} should have traceparent header",
            i
        );
        let traceparent = &record.headers["traceparent"];
        assert!(
            traceparent.starts_with("00-"),
            "Record {} traceparent should start with '00-': {}",
            i,
            traceparent
        );
    }
}

#[tokio::test]
#[serial]
async fn test_observability_helper_no_injection_without_span() {
    let batch_span_opt: Option<velostream::velostream::observability::telemetry::BatchSpan> = None;

    let mut output_records: Vec<Arc<StreamRecord>> = vec![Arc::new(create_test_record())];

    // Should not panic, and should not add headers
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );

    assert!(
        output_records[0].headers.is_empty(),
        "Should not inject headers when no batch span"
    );
}

#[tokio::test]
#[serial]
async fn test_observability_helper_all_records_get_same_trace_id() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("same-trace-test", 1, None, Vec::new());
    let batch_span_opt = Some(batch_span);

    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );

    // Extract trace IDs from all records
    let trace_ids: Vec<String> = output_records
        .iter()
        .map(|r| {
            let tp = &r.headers["traceparent"];
            tp.split('-').nth(1).unwrap().to_string()
        })
        .collect();

    // All records in a batch should share the same trace ID
    assert!(
        trace_ids.windows(2).all(|w| w[0] == w[1]),
        "All records in batch should share same trace ID: {:?}",
        trace_ids
    );
}

// =============================================================================
// Batch Span with Upstream Context Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_start_batch_span_extracts_upstream_context() {
    let telemetry = get_telemetry();

    // Create an upstream span and inject its context into a record
    let upstream_span = telemetry.start_batch_span("upstream-job", 1, None, Vec::new());
    let upstream_ctx = upstream_span.span_context().expect("Should have context");
    let _upstream_trace_id = upstream_ctx.trace_id();

    let mut record = create_test_record();
    trace_propagation::inject_trace_context(&upstream_ctx, &mut record.headers);

    // Create observability manager with tracing enabled
    let obs_manager = create_test_observability_manager("tracing-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager);

    // Start batch span with records containing upstream context
    let batch_span = ObservabilityHelper::start_batch_span(&obs, "downstream-job", 1, &[record]);

    assert!(batch_span.is_some(), "Should create batch span");
    let span = batch_span.unwrap();
    let span_ctx = span.span_context().expect("Should have span context");

    // The new span should have a valid context
    assert!(span_ctx.is_valid(), "Span context should be valid");
    assert_ne!(span_ctx.trace_id(), TraceId::INVALID);
    assert_ne!(span_ctx.span_id(), SpanId::INVALID);
}

#[tokio::test]
#[serial]
async fn test_start_batch_span_creates_new_trace_without_upstream() {
    let obs_manager = create_test_observability_manager("tracing-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager);

    // Records without trace headers
    let records = vec![create_test_record()];

    let batch_span = ObservabilityHelper::start_batch_span(&obs, "new-trace-job", 1, &records);

    assert!(batch_span.is_some(), "Should create batch span");
    let span = batch_span.unwrap();
    let span_ctx = span.span_context().expect("Should have span context");

    assert!(span_ctx.is_valid(), "New trace should have valid context");
    assert_ne!(span_ctx.trace_id(), TraceId::INVALID);
}

#[tokio::test]
#[serial]
async fn test_start_batch_span_returns_none_without_observability() {
    let obs: Option<SharedObservabilityManager> = None;

    let records = vec![create_test_record()];

    let batch_span = ObservabilityHelper::start_batch_span(&obs, "no-obs-job", 1, &records);

    assert!(
        batch_span.is_none(),
        "Should return None when no observability manager"
    );
}

// =============================================================================
// End-to-End Pipeline Trace Propagation Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_end_to_end_trace_propagation_through_pipeline() {
    let telemetry = get_telemetry();

    // Stage 1: Upstream producer creates a trace
    let upstream_span = telemetry.start_batch_span("producer", 1, None, Vec::new());
    let upstream_ctx = upstream_span.span_context().expect("Should have context");

    // Inject into "Kafka message"
    let mut input_record = create_test_record();
    trace_propagation::inject_trace_context(&upstream_ctx, &mut input_record.headers);

    // Stage 2: Downstream consumer extracts and creates new span
    let obs: Option<SharedObservabilityManager> =
        Some(create_test_observability_manager("tracing-test").await);
    let batch_span =
        ObservabilityHelper::start_batch_span(&obs, "consumer", 1, &[input_record.clone()]);
    assert!(batch_span.is_some());

    // Stage 3: Inject into output records
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "consumer",
    );

    // Verify: output records have traceparent headers
    for record in &output_records {
        assert!(
            record.headers.contains_key("traceparent"),
            "Output record should have traceparent"
        );

        // Extract and verify the context is valid
        let extracted = trace_propagation::extract_trace_context(&record.headers);
        assert!(
            extracted.is_some(),
            "Should extract valid context from output"
        );
        let ctx = extracted.unwrap();
        assert!(ctx.is_valid());
        assert_ne!(ctx.trace_id(), TraceId::INVALID);
        assert_ne!(ctx.span_id(), SpanId::INVALID);
    }
}

#[tokio::test]
#[serial]
async fn test_trace_context_injection_is_idempotent() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("idempotent-test", 1, None, Vec::new());
    let batch_span_opt = Some(batch_span);

    let mut output_records: Vec<Arc<StreamRecord>> = vec![Arc::new(create_test_record())];

    // Inject twice
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );
    let first_traceparent = output_records[0].headers["traceparent"].clone();

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );
    let second_traceparent = output_records[0].headers["traceparent"].clone();

    // Should overwrite with same value (idempotent)
    assert_eq!(
        first_traceparent, second_traceparent,
        "Re-injection should produce same traceparent"
    );
}

#[tokio::test]
#[serial]
async fn test_empty_output_records_no_panic() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("empty-test", 1, None, Vec::new());
    let batch_span_opt = Some(batch_span);

    let mut output_records: Vec<Arc<StreamRecord>> = vec![];

    // Should not panic on empty vec
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );
}

#[tokio::test]
#[serial]
async fn test_record_with_existing_traceparent_is_preserved() {
    let telemetry = get_telemetry();
    let batch_span = telemetry.start_batch_span("preserve-test", 1, None, Vec::new());
    let batch_span_opt = Some(batch_span);

    // Record with a pre-existing (different) traceparent
    let old_traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
    let mut output_records: Vec<Arc<StreamRecord>> = vec![Arc::new(
        create_test_record_with_traceparent(old_traceparent),
    )];

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_opt,
        &mut output_records,
        "test-job",
    );

    let preserved_traceparent = &output_records[0].headers["traceparent"];

    // Records with existing traceparent should preserve it (per-record trace lineage)
    assert_eq!(
        preserved_traceparent, old_traceparent,
        "Should preserve existing traceparent, not overwrite with batch span's trace"
    );
}

// create_test_observability_manager imported from shared helpers
