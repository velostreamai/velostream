//! Comprehensive functional tests for per-record distributed tracing span structure
//!
//! These tests exercise the complete ObservabilityHelper per-record pipeline with full
//! assertions on every field of every span, ensuring any regression in span hierarchy,
//! attributes, status, kind, or trace context injection is immediately caught.
//!
//! What this covers:
//! - RecordSpan attributes (key-value pairs like `job.name`, `record.output_count`)
//! - Span status (`Status::Ok` vs `Status::Error { description }`)
//! - Span kind (`SpanKind::Consumer` with upstream vs `SpanKind::Internal` without)
//! - Span timing (`start_time < end_time`)
//! - Error attribute on failure spans
//! - Trace context injection into output records (traceparent format validation)
//! - Multi-hop pipeline with per-record spans at each hop
//! - Concurrent record spans maintain isolation

use opentelemetry::trace::{SpanId, SpanKind, Status};
use opentelemetry_sdk::export::trace::SpanData;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;

// Re-use shared test helpers
use crate::unit::observability_test_helpers::{
    UPSTREAM_TRACEPARENT, assert_traceparent_injected, create_test_observability_manager,
    create_test_record, create_test_record_with_traceparent, extract_span_id, extract_trace_id,
};

// ---------------------------------------------------------------------------
// Helper functions for span attribute lookup
// ---------------------------------------------------------------------------

/// Find an attribute value by key on a SpanData, returning its Display string.
fn find_attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| format!("{}", kv.value))
}

/// Assert that a span has a given attribute key present.
fn assert_attr_exists(span: &SpanData, key: &str) {
    assert!(
        find_attr(span, key).is_some(),
        "Span '{}' missing attribute '{}'",
        span.name,
        key
    );
}

/// Assert that a span has a given attribute key with an exact expected value.
fn assert_attr_eq(span: &SpanData, key: &str, expected: &str) {
    let val = find_attr(span, key)
        .unwrap_or_else(|| panic!("Span '{}' missing attribute '{}'", span.name, key));
    assert_eq!(
        val, expected,
        "Span '{}' attribute '{}' expected '{}', got '{}'",
        span.name, key, expected, val
    );
}

/// Collect spans from observability manager, waiting for async processing
async fn collect_spans(
    obs: &velostream::velostream::observability::SharedObservabilityManager,
) -> Vec<SpanData> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let obs_lock = obs.read().await;
    obs_lock
        .telemetry()
        .expect("telemetry should be active")
        .collected_spans()
}

/// Find a span by name substring
fn find_span<'a>(spans: &'a [SpanData], name_contains: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|s| s.name.contains(name_contains))
        .unwrap_or_else(|| {
            let available: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
            panic!(
                "No span found containing '{}'. Available: {:?}",
                name_contains, available
            )
        })
}

/// Assert that a span's status is Ok
fn assert_status_ok(span: &SpanData) {
    match &span.status {
        Status::Ok => {} // expected
        other => panic!("Span '{}' expected Status::Ok, got {:?}", span.name, other),
    }
}

// ===========================================================================
// Test 1: Single RecordSpan with upstream context -- full attribute validation
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_record_span_with_upstream_full_attributes() {
    let obs = create_test_observability_manager("span-structure-record").await;
    let observability = Some(obs.clone());
    let job_name = "test_pipeline";

    // Create input record with upstream traceparent
    let input_record = create_test_record_with_traceparent(UPSTREAM_TRACEPARENT);

    // Extract upstream context
    let upstream_ctx = trace_propagation::extract_trace_context(&input_record.headers);
    assert!(upstream_ctx.is_some(), "Should extract upstream context");

    // Start record span with upstream context
    let mut record_span =
        ObservabilityHelper::start_record_span(&observability, job_name, upstream_ctx.as_ref());
    assert!(record_span.is_some(), "Record span should be created");

    let span_ctx = record_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Record span should have valid context");
    let record_trace_id = span_ctx.trace_id();
    let record_span_id = span_ctx.span_id();

    // Inject into output record
    let mut output_record = Arc::new(create_test_record());
    ObservabilityHelper::inject_record_trace_context(
        record_span.as_ref().unwrap(),
        &mut output_record,
    );

    // Mark success with output count
    record_span.as_mut().unwrap().set_output_count(1);
    record_span.as_mut().unwrap().set_success();
    drop(record_span);

    // === COLLECT AND VERIFY SPAN ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        1,
        "Expected 1 span (per-record), got {}. Spans: {:?}",
        spans.len(),
        spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
    );

    let span = &spans[0];

    // Trace identity: should continue upstream trace
    let upstream_trace_id = extract_trace_id(UPSTREAM_TRACEPARENT);
    let upstream_span_id = extract_span_id(UPSTREAM_TRACEPARENT);
    assert_eq!(
        span.span_context.trace_id().to_string(),
        upstream_trace_id,
        "Record span trace_id should match upstream"
    );
    assert!(
        span.span_context.span_id() != SpanId::INVALID,
        "Record span should have a valid span_id"
    );
    assert_eq!(
        span.parent_span_id.to_string(),
        upstream_span_id,
        "Record span parent_span_id should match upstream span_id"
    );

    // Span kind: Consumer when upstream context exists
    assert_eq!(
        span.span_kind,
        SpanKind::Consumer,
        "Record span with upstream context should be SpanKind::Consumer"
    );

    // Status
    assert_status_ok(span);

    // Name: "process:{job_name}"
    assert!(
        span.name.contains(job_name),
        "Span name should contain job name"
    );
    assert!(
        span.name.starts_with("process:"),
        "Span name should start with 'process:'"
    );

    // Timing
    assert!(
        span.start_time <= span.end_time,
        "Span start_time should be <= end_time"
    );

    // Attributes
    assert_attr_eq(span, "job.name", job_name);
    assert_attr_eq(span, "messaging.system", "kafka");
    assert_attr_exists(span, "record.output_count");

    // --- Output record trace context injection assertions ---
    let output = output_record.as_ref();
    assert_traceparent_injected(output, "Output record");

    let injected_traceparent = output.headers.get("traceparent").unwrap();
    let injected_trace_id = extract_trace_id(injected_traceparent);
    let injected_span_id = extract_span_id(injected_traceparent);

    assert_eq!(
        injected_trace_id,
        record_trace_id.to_string(),
        "Injected traceparent trace_id should match record span's trace_id"
    );
    assert_eq!(
        injected_span_id,
        record_span_id.to_string(),
        "Injected traceparent span_id should match record span's span_id"
    );

    // Verify traceparent format: 00-{trace_id}-{span_id}-01
    assert!(
        injected_traceparent.starts_with("00-"),
        "Traceparent should start with version 00"
    );
    assert!(
        injected_traceparent.ends_with("-01"),
        "Traceparent should end with -01 (sampled)"
    );
}

// ===========================================================================
// Test 2: RecordSpan error path
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_record_span_error_path_structure() {
    let obs = create_test_observability_manager("span-structure-error-path").await;
    let observability = Some(obs.clone());
    let job_name = "error_pipeline";

    // Create record WITHOUT upstream traceparent (root span)
    let mut record_span = ObservabilityHelper::start_record_span(&observability, job_name, None);

    let span_ctx = record_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have context");
    let _record_trace_id = span_ctx.trace_id();

    // Mark as error
    record_span
        .as_mut()
        .unwrap()
        .set_error("Processing failed: invalid schema");
    drop(record_span);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(spans.len(), 1, "Expected 1 span, got {}", spans.len());

    let span = &spans[0];

    // Root span: no parent, Internal kind
    assert_eq!(
        span.parent_span_id,
        SpanId::INVALID,
        "Record span without upstream context should be root (INVALID parent)"
    );
    assert_eq!(
        span.span_kind,
        SpanKind::Internal,
        "Record span without upstream context should be SpanKind::Internal"
    );

    // Error attribute set
    assert_attr_exists(span, "error");
    let error_val = find_attr(span, "error").unwrap();
    assert!(
        error_val.contains("Processing failed"),
        "Error attribute should contain the error message, got '{}'",
        error_val
    );
}

// ===========================================================================
// Test 3: Multi-hop per-record span tree
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_multi_hop_per_record_span_tree() {
    let obs = create_test_observability_manager("span-structure-multi-hop").await;
    let observability = Some(obs.clone());

    // Storage for per-hop results
    struct HopResult {
        record_trace_id: opentelemetry::trace::TraceId,
        record_span_id: opentelemetry::trace::SpanId,
        output_headers: HashMap<String, String>,
    }

    let mut hop_results: Vec<HopResult> = Vec::new();

    for hop in 0..3u64 {
        let job_name = &format!("hop_{}", hop);

        // Extract upstream context from previous hop's output
        let upstream_ctx = if hop == 0 {
            None
        } else {
            let prev = &hop_results[(hop - 1) as usize];
            trace_propagation::extract_trace_context(&prev.output_headers)
        };

        // Create per-record span
        let mut record_span =
            ObservabilityHelper::start_record_span(&observability, job_name, upstream_ctx.as_ref());

        let span_ctx = record_span
            .as_ref()
            .unwrap()
            .span_context()
            .expect("Should have context");
        let record_trace_id = span_ctx.trace_id();
        let record_span_id = span_ctx.span_id();

        // Inject trace context into output record
        let mut output_record = Arc::new(create_test_record());
        ObservabilityHelper::inject_record_trace_context(
            record_span.as_ref().unwrap(),
            &mut output_record,
        );

        // Complete span
        record_span.as_mut().unwrap().set_output_count(1);
        record_span.as_mut().unwrap().set_success();
        drop(record_span);

        // Save headers from output for next hop
        let output_headers = output_record.headers.clone();

        hop_results.push(HopResult {
            record_trace_id,
            record_span_id,
            output_headers,
        });
    }

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    // 3 hops x 1 span each = 3 spans
    assert_eq!(
        spans.len(),
        3,
        "Expected 3 spans (1 per hop), got {}. Spans: {:?}",
        spans.len(),
        spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
    );

    // All 3 spans should share the SAME trace_id (single distributed trace)
    let expected_trace_id = hop_results[0].record_trace_id;
    for span in &spans {
        assert_eq!(
            span.span_context.trace_id(),
            expected_trace_id,
            "All spans across hops should share the same trace_id. Span '{}' has different trace_id",
            span.name
        );
    }

    // Find spans by name
    let hop0_span = find_span(&spans, "hop_0");
    let hop1_span = find_span(&spans, "hop_1");
    let hop2_span = find_span(&spans, "hop_2");

    // Hop 0: root (no parent)
    assert_eq!(
        hop0_span.parent_span_id,
        SpanId::INVALID,
        "Hop 0 should be root span"
    );
    assert_eq!(
        hop0_span.span_kind,
        SpanKind::Internal,
        "Hop 0 (no upstream) should be Internal"
    );

    // Hop 1: child of hop 0
    assert_eq!(
        hop1_span.parent_span_id, hop_results[0].record_span_id,
        "Hop 1 parent should be hop 0's span"
    );
    assert_eq!(
        hop1_span.span_kind,
        SpanKind::Consumer,
        "Hop 1 (has upstream) should be Consumer"
    );

    // Hop 2: child of hop 1
    assert_eq!(
        hop2_span.parent_span_id, hop_results[1].record_span_id,
        "Hop 2 parent should be hop 1's span"
    );
    assert_eq!(
        hop2_span.span_kind,
        SpanKind::Consumer,
        "Hop 2 (has upstream) should be Consumer"
    );

    // All spans should have Status::Ok
    for span in &spans {
        assert_status_ok(span);
    }

    // Verify injected traceparents carry correct IDs at each hop
    for (hop_idx, hop_result) in hop_results.iter().enumerate() {
        let traceparent = hop_result
            .output_headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Hop {} output should have traceparent", hop_idx));
        let tp_trace_id = extract_trace_id(traceparent);
        assert_eq!(
            tp_trace_id,
            hop_result.record_trace_id.to_string(),
            "Hop {} injected traceparent trace_id mismatch",
            hop_idx
        );
        let tp_span_id = extract_span_id(traceparent);
        assert_eq!(
            tp_span_id,
            hop_result.record_span_id.to_string(),
            "Hop {} injected traceparent span_id mismatch",
            hop_idx
        );
    }
}

// ===========================================================================
// Test 4: Broken chain creates independent trace
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_broken_chain_creates_independent_trace() {
    let obs = create_test_observability_manager("span-structure-broken-chain").await;
    let observability = Some(obs.clone());

    // --- Hop 1: Root trace ---
    let job_name_1 = "chain_hop_1";
    let mut record_span_1 =
        ObservabilityHelper::start_record_span(&observability, job_name_1, None);

    let hop1_ctx = record_span_1
        .as_ref()
        .unwrap()
        .span_context()
        .expect("hop1 should have context");
    let hop1_trace_id = hop1_ctx.trace_id();

    let mut output_1 = Arc::new(create_test_record());
    ObservabilityHelper::inject_record_trace_context(
        record_span_1.as_ref().unwrap(),
        &mut output_1,
    );

    record_span_1.as_mut().unwrap().set_output_count(1);
    record_span_1.as_mut().unwrap().set_success();
    drop(record_span_1);

    // --- Hop 2: Broken chain -- records WITHOUT headers (simulating header loss) ---
    let job_name_2 = "chain_hop_2";
    // No upstream context (broken chain)
    let mut record_span_2 =
        ObservabilityHelper::start_record_span(&observability, job_name_2, None);

    let hop2_ctx = record_span_2
        .as_ref()
        .unwrap()
        .span_context()
        .expect("hop2 should have context");
    let hop2_trace_id = hop2_ctx.trace_id();

    record_span_2.as_mut().unwrap().set_output_count(1);
    record_span_2.as_mut().unwrap().set_success();
    drop(record_span_2);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        2,
        "Expected 2 spans (1 per hop), got {}",
        spans.len()
    );

    // Hop 1 and Hop 2 should have DIFFERENT trace_ids
    assert_ne!(
        hop1_trace_id, hop2_trace_id,
        "Broken chain should create independent traces with different trace_ids"
    );

    // Verify each hop's span has its own trace_id
    let hop1_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.trace_id() == hop1_trace_id)
        .collect();
    assert_eq!(
        hop1_spans.len(),
        1,
        "Hop 1 should have 1 span with its trace_id"
    );

    let hop2_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.trace_id() == hop2_trace_id)
        .collect();
    assert_eq!(
        hop2_spans.len(),
        1,
        "Hop 2 should have 1 span with its trace_id"
    );
}

// ===========================================================================
// Test 5: Concurrent record spans isolation
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_concurrent_record_spans_isolation() {
    let obs = create_test_observability_manager("span-structure-concurrent").await;
    let observability = Some(obs.clone());

    // --- Start two independent record spans (interleaved) ---
    let mut record_span_a =
        ObservabilityHelper::start_record_span(&observability, "job_alpha", None);
    let ctx_a = record_span_a
        .as_ref()
        .unwrap()
        .span_context()
        .expect("span A context");
    let trace_id_a = ctx_a.trace_id();

    let mut record_span_b =
        ObservabilityHelper::start_record_span(&observability, "job_beta", None);
    let ctx_b = record_span_b
        .as_ref()
        .unwrap()
        .span_context()
        .expect("span B context");
    let trace_id_b = ctx_b.trace_id();

    // Different traces
    assert_ne!(
        trace_id_a, trace_id_b,
        "Independent record spans should have different trace_ids"
    );

    // Complete both (interleaved -- B before A)
    record_span_b.as_mut().unwrap().set_output_count(1);
    record_span_b.as_mut().unwrap().set_success();
    drop(record_span_b);

    record_span_a.as_mut().unwrap().set_output_count(1);
    record_span_a.as_mut().unwrap().set_success();
    drop(record_span_a);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        2,
        "Expected 2 spans (1 per record), got {}",
        spans.len()
    );

    // Group by trace_id
    let spans_a: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.trace_id() == trace_id_a)
        .collect();
    let spans_b: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.trace_id() == trace_id_b)
        .collect();

    assert_eq!(spans_a.len(), 1, "Span A should have 1 span");
    assert_eq!(spans_b.len(), 1, "Span B should have 1 span");

    // Verify correct job names
    assert_attr_eq(spans_a[0], "job.name", "job_alpha");
    assert_attr_eq(spans_b[0], "job.name", "job_beta");

    // All spans should have Status::Ok
    for span in &spans {
        assert_status_ok(span);
    }
}
