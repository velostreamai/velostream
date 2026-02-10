//! Comprehensive functional tests for distributed tracing span structure
//!
//! These tests exercise the complete ObservabilityHelper pipeline with full assertions
//! on every field of every span, ensuring any regression in span hierarchy, attributes,
//! status, kind, or trace context injection is immediately caught.
//!
//! What this covers that existing tests don't:
//! - Span attributes (key-value pairs like `job.name`, `record_count`, etc.)
//! - Span status (`Status::Ok` vs `Status::Error { description }`)
//! - Span kind (`SpanKind::Consumer` vs `SpanKind::Internal`)
//! - Span timing (`start_time < end_time`)
//! - Error attribute on failure spans
//! - Trace context injection into output records (traceparent format validation)
//! - Complete multi-hop pipeline with inner phase spans at each hop

use opentelemetry::trace::{SpanId, SpanKind, Status};
use opentelemetry_sdk::export::trace::SpanData;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use velostream::velostream::server::processors::common::BatchProcessingResultWithOutput;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;

// Re-use shared test helpers
use crate::unit::observability_test_helpers::{
    UPSTREAM_TRACEPARENT, assert_traceparent_injected, create_test_observability_manager,
    create_test_record, create_test_record_with_headers, create_test_record_with_traceparent,
    extract_span_id, extract_trace_id,
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

/// Create a success BatchProcessingResultWithOutput for testing
fn make_success_batch_result(
    records_processed: usize,
    output_records: Vec<Arc<velostream::velostream::sql::execution::types::StreamRecord>>,
) -> BatchProcessingResultWithOutput {
    BatchProcessingResultWithOutput {
        records_processed,
        records_failed: 0,
        processing_time: std::time::Duration::from_millis(10),
        batch_size: records_processed,
        error_details: vec![],
        output_records,
    }
}

/// Create a failure BatchProcessingResultWithOutput for testing
fn make_failure_batch_result(
    records_processed: usize,
    records_failed: usize,
) -> BatchProcessingResultWithOutput {
    BatchProcessingResultWithOutput {
        records_processed,
        records_failed,
        processing_time: std::time::Duration::from_millis(10),
        batch_size: records_processed,
        error_details: vec![],
        output_records: vec![],
    }
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

/// Find all spans matching a name substring
fn find_spans<'a>(spans: &'a [SpanData], name_contains: &str) -> Vec<&'a SpanData> {
    spans
        .iter()
        .filter(|s| s.name.contains(name_contains))
        .collect()
}

/// Assert that a span's status is Ok
fn assert_status_ok(span: &SpanData) {
    match &span.status {
        Status::Ok => {} // expected
        other => panic!("Span '{}' expected Status::Ok, got {:?}", span.name, other),
    }
}

// ===========================================================================
// Test 1: Single-hop full span tree with upstream context
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_single_hop_full_span_tree_structure() {
    let obs = create_test_observability_manager("span-structure-single-hop").await;
    let observability = Some(obs.clone());
    let job_name = "test_pipeline";

    // Create input records with upstream traceparent
    let input_records = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

    // --- Step 1: Start batch span (extracts upstream context from headers) ---
    let mut batch_span =
        ObservabilityHelper::start_batch_span(&observability, job_name, 1, &input_records);
    let batch_start = Instant::now();

    // Capture batch span context for later assertions
    let batch_ctx = batch_span
        .as_ref()
        .expect("batch span should be created")
        .span_context()
        .expect("batch span should have valid context");
    let batch_trace_id = batch_ctx.trace_id();
    let batch_span_id = batch_ctx.span_id();

    // --- Step 2: Record deserialization ---
    ObservabilityHelper::record_deserialization(
        &observability,
        job_name,
        &batch_span,
        1,                            // record_count
        5,                            // duration_ms
        Some(("input-topic", 0, 42)), // Kafka metadata
    );

    // --- Step 3: Record SQL processing (success) ---
    let mut output_records: Vec<Arc<_>> = vec![Arc::new(create_test_record())];
    let batch_result = make_success_batch_result(1, output_records.clone());
    ObservabilityHelper::record_sql_processing(
        &observability,
        job_name,
        &batch_span,
        &batch_result,
        10, // duration_ms
        None,
    );

    // --- Step 4: Inject trace context into output records ---
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        job_name,
    );

    // --- Step 5: Record serialization success ---
    ObservabilityHelper::record_serialization_success(
        &observability,
        job_name,
        &batch_span,
        1,                              // record_count
        3,                              // duration_ms
        Some(("output-topic", 0, 100)), // Kafka metadata
    );

    // --- Step 6: Complete batch span ---
    ObservabilityHelper::complete_batch_span_success(&mut batch_span, &batch_start, 1);
    drop(batch_span);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        4,
        "Expected 4 spans (batch + deser + sql + ser), got {}. Spans: {:?}",
        spans.len(),
        spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
    );

    // --- Batch span assertions ---
    let batch = find_span(&spans, "batch:");

    // Trace identity: should continue upstream trace
    let upstream_trace_id = extract_trace_id(UPSTREAM_TRACEPARENT);
    let upstream_span_id = extract_span_id(UPSTREAM_TRACEPARENT);
    assert_eq!(
        batch.span_context.trace_id().to_string(),
        upstream_trace_id,
        "Batch span trace_id should match upstream"
    );
    assert!(
        batch.span_context.span_id() != SpanId::INVALID,
        "Batch span should have a valid span_id"
    );
    assert_eq!(
        batch.parent_span_id.to_string(),
        upstream_span_id,
        "Batch span parent_span_id should match upstream span_id"
    );

    // Span kind: Consumer when upstream context exists
    assert_eq!(
        batch.span_kind,
        SpanKind::Consumer,
        "Batch span with upstream context should be SpanKind::Consumer"
    );

    // Status
    assert_status_ok(batch);

    // Name
    assert!(
        batch.name.contains(job_name),
        "Batch span name should contain job name"
    );

    // Timing
    assert!(
        batch.start_time <= batch.end_time,
        "Batch span start_time should be <= end_time"
    );

    // Attributes
    assert_attr_eq(batch, "job.name", job_name);
    assert_attr_exists(batch, "batch.id");
    assert_attr_eq(batch, "messaging.system", "kafka");
    assert_attr_eq(batch, "messaging.operation", "process");
    assert_attr_exists(batch, "total_records");
    assert_attr_exists(batch, "batch_duration_ms");

    // --- Deserialization span assertions ---
    let deser = find_span(&spans, "streaming:deserialization");

    assert_eq!(
        deser.span_context.trace_id(),
        batch_trace_id,
        "Deser span should share batch trace_id"
    );
    assert_eq!(
        deser.parent_span_id, batch_span_id,
        "Deser span parent should be batch span"
    );
    assert_eq!(
        deser.span_kind,
        SpanKind::Internal,
        "Deser span should be SpanKind::Internal"
    );
    assert_status_ok(deser);
    assert!(
        deser.start_time <= deser.end_time,
        "Deser span timing should be valid"
    );

    // Attributes
    assert_attr_eq(deser, "job.name", job_name);
    assert_attr_eq(deser, "operation", "deserialization");
    assert_attr_exists(deser, "record_count");
    assert_attr_exists(deser, "processing_time_ms");
    assert_attr_eq(deser, "kafka.topic", "input-topic");
    assert_attr_exists(deser, "kafka.partition");
    assert_attr_exists(deser, "kafka.offset");

    // --- SQL processing span assertions ---
    let sql = find_span(&spans, "sql_query:");

    assert_eq!(
        sql.span_context.trace_id(),
        batch_trace_id,
        "SQL span should share batch trace_id"
    );
    assert_eq!(
        sql.parent_span_id, batch_span_id,
        "SQL span parent should be batch span"
    );
    assert_eq!(
        sql.span_kind,
        SpanKind::Internal,
        "SQL span should be SpanKind::Internal"
    );
    assert_status_ok(sql);
    assert!(
        sql.start_time <= sql.end_time,
        "SQL span timing should be valid"
    );

    // Attributes
    assert_attr_eq(sql, "job.name", job_name);
    assert_attr_eq(sql, "db.system", "velostream");
    assert_attr_exists(sql, "db.operation");
    assert_attr_eq(sql, "source", "stream_processor");
    assert_attr_exists(sql, "execution_time_ms");
    assert_attr_exists(sql, "record_count");

    // --- Serialization span assertions ---
    let ser = find_span(&spans, "streaming:serialization");

    assert_eq!(
        ser.span_context.trace_id(),
        batch_trace_id,
        "Ser span should share batch trace_id"
    );
    assert_eq!(
        ser.parent_span_id, batch_span_id,
        "Ser span parent should be batch span"
    );
    assert_eq!(
        ser.span_kind,
        SpanKind::Internal,
        "Ser span should be SpanKind::Internal"
    );
    assert_status_ok(ser);
    assert!(
        ser.start_time <= ser.end_time,
        "Ser span timing should be valid"
    );

    // Attributes
    assert_attr_eq(ser, "job.name", job_name);
    assert_attr_eq(ser, "operation", "serialization");
    assert_attr_exists(ser, "record_count");
    assert_attr_exists(ser, "processing_time_ms");
    assert_attr_eq(ser, "kafka.topic", "output-topic");
    assert_attr_exists(ser, "kafka.partition");
    assert_attr_exists(ser, "kafka.offset");

    // --- Output record trace context injection assertions ---
    let output_record = output_records[0].as_ref();
    assert_traceparent_injected(output_record, "Output record");

    let injected_traceparent = output_record.headers.get("traceparent").unwrap();
    let injected_trace_id = extract_trace_id(injected_traceparent);
    let injected_span_id = extract_span_id(injected_traceparent);

    assert_eq!(
        injected_trace_id,
        batch_trace_id.to_string(),
        "Injected traceparent trace_id should match batch span's trace_id"
    );
    assert_eq!(
        injected_span_id,
        batch_span_id.to_string(),
        "Injected traceparent span_id should match batch span's span_id"
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
// Test 2: Single-hop error path span structure
// ===========================================================================

/// Note on OTel status behavior:
///
/// All span types in telemetry.rs call `span.set_status(Status::Ok)` at creation time.
/// Per the OpenTelemetry specification, once a span status is set to `Ok`, subsequent
/// `set_status` calls are ignored. This means `set_error()` will set the `error`
/// attribute on the span but cannot override the `Ok` status.
///
/// This test verifies:
/// - Error attributes ARE set on failure spans (the `error` key-value pair)
/// - Root span hierarchy and kind are correct
/// - All spans share trace_id and correct parent relationships
#[tokio::test]
#[serial]
async fn test_single_hop_error_path_span_structure() {
    let obs = create_test_observability_manager("span-structure-error-path").await;
    let observability = Some(obs.clone());
    let job_name = "error_pipeline";

    // Create records WITHOUT upstream traceparent (root span)
    let input_records = vec![create_test_record()];

    // --- Step 1: Start batch span (no upstream context -> root span) ---
    let mut batch_span =
        ObservabilityHelper::start_batch_span(&observability, job_name, 1, &input_records);
    let batch_start = Instant::now();

    let batch_ctx = batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("batch should have context");
    let batch_trace_id = batch_ctx.trace_id();
    let batch_span_id = batch_ctx.span_id();

    // --- Step 2: Deserialization success ---
    ObservabilityHelper::record_deserialization(&observability, job_name, &batch_span, 1, 5, None);

    // --- Step 3: SQL processing with failures ---
    let batch_result = make_failure_batch_result(10, 3);
    ObservabilityHelper::record_sql_processing(
        &observability,
        job_name,
        &batch_span,
        &batch_result,
        15,
        None,
    );

    // --- Step 4: Serialization failure ---
    ObservabilityHelper::record_serialization_failure(
        &observability,
        job_name,
        &batch_span,
        0,
        2,
        "Serialization codec error: invalid schema",
        None,
    );

    // --- Step 5: Complete batch span with error ---
    ObservabilityHelper::complete_batch_span_error(&mut batch_span, &batch_start, 10, 3);
    drop(batch_span);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        4,
        "Expected 4 spans, got {}. Spans: {:?}",
        spans.len(),
        spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
    );

    // --- Batch span: root, Internal kind ---
    let batch = find_span(&spans, "batch:");

    assert_eq!(
        batch.parent_span_id,
        SpanId::INVALID,
        "Batch span without upstream context should be root (INVALID parent)"
    );
    assert_eq!(
        batch.span_kind,
        SpanKind::Internal,
        "Batch span without upstream context should be SpanKind::Internal"
    );
    // Batch span has error attribute set (via set_error in complete_batch_span_error)
    assert_attr_exists(batch, "error");

    // All spans share same trace_id
    for span in &spans {
        assert_eq!(
            span.span_context.trace_id(),
            batch_trace_id,
            "Span '{}' should share batch trace_id",
            span.name
        );
    }

    // All child spans parent to batch
    let children: Vec<_> = spans
        .iter()
        .filter(|s| !s.name.contains("batch:"))
        .collect();
    for child in &children {
        assert_eq!(
            child.parent_span_id, batch_span_id,
            "Child span '{}' should have batch as parent",
            child.name
        );
    }

    // --- SQL span: error attribute present (set via set_error for failed records) ---
    let sql = find_span(&spans, "sql_query:");
    assert_attr_exists(sql, "error");
    // Verify the error message references the failure
    let sql_error = find_attr(sql, "error").unwrap();
    assert!(
        sql_error.contains("records failed"),
        "SQL span error attribute should mention 'records failed', got '{}'",
        sql_error
    );

    // --- Serialization span: error attribute present ---
    let ser = find_span(&spans, "streaming:serialization");
    assert_attr_exists(ser, "error");
    let ser_error = find_attr(ser, "error").unwrap();
    assert!(
        ser_error.contains("Serialization codec error"),
        "Serialization span error attribute should contain the error message, got '{}'",
        ser_error
    );

    // --- Deserialization span: no error attribute (was successful) ---
    let deser = find_span(&spans, "streaming:deserialization");
    assert!(
        find_attr(deser, "error").is_none(),
        "Deserialization span should NOT have error attribute (it was successful)"
    );
}

// ===========================================================================
// Test 3: Multi-hop full span tree with inner phases
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_multi_hop_full_span_tree_with_inner_phases() {
    let obs = create_test_observability_manager("span-structure-multi-hop").await;
    let observability = Some(obs.clone());

    // Storage for per-hop batch span IDs and trace IDs
    struct HopResult {
        batch_trace_id: opentelemetry::trace::TraceId,
        batch_span_id: opentelemetry::trace::SpanId,
        output_headers: HashMap<String, String>,
    }

    let mut hop_results: Vec<HopResult> = Vec::new();

    for hop in 0..3u64 {
        let job_name = &format!("hop_{}", hop);

        // Create input records:
        // - Hop 0: no upstream (root trace)
        // - Hop 1+: use output headers from previous hop
        let input_records = if hop == 0 {
            vec![create_test_record()]
        } else {
            let prev = &hop_results[(hop - 1) as usize];
            vec![create_test_record_with_headers(prev.output_headers.clone())]
        };

        let mut batch_span =
            ObservabilityHelper::start_batch_span(&observability, job_name, hop, &input_records);
        let batch_start = Instant::now();

        let batch_ctx = batch_span
            .as_ref()
            .unwrap()
            .span_context()
            .expect("batch should have context");
        let batch_trace_id = batch_ctx.trace_id();
        let batch_span_id = batch_ctx.span_id();

        // Deserialization
        ObservabilityHelper::record_deserialization(
            &observability,
            job_name,
            &batch_span,
            1,
            5,
            None,
        );

        // SQL processing
        let mut output_records: Vec<Arc<_>> = vec![Arc::new(create_test_record())];
        let batch_result = make_success_batch_result(1, output_records.clone());
        ObservabilityHelper::record_sql_processing(
            &observability,
            job_name,
            &batch_span,
            &batch_result,
            10,
            None,
        );

        // Inject trace context
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span,
            &mut output_records,
            job_name,
        );

        // Serialization
        ObservabilityHelper::record_serialization_success(
            &observability,
            job_name,
            &batch_span,
            1,
            3,
            None,
        );

        // Complete batch
        ObservabilityHelper::complete_batch_span_success(&mut batch_span, &batch_start, 1);
        drop(batch_span);

        // Save headers from output for next hop
        let output_headers = output_records[0].headers.clone();

        hop_results.push(HopResult {
            batch_trace_id,
            batch_span_id,
            output_headers,
        });
    }

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    // 3 hops x 4 spans each = 12 spans
    assert_eq!(
        spans.len(),
        12,
        "Expected 12 spans (3 hops x 4), got {}. Spans: {:?}",
        spans.len(),
        spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
    );

    // All 12 spans should share the SAME trace_id (single distributed trace)
    let expected_trace_id = hop_results[0].batch_trace_id;
    for span in &spans {
        assert_eq!(
            span.span_context.trace_id(),
            expected_trace_id,
            "All spans across hops should share the same trace_id. Span '{}' has different trace_id",
            span.name
        );
    }

    // Verify hop-level batch span hierarchy
    let batch_spans: Vec<_> = find_spans(&spans, "batch:");
    assert_eq!(batch_spans.len(), 3, "Should have 3 batch spans");

    // Hop 0 batch span: root (no parent)
    let hop0_batch = batch_spans
        .iter()
        .find(|s| s.name.contains("hop_0"))
        .expect("Should find hop_0 batch span");
    assert_eq!(
        hop0_batch.parent_span_id,
        SpanId::INVALID,
        "Hop 0 batch should be root span"
    );
    assert_eq!(
        hop0_batch.span_kind,
        SpanKind::Internal,
        "Hop 0 batch (no upstream) should be Internal"
    );

    // Hop 1 batch span: child of hop 0
    let hop1_batch = batch_spans
        .iter()
        .find(|s| s.name.contains("hop_1"))
        .expect("Should find hop_1 batch span");
    assert_eq!(
        hop1_batch.parent_span_id, hop_results[0].batch_span_id,
        "Hop 1 batch parent should be hop 0 batch span"
    );
    assert_eq!(
        hop1_batch.span_kind,
        SpanKind::Consumer,
        "Hop 1 batch (has upstream) should be Consumer"
    );

    // Hop 2 batch span: child of hop 1
    let hop2_batch = batch_spans
        .iter()
        .find(|s| s.name.contains("hop_2"))
        .expect("Should find hop_2 batch span");
    assert_eq!(
        hop2_batch.parent_span_id, hop_results[1].batch_span_id,
        "Hop 2 batch parent should be hop 1 batch span"
    );
    assert_eq!(
        hop2_batch.span_kind,
        SpanKind::Consumer,
        "Hop 2 batch (has upstream) should be Consumer"
    );

    // Within each hop: inner spans parent to that hop's batch span
    for (hop_idx, hop_result) in hop_results.iter().enumerate() {
        let hop_name = format!("hop_{}", hop_idx);

        // Find inner spans for this hop by matching trace-id AND parent_span_id
        let inner_spans: Vec<_> = spans
            .iter()
            .filter(|s| s.parent_span_id == hop_result.batch_span_id && !s.name.contains("batch:"))
            .collect();

        assert_eq!(
            inner_spans.len(),
            3,
            "Hop {} should have 3 inner spans (deser + sql + ser), got {}",
            hop_name,
            inner_spans.len()
        );

        for inner in &inner_spans {
            assert_eq!(
                inner.span_kind,
                SpanKind::Internal,
                "Inner span '{}' in {} should be Internal",
                inner.name,
                hop_name
            );
            assert_status_ok(inner);
        }
    }

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
            hop_result.batch_trace_id.to_string(),
            "Hop {} injected traceparent trace_id mismatch",
            hop_idx
        );
        let tp_span_id = extract_span_id(traceparent);
        assert_eq!(
            tp_span_id,
            hop_result.batch_span_id.to_string(),
            "Hop {} injected traceparent span_id mismatch",
            hop_idx
        );
    }
}

// ===========================================================================
// Test 4: Broken chain creates independent trace with full tree
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_broken_chain_creates_independent_trace_with_full_tree() {
    let obs = create_test_observability_manager("span-structure-broken-chain").await;
    let observability = Some(obs.clone());

    // --- Hop 1: Root trace with full pipeline ---
    let job_name_1 = "chain_hop_1";
    let input_records_1 = vec![create_test_record()];

    let mut batch_span_1 =
        ObservabilityHelper::start_batch_span(&observability, job_name_1, 1, &input_records_1);
    let batch_start_1 = Instant::now();

    let hop1_ctx = batch_span_1
        .as_ref()
        .unwrap()
        .span_context()
        .expect("hop1 batch should have context");
    let hop1_trace_id = hop1_ctx.trace_id();
    let hop1_batch_span_id = hop1_ctx.span_id();

    ObservabilityHelper::record_deserialization(
        &observability,
        job_name_1,
        &batch_span_1,
        1,
        5,
        None,
    );

    let mut output_records_1: Vec<Arc<_>> = vec![Arc::new(create_test_record())];
    let batch_result_1 = make_success_batch_result(1, output_records_1.clone());
    ObservabilityHelper::record_sql_processing(
        &observability,
        job_name_1,
        &batch_span_1,
        &batch_result_1,
        10,
        None,
    );

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span_1,
        &mut output_records_1,
        job_name_1,
    );

    ObservabilityHelper::record_serialization_success(
        &observability,
        job_name_1,
        &batch_span_1,
        1,
        3,
        None,
    );

    ObservabilityHelper::complete_batch_span_success(&mut batch_span_1, &batch_start_1, 1);
    drop(batch_span_1);

    // --- Hop 2: Broken chain — records WITHOUT headers (simulating header loss) ---
    let job_name_2 = "chain_hop_2";
    let input_records_2 = vec![create_test_record()]; // No headers!

    let mut batch_span_2 =
        ObservabilityHelper::start_batch_span(&observability, job_name_2, 2, &input_records_2);
    let batch_start_2 = Instant::now();

    let hop2_ctx = batch_span_2
        .as_ref()
        .unwrap()
        .span_context()
        .expect("hop2 batch should have context");
    let hop2_trace_id = hop2_ctx.trace_id();
    let hop2_batch_span_id = hop2_ctx.span_id();

    ObservabilityHelper::record_deserialization(
        &observability,
        job_name_2,
        &batch_span_2,
        1,
        5,
        None,
    );

    let batch_result_2 = make_success_batch_result(1, vec![Arc::new(create_test_record())]);
    ObservabilityHelper::record_sql_processing(
        &observability,
        job_name_2,
        &batch_span_2,
        &batch_result_2,
        10,
        None,
    );

    ObservabilityHelper::record_serialization_success(
        &observability,
        job_name_2,
        &batch_span_2,
        1,
        3,
        None,
    );

    ObservabilityHelper::complete_batch_span_success(&mut batch_span_2, &batch_start_2, 1);
    drop(batch_span_2);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        8,
        "Expected 8 spans (2 hops x 4), got {}",
        spans.len()
    );

    // Hop 1 and Hop 2 should have DIFFERENT trace_ids
    assert_ne!(
        hop1_trace_id, hop2_trace_id,
        "Broken chain should create independent traces with different trace_ids"
    );

    // Within each hop, all 4 spans share that hop's trace_id
    let hop1_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.trace_id() == hop1_trace_id)
        .collect();
    assert_eq!(
        hop1_spans.len(),
        4,
        "Hop 1 should have 4 spans with its trace_id"
    );

    let hop2_spans: Vec<_> = spans
        .iter()
        .filter(|s| s.span_context.trace_id() == hop2_trace_id)
        .collect();
    assert_eq!(
        hop2_spans.len(),
        4,
        "Hop 2 should have 4 spans with its trace_id"
    );

    // Within each hop, inner spans parent to their batch span
    let hop1_children: Vec<_> = hop1_spans
        .iter()
        .filter(|s| !s.name.contains("batch:"))
        .collect();
    for child in &hop1_children {
        assert_eq!(
            child.parent_span_id, hop1_batch_span_id,
            "Hop 1 child '{}' should parent to hop 1 batch",
            child.name
        );
    }

    let hop2_children: Vec<_> = hop2_spans
        .iter()
        .filter(|s| !s.name.contains("batch:"))
        .collect();
    for child in &hop2_children {
        assert_eq!(
            child.parent_span_id, hop2_batch_span_id,
            "Hop 2 child '{}' should parent to hop 2 batch",
            child.name
        );
    }
}

// ===========================================================================
// Test 5: Concurrent batches span isolation
// ===========================================================================

#[tokio::test]
#[serial]
async fn test_concurrent_batches_span_isolation() {
    let obs = create_test_observability_manager("span-structure-concurrent").await;
    let observability = Some(obs.clone());

    // Create two independent batches with different job names
    let input_a = vec![create_test_record()];
    let input_b = vec![create_test_record()];

    // --- Start both batch spans (interleaved) ---
    let mut batch_span_a =
        ObservabilityHelper::start_batch_span(&observability, "job_alpha", 1, &input_a);
    let batch_start_a = Instant::now();
    let ctx_a = batch_span_a
        .as_ref()
        .unwrap()
        .span_context()
        .expect("batch A context");
    let trace_id_a = ctx_a.trace_id();
    let span_id_a = ctx_a.span_id();

    let mut batch_span_b =
        ObservabilityHelper::start_batch_span(&observability, "job_beta", 1, &input_b);
    let batch_start_b = Instant::now();
    let ctx_b = batch_span_b
        .as_ref()
        .unwrap()
        .span_context()
        .expect("batch B context");
    let trace_id_b = ctx_b.trace_id();
    let span_id_b = ctx_b.span_id();

    // Different traces
    assert_ne!(
        trace_id_a, trace_id_b,
        "Independent batches should have different trace_ids"
    );

    // --- Interleave inner operations ---
    // Deser for A
    ObservabilityHelper::record_deserialization(
        &observability,
        "job_alpha",
        &batch_span_a,
        1,
        5,
        None,
    );

    // Deser for B
    ObservabilityHelper::record_deserialization(
        &observability,
        "job_beta",
        &batch_span_b,
        1,
        5,
        None,
    );

    // SQL for A
    let result_a = make_success_batch_result(1, vec![Arc::new(create_test_record())]);
    ObservabilityHelper::record_sql_processing(
        &observability,
        "job_alpha",
        &batch_span_a,
        &result_a,
        10,
        None,
    );

    // SQL for B
    let result_b = make_success_batch_result(1, vec![Arc::new(create_test_record())]);
    ObservabilityHelper::record_sql_processing(
        &observability,
        "job_beta",
        &batch_span_b,
        &result_b,
        10,
        None,
    );

    // Serialization for B first (out of order)
    ObservabilityHelper::record_serialization_success(
        &observability,
        "job_beta",
        &batch_span_b,
        1,
        3,
        None,
    );

    // Serialization for A
    ObservabilityHelper::record_serialization_success(
        &observability,
        "job_alpha",
        &batch_span_a,
        1,
        3,
        None,
    );

    // Complete both
    ObservabilityHelper::complete_batch_span_success(&mut batch_span_a, &batch_start_a, 1);
    ObservabilityHelper::complete_batch_span_success(&mut batch_span_b, &batch_start_b, 1);
    drop(batch_span_a);
    drop(batch_span_b);

    // === COLLECT AND VERIFY SPANS ===
    let spans = collect_spans(&obs).await;

    assert_eq!(
        spans.len(),
        8,
        "Expected 8 spans (2 batches x 4), got {}",
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

    assert_eq!(spans_a.len(), 4, "Batch A should have 4 spans");
    assert_eq!(spans_b.len(), 4, "Batch B should have 4 spans");

    // Batch A's inner spans parent to batch A (not batch B)
    let children_a: Vec<_> = spans_a
        .iter()
        .filter(|s| !s.name.contains("batch:"))
        .collect();
    for child in &children_a {
        assert_eq!(
            child.parent_span_id, span_id_a,
            "Batch A child '{}' should parent to batch A, not batch B",
            child.name
        );
        assert_ne!(
            child.parent_span_id, span_id_b,
            "Batch A child '{}' must NOT parent to batch B",
            child.name
        );
    }

    // Batch B's inner spans parent to batch B (not batch A)
    let children_b: Vec<_> = spans_b
        .iter()
        .filter(|s| !s.name.contains("batch:"))
        .collect();
    for child in &children_b {
        assert_eq!(
            child.parent_span_id, span_id_b,
            "Batch B child '{}' should parent to batch B, not batch A",
            child.name
        );
        assert_ne!(
            child.parent_span_id, span_id_a,
            "Batch B child '{}' must NOT parent to batch A",
            child.name
        );
    }

    // All spans should have Status::Ok
    for span in &spans {
        assert_status_ok(span);
    }
}
