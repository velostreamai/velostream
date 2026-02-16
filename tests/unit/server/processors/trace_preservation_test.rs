//! Tests for per-record trace context injection via inject_record_trace_context
//!
//! Verifies that:
//! - RecordSpan creates a span with valid context
//! - inject_record_trace_context adds traceparent to a single output record
//! - Multiple output records can be injected from the same RecordSpan
//! - Multiple unique upstream trace contexts are extracted from input records

use serial_test::serial;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

use crate::unit::observability_test_helpers::create_test_observability_manager;

fn create_record_no_traceparent() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
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

fn create_record_with_traceparent(traceparent: &str) -> StreamRecord {
    let mut record = create_record_no_traceparent();
    record
        .headers
        .insert("traceparent".to_string(), traceparent.to_string());
    record
}

// =============================================================================
// Per-Record inject_record_trace_context Tests
// =============================================================================

#[tokio::test]
#[serial]
async fn test_record_span_creates_valid_context_and_injects_traceparent() {
    let obs_manager = create_test_observability_manager("inject-test").await;
    let obs = Some(obs_manager);

    let record_span = ObservabilityHelper::start_record_span(&obs, "inject-test", None);
    assert!(record_span.is_some(), "Should create record span");
    let record_span = record_span.unwrap();

    // Verify span has valid context
    let span_ctx = record_span
        .span_context()
        .expect("Should have span context");
    assert!(span_ctx.is_valid(), "Span context should be valid");

    // Inject into a single output record
    let mut output_record = Arc::new(create_record_no_traceparent());
    ObservabilityHelper::inject_record_trace_context(&record_span, &mut output_record);

    // Verify traceparent was injected
    let tp = output_record
        .headers
        .get("traceparent")
        .expect("Should have traceparent");

    let expected_trace_id = format!("{}", span_ctx.trace_id());
    assert!(
        tp.contains(&expected_trace_id),
        "Injected traceparent should contain the span's trace_id. Got: {}, expected to contain: {}",
        tp,
        expected_trace_id
    );
}

#[tokio::test]
#[serial]
async fn test_inject_record_trace_context_into_multiple_outputs() {
    let obs_manager = create_test_observability_manager("multi-output-test").await;
    let obs = Some(obs_manager);

    let record_span = ObservabilityHelper::start_record_span(&obs, "multi-output-test", None);
    assert!(record_span.is_some());
    let record_span = record_span.unwrap();

    let span_ctx = record_span
        .span_context()
        .expect("Should have span context");
    let expected_trace_id = format!("{}", span_ctx.trace_id());

    // Inject into multiple output records from the same RecordSpan
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_record_no_traceparent()),
        Arc::new(create_record_no_traceparent()),
        Arc::new(create_record_no_traceparent()),
    ];

    for record in &mut output_records {
        ObservabilityHelper::inject_record_trace_context(&record_span, record);
    }

    // All records should have the same traceparent from the same span
    for (i, record) in output_records.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Record {} should have traceparent", i));
        assert!(
            tp.contains(&expected_trace_id),
            "Record {}: traceparent should contain span's trace_id. Got: {}",
            i,
            tp
        );
    }
}

#[tokio::test]
#[serial]
async fn test_inject_record_trace_context_overwrites_existing_traceparent() {
    let obs_manager = create_test_observability_manager("overwrite-test").await;
    let obs = Some(obs_manager);

    let record_span = ObservabilityHelper::start_record_span(&obs, "overwrite-test", None);
    assert!(record_span.is_some());
    let record_span = record_span.unwrap();

    let span_ctx = record_span
        .span_context()
        .expect("Should have span context");
    let expected_trace_id = format!("{}", span_ctx.trace_id());

    // Record with a pre-existing traceparent
    let old_traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
    let mut output_record = Arc::new(create_record_with_traceparent(old_traceparent));

    // inject_record_trace_context always injects (overwrites)
    ObservabilityHelper::inject_record_trace_context(&record_span, &mut output_record);

    let tp = output_record.headers.get("traceparent").unwrap();
    assert!(
        tp.contains(&expected_trace_id),
        "inject_record_trace_context should overwrite existing traceparent. Got: {}, expected to contain: {}",
        tp,
        expected_trace_id
    );
}

#[test]
fn test_multiple_unique_upstream_traces_extracted() {
    let trace1 = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1-1111111111111111-01";
    let trace2 = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2-2222222222222222-01";
    let trace3 = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3-3333333333333333-01";

    let batch_records = vec![
        create_record_with_traceparent(trace1),
        create_record_with_traceparent(trace1), // duplicate of trace1
        create_record_with_traceparent(trace2),
        create_record_with_traceparent(trace3),
        create_record_with_traceparent(trace2), // duplicate of trace2
    ];

    // Collect unique trace contexts (same logic used in per-record sampling)
    let mut upstream_contexts = Vec::new();
    let mut seen_trace_ids = HashSet::new();

    for record in &batch_records {
        if let Some(ctx) = trace_propagation::extract_trace_context(&record.headers) {
            if seen_trace_ids.insert(ctx.trace_id()) {
                upstream_contexts.push(ctx);
            }
        }
    }

    assert_eq!(
        upstream_contexts.len(),
        3,
        "Should extract exactly 3 unique trace contexts from 5 records"
    );

    // Verify the trace IDs are the expected ones
    let extracted_trace_ids: Vec<String> = upstream_contexts
        .iter()
        .map(|ctx| format!("{}", ctx.trace_id()))
        .collect();

    assert!(extracted_trace_ids.contains(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1".to_string()));
    assert!(extracted_trace_ids.contains(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2".to_string()));
    assert!(extracted_trace_ids.contains(&"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3".to_string()));
}

#[test]
fn test_case_insensitive_traceparent_detection() {
    let mut record = create_record_no_traceparent();
    record.headers.insert(
        "Traceparent".to_string(),
        "00-abababababababababababababababab-cdcdcdcdcdcdcdcd-01".to_string(),
    );

    // The preservation logic should detect "Traceparent" (capitalized) as existing
    let has_traceparent = record
        .headers
        .keys()
        .any(|k| k.eq_ignore_ascii_case("traceparent"));

    assert!(
        has_traceparent,
        "Should detect traceparent regardless of case"
    );
}

#[tokio::test]
#[serial]
async fn test_record_span_set_output_count_and_success() {
    let obs_manager = create_test_observability_manager("set-attrs-test").await;
    let obs = Some(obs_manager);

    let record_span = ObservabilityHelper::start_record_span(&obs, "attr-test", None);
    assert!(record_span.is_some());
    let mut record_span = record_span.unwrap();

    // These should not panic
    record_span.set_output_count(5);
    record_span.set_success();

    // Verify the span still has valid context after setting attributes
    let ctx = record_span.span_context();
    assert!(
        ctx.is_some(),
        "Span should still have context after setting attributes"
    );
    assert!(ctx.unwrap().is_valid(), "Context should still be valid");
}

#[tokio::test]
#[serial]
async fn test_record_span_set_error() {
    let obs_manager = create_test_observability_manager("set-error-test").await;
    let obs = Some(obs_manager);

    let record_span = ObservabilityHelper::start_record_span(&obs, "error-test", None);
    assert!(record_span.is_some());
    let mut record_span = record_span.unwrap();

    // Setting error should not panic
    record_span.set_error("test error message");

    // Span should still have valid context
    let ctx = record_span.span_context();
    assert!(ctx.is_some(), "Span should still have context after error");
}
