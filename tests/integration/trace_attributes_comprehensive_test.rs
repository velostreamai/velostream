//! Comprehensive test for per-record trace span attributes
//!
//! This test validates that RecordSpan attributes are correctly set:
//! - job.name attribute
//! - messaging.system=kafka attribute
//! - Span name format: "process:{job_name}"
//! - SpanKind::Consumer when upstream context exists
//! - SpanKind::Internal when no upstream context
//! - set_output_count attribute
//! - set_error attribute
//! - set_success status

use opentelemetry::trace::SpanKind;
use opentelemetry_sdk::export::trace::SpanData;
use serial_test::serial;
use std::time::Duration;
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
// Use shared helpers from mod.rs
use super::*;

/// Helper to extract string attribute value from span
fn get_attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::String(s) => Some(s.to_string()),
            _ => None,
        })
}

/// Helper to extract numeric attribute value
fn get_numeric_attribute(span: &SpanData, key: &str) -> Option<i64> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::I64(v) => Some(*v),
            _ => None,
        })
}

/// Test that validates RecordSpan has correct basic attributes (job.name, messaging.system)
#[tokio::test]
#[serial]
async fn test_record_span_basic_attributes() {
    let obs = create_test_observability_manager("test_record_attrs").await;

    // Create a record span without upstream context (root span)
    let mut record_span =
        ObservabilityHelper::start_record_span(&Some(obs.clone()), "market_data_processor", None);
    assert!(record_span.is_some(), "Record span should be created");

    record_span.as_mut().unwrap().set_output_count(5);
    record_span.as_mut().unwrap().set_success();

    // Drop span to trigger collection
    drop(record_span);

    // Allow time for span export
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Collect spans
    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(
        !collected_spans.is_empty(),
        "Should collect at least 1 span"
    );

    let span = &collected_spans[0];

    // Validate basic attributes
    assert_eq!(
        get_attribute(span, "job.name"),
        Some("market_data_processor".to_string()),
        "Should have job.name attribute"
    );

    assert_eq!(
        get_attribute(span, "messaging.system"),
        Some("kafka".to_string()),
        "Should have messaging.system=kafka"
    );

    // Validate output count attribute
    assert_eq!(
        get_numeric_attribute(span, "record.output_count"),
        Some(5),
        "Should have record.output_count=5"
    );

    // Validate span name format
    assert!(
        span.name.contains("process:market_data_processor"),
        "Span name should be 'process:market_data_processor', got '{}'",
        span.name
    );
}

/// Test that validates RecordSpan with upstream context is SpanKind::Consumer
#[tokio::test]
#[serial]
async fn test_record_span_with_upstream_context_is_consumer() {
    let obs = create_test_observability_manager("test_consumer_kind").await;

    let input_record = create_test_record_with_traceparent(UPSTREAM_TRACEPARENT);
    let upstream_ctx = trace_propagation::extract_trace_context(&input_record.headers);

    let mut record_span = ObservabilityHelper::start_record_span(
        &Some(obs.clone()),
        "consumer_job",
        upstream_ctx.as_ref(),
    );
    assert!(record_span.is_some());
    record_span.as_mut().unwrap().set_success();
    drop(record_span);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    assert_eq!(
        span.span_kind,
        SpanKind::Consumer,
        "RecordSpan with upstream context should be SpanKind::Consumer"
    );

    // Should inherit upstream trace_id
    let upstream_trace_id = extract_trace_id(UPSTREAM_TRACEPARENT);
    assert_eq!(
        span.span_context.trace_id().to_string(),
        upstream_trace_id,
        "RecordSpan should inherit upstream trace_id"
    );
}

/// Test that validates RecordSpan without upstream context is SpanKind::Internal
#[tokio::test]
#[serial]
async fn test_record_span_without_upstream_context_is_internal() {
    let obs = create_test_observability_manager("test_internal_kind").await;

    let mut record_span =
        ObservabilityHelper::start_record_span(&Some(obs.clone()), "root_job", None);
    assert!(record_span.is_some());
    record_span.as_mut().unwrap().set_success();
    drop(record_span);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    assert_eq!(
        span.span_kind,
        SpanKind::Internal,
        "RecordSpan without upstream context should be SpanKind::Internal"
    );
}

/// Test that validates RecordSpan error attributes
#[tokio::test]
#[serial]
async fn test_record_span_error_attributes() {
    let obs = create_test_observability_manager("test_error_attrs").await;

    let mut record_span =
        ObservabilityHelper::start_record_span(&Some(obs.clone()), "error_job", None);
    assert!(record_span.is_some());
    record_span
        .as_mut()
        .unwrap()
        .set_error("serialization failed: invalid schema");
    drop(record_span);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Verify error attribute is present
    let error_attr = get_attribute(span, "error");
    assert!(
        error_attr.is_some(),
        "Error span should have 'error' attribute"
    );
    assert!(
        error_attr.unwrap().contains("serialization failed"),
        "Error attribute should contain the error message"
    );
}

/// Test that validates multiple RecordSpans for different jobs have correct job names
#[tokio::test]
#[serial]
async fn test_multiple_record_spans_correct_job_names() {
    let obs = create_test_observability_manager("test_multi_jobs").await;

    let job_names = vec![
        "simple_job",
        "transactional_job",
        "partition_job",
        "join_job",
    ];

    for job_name in &job_names {
        let mut record_span =
            ObservabilityHelper::start_record_span(&Some(obs.clone()), job_name, None);
        assert!(record_span.is_some());
        record_span.as_mut().unwrap().set_success();
        drop(record_span);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert_eq!(
        collected_spans.len(),
        4,
        "Should collect 4 spans (one per job)"
    );

    // Verify each span has the correct job.name
    for (idx, job_name) in job_names.iter().enumerate() {
        let span = &collected_spans[idx];
        assert_eq!(
            get_attribute(span, "job.name"),
            Some(job_name.to_string()),
            "Span {} should have job.name={}",
            idx,
            job_name
        );
    }
}
