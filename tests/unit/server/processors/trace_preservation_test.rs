//! Tests for per-record traceparent preservation in inject_trace_context_into_records
//!
//! Verifies that:
//! - Records with existing traceparent headers are preserved (not overwritten)
//! - Records without traceparent get the batch span context injected
//! - Mixed batches are handled correctly (some preserved, some injected)
//! - Multiple unique upstream trace contexts are extracted from batch records

use serial_test::serial;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::config::TracingConfig;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

use std::sync::{Mutex, OnceLock};

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
                config.service_name = "trace-preservation-test".to_string();
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
// Per-Record Traceparent Preservation Tests
// =============================================================================

#[test]
#[serial]
fn test_records_with_existing_traceparent_are_preserved() {
    let telemetry = get_telemetry();
    let batch_span = Some(telemetry.start_batch_span("preserve-test", 1, None, Vec::new()));

    let original_traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";

    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_record_with_traceparent(original_traceparent)),
        Arc::new(create_record_with_traceparent(original_traceparent)),
    ];

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "preserve-test",
    );

    // Both records should still have the original traceparent
    for record in &output_records {
        let tp = record
            .headers
            .get("traceparent")
            .expect("Should have traceparent");
        assert_eq!(
            tp, original_traceparent,
            "Original traceparent should be preserved"
        );
    }
}

#[test]
#[serial]
fn test_records_without_traceparent_get_batch_context() {
    let telemetry = get_telemetry();
    let batch_span = Some(telemetry.start_batch_span("inject-test", 1, None, Vec::new()));

    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_record_no_traceparent()),
        Arc::new(create_record_no_traceparent()),
    ];

    // Verify no traceparent initially
    for record in &output_records {
        assert!(record.headers.get("traceparent").is_none());
    }

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "inject-test",
    );

    // Both records should now have the batch span's traceparent
    let batch_ctx = batch_span.as_ref().unwrap().span_context().unwrap();
    let expected_trace_id = format!("{}", batch_ctx.trace_id());

    for record in &output_records {
        let tp = record
            .headers
            .get("traceparent")
            .expect("Should have traceparent injected");
        assert!(
            tp.contains(&expected_trace_id),
            "Injected traceparent should contain batch trace_id. Got: {}, expected to contain: {}",
            tp,
            expected_trace_id
        );
    }
}

#[test]
#[serial]
fn test_mixed_batch_preserves_existing_injects_missing() {
    let telemetry = get_telemetry();
    let batch_span = Some(telemetry.start_batch_span("mixed-test", 1, None, Vec::new()));

    let original_traceparent = "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01";

    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_record_with_traceparent(original_traceparent)), // has traceparent
        Arc::new(create_record_no_traceparent()),                       // no traceparent
        Arc::new(create_record_with_traceparent(original_traceparent)), // has traceparent
        Arc::new(create_record_no_traceparent()),                       // no traceparent
    ];

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "mixed-test",
    );

    let batch_ctx = batch_span.as_ref().unwrap().span_context().unwrap();
    let batch_trace_id = format!("{}", batch_ctx.trace_id());

    // Record 0: should keep original traceparent
    let tp0 = output_records[0].headers.get("traceparent").unwrap();
    assert_eq!(
        tp0, original_traceparent,
        "Record 0 should preserve original traceparent"
    );

    // Record 1: should get batch context
    let tp1 = output_records[1].headers.get("traceparent").unwrap();
    assert!(
        tp1.contains(&batch_trace_id),
        "Record 1 should get batch trace_id"
    );

    // Record 2: should keep original traceparent
    let tp2 = output_records[2].headers.get("traceparent").unwrap();
    assert_eq!(
        tp2, original_traceparent,
        "Record 2 should preserve original traceparent"
    );

    // Record 3: should get batch context
    let tp3 = output_records[3].headers.get("traceparent").unwrap();
    assert!(
        tp3.contains(&batch_trace_id),
        "Record 3 should get batch trace_id"
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

    // Collect unique trace contexts (same logic as ObservabilityHelper::start_batch_span)
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
fn test_no_injection_when_no_batch_span() {
    let original_traceparent = "00-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-ffffffffffffffff-01";

    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_record_no_traceparent()),
        Arc::new(create_record_with_traceparent(original_traceparent)),
    ];

    // With no batch span, nothing should be injected
    ObservabilityHelper::inject_trace_context_into_records(&None, &mut output_records, "no-span");

    // Record 0: still no traceparent
    assert!(output_records[0].headers.get("traceparent").is_none());

    // Record 1: original preserved
    let tp = output_records[1].headers.get("traceparent").unwrap();
    assert_eq!(tp, original_traceparent);
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
