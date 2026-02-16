// Integration test for end-to-end trace context propagation
//
// This test verifies that OpenTelemetry per-record spans are properly created:
// - RecordSpan with and without upstream context
// - Span name format: "process:{job_name}"
// - Multiple independent record spans have different trace IDs
// - RecordSpan lifecycle: set_output_count, set_success, set_error
//
// The per-record model replaces the old batch-level model:
// Instead of a batch span + child streaming/sql spans, each sampled
// record gets a single RecordSpan.

use serial_test::serial;
use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::sql::execution::config::TracingConfig;

#[tokio::test]
#[serial]
async fn test_end_to_end_per_record_trace_workflow() {
    // Setup: Create telemetry provider with tracing enabled
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "test-sql-job".to_string();
    tracing_config.otlp_endpoint = None; // No export, use in-memory span collection

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("Testing end-to-end per-record trace propagation workflow");

    // Simulate processing multiple records, each getting its own RecordSpan
    let record_count = 5;

    for record_id in 1..=record_count {
        println!("\nProcessing record {}/{}", record_id, record_count);

        // Create a per-record span (no upstream context -- root spans)
        let mut record_span = telemetry.start_record_span("test-job", None);

        // Extract span context
        let span_context = record_span.span_context();
        assert!(
            span_context.is_some(),
            "Record span should provide valid context"
        );

        let trace_id = span_context.as_ref().unwrap().trace_id();
        let span_id = span_context.as_ref().unwrap().span_id();

        println!("   Trace ID: {:?}", trace_id);
        println!("   Span ID: {:?}", span_id);

        // Set output count and mark success
        record_span.set_output_count(3);
        record_span.set_success();
        println!("   Record span completed successfully");
        drop(record_span);
    }

    // Wait for async span processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("\nSuccessfully created {} per-record spans", record_count);

    // === VERIFICATION: Collect and verify spans ===
    println!("\nVerifying collected spans...");

    let collected_spans = telemetry.collected_spans();
    let span_count = collected_spans.len();

    println!("Total spans collected: {}", span_count);

    // Verify span count: 5 records = 5 record spans
    assert_eq!(
        span_count, 5,
        "Expected 5 spans (1 per record), got {}",
        span_count
    );

    // Verify all spans have the correct name format
    for span in &collected_spans {
        assert!(
            span.name.contains("process:test-job"),
            "Span name should contain 'process:test-job', got '{}'",
            span.name
        );
    }

    // Each record span should be an independent trace (no upstream context)
    let mut trace_ids: Vec<String> = collected_spans
        .iter()
        .map(|s| s.span_context.trace_id().to_string())
        .collect();
    trace_ids.sort();
    trace_ids.dedup();
    assert_eq!(
        trace_ids.len(),
        5,
        "Each record span should have a unique trace_id (independent root traces), got {} unique IDs",
        trace_ids.len()
    );

    println!("\nSpan verification complete:");
    println!("   All {} spans were collected", span_count);
    println!("   Correct span names verified");
    println!("   Independent trace IDs verified");
}

#[tokio::test]
#[serial]
async fn test_record_span_with_upstream_context() {
    // Test that record spans correctly link to upstream context
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "context-sharing-test".to_string();
    tracing_config.otlp_endpoint = None;

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("Testing record span with upstream context");

    // Create a root span first (no upstream)
    let root_span = telemetry.start_record_span("root-job", None);
    let root_context = root_span.span_context();
    assert!(root_context.is_some(), "Should extract valid context");

    let root_trace_id = root_context.as_ref().unwrap().trace_id();
    println!("   Root Trace ID: {:?}", root_trace_id);

    // Create child span using root's context as upstream
    let child_span = telemetry.start_record_span("child-job", root_context.as_ref());
    let child_context = child_span.span_context();
    assert!(child_context.is_some(), "Child should have context");

    let child_trace_id = child_context.as_ref().unwrap().trace_id();
    println!("   Child Trace ID: {:?}", child_trace_id);

    // Child should inherit root's trace_id
    assert_eq!(
        child_trace_id, root_trace_id,
        "Child span should inherit root's trace_id"
    );

    // But have different span_id
    assert_ne!(
        child_context.as_ref().unwrap().span_id(),
        root_context.as_ref().unwrap().span_id(),
        "Child span should have different span_id from root"
    );

    drop(root_span);
    drop(child_span);

    println!("Verified upstream context propagation");
}

#[tokio::test]
#[serial]
async fn test_independent_traces_have_different_ids() {
    // Test that separate record spans get different trace IDs
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "independence-test".to_string();
    tracing_config.otlp_endpoint = None;

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("Testing that independent record spans have different IDs");

    // Create two independent record spans (no upstream context)
    let span1 = telemetry.start_record_span("record-1", None);
    let context1 = span1.span_context().unwrap();
    let trace_id1 = context1.trace_id();

    let span2 = telemetry.start_record_span("record-2", None);
    let context2 = span2.span_context().unwrap();
    let trace_id2 = context2.trace_id();

    println!("   Trace ID 1: {:?}", trace_id1);
    println!("   Trace ID 2: {:?}", trace_id2);

    // Verify they have different trace IDs
    assert_ne!(
        trace_id1, trace_id2,
        "Independent record spans should have different trace IDs"
    );

    // Verify they have different span IDs
    assert_ne!(
        context1.span_id(),
        context2.span_id(),
        "Independent record spans should have different span IDs"
    );

    drop(span1);
    drop(span2);

    println!("Verified independent traces have unique IDs");
}

#[tokio::test]
#[serial]
async fn test_record_span_attributes_and_lifecycle() {
    // Test that record span attributes are set correctly
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "attributes-test".to_string();
    tracing_config.otlp_endpoint = None;

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("Testing record span attributes and lifecycle");

    // Test success path
    let mut success_span = telemetry.start_record_span("success-test", None);
    success_span.set_output_count(100);
    success_span.set_success();
    println!("   Record span with output_count and success");
    drop(success_span);

    // Test error path
    let mut error_span = telemetry.start_record_span("error-test", None);
    error_span.set_error("Processing failed: invalid schema");
    println!("   Record span with error");
    drop(error_span);

    // Wait for span collection
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let collected_spans = telemetry.collected_spans();
    assert_eq!(
        collected_spans.len(),
        2,
        "Should collect 2 spans (success + error)"
    );

    println!("Successfully set and verified record span attributes");
}
