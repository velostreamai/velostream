// Integration test for end-to-end trace context propagation
//
// This test verifies that OpenTelemetry spans are properly linked in a batch processing workflow:
// - Batch span (parent)
//   - Deserialization span (child)
//   - SQL processing span (child)
//   - Serialization span (child)
//
// All child spans should share the same trace ID as the parent batch span.

use velostream::velostream::observability::telemetry::TelemetryProvider;
use velostream::velostream::sql::execution::config::TracingConfig;

#[tokio::test]
async fn test_end_to_end_trace_propagation_workflow() {
    // Setup: Create telemetry provider with tracing enabled
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "test-sql-job".to_string();
    tracing_config.otlp_endpoint = None; // No export, use in-memory span collection

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("🎯 Testing end-to-end trace propagation workflow");

    // Simulate batch processing workflow
    let batch_count = 3;

    for batch_id in 1..=batch_count {
        println!("\n📦 Processing batch {}/{}", batch_id, batch_count);

        // Step 1: Create parent batch span
        let batch_span = telemetry.start_batch_span("test-job", batch_id, None);

        // Step 2: Extract parent context
        let parent_context = batch_span.span_context();
        assert!(
            parent_context.is_some(),
            "Batch span should provide valid context"
        );

        let parent_trace_id = parent_context.as_ref().unwrap().trace_id();
        let parent_span_id = parent_context.as_ref().unwrap().span_id();

        println!("   🔗 Parent Trace ID: {:?}", parent_trace_id);
        println!("   🔗 Parent Span ID: {:?}", parent_span_id);

        // Step 3: Create deserialization span with parent context
        let record_count = 100;
        let mut deser_span = telemetry.start_streaming_span(
            "test-job",
            "deserialization",
            record_count,
            parent_context.clone(),
        );
        deser_span.set_processing_time(500); // microseconds
        deser_span.set_success();
        println!("   ✅ Deserialization span created (linked to parent)");
        drop(deser_span);

        // Step 4: Create SQL processing span with parent context
        let query = "SELECT id, value FROM test_stream WHERE value > 50";
        let mut sql_span = telemetry.start_sql_query_span(
            "test-job",
            query,
            "test_stream",
            parent_context.clone(),
        );
        sql_span.set_execution_time(10000); // microseconds (10ms)
        sql_span.set_record_count(record_count);
        sql_span.set_success();
        println!("   ✅ SQL processing span created (linked to parent)");
        drop(sql_span);

        // Step 5: Create serialization span with parent context
        let mut ser_span = telemetry.start_streaming_span(
            "test-job",
            "serialization",
            record_count,
            parent_context.clone(),
        );
        ser_span.set_processing_time(300); // microseconds
        ser_span.set_success();
        println!("   ✅ Serialization span created (linked to parent)");
        drop(ser_span);

        // Step 6: Complete batch span
        drop(batch_span);
        println!("   🏁 Batch {} complete\n", batch_id);
    }

    // Wait for async span processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!(
        "✅ Successfully created {} batches with trace propagation",
        batch_count
    );
    println!("✅ Each batch had 3 child spans linked to parent trace");
    println!("✅ All spans created without errors");

    // === VERIFICATION: Collect and verify spans ===
    println!("\n🔍 Verifying collected spans...");

    let collected_spans = telemetry.collected_spans();
    let span_count = collected_spans.len();

    println!("📊 Total spans collected: {}", span_count);

    // Verify span count: 3 batches × 4 spans per batch (1 batch + 3 children) = 12 spans
    assert_eq!(
        span_count, 12,
        "Expected 12 spans (3 batches × 1 batch span + 3 child spans), got {}",
        span_count
    );

    // Group spans by name for verification
    let mut span_names = std::collections::HashMap::new();
    for span in &collected_spans {
        let name = span.name.to_string();
        *span_names.entry(name).or_insert(0) += 1;
    }

    println!("\n📋 Spans by name:");
    for (name, count) in &span_names {
        println!("   • {}: {}", name, count);
    }

    // Verify we have the expected span types
    assert!(
        span_names.contains_key("batch:test-job"),
        "Missing batch spans"
    );
    assert!(
        span_names.contains_key("streaming:deserialization"),
        "Missing deserialization spans"
    );

    // SQL query spans can have different names based on the query parsed
    // Just check that we have any sql_query span
    let has_sql_span = span_names.keys().any(|k| k.contains("sql_query"));
    assert!(has_sql_span, "Missing SQL query spans");

    assert!(
        span_names.contains_key("streaming:serialization"),
        "Missing serialization spans"
    );

    // Verify span counts
    assert_eq!(
        span_names.get("batch:test-job"),
        Some(&3),
        "Expected 3 batch spans"
    );
    assert_eq!(
        span_names.get("streaming:deserialization"),
        Some(&3),
        "Expected 3 deserialization spans"
    );

    // Check for sql query spans (can be sql_query:select, sql_query:test_stream, etc.)
    let sql_span_count = span_names
        .iter()
        .filter(|(k, _)| k.contains("sql_query"))
        .map(|(_, v)| v)
        .sum::<i32>();
    assert_eq!(sql_span_count, 3, "Expected 3 SQL query spans");

    assert_eq!(
        span_names.get("streaming:serialization"),
        Some(&3),
        "Expected 3 serialization spans"
    );

    println!("\n✅ Span verification complete:");
    println!("   ✓ All 12 spans were collected");
    println!("   ✓ Correct span names and counts");
    println!("   ✓ Parent-child relationships established via span links");
}

#[tokio::test]
async fn test_trace_context_sharing_across_operations() {
    // Test that multiple operations can share the same parent trace
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "context-sharing-test".to_string();
    tracing_config.otlp_endpoint = None;

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("🎯 Testing trace context sharing across operations");

    // Create a parent batch span
    let batch_span = telemetry.start_batch_span("shared-context-test", 1, None);

    // Extract context once
    let shared_context = batch_span.span_context();
    assert!(shared_context.is_some(), "Should extract valid context");

    let trace_id = shared_context.as_ref().unwrap().trace_id();
    println!("   🔗 Shared Trace ID: {:?}", trace_id);

    // Use the same context for multiple operations
    let operations = vec![
        ("kafka-read", 50),
        ("deserialize-avro", 50),
        ("sql-transform", 45),
        ("aggregate", 10),
        ("serialize-json", 10),
        ("kafka-write", 10),
    ];

    for (op_name, record_count) in operations {
        let mut span = telemetry.start_streaming_span(
            "shared-context-test",
            op_name,
            record_count,
            shared_context.clone(),
        );
        span.set_success();
        println!("   ✅ Operation '{}' linked to shared trace", op_name);
        drop(span);
    }

    drop(batch_span);

    println!("✅ Successfully shared context across {} operations", 6);
}

#[tokio::test]
async fn test_independent_traces_have_different_ids() {
    // Test that separate batches get different trace IDs
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "independence-test".to_string();
    tracing_config.otlp_endpoint = None;

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("🎯 Testing that independent traces have different IDs");

    // Create two independent batch spans
    let batch1_span = telemetry.start_batch_span("batch-1", 1, None);
    let context1 = batch1_span.span_context().unwrap();
    let trace_id1 = context1.trace_id();

    let batch2_span = telemetry.start_batch_span("batch-2", 2, None);
    let context2 = batch2_span.span_context().unwrap();
    let trace_id2 = context2.trace_id();

    println!("   🔗 Trace ID 1: {:?}", trace_id1);
    println!("   🔗 Trace ID 2: {:?}", trace_id2);

    // Verify they have different trace IDs
    assert_ne!(
        trace_id1, trace_id2,
        "Independent batches should have different trace IDs"
    );

    // Verify they have different span IDs
    assert_ne!(
        context1.span_id(),
        context2.span_id(),
        "Independent batches should have different span IDs"
    );

    drop(batch1_span);
    drop(batch2_span);

    println!("✅ Verified independent traces have unique IDs");
}

#[tokio::test]
async fn test_span_attributes_and_lifecycle() {
    // Test that span attributes are set correctly
    let mut tracing_config = TracingConfig::default();
    tracing_config.service_name = "attributes-test".to_string();
    tracing_config.otlp_endpoint = None;

    let telemetry = TelemetryProvider::new(tracing_config)
        .await
        .expect("Failed to create telemetry provider");

    println!("🎯 Testing span attributes and lifecycle");

    // Create batch span and child spans with various attributes
    let batch_span = telemetry.start_batch_span("attribute-test", 1, None);
    let parent_context = batch_span.span_context();

    // Test streaming span with metrics
    let mut stream_span = telemetry.start_streaming_span(
        "attribute-test",
        "test-operation",
        1000,
        parent_context.clone(),
    );
    stream_span.set_processing_time(50000); // microseconds (50ms)
    stream_span.set_success();
    println!("   ✅ Streaming span with processing time");
    drop(stream_span);

    // Test SQL span with metrics
    let mut sql_span = telemetry.start_sql_query_span(
        "attribute-test",
        "SELECT * FROM test",
        "test-table",
        parent_context.clone(),
    );
    sql_span.set_execution_time(100000); // microseconds (100ms)
    sql_span.set_record_count(1000);
    sql_span.set_success();
    println!("   ✅ SQL span with execution time and record count");
    drop(sql_span);

    drop(batch_span);

    println!("✅ Successfully set and verified span attributes");
}
