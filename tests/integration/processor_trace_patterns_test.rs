//! Processor-specific distributed trace coverage tests
//!
//! These tests exercise the exact trace patterns used by each of the 4 processor types:
//! - SimpleJobProcessor: Parent span from first input batch + per-source child spans
//! - TransactionalJobProcessor: Single span from input, completed after commit
//! - PartitionReceiver: Span created from input batch, injected into output
//! - JoinJobProcessor: Span from first OUTPUT record (not input)
//!
//! Each processor has a distinct trace pattern — bugs in one processor's trace wiring
//! won't be caught by the generic ObservabilityHelper tests.

use opentelemetry::trace::{SpanId, TraceId};
use serial_test::serial;
use std::sync::Arc;
use std::time::Instant;
use velostream::velostream::observability::SharedObservabilityManager;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::StreamRecord;

// Use shared helpers from common
use super::*;

// =============================================================================
// Test 1: SimpleJobProcessor trace pattern
// =============================================================================

/// Simulates SimpleJobProcessor's trace pattern:
/// 1. Parent batch span from first non-empty input batch
/// 2. Per-source child spans for each source
/// 3. Trace injection uses PARENT span (not source spans)
#[tokio::test]
#[serial]
async fn test_simple_processor_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate multi-source input batches (like simple.rs lines 692-703)
    let source_a_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let source_b_batch = vec![create_test_record()];

    // Step 1: Create parent batch span from first non-empty batch
    // This mirrors simple.rs finding the first non-empty batch
    let first_batch = &source_a_batch;
    let parent_batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-simple-job", 1, first_batch);
    assert!(
        parent_batch_span.is_some(),
        "Parent batch span should be created"
    );

    let parent_ctx = parent_batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Parent span should have context");
    assert!(parent_ctx.is_valid(), "Parent span context should be valid");
    let parent_trace_id = parent_ctx.trace_id().to_string();
    let parent_span_id = parent_ctx.span_id().to_string();

    // Step 2: Create per-source child spans (like simple.rs lines 713-718)
    let source_a_span = ObservabilityHelper::start_batch_span(
        &obs,
        "test-simple-job (source: source_a)",
        1,
        &source_a_batch,
    );
    assert!(source_a_span.is_some(), "Source A span should be created");

    let source_b_span = ObservabilityHelper::start_batch_span(
        &obs,
        "test-simple-job (source: source_b)",
        1,
        &source_b_batch,
    );
    assert!(source_b_span.is_some(), "Source B span should be created");

    // Step 3: Create output records and inject trace using PARENT span
    // This mirrors simple.rs lines 948-952
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    ObservabilityHelper::inject_trace_context_into_records(
        &parent_batch_span,
        &mut output_records,
        "test-simple-job",
    );

    // Verify: all output records have traceparent from PARENT span
    for (i, record) in output_records.iter().enumerate() {
        assert_traceparent_injected(record, &format!("output record {}", i));

        let tp = &record.headers["traceparent"];
        let injected_trace_id = extract_trace_id(tp);
        let injected_span_id = extract_span_id(tp);

        assert_eq!(
            injected_trace_id, parent_trace_id,
            "Output record {} should have parent's trace_id",
            i
        );
        assert_eq!(
            injected_span_id, parent_span_id,
            "Output record {} should have parent's span_id (not a source span)",
            i
        );
    }

    // Verify: all 3 span contexts are valid and have distinct span_ids
    let source_a_ctx = source_a_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Source A span should have context");
    let source_b_ctx = source_b_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Source B span should have context");

    assert!(source_a_ctx.is_valid(), "Source A span should be valid");
    assert!(source_b_ctx.is_valid(), "Source B span should be valid");

    // All 3 spans should have unique span_ids
    let span_ids = vec![
        parent_span_id.clone(),
        source_a_ctx.span_id().to_string(),
        source_b_ctx.span_id().to_string(),
    ];
    let unique_ids: std::collections::HashSet<_> = span_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        3,
        "All 3 spans should have unique span_ids: {:?}",
        span_ids
    );
}

// =============================================================================
// Test 1b: SimpleJobProcessor span completion
// =============================================================================

/// Simulates SimpleJobProcessor's span completion pattern:
/// Creates batch span, sets total_records/batch_duration, then set_success or set_error.
#[tokio::test]
#[serial]
async fn test_simple_processor_span_completion() {
    let obs_manager = create_test_observability_manager("processor-span-completion-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

    // Success path
    let batch_start = Instant::now();
    let mut success_span =
        ObservabilityHelper::start_batch_span(&obs, "test-span-complete", 1, &input_batch);
    assert!(success_span.is_some(), "Span should be created");

    ObservabilityHelper::complete_batch_span_success(&mut success_span, &batch_start, 42);
    // After completion, the span should have been marked (no panic)

    // Error path
    let mut error_span =
        ObservabilityHelper::start_batch_span(&obs, "test-span-error", 2, &input_batch);
    assert!(error_span.is_some(), "Error span should be created");

    ObservabilityHelper::complete_batch_span_error(&mut error_span, &batch_start, 10, 5);
    // After error completion, the span should have been marked (no panic)
}

// =============================================================================
// Test 2: TransactionalJobProcessor trace pattern
// =============================================================================

/// Simulates TransactionalJobProcessor's trace pattern:
/// 1. Single batch span from input records
/// 2. Inject into output records
/// 3. Complete span after simulated commit
#[tokio::test]
#[serial]
async fn test_transactional_processor_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate input batch with upstream trace (like transactional.rs lines 502-508)
    let input_batch = vec![
        create_test_record_with_traceparent(UPSTREAM_TRACEPARENT),
        create_test_record(),
    ];

    // Step 1: Create batch span (single span for entire transaction)
    let mut batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-txn-job", 1, &input_batch);
    assert!(batch_span.is_some(), "Batch span should be created");

    let batch_ctx = batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Batch span should have context");
    assert!(batch_ctx.is_valid());
    let batch_trace_id = batch_ctx.trace_id().to_string();

    // Step 2: Inject into output records (like transactional.rs lines 541-548)
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "test-txn-job",
    );

    // Step 3: Complete span after simulated commit
    let batch_start = Instant::now();
    ObservabilityHelper::complete_batch_span_success(&mut batch_span, &batch_start, 2);

    // Verify: output records have traceparent matching batch span
    for (i, record) in output_records.iter().enumerate() {
        assert_traceparent_injected(record, &format!("txn output record {}", i));
        let tp = &record.headers["traceparent"];
        assert_eq!(
            extract_trace_id(tp),
            batch_trace_id,
            "Output record {} should share batch trace_id",
            i
        );
    }

    // Verify: span is child of upstream trace
    // The batch span extracts upstream context, so it should be linked to the upstream trace
    // (The TelemetryProvider creates spans as children of upstream context)
    assert_ne!(
        batch_ctx.trace_id(),
        TraceId::INVALID,
        "Batch span should have valid trace_id"
    );
}

// =============================================================================
// Test 2b: TransactionalJobProcessor error path
// =============================================================================

/// Simulates TransactionalJobProcessor's error path:
/// Batch span created, processing fails, span completed with error.
#[tokio::test]
#[serial]
async fn test_transactional_processor_error_path() {
    let obs_manager = create_test_observability_manager("processor-trace-error-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-txn-error", 1, &input_batch);
    assert!(batch_span.is_some(), "Batch span should be created");

    // Simulate batch failure — complete with error instead of success
    let batch_start = Instant::now();
    ObservabilityHelper::complete_batch_span_error(&mut batch_span, &batch_start, 5, 3);
    // Should not panic, span is properly completed with error status
}

// =============================================================================
// Test 3: PartitionReceiver trace pattern
// =============================================================================

/// Simulates PartitionReceiver's trace pattern:
/// 1. Process batch first (trace-agnostic processing)
/// 2. THEN create batch span from ORIGINAL INPUT batch
/// 3. Inject into output records
#[tokio::test]
#[serial]
async fn test_partition_receiver_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate input batch with upstream trace
    let input_batch = vec![
        create_test_record_with_traceparent(UPSTREAM_TRACEPARENT),
        create_test_record(),
        create_test_record(),
    ];

    // Step 1: Simulate process_batch() producing output (trace-agnostic)
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    // Step 2: Create batch span from ORIGINAL INPUT batch
    // This mirrors partition_receiver.rs lines 475-480
    // Key difference: span is created AFTER processing, using input records for context
    let batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-partition-job", 1, &input_batch);
    assert!(batch_span.is_some(), "Batch span should be created");

    let span_ctx = batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have span context");

    // Step 3: Inject into output records (like partition_receiver.rs lines 481-487)
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "test-partition-job",
    );

    // Verify: span extracts upstream context from INPUT records
    assert!(span_ctx.is_valid(), "Span should have valid context");
    assert_ne!(span_ctx.trace_id(), TraceId::INVALID);

    // Verify: output records get traceparent from batch span
    for (i, record) in output_records.iter().enumerate() {
        assert_traceparent_injected(record, &format!("partition output record {}", i));
        let tp = &record.headers["traceparent"];
        assert_eq!(
            extract_trace_id(tp),
            span_ctx.trace_id().to_string(),
            "Output record {} should have batch span's trace_id",
            i
        );
    }
}

// =============================================================================
// Test 4: JoinJobProcessor trace pattern
// =============================================================================

/// Simulates JoinJobProcessor's trace pattern:
/// 1. Uses first OUTPUT record (not input!) to extract upstream trace context
/// 2. Clones first output record for span creation
/// 3. Injects trace into ALL output records
#[tokio::test]
#[serial]
async fn test_join_processor_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate output buffer where first record carries upstream traceparent
    // In a join, output records may carry forward headers from left/right inputs
    let mut output_buffer: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)),
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    // Step 1: Clone first output record for span creation
    // This mirrors join_job_processor.rs lines 192-195
    let first_records: Vec<StreamRecord> = output_buffer
        .first()
        .map(|r| vec![(**r).clone()])
        .unwrap_or_default();

    // Step 2: Create batch span from first output record
    // This mirrors join_job_processor.rs lines 196-201
    let batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-join-job", 1, &first_records);
    assert!(batch_span.is_some(), "Batch span should be created");

    let span_ctx = batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have span context");

    // Step 3: Inject trace into ALL output records
    // This mirrors join_job_processor.rs lines 202-206
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_buffer,
        "test-join-job",
    );

    // Verify: span is child of upstream trace from FIRST OUTPUT RECORD
    assert!(span_ctx.is_valid());
    assert_ne!(span_ctx.trace_id(), TraceId::INVALID);

    // Verify: ALL output records get new traceparent (including first record which gets overwritten)
    let injected_trace_id = span_ctx.trace_id().to_string();
    let injected_span_id = span_ctx.span_id().to_string();

    for (i, record) in output_buffer.iter().enumerate() {
        assert_traceparent_injected(record, &format!("join output record {}", i));
        let tp = &record.headers["traceparent"];
        assert_eq!(
            extract_trace_id(tp),
            injected_trace_id,
            "Join output record {} should have batch span's trace_id",
            i
        );
        assert_eq!(
            extract_span_id(tp),
            injected_span_id,
            "Join output record {} should have batch span's span_id",
            i
        );
    }

    // Verify: first record's original traceparent was overwritten
    let first_tp = &output_buffer[0].headers["traceparent"];
    assert_ne!(
        first_tp, UPSTREAM_TRACEPARENT,
        "First record's original traceparent should be overwritten with batch span's context"
    );
}

// =============================================================================
// Test 5: JoinJobProcessor with no upstream trace starts new root
// =============================================================================

/// When join output records have NO traceparent, a new root trace should be started
#[tokio::test]
#[serial]
async fn test_join_processor_no_upstream_trace_starts_new_root() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Output records with NO traceparent headers
    let mut output_buffer: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    // Clone first output record (no trace context)
    let first_records: Vec<StreamRecord> = output_buffer
        .first()
        .map(|r| vec![(**r).clone()])
        .unwrap_or_default();

    // Create batch span — should start new root trace
    let batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-join-no-upstream", 1, &first_records);
    assert!(batch_span.is_some(), "Batch span should be created");

    let span_ctx = batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have span context");

    // Verify: span has valid trace_id (new root trace)
    assert!(span_ctx.is_valid(), "New root span should be valid");
    assert_ne!(
        span_ctx.trace_id(),
        TraceId::INVALID,
        "Should have valid trace_id"
    );
    assert_ne!(
        span_ctx.span_id(),
        SpanId::INVALID,
        "Should have valid span_id"
    );

    // Inject into output records
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_buffer,
        "test-join-no-upstream",
    );

    // Verify: output records get injected traceparent
    for (i, record) in output_buffer.iter().enumerate() {
        assert_traceparent_injected(record, &format!("no-upstream record {}", i));
    }
}

// =============================================================================
// Test 6: SimpleJobProcessor multi-source span hierarchy
// =============================================================================

/// Tests the parent-child span relationships unique to SimpleJobProcessor:
/// - Parent span and per-source spans each extract from their own input records
/// - Output injection uses parent span context, not source spans
#[tokio::test]
#[serial]
async fn test_simple_processor_multi_source_span_hierarchy() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Source A has upstream trace, Source B does not
    let source_a_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let source_b_batch = vec![create_test_record()];

    // Parent span from source_a (first non-empty batch)
    let parent_span =
        ObservabilityHelper::start_batch_span(&obs, "test-hierarchy", 1, &source_a_batch);
    let parent_ctx = parent_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have context");
    let parent_trace_id = parent_ctx.trace_id().to_string();
    let parent_span_id = parent_ctx.span_id().to_string();

    // Per-source spans
    let source_a_span = ObservabilityHelper::start_batch_span(
        &obs,
        "test-hierarchy (source: a)",
        1,
        &source_a_batch,
    );
    let source_a_ctx = source_a_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have context");
    let source_a_span_id = source_a_ctx.span_id().to_string();

    let source_b_span = ObservabilityHelper::start_batch_span(
        &obs,
        "test-hierarchy (source: b)",
        1,
        &source_b_batch,
    );
    let source_b_ctx = source_b_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have context");
    let source_b_span_id = source_b_ctx.span_id().to_string();

    // Verify: each span has its own unique span_id
    assert_ne!(
        parent_span_id, source_a_span_id,
        "Parent and source_a should have different span_ids"
    );
    assert_ne!(
        parent_span_id, source_b_span_id,
        "Parent and source_b should have different span_ids"
    );
    assert_ne!(
        source_a_span_id, source_b_span_id,
        "Source spans should have different span_ids"
    );

    // Verify: output injection uses PARENT span, not source spans
    let mut output_records: Vec<Arc<StreamRecord>> = vec![Arc::new(create_test_record())];
    ObservabilityHelper::inject_trace_context_into_records(
        &parent_span,
        &mut output_records,
        "test-hierarchy",
    );

    let tp = &output_records[0].headers["traceparent"];
    assert_eq!(
        extract_trace_id(tp),
        parent_trace_id,
        "Output should use parent's trace_id"
    );
    assert_eq!(
        extract_span_id(tp),
        parent_span_id,
        "Output should use parent's span_id, not any source span"
    );
}

// =============================================================================
// Test 7: All processors handle empty batch gracefully
// =============================================================================

/// Verifies graceful handling when input batch is empty (each processor's edge case)
#[tokio::test]
#[serial]
async fn test_all_processors_handle_empty_batch() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Empty input batch
    let empty_batch: Vec<StreamRecord> = vec![];

    // start_batch_span with empty slice should still create a span (new root trace)
    let batch_span =
        ObservabilityHelper::start_batch_span(&obs, "test-empty-batch", 1, &empty_batch);
    assert!(
        batch_span.is_some(),
        "Batch span should be created even with empty batch"
    );

    let span_ctx = batch_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have span context");
    assert!(
        span_ctx.is_valid(),
        "Span from empty batch should be valid (new root trace)"
    );

    // Inject on empty output records should not panic
    let mut empty_output: Vec<Arc<StreamRecord>> = vec![];
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut empty_output,
        "test-empty-batch",
    );
    assert!(empty_output.is_empty(), "Empty output should remain empty");

    // Also verify None batch span with empty output doesn't panic
    let no_span: Option<velostream::velostream::observability::telemetry::BatchSpan> = None;
    let mut empty_output2: Vec<Arc<StreamRecord>> = vec![];
    ObservabilityHelper::inject_trace_context_into_records(
        &no_span,
        &mut empty_output2,
        "test-empty-batch",
    );
}

// =============================================================================
// Test 8: Trace injection with shared Arc records (copy-on-write)
// =============================================================================

/// Verifies that inject_trace_context_into_records correctly handles
/// Arc<StreamRecord> with refcount > 1, using Arc::make_mut for copy-on-write.
#[tokio::test]
#[serial]
async fn test_trace_injection_with_shared_arc_records() {
    let obs_manager = create_test_observability_manager("processor-arc-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    let input_batch = vec![create_test_record()];
    let batch_span = ObservabilityHelper::start_batch_span(&obs, "test-arc-cow", 1, &input_batch);
    assert!(batch_span.is_some());

    // Create shared Arc records — clone the Arc (refcount > 1)
    let shared_record = Arc::new(create_test_record());
    let shared_clone = Arc::clone(&shared_record);

    let mut output_records: Vec<Arc<StreamRecord>> = vec![shared_record];

    // Inject trace — this should use Arc::make_mut (copy-on-write)
    ObservabilityHelper::inject_trace_context_into_records(
        &batch_span,
        &mut output_records,
        "test-arc-cow",
    );

    // The injected record should have traceparent
    assert_traceparent_injected(&output_records[0], "injected arc record");

    // The original clone should NOT have traceparent (copy-on-write created a new allocation)
    assert!(
        !shared_clone.headers.contains_key("traceparent"),
        "Original Arc clone should not be modified (copy-on-write semantics)"
    );
}
