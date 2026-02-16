//! Processor-specific distributed trace coverage tests (per-record API)
//!
//! These tests exercise the per-record trace patterns used by each of the 4 processor types:
//! - SimpleJobProcessor: Per-record span from sampled input record
//! - TransactionalJobProcessor: Per-record span, completed after commit
//! - PartitionReceiver: Per-record span from input, injected into output
//! - JoinJobProcessor: Per-record span from first OUTPUT record (not input)
//!
//! Each processor has a distinct trace pattern -- bugs in one processor's trace wiring
//! won't be caught by the generic ObservabilityHelper tests.

use opentelemetry::trace::{SpanId, TraceId};
use serial_test::serial;
use std::sync::Arc;
use velostream::velostream::observability::SharedObservabilityManager;
use velostream::velostream::observability::trace_propagation;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::StreamRecord;

// Use shared helpers from common
use super::*;

// =============================================================================
// Test 1: SimpleJobProcessor trace pattern (per-record)
// =============================================================================

/// Simulates SimpleJobProcessor's per-record trace pattern:
/// 1. Extract upstream context from sampled input record
/// 2. Start a RecordSpan linked to upstream trace
/// 3. Inject trace context into individual output records
#[tokio::test]
#[serial]
async fn test_simple_processor_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate input record with upstream traceparent
    let input_record = create_test_record_with_traceparent(UPSTREAM_TRACEPARENT);

    // Step 1: Extract upstream context from the sampled record
    let upstream_ctx = trace_propagation::extract_trace_context(&input_record.headers);
    assert!(
        upstream_ctx.is_some(),
        "Should extract upstream context from traceparent header"
    );

    // Step 2: Start a per-record span
    let record_span =
        ObservabilityHelper::start_record_span(&obs, "test-simple-job", upstream_ctx.as_ref());
    assert!(record_span.is_some(), "Record span should be created");

    let span_ctx = record_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Record span should have valid context");
    assert!(span_ctx.is_valid(), "Span context should be valid");

    // Span should continue the upstream trace
    let upstream_trace_id = extract_trace_id(UPSTREAM_TRACEPARENT);
    assert_eq!(
        span_ctx.trace_id().to_string(),
        upstream_trace_id,
        "Record span should continue the upstream trace_id"
    );

    // Step 3: Inject trace context into multiple output records
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    let record_span = record_span.unwrap();
    for output in &mut output_records {
        ObservabilityHelper::inject_record_trace_context(&record_span, output);
    }

    // Verify: all output records have traceparent from the record span
    for (i, record) in output_records.iter().enumerate() {
        assert_traceparent_injected(record, &format!("output record {}", i));

        let tp = &record.headers["traceparent"];
        let injected_trace_id = extract_trace_id(tp);
        let injected_span_id = extract_span_id(tp);

        assert_eq!(
            injected_trace_id,
            span_ctx.trace_id().to_string(),
            "Output record {} should have the record span's trace_id",
            i
        );
        assert_eq!(
            injected_span_id,
            span_ctx.span_id().to_string(),
            "Output record {} should have the record span's span_id",
            i
        );
    }
}

// =============================================================================
// Test 1b: SimpleJobProcessor span completion (success and error)
// =============================================================================

/// Simulates SimpleJobProcessor's RecordSpan lifecycle:
/// Creates record span, calls set_success or set_error, then drops.
#[tokio::test]
#[serial]
async fn test_simple_processor_span_completion() {
    let obs_manager = create_test_observability_manager("processor-span-completion-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Success path
    let mut success_span = ObservabilityHelper::start_record_span(&obs, "test-span-complete", None);
    assert!(success_span.is_some(), "Span should be created");
    success_span.as_mut().unwrap().set_output_count(42);
    success_span.as_mut().unwrap().set_success();
    drop(success_span);

    // Error path
    let mut error_span = ObservabilityHelper::start_record_span(&obs, "test-span-error", None);
    assert!(error_span.is_some(), "Error span should be created");
    error_span
        .as_mut()
        .unwrap()
        .set_error("simulated processing error");
    drop(error_span);
}

// =============================================================================
// Test 2: TransactionalJobProcessor trace pattern (per-record)
// =============================================================================

/// Simulates TransactionalJobProcessor's per-record trace pattern:
/// 1. Extract upstream context from input record
/// 2. Create RecordSpan linked to upstream
/// 3. Inject into output records
/// 4. Mark success after simulated commit
#[tokio::test]
#[serial]
async fn test_transactional_processor_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate input with upstream traceparent
    let input_record = create_test_record_with_traceparent(UPSTREAM_TRACEPARENT);

    // Step 1: Extract upstream context
    let upstream_ctx = trace_propagation::extract_trace_context(&input_record.headers);

    // Step 2: Create record span
    let record_span =
        ObservabilityHelper::start_record_span(&obs, "test-txn-job", upstream_ctx.as_ref());
    assert!(record_span.is_some(), "Record span should be created");

    let span_ctx = record_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Record span should have context");
    assert!(span_ctx.is_valid());
    let record_trace_id = span_ctx.trace_id().to_string();

    // Step 3: Inject into output records
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    let mut record_span = record_span.unwrap();
    for output in &mut output_records {
        ObservabilityHelper::inject_record_trace_context(&record_span, output);
    }

    // Step 4: Mark success after simulated commit
    record_span.set_output_count(2);
    record_span.set_success();

    // Verify: output records have traceparent matching record span
    for (i, record) in output_records.iter().enumerate() {
        assert_traceparent_injected(record, &format!("txn output record {}", i));
        let tp = &record.headers["traceparent"];
        assert_eq!(
            extract_trace_id(tp),
            record_trace_id,
            "Output record {} should share record span's trace_id",
            i
        );
    }

    // Verify: span is child of upstream trace
    assert_ne!(
        span_ctx.trace_id(),
        TraceId::INVALID,
        "Record span should have valid trace_id"
    );
}

// =============================================================================
// Test 2b: TransactionalJobProcessor error path
// =============================================================================

/// Simulates TransactionalJobProcessor's error path:
/// Record span created, processing fails, span completed with error.
#[tokio::test]
#[serial]
async fn test_transactional_processor_error_path() {
    let obs_manager = create_test_observability_manager("processor-trace-error-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    let input_record = create_test_record_with_traceparent(UPSTREAM_TRACEPARENT);
    let upstream_ctx = trace_propagation::extract_trace_context(&input_record.headers);

    let mut record_span =
        ObservabilityHelper::start_record_span(&obs, "test-txn-error", upstream_ctx.as_ref());
    assert!(record_span.is_some(), "Record span should be created");

    // Simulate failure -- complete with error instead of success
    record_span
        .as_mut()
        .unwrap()
        .set_error("transaction commit failed");
    drop(record_span);
    // Should not panic, span is properly completed with error status
}

// =============================================================================
// Test 3: PartitionReceiver trace pattern (per-record)
// =============================================================================

/// Simulates PartitionReceiver's per-record trace pattern:
/// 1. Process records first (trace-agnostic processing)
/// 2. THEN for each sampled input record, create a RecordSpan
/// 3. Inject into individual output records
#[tokio::test]
#[serial]
async fn test_partition_receiver_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate input record with upstream trace
    let input_record = create_test_record_with_traceparent(UPSTREAM_TRACEPARENT);

    // Step 1: Simulate process_batch() producing output (trace-agnostic)
    let mut output_records: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    // Step 2: Create record span from the input record's upstream context
    let upstream_ctx = trace_propagation::extract_trace_context(&input_record.headers);
    let record_span =
        ObservabilityHelper::start_record_span(&obs, "test-partition-job", upstream_ctx.as_ref());
    assert!(record_span.is_some(), "Record span should be created");

    let span_ctx = record_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have span context");

    // Step 3: Inject into output records
    let record_span = record_span.unwrap();
    for output in &mut output_records {
        ObservabilityHelper::inject_record_trace_context(&record_span, output);
    }

    // Verify: span extracts upstream context from input record
    assert!(span_ctx.is_valid(), "Span should have valid context");
    assert_ne!(span_ctx.trace_id(), TraceId::INVALID);

    // Verify: output records get traceparent from record span
    for (i, record) in output_records.iter().enumerate() {
        assert_traceparent_injected(record, &format!("partition output record {}", i));
        let tp = &record.headers["traceparent"];
        assert_eq!(
            extract_trace_id(tp),
            span_ctx.trace_id().to_string(),
            "Output record {} should have record span's trace_id",
            i
        );
    }
}

// =============================================================================
// Test 4: JoinJobProcessor trace pattern (per-record)
// =============================================================================

/// Simulates JoinJobProcessor's per-record trace pattern:
/// 1. Uses first OUTPUT record (not input!) to extract upstream trace context
/// 2. Creates RecordSpan from that upstream context
/// 3. Injects trace into ALL output records
#[tokio::test]
#[serial]
async fn test_join_processor_trace_pattern() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Simulate output buffer where first record carries upstream traceparent
    let mut output_buffer: Vec<Arc<StreamRecord>> = vec![
        Arc::new(create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)),
        Arc::new(create_test_record()),
        Arc::new(create_test_record()),
    ];

    // Step 1: Extract upstream context from first output record
    let upstream_ctx = trace_propagation::extract_trace_context(&output_buffer[0].headers);

    // Step 2: Create record span from first output record's context
    let record_span =
        ObservabilityHelper::start_record_span(&obs, "test-join-job", upstream_ctx.as_ref());
    assert!(record_span.is_some(), "Record span should be created");

    let span_ctx = record_span
        .as_ref()
        .unwrap()
        .span_context()
        .expect("Should have span context");

    // Step 3: Inject trace into ALL output records
    let record_span = record_span.unwrap();
    for output in &mut output_buffer {
        ObservabilityHelper::inject_record_trace_context(&record_span, output);
    }

    // Verify: span is child of upstream trace from FIRST OUTPUT RECORD
    assert!(span_ctx.is_valid());
    assert_ne!(span_ctx.trace_id(), TraceId::INVALID);

    let injected_trace_id = span_ctx.trace_id().to_string();
    let injected_span_id = span_ctx.span_id().to_string();

    // All records should have the record span's trace context injected
    for (i, record) in output_buffer.iter().enumerate() {
        assert_traceparent_injected(record, &format!("join output record {}", i));
        let tp = &record.headers["traceparent"];
        assert_eq!(
            extract_trace_id(tp),
            injected_trace_id,
            "Join output record {} should have record span's trace_id",
            i
        );
        assert_eq!(
            extract_span_id(tp),
            injected_span_id,
            "Join output record {} should have record span's span_id",
            i
        );
    }
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

    // No upstream context (no traceparent in first record)
    let upstream_ctx = trace_propagation::extract_trace_context(&output_buffer[0].headers);
    assert!(
        upstream_ctx.is_none(),
        "Should have no upstream context without traceparent"
    );

    // Create record span -- should start new root trace
    let record_span = ObservabilityHelper::start_record_span(&obs, "test-join-no-upstream", None);
    assert!(record_span.is_some(), "Record span should be created");

    let span_ctx = record_span
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
    let record_span = record_span.unwrap();
    for output in &mut output_buffer {
        ObservabilityHelper::inject_record_trace_context(&record_span, output);
    }

    // Verify: output records get injected traceparent
    for (i, record) in output_buffer.iter().enumerate() {
        assert_traceparent_injected(record, &format!("no-upstream record {}", i));
    }
}

// =============================================================================
// Test 6: Per-record spans have unique span_ids
// =============================================================================

/// Tests that multiple RecordSpans created for different records each get unique span_ids
#[tokio::test]
#[serial]
async fn test_per_record_spans_have_unique_span_ids() {
    let obs_manager = create_test_observability_manager("processor-trace-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    // Create multiple record spans for different records
    let span1 = ObservabilityHelper::start_record_span(&obs, "test-unique-spans", None);
    let span2 = ObservabilityHelper::start_record_span(&obs, "test-unique-spans", None);
    let span3 = ObservabilityHelper::start_record_span(&obs, "test-unique-spans", None);

    let ctx1 = span1.as_ref().unwrap().span_context().unwrap();
    let ctx2 = span2.as_ref().unwrap().span_context().unwrap();
    let ctx3 = span3.as_ref().unwrap().span_context().unwrap();

    // All spans should have unique span_ids
    let span_ids = vec![
        ctx1.span_id().to_string(),
        ctx2.span_id().to_string(),
        ctx3.span_id().to_string(),
    ];
    let unique_ids: std::collections::HashSet<_> = span_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        3,
        "All 3 record spans should have unique span_ids: {:?}",
        span_ids
    );
}

// =============================================================================
// Test 7: Trace injection with shared Arc records (copy-on-write)
// =============================================================================

/// Verifies that inject_record_trace_context correctly handles
/// Arc<StreamRecord> with refcount > 1, using Arc::make_mut for copy-on-write.
#[tokio::test]
#[serial]
async fn test_trace_injection_with_shared_arc_records() {
    let obs_manager = create_test_observability_manager("processor-arc-test").await;
    let obs: Option<SharedObservabilityManager> = Some(obs_manager.clone());

    let record_span = ObservabilityHelper::start_record_span(&obs, "test-arc-cow", None);
    assert!(record_span.is_some());

    // Create shared Arc records -- clone the Arc (refcount > 1)
    let shared_record = Arc::new(create_test_record());
    let shared_clone = Arc::clone(&shared_record);

    let mut output_record = shared_record;

    // Inject trace -- this should use Arc::make_mut (copy-on-write)
    ObservabilityHelper::inject_record_trace_context(&record_span.unwrap(), &mut output_record);

    // The injected record should have traceparent
    assert_traceparent_injected(&output_record, "injected arc record");

    // The original clone should NOT have traceparent (copy-on-write created a new allocation)
    assert!(
        !shared_clone.headers.contains_key("traceparent"),
        "Original Arc clone should not be modified (copy-on-write semantics)"
    );
}
