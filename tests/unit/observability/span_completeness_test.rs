//! Tests for span completeness
//!
//! Validates that child spans (deserialization, sql_query, serialization)
//! are correctly created by all processor types and that the span tree is
//! complete across multiple batches.
//!
//! `with_observability_try_lock()` uses a single `try_read()` call.
//! During normal runtime, only read locks are acquired (metrics emission,
//! span creation), so `try_read()` virtually always succeeds. The only
//! write lock is held during initialization — before any spans are created.
//! If `try_read()` fails, the span is gracefully dropped with a warning log.

use serial_test::serial;
use std::sync::Arc;
use std::time::{Duration, Instant};

use velostream::velostream::server::processors::common::BatchProcessingResultWithOutput;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::StreamRecord;

use crate::unit::observability_test_helpers::{
    UPSTREAM_TRACEPARENT, create_test_observability_manager, create_test_record,
    create_test_record_with_traceparent,
};

/// Helper to collect span names from the observability manager
async fn collect_span_names(
    obs_manager: &velostream::velostream::observability::SharedObservabilityManager,
) -> Vec<String> {
    let lock = obs_manager.read().await;
    if let Some(telemetry) = lock.telemetry() {
        telemetry
            .collected_spans()
            .iter()
            .map(|s| s.name.to_string())
            .collect()
    } else {
        vec![]
    }
}

/// Helper to create a success batch result for sql_processing
fn make_success_batch_result(records_processed: usize) -> BatchProcessingResultWithOutput {
    BatchProcessingResultWithOutput {
        records_processed,
        records_failed: 0,
        processing_time: Duration::from_millis(10),
        batch_size: records_processed,
        error_details: vec![],
        output_records: vec![],
    }
}

/// Test that a simple processor creates ALL expected child spans
/// Expected: batch + source + deserialization + sql_query + serialization = 5 spans
#[tokio::test]
#[serial]
async fn test_simple_processor_span_completeness() {
    let obs_manager = create_test_observability_manager("simple-completeness").await;
    let obs = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let job_name = "test_simple_job";

    // Parent batch span
    let mut parent_span = ObservabilityHelper::start_batch_span(&obs, job_name, 0, &input_batch);
    assert!(parent_span.is_some(), "Parent batch span should be created");

    // Source child span
    let _source_span = ObservabilityHelper::start_batch_span(
        &obs,
        &format!("{} (source: input_topic)", job_name),
        0,
        &input_batch,
    );

    // Deserialization child span
    ObservabilityHelper::record_deserialization(
        &obs,
        job_name,
        &parent_span,
        10,
        5,
        Some(("input_topic", 0, 0)),
    );

    // SQL processing child span
    let batch_result = make_success_batch_result(10);
    ObservabilityHelper::record_sql_processing(
        &obs,
        job_name,
        &parent_span,
        &batch_result,
        8,
        None,
        "SELECT * FROM test",
    );

    // Serialization child span
    ObservabilityHelper::record_serialization_success(&obs, job_name, &parent_span, 10, 3, None);

    // Inject trace context into output
    let mut output_records: Vec<Arc<StreamRecord>> =
        (0..10).map(|_| Arc::new(create_test_record())).collect();
    ObservabilityHelper::inject_trace_context_into_records(
        &parent_span,
        &mut output_records,
        job_name,
    );

    // Complete batch span
    let batch_start = Instant::now();
    ObservabilityHelper::complete_batch_span_success(&mut parent_span, &batch_start, 10);

    // Drop spans to trigger export to collector
    drop(parent_span);
    drop(_source_span);

    // Verify all expected spans were created
    let span_names = collect_span_names(&obs_manager).await;

    assert!(
        span_names
            .iter()
            .any(|n| n.contains("batch:test_simple_job")),
        "Missing parent batch span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("source: input_topic")),
        "Missing source child span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("deserialization")),
        "Missing deserialization span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("sql_query")),
        "Missing sql_query span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("serialization")),
        "Missing serialization span. Spans: {:?}",
        span_names
    );

    // Verify output records got traceparent
    for record in &output_records {
        assert!(
            record.headers.contains_key("traceparent"),
            "Output record should have traceparent header"
        );
    }
}

/// Test that a transactional processor creates ALL expected child spans
/// Expected: batch + deserialization + sql_query + serialization = 4 spans
#[tokio::test]
#[serial]
async fn test_transactional_processor_span_completeness() {
    let obs_manager = create_test_observability_manager("txn-completeness").await;
    let obs = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let job_name = "test_txn_job";

    // Batch span
    let mut batch_span = ObservabilityHelper::start_batch_span(&obs, job_name, 0, &input_batch);
    assert!(batch_span.is_some(), "Batch span should be created");

    // Deserialization
    ObservabilityHelper::record_deserialization(&obs, job_name, &batch_span, 10, 5, None);

    // SQL processing
    let batch_result = make_success_batch_result(10);
    ObservabilityHelper::record_sql_processing(
        &obs,
        job_name,
        &batch_span,
        &batch_result,
        8,
        None,
        "SELECT * FROM test",
    );

    // Serialization
    ObservabilityHelper::record_serialization_success(&obs, job_name, &batch_span, 10, 3, None);

    // Complete
    let batch_start = Instant::now();
    ObservabilityHelper::complete_batch_span_success(&mut batch_span, &batch_start, 10);
    drop(batch_span);

    // Verify all spans
    let span_names = collect_span_names(&obs_manager).await;

    assert!(
        span_names.iter().any(|n| n.contains("batch:test_txn_job")),
        "Missing batch span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("deserialization")),
        "Missing deserialization span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("sql_query")),
        "Missing sql_query span. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("serialization")),
        "Missing serialization span. Spans: {:?}",
        span_names
    );

    assert!(
        span_names.len() >= 4,
        "Expected at least 4 spans (batch + deser + sql + ser), got {}. Spans: {:?}",
        span_names.len(),
        span_names
    );
}

/// Test that `with_observability_try_lock` gracefully returns None when a
/// write lock is held (no panics, no hangs).
///
/// During normal runtime, only read locks are acquired so `try_read()`
/// always succeeds. The only write lock is during initialization, before
/// any spans are created. This test validates the graceful degradation
/// path: if `try_read()` fails, spans are simply not created (None).
#[tokio::test]
#[serial]
async fn test_graceful_degradation_under_write_lock() {
    let obs_manager = create_test_observability_manager("lock-contention").await;
    let obs = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let job_name = "contention_test";

    // Hold a write lock and attempt span creation from a blocking thread.
    // try_read() should fail and return None gracefully.
    let obs_clone = obs_manager.clone();
    let obs_for_spans = obs.clone();
    let input_batch_clone = input_batch.clone();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let span_task = tokio::task::spawn_blocking(move || {
        // Wait for write lock to be acquired
        rx.blocking_recv().unwrap();

        // try_read() will fail because a write lock is held.
        // start_batch_span should return None gracefully.
        let batch_span =
            ObservabilityHelper::start_batch_span(&obs_for_spans, job_name, 0, &input_batch_clone);
        assert!(
            batch_span.is_none(),
            "start_batch_span should return None when write lock is held"
        );

        // record_deserialization should also handle the failure gracefully
        ObservabilityHelper::record_deserialization(&obs_for_spans, job_name, &None, 10, 5, None);

        // record_sql_processing should handle gracefully too
        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(
            &obs_for_spans,
            job_name,
            &None,
            &batch_result,
            8,
            None,
            "SELECT * FROM test",
        );
    });

    // Hold write lock while the blocking thread attempts span creation
    let write_lock_task = tokio::spawn(async move {
        let _write_guard = obs_clone.write().await;
        let _ = tx.send(());
        // Hold long enough for the blocking thread to attempt and fail
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    span_task.await.unwrap();
    write_lock_task.await.unwrap();

    // No panics or hangs = test passes. Spans were gracefully skipped.
}

/// Test that multiple batches all produce complete span trees
/// (no span dropoff after the first batch)
#[tokio::test]
#[serial]
async fn test_span_completeness_across_multiple_batches() {
    let obs_manager = create_test_observability_manager("multi-batch-completeness").await;
    let obs = Some(obs_manager.clone());

    let job_name = "multi_batch_test";
    let batch_count = 10;

    for batch_id in 0..batch_count {
        let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

        let mut batch_span =
            ObservabilityHelper::start_batch_span(&obs, job_name, batch_id, &input_batch);
        assert!(
            batch_span.is_some(),
            "Batch #{}: span should be created",
            batch_id
        );

        ObservabilityHelper::record_deserialization(&obs, job_name, &batch_span, 10, 2, None);

        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(
            &obs,
            job_name,
            &batch_span,
            &batch_result,
            5,
            None,
            "SELECT * FROM test",
        );

        ObservabilityHelper::record_serialization_success(&obs, job_name, &batch_span, 10, 1, None);

        let batch_start = Instant::now();
        ObservabilityHelper::complete_batch_span_success(&mut batch_span, &batch_start, 10);
        drop(batch_span);
    }

    let span_names = collect_span_names(&obs_manager).await;

    // Each batch should produce 4 spans: batch + deser + sql + ser
    let batch_spans: Vec<_> = span_names.iter().filter(|n| n.contains("batch:")).collect();
    let deser_spans: Vec<_> = span_names
        .iter()
        .filter(|n| n.as_str() == "streaming:deserialization")
        .collect();
    let sql_spans: Vec<_> = span_names
        .iter()
        .filter(|n| n.contains("sql_query"))
        .collect();
    let ser_spans: Vec<_> = span_names
        .iter()
        .filter(|n| n.as_str() == "streaming:serialization")
        .collect();

    assert_eq!(
        batch_spans.len(),
        batch_count as usize,
        "Expected {} batch spans, got {}",
        batch_count,
        batch_spans.len()
    );
    assert_eq!(
        deser_spans.len(),
        batch_count as usize,
        "Expected {} deserialization spans, got {} (dropoff!)",
        batch_count,
        deser_spans.len()
    );
    assert_eq!(
        sql_spans.len(),
        batch_count as usize,
        "Expected {} sql_query spans, got {} (dropoff!)",
        batch_count,
        sql_spans.len()
    );
    assert_eq!(
        ser_spans.len(),
        batch_count as usize,
        "Expected {} serialization spans, got {} (dropoff!)",
        batch_count,
        ser_spans.len()
    );
}

/// Test that `with_observability_try_lock` succeeds when only read locks
/// are held (the normal runtime scenario).
///
/// Multiple concurrent readers should all succeed with `try_read()`.
#[tokio::test]
#[serial]
async fn test_concurrent_read_locks_all_succeed() {
    let obs_manager = create_test_observability_manager("concurrent-reads").await;
    let obs = Some(obs_manager.clone());

    let job_name = "concurrent_read_test";

    // Hold a read lock from the main task (simulating another processor reading)
    let _read_guard = obs_manager.read().await;

    // Create spans while the read lock is held — try_read() should succeed
    // because RwLock allows multiple concurrent readers.
    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let obs_for_blocking = obs.clone();
    let input_batch_clone = input_batch.clone();

    let span_task = tokio::task::spawn_blocking(move || {
        let batch_span = ObservabilityHelper::start_batch_span(
            &obs_for_blocking,
            job_name,
            0,
            &input_batch_clone,
        );
        assert!(
            batch_span.is_some(),
            "Batch span should succeed with concurrent read locks"
        );

        ObservabilityHelper::record_deserialization(
            &obs_for_blocking,
            job_name,
            &batch_span,
            10,
            5,
            None,
        );

        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(
            &obs_for_blocking,
            job_name,
            &batch_span,
            &batch_result,
            8,
            None,
            "SELECT * FROM test",
        );

        ObservabilityHelper::record_serialization_success(
            &obs_for_blocking,
            job_name,
            &batch_span,
            10,
            3,
            None,
        );
    });

    span_task.await.unwrap();
    drop(_read_guard);

    // Verify all spans were created successfully
    let span_names = collect_span_names(&obs_manager).await;
    assert!(
        span_names.iter().any(|n| n.contains("deserialization")),
        "Deserialization span should be created with concurrent reads. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("sql_query")),
        "sql_query span should be created with concurrent reads. Spans: {:?}",
        span_names
    );
    assert!(
        span_names.iter().any(|n| n.contains("serialization")),
        "Serialization span should be created with concurrent reads. Spans: {:?}",
        span_names
    );
}
