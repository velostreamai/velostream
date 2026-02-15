//! Tests for span completeness under lock contention
//!
//! Validates that child spans (deserialization, sql_query, serialization)
//! are NOT silently dropped when the ObservabilityManager RwLock is contended.
//!
//! Root cause discovered in production: `with_observability_try_lock()` uses
//! a single `try_read()` with no retry and no logging. When a write lock is
//! held (e.g., during metrics registration/emission), child spans are silently dropped.
//!
//! This manifests as incomplete traces in Tempo:
//! - Simple processor: batch span only (missing deser, sql_query, serialization)
//! - Transactional: batch + deser (missing sql_query, serialization)
//! - Adaptive: mostly complete but occasionally drops sql_query

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
    ObservabilityHelper::record_sql_processing(&obs, job_name, &batch_span, &batch_result, 8, None);

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

/// Test that child spans survive under write lock contention
///
/// This reproduces the production bug: when a write lock is held
/// (e.g., metrics emission), `with_observability_try_lock()` fails
/// and child spans are silently dropped.
#[tokio::test]
#[serial]
async fn test_child_spans_not_dropped_under_write_lock_contention() {
    let obs_manager = create_test_observability_manager("lock-contention").await;
    let obs = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let job_name = "contention_test";

    // Start batch span (succeeds - uses try_read with retry)
    let mut batch_span = ObservabilityHelper::start_batch_span(&obs, job_name, 0, &input_batch);
    assert!(batch_span.is_some(), "Batch span should be created");

    // Simulate realistic write lock contention: hold a write lock briefly
    // while trying to create child spans on another thread.
    // In production, write locks are held during metrics registration (~1ms).
    // The fix adds retry with 1ms sleeps, so a 3ms hold should be survivable.
    let obs_clone = obs_manager.clone();
    let obs_for_spans = obs.clone();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn child span creation on a blocking thread (simulates sync processor context)
    // We create a fresh batch span on the blocking thread since BatchSpan isn't Clone
    let obs_for_batch = obs.clone();
    let input_batch_clone = input_batch.clone();
    let span_task = tokio::task::spawn_blocking(move || {
        // Wait for write lock to be acquired on the main task
        rx.blocking_recv().unwrap();

        // Create child spans while write lock is held by another task.
        // We need a batch span for parent context - create one directly
        // (this will also contend on the lock, testing start_batch_span retry too)
        let local_batch_span =
            ObservabilityHelper::start_batch_span(&obs_for_batch, job_name, 1, &input_batch_clone);

        ObservabilityHelper::record_deserialization(
            &obs_for_spans,
            job_name,
            &local_batch_span,
            10,
            5,
            None,
        );

        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(
            &obs_for_spans,
            job_name,
            &local_batch_span,
            &batch_result,
            8,
            None,
        );

        ObservabilityHelper::record_serialization_success(
            &obs_for_spans,
            job_name,
            &local_batch_span,
            10,
            3,
            None,
        );
    });

    // Hold write lock briefly (3ms - simulates metrics registration)
    let write_lock_task = tokio::spawn(async move {
        let _write_guard = obs_clone.write().await;
        // Signal that we hold the write lock
        let _ = tx.send(());
        // Hold it for 3ms to simulate brief contention
        tokio::time::sleep(Duration::from_millis(3)).await;
        // Write guard dropped here
    });

    // Wait for both tasks to complete
    span_task.await.unwrap();
    write_lock_task.await.unwrap();

    // Complete batch
    let batch_start = Instant::now();
    ObservabilityHelper::complete_batch_span_success(&mut batch_span, &batch_start, 10);
    drop(batch_span);

    // Check how many spans were created
    let span_names = collect_span_names(&obs_manager).await;

    // This test documents the CURRENT (buggy) behavior:
    // Under write lock contention, child spans are dropped.
    // After the fix, all 4 spans should be present.
    let has_deser = span_names.iter().any(|n| n.contains("deserialization"));
    let has_sql = span_names.iter().any(|n| n.contains("sql_query"));
    let has_ser = span_names.iter().any(|n| n.contains("serialization"));

    let dropped_count = [!has_deser, !has_sql, !has_ser]
        .iter()
        .filter(|&&dropped| dropped)
        .count();

    if dropped_count > 0 {
        eprintln!(
            "WARNING: {} child span(s) were silently dropped under write lock contention!",
            dropped_count
        );
        eprintln!(
            "  deserialization: {}",
            if has_deser { "OK" } else { "DROPPED" }
        );
        eprintln!(
            "  sql_query:       {}",
            if has_sql { "OK" } else { "DROPPED" }
        );
        eprintln!(
            "  serialization:   {}",
            if has_ser { "OK" } else { "DROPPED" }
        );
        eprintln!("  All spans: {:?}", span_names);

        // FAIL: This is the bug we need to fix
        panic!(
            "Child spans silently dropped under write lock contention. \
             This causes incomplete traces in Tempo. \
             Fix: Add retry with backoff to with_observability_try_lock()"
        );
    }
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

/// Test that child spans survive brief write lock contention
///
/// The retry mechanism (5 attempts x 1ms sleep) should handle
/// typical write lock holds during metrics registration (~1-3ms).
#[tokio::test]
#[serial]
async fn test_try_lock_retry_survives_brief_contention() {
    let obs_manager = create_test_observability_manager("try-lock-retry").await;
    let obs = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let job_name = "try_lock_test";

    // Create batch span first (succeeds)
    let batch_span = ObservabilityHelper::start_batch_span(&obs, job_name, 0, &input_batch);
    assert!(batch_span.is_some());

    // Hold write lock briefly from another task while child span is created
    let obs_clone = obs_manager.clone();
    let obs_for_ser = obs.clone();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let ser_task = tokio::task::spawn_blocking(move || {
        // Wait for write lock to be acquired
        rx.blocking_recv().unwrap();

        // Try to create serialization span while write lock is held
        // Pass None for batch_span since we can't clone it - the span
        // will still be created, just without parent context
        ObservabilityHelper::record_serialization_success(
            &obs_for_ser,
            job_name,
            &None,
            10,
            3,
            None,
        );
    });

    // Hold write lock briefly (2ms)
    let write_task = tokio::spawn(async move {
        let _guard = obs_clone.write().await;
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(2)).await;
    });

    ser_task.await.unwrap();
    write_task.await.unwrap();

    // Verify the span was NOT dropped
    let span_names = collect_span_names(&obs_manager).await;
    let has_ser = span_names
        .iter()
        .any(|n| n.as_str() == "streaming:serialization");

    assert!(
        has_ser,
        "Serialization span should survive brief write lock contention. \
         with_observability_try_lock retries should handle ~2ms contention. \
         Got spans: {:?}",
        span_names
    );
}

/// Test record_sql_processing survives brief write lock contention
///
/// Previously used bare try_read() with no retry - now uses
/// with_observability_try_lock which retries up to 5 times.
#[tokio::test]
#[serial]
async fn test_sql_processing_span_not_dropped_under_contention() {
    let obs_manager = create_test_observability_manager("sql-contention").await;
    let obs = Some(obs_manager.clone());

    let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];
    let job_name = "sql_contention_test";

    let _batch_span = ObservabilityHelper::start_batch_span(&obs, job_name, 0, &input_batch);
    assert!(_batch_span.is_some());

    // Hold write lock briefly from another task
    let obs_clone = obs_manager.clone();
    let obs_for_sql = obs.clone();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let sql_task = tokio::task::spawn_blocking(move || {
        // Wait for write lock to be acquired
        rx.blocking_recv().unwrap();

        // record_sql_processing should now retry via with_observability_try_lock
        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(
            &obs_for_sql,
            job_name,
            &None, // No parent context needed for this test
            &batch_result,
            8,
            None,
        );
    });

    // Hold write lock briefly (2ms)
    let write_task = tokio::spawn(async move {
        let _guard = obs_clone.write().await;
        let _ = tx.send(());
        tokio::time::sleep(Duration::from_millis(2)).await;
    });

    sql_task.await.unwrap();
    write_task.await.unwrap();

    let span_names = collect_span_names(&obs_manager).await;
    let has_sql = span_names.iter().any(|n| n.contains("sql_query"));

    assert!(
        has_sql,
        "sql_query span should survive brief write lock contention. \
         record_sql_processing now uses with_observability_try_lock with retry. \
         Got spans: {:?}",
        span_names
    );
}
