//! Tests for per-record span completeness
//!
//! Validates that the per-record head-based sampling API works correctly:
//! - Sampled record -> 1 `process:job_name` span created
//! - Non-sampled record -> 0 spans
//! - record_deserialization / record_sql_processing / record_serialization_success
//!   now take 4 args and work with metrics-only (no spans)
//! - Graceful degradation under write lock with the new 4-arg API
//!
//! `with_observability_try_lock()` uses a single `try_read()` call.
//! During normal runtime, only read locks are acquired (metrics emission,
//! span creation), so `try_read()` virtually always succeeds. The only
//! write lock is held during initialization -- before any spans are created.
//! If `try_read()` fails, the span is gracefully dropped with a warning log.

use serial_test::serial;
use std::time::Duration;

use velostream::velostream::server::processors::common::BatchProcessingResultWithOutput;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;

use crate::unit::observability_test_helpers::create_test_observability_manager;

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

/// Test that a sampled record creates exactly 1 `process:job_name` span
#[tokio::test]
#[serial]
async fn test_sampled_record_creates_one_process_span() {
    let obs_manager = create_test_observability_manager("sampled-completeness").await;
    let obs = Some(obs_manager.clone());

    let job_name = "test_sampled_job";

    // Create a record span (simulates a sampled record)
    let record_span = ObservabilityHelper::start_record_span(&obs, job_name, None);
    assert!(record_span.is_some(), "Record span should be created");

    let mut span = record_span.unwrap();
    span.set_output_count(5);
    span.set_success();

    // Drop span to trigger export to collector
    drop(span);

    // Verify exactly 1 process span was created
    let span_names = collect_span_names(&obs_manager).await;

    let process_spans: Vec<_> = span_names
        .iter()
        .filter(|n| n.contains("process:test_sampled_job"))
        .collect();

    assert_eq!(
        process_spans.len(),
        1,
        "Should have exactly 1 process span for sampled record. Spans: {:?}",
        span_names
    );
}

/// Test that a non-sampled record creates 0 spans
/// (start_record_span is only called for sampled records, so this tests
///  that no spans appear when we simply don't call start_record_span)
#[tokio::test]
#[serial]
async fn test_non_sampled_record_creates_zero_spans() {
    let obs_manager = create_test_observability_manager("non-sampled-completeness").await;
    let obs = Some(obs_manager.clone());

    let job_name = "test_non_sampled_job";

    // For a non-sampled record, we do NOT call start_record_span.
    // We only call the metrics-only functions.
    ObservabilityHelper::record_deserialization(&obs, job_name, 10, 5);

    let batch_result = make_success_batch_result(10);
    ObservabilityHelper::record_sql_processing(&obs, job_name, &batch_result, 8);

    ObservabilityHelper::record_serialization_success(&obs, job_name, 10, 3);

    // Verify 0 process spans were created
    let span_names = collect_span_names(&obs_manager).await;

    let process_spans: Vec<_> = span_names
        .iter()
        .filter(|n| n.contains("process:test_non_sampled_job"))
        .collect();

    assert_eq!(
        process_spans.len(),
        0,
        "Should have 0 process spans for non-sampled record. Spans: {:?}",
        span_names
    );
}

/// Test that record_deserialization takes 4 args (obs, job_name, record_count, duration_ms)
/// and does NOT create spans (metrics-only)
#[tokio::test]
#[serial]
async fn test_record_deserialization_4_args_metrics_only() {
    let obs_manager = create_test_observability_manager("deser-4-args").await;
    let obs = Some(obs_manager.clone());

    let initial_span_count = {
        let lock = obs_manager.read().await;
        lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
    };

    // Call with 4 args: obs, job_name, record_count, duration_ms
    ObservabilityHelper::record_deserialization(&obs, "deser-test", 100, 10);

    let final_span_count = {
        let lock = obs_manager.read().await;
        lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
    };

    assert_eq!(
        initial_span_count, final_span_count,
        "record_deserialization should not create any spans (metrics-only)"
    );
}

/// Test that record_sql_processing takes 4 args (obs, job_name, batch_result, duration_ms)
/// and does NOT create spans (metrics-only)
#[tokio::test]
#[serial]
async fn test_record_sql_processing_4_args_metrics_only() {
    let obs_manager = create_test_observability_manager("sql-4-args").await;
    let obs = Some(obs_manager.clone());

    let initial_span_count = {
        let lock = obs_manager.read().await;
        lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
    };

    let batch_result = make_success_batch_result(50);
    // Call with 4 args: obs, job_name, batch_result, duration_ms
    ObservabilityHelper::record_sql_processing(&obs, "sql-test", &batch_result, 15);

    let final_span_count = {
        let lock = obs_manager.read().await;
        lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
    };

    assert_eq!(
        initial_span_count, final_span_count,
        "record_sql_processing should not create any spans (metrics-only)"
    );
}

/// Test that record_serialization_success takes 4 args (obs, job_name, record_count, duration_ms)
/// and does NOT create spans (metrics-only)
#[tokio::test]
#[serial]
async fn test_record_serialization_success_4_args_metrics_only() {
    let obs_manager = create_test_observability_manager("ser-4-args").await;
    let obs = Some(obs_manager.clone());

    let initial_span_count = {
        let lock = obs_manager.read().await;
        lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
    };

    // Call with 4 args: obs, job_name, record_count, duration_ms
    ObservabilityHelper::record_serialization_success(&obs, "ser-test", 100, 5);

    let final_span_count = {
        let lock = obs_manager.read().await;
        lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
    };

    assert_eq!(
        initial_span_count, final_span_count,
        "record_serialization_success should not create any spans (metrics-only)"
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

    let job_name = "contention_test";

    // Hold a write lock and attempt span creation from a blocking thread.
    // try_read() should fail and return None gracefully.
    let obs_clone = obs_manager.clone();
    let obs_for_spans = obs.clone();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let span_task = tokio::task::spawn_blocking(move || {
        // Wait for write lock to be acquired
        rx.blocking_recv().unwrap();

        // try_read() will fail because a write lock is held.
        // start_record_span should return None gracefully.
        let record_span = ObservabilityHelper::start_record_span(&obs_for_spans, job_name, None);
        assert!(
            record_span.is_none(),
            "start_record_span should return None when write lock is held"
        );

        // 4-arg metrics-only calls should also handle the failure gracefully
        ObservabilityHelper::record_deserialization(&obs_for_spans, job_name, 10, 5);

        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(&obs_for_spans, job_name, &batch_result, 8);

        ObservabilityHelper::record_serialization_success(&obs_for_spans, job_name, 10, 3);
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

/// Test that multiple sampled records each produce their own process span
#[tokio::test]
#[serial]
async fn test_span_completeness_across_multiple_sampled_records() {
    let obs_manager = create_test_observability_manager("multi-record-completeness").await;
    let obs = Some(obs_manager.clone());

    let job_name = "multi_record_test";
    let record_count = 10;

    for i in 0..record_count {
        let mut record_span = ObservabilityHelper::start_record_span(&obs, job_name, None);
        assert!(
            record_span.is_some(),
            "Record #{}: span should be created",
            i
        );

        let span = record_span.as_mut().unwrap();
        span.set_output_count(1);
        span.set_success();
        drop(record_span);
    }

    let span_names = collect_span_names(&obs_manager).await;

    let process_spans: Vec<_> = span_names
        .iter()
        .filter(|n| n.contains("process:multi_record_test"))
        .collect();

    assert_eq!(
        process_spans.len(),
        record_count,
        "Expected {} process spans (one per sampled record), got {}. Spans: {:?}",
        record_count,
        process_spans.len(),
        span_names
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

    // Create spans while the read lock is held -- try_read() should succeed
    // because RwLock allows multiple concurrent readers.
    let obs_for_blocking = obs.clone();

    let span_task = tokio::task::spawn_blocking(move || {
        let record_span = ObservabilityHelper::start_record_span(&obs_for_blocking, job_name, None);
        assert!(
            record_span.is_some(),
            "Record span should succeed with concurrent read locks"
        );

        // 4-arg metrics calls should also work
        ObservabilityHelper::record_deserialization(&obs_for_blocking, job_name, 10, 5);

        let batch_result = make_success_batch_result(10);
        ObservabilityHelper::record_sql_processing(&obs_for_blocking, job_name, &batch_result, 8);

        ObservabilityHelper::record_serialization_success(&obs_for_blocking, job_name, 10, 3);
    });

    span_task.await.unwrap();
    drop(_read_guard);

    // Verify the record span was created successfully
    let span_names = collect_span_names(&obs_manager).await;
    assert!(
        span_names
            .iter()
            .any(|n| n.contains("process:concurrent_read_test")),
        "Record span should be created with concurrent reads. Spans: {:?}",
        span_names
    );
}
