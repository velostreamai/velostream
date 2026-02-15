//! Diagnostic test: Compare span creation and logging across all 4 processor types
//!
//! This test initializes env_logger and exercises each processor type's trace pattern,
//! printing detailed span creation logs so we can compare behavior.
//!
//! Run with: RUST_LOG=debug cargo test --tests --no-default-features -- processor_span_diagnostic --nocapture

use serial_test::serial;
use std::sync::Arc;
use std::time::Instant;

use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::StreamRecord;

use crate::unit::observability_test_helpers::{
    UPSTREAM_TRACEPARENT, create_test_observability_manager, create_test_record,
    create_test_record_with_traceparent,
};

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Helper to get span count from the observability manager's telemetry
async fn get_span_count(
    obs_manager: &velostream::velostream::observability::SharedObservabilityManager,
) -> usize {
    let lock = obs_manager.read().await;
    lock.telemetry().map(|t| t.span_count()).unwrap_or(0)
}

/// Run all 4 processor types through 5 batch cycles and compare collected span counts
#[tokio::test]
#[serial]
async fn test_processor_span_diagnostic_all_types() {
    init_logger();

    let obs_manager = create_test_observability_manager("processor-diagnostic").await;
    let obs = Some(obs_manager.clone());

    let batch_count = 5u64;
    let records_per_batch = 3;

    // =========================================================================
    // Processor 1: SimpleJobProcessor pattern
    // =========================================================================
    println!("\n========== SIMPLE JOB PROCESSOR ==========");
    let initial_count = get_span_count(&obs_manager).await;
    for batch_id in 0..batch_count {
        let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

        // Parent batch span
        let parent_span =
            ObservabilityHelper::start_batch_span(&obs, "simple-job", batch_id, &input_batch);
        assert!(
            parent_span.is_some(),
            "Simple: batch #{} parent span should be Some",
            batch_id
        );

        // Per-source child span
        let source_span = ObservabilityHelper::start_batch_span(
            &obs,
            "simple-job (source: input_topic)",
            batch_id,
            &input_batch,
        );
        assert!(
            source_span.is_some(),
            "Simple: batch #{} source span should be Some",
            batch_id
        );

        // Child spans: deserialization, SQL, serialization
        ObservabilityHelper::record_deserialization(
            &obs,
            "simple-job",
            &parent_span,
            records_per_batch,
            2,
            Some(("input_topic", 0, batch_id as i64)),
        );

        ObservabilityHelper::record_serialization_success(
            &obs,
            "simple-job",
            &parent_span,
            records_per_batch,
            1,
            None,
        );

        // Inject trace context into output records
        let mut output_records: Vec<Arc<StreamRecord>> = (0..records_per_batch)
            .map(|_| Arc::new(create_test_record()))
            .collect();
        ObservabilityHelper::inject_trace_context_into_records(
            &parent_span,
            &mut output_records,
            "simple-job",
        );

        // Complete batch span
        let batch_start = Instant::now();
        let mut parent_span = parent_span;
        ObservabilityHelper::complete_batch_span_success(
            &mut parent_span,
            &batch_start,
            records_per_batch as u64,
        );

        // Verify output records got traceparent
        for record in &output_records {
            assert!(
                record.headers.contains_key("traceparent"),
                "Simple: batch #{} output should have traceparent",
                batch_id
            );
        }

        drop(parent_span);
        drop(source_span);
    }
    let simple_spans = get_span_count(&obs_manager).await - initial_count;
    println!(
        "Simple: {} spans created across {} batches ({} per batch)",
        simple_spans,
        batch_count,
        simple_spans as f64 / batch_count as f64
    );

    // =========================================================================
    // Processor 2: TransactionalJobProcessor pattern
    // =========================================================================
    println!("\n========== TRANSACTIONAL JOB PROCESSOR ==========");
    let initial_count = get_span_count(&obs_manager).await;
    for batch_id in 0..batch_count {
        let input_batch = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

        // Single batch span for entire transaction
        let mut batch_span =
            ObservabilityHelper::start_batch_span(&obs, "txn-job", batch_id, &input_batch);
        assert!(
            batch_span.is_some(),
            "Txn: batch #{} span should be Some",
            batch_id
        );

        // Child spans
        ObservabilityHelper::record_deserialization(
            &obs,
            "txn-job",
            &batch_span,
            records_per_batch,
            2,
            None,
        );

        ObservabilityHelper::record_serialization_success(
            &obs,
            "txn-job",
            &batch_span,
            records_per_batch,
            1,
            None,
        );

        // Inject trace context
        let mut output_records: Vec<Arc<StreamRecord>> = (0..records_per_batch)
            .map(|_| Arc::new(create_test_record()))
            .collect();
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span,
            &mut output_records,
            "txn-job",
        );

        // Complete after commit
        let batch_start = Instant::now();
        ObservabilityHelper::complete_batch_span_success(
            &mut batch_span,
            &batch_start,
            records_per_batch as u64,
        );

        for record in &output_records {
            assert!(
                record.headers.contains_key("traceparent"),
                "Txn: batch #{} output should have traceparent",
                batch_id
            );
        }

        drop(batch_span);
    }
    let txn_spans = get_span_count(&obs_manager).await - initial_count;
    println!(
        "Transactional: {} spans created across {} batches ({} per batch)",
        txn_spans,
        batch_count,
        txn_spans as f64 / batch_count as f64
    );

    // =========================================================================
    // Processor 3: JoinJobProcessor pattern
    // =========================================================================
    println!("\n========== JOIN JOB PROCESSOR ==========");
    let initial_count = get_span_count(&obs_manager).await;
    for batch_id in 0..batch_count {
        // Join uses OUTPUT records to extract trace context
        let mut output_buffer: Vec<Arc<StreamRecord>> = vec![
            Arc::new(create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)),
            Arc::new(create_test_record()),
            Arc::new(create_test_record()),
        ];

        // Clone first output record for span creation
        let first_records: Vec<StreamRecord> = output_buffer
            .first()
            .map(|r| vec![(**r).clone()])
            .unwrap_or_default();

        // Batch span from first output record
        let mut batch_span =
            ObservabilityHelper::start_batch_span(&obs, "join-job", batch_id, &first_records);
        assert!(
            batch_span.is_some(),
            "Join: batch #{} span should be Some",
            batch_id
        );

        // Inject into ALL output records
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span,
            &mut output_buffer,
            "join-job",
        );

        // Serialization child span
        ObservabilityHelper::record_serialization_success(
            &obs,
            "join-job",
            &batch_span,
            output_buffer.len(),
            1,
            None,
        );

        // Complete
        let batch_start = Instant::now();
        ObservabilityHelper::complete_batch_span_success(
            &mut batch_span,
            &batch_start,
            output_buffer.len() as u64,
        );

        for record in &output_buffer {
            assert!(
                record.headers.contains_key("traceparent"),
                "Join: batch #{} output should have traceparent",
                batch_id
            );
        }

        drop(batch_span);
    }
    let join_spans = get_span_count(&obs_manager).await - initial_count;
    println!(
        "Join: {} spans created across {} batches ({} per batch)",
        join_spans,
        batch_count,
        join_spans as f64 / batch_count as f64
    );

    // =========================================================================
    // Processor 4: PartitionReceiver pattern
    // =========================================================================
    println!("\n========== PARTITION RECEIVER ==========");
    let initial_count = get_span_count(&obs_manager).await;
    for batch_id in 0..batch_count {
        let input_batch = vec![
            create_test_record_with_traceparent(UPSTREAM_TRACEPARENT),
            create_test_record(),
            create_test_record(),
        ];

        // Batch span from input batch
        let mut batch_span =
            ObservabilityHelper::start_batch_span(&obs, "partition-job", batch_id, &input_batch);
        assert!(
            batch_span.is_some(),
            "Partition: batch #{} span should be Some",
            batch_id
        );

        // SQL processing child span
        // (PartitionReceiver records SQL but not deserialization)
        ObservabilityHelper::record_serialization_success(
            &obs,
            "partition-job",
            &batch_span,
            records_per_batch,
            1,
            None,
        );

        // Inject trace context into output
        let mut output_records: Vec<Arc<StreamRecord>> = (0..records_per_batch)
            .map(|_| Arc::new(create_test_record()))
            .collect();
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span,
            &mut output_records,
            "partition-job",
        );

        // Complete
        let batch_start = Instant::now();
        ObservabilityHelper::complete_batch_span_success(
            &mut batch_span,
            &batch_start,
            records_per_batch as u64,
        );

        for record in &output_records {
            assert!(
                record.headers.contains_key("traceparent"),
                "Partition: batch #{} output should have traceparent",
                batch_id
            );
        }

        drop(batch_span);
    }
    let partition_spans = get_span_count(&obs_manager).await - initial_count;
    println!(
        "Partition: {} spans created across {} batches ({} per batch)",
        partition_spans,
        batch_count,
        partition_spans as f64 / batch_count as f64
    );

    // =========================================================================
    // Summary comparison
    // =========================================================================
    let total = get_span_count(&obs_manager).await;
    println!("\n========== SUMMARY ==========");
    println!(
        "Simple:        {} spans ({} per batch)",
        simple_spans,
        simple_spans as f64 / batch_count as f64
    );
    println!(
        "Transactional: {} spans ({} per batch)",
        txn_spans,
        txn_spans as f64 / batch_count as f64
    );
    println!(
        "Join:          {} spans ({} per batch)",
        join_spans,
        join_spans as f64 / batch_count as f64
    );
    println!(
        "Partition:     {} spans ({} per batch)",
        partition_spans,
        partition_spans as f64 / batch_count as f64
    );
    println!("Total collected: {} spans", total);
    println!("=============================\n");

    // All processor types should create spans for every batch
    assert!(simple_spans > 0, "Simple processor should create spans");
    assert!(txn_spans > 0, "Transactional processor should create spans");
    assert!(join_spans > 0, "Join processor should create spans");
    assert!(
        partition_spans > 0,
        "Partition processor should create spans"
    );

    // Each processor should create at least 2 spans per batch (parent + at least 1 child)
    assert!(
        simple_spans >= batch_count as usize * 2,
        "Simple: expected at least {} spans, got {}",
        batch_count * 2,
        simple_spans
    );
    assert!(
        txn_spans >= batch_count as usize * 2,
        "Txn: expected at least {} spans, got {}",
        batch_count * 2,
        txn_spans
    );
    assert!(
        join_spans >= batch_count as usize * 2,
        "Join: expected at least {} spans, got {}",
        batch_count * 2,
        join_spans
    );
    assert!(
        partition_spans >= batch_count as usize * 2,
        "Partition: expected at least {} spans, got {}",
        batch_count * 2,
        partition_spans
    );
}

/// Test that span creation is consistent across batches for each processor type
/// (no span creation dropoff after the first batch)
#[tokio::test]
#[serial]
async fn test_no_span_dropoff_across_batches() {
    init_logger();

    let obs_manager = create_test_observability_manager("span-dropoff-test").await;
    let obs = Some(obs_manager.clone());

    let batch_count = 10u64;
    let records = vec![create_test_record_with_traceparent(UPSTREAM_TRACEPARENT)];

    let mut spans_per_batch: Vec<bool> = Vec::new();

    for batch_id in 0..batch_count {
        let batch_span =
            ObservabilityHelper::start_batch_span(&obs, "dropoff-test-job", batch_id, &records);

        let created = batch_span.is_some();
        if let Some(span) = &batch_span {
            let ctx = span.span_context();
            let valid = ctx.map(|c| c.is_valid()).unwrap_or(false);
            spans_per_batch.push(created && valid);

            if !valid {
                println!(
                    "WARNING: Batch #{} - span created but context INVALID",
                    batch_id
                );
            }
        } else {
            spans_per_batch.push(false);
            println!(
                "WARNING: Batch #{} - start_batch_span returned None!",
                batch_id
            );
        }

        // Also test child span creation (deserialization)
        ObservabilityHelper::record_deserialization(
            &obs,
            "dropoff-test-job",
            &batch_span,
            3,
            1,
            None,
        );

        // Inject trace context
        let mut output: Vec<Arc<StreamRecord>> = vec![Arc::new(create_test_record())];
        ObservabilityHelper::inject_trace_context_into_records(
            &batch_span,
            &mut output,
            "dropoff-test-job",
        );

        let injected = output[0].headers.contains_key("traceparent");
        if !injected {
            println!(
                "WARNING: Batch #{} - trace context NOT injected into output",
                batch_id
            );
        }

        drop(batch_span);
    }

    // Print per-batch results
    println!("\nPer-batch span creation results:");
    for (i, created) in spans_per_batch.iter().enumerate() {
        println!("  Batch #{}: {}", i, if *created { "OK" } else { "FAILED" });
    }

    // Verify NO dropoff - every batch should have created a valid span
    let failures: Vec<usize> = spans_per_batch
        .iter()
        .enumerate()
        .filter(|(_, created)| !**created)
        .map(|(i, _)| i)
        .collect();

    assert!(
        failures.is_empty(),
        "Span creation failed for batches: {:?}",
        failures
    );
}
