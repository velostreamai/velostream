//! Diagnostic test: Per-record span API verification
//!
//! Tests that the per-record head-based sampling API works correctly:
//! - create observability manager
//! - start_record_span for sampled records
//! - record_deserialization / record_serialization_success with 4 args (metrics-only)
//! - verify spans collected via in-memory collector
//!
//! Run with: RUST_LOG=debug cargo test --tests --no-default-features -- processor_span_diagnostic --nocapture

use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;

use crate::unit::observability_test_helpers::{
    UPSTREAM_TRACEPARENT, create_test_observability_manager, create_test_record,
};
use velostream::velostream::server::processors::common::BatchProcessingResultWithOutput;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;

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

/// Test per-record API: sampled records create process spans, metrics calls are 4-arg
#[tokio::test]
#[serial]
async fn test_per_record_api_sampled_and_metrics() {
    init_logger();

    let obs_manager = create_test_observability_manager("per-record-diagnostic").await;
    let obs = Some(obs_manager.clone());

    let batch_count = 5u64;
    let sampled_records_per_batch = 3;

    println!("\n========== PER-RECORD API DIAGNOSTIC ==========");
    let initial_count = get_span_count(&obs_manager).await;

    for batch_id in 0..batch_count {
        // For each batch, create record spans for sampled records
        for record_idx in 0..sampled_records_per_batch {
            let mut record_span =
                ObservabilityHelper::start_record_span(&obs, "per-record-job", None);
            assert!(
                record_span.is_some(),
                "Batch #{}, Record #{}: record span should be Some",
                batch_id,
                record_idx
            );

            let span = record_span.as_mut().unwrap();
            span.set_output_count(1);
            span.set_success();

            // Inject into output record
            let mut output_record = Arc::new(create_test_record());
            ObservabilityHelper::inject_record_trace_context(
                record_span.as_ref().unwrap(),
                &mut output_record,
            );

            assert!(
                output_record.headers.contains_key("traceparent"),
                "Batch #{}, Record #{}: output should have traceparent",
                batch_id,
                record_idx
            );

            drop(record_span);
        }

        // Metrics-only calls (4 args, no spans)
        ObservabilityHelper::record_deserialization(
            &obs,
            "per-record-job",
            sampled_records_per_batch,
            2,
        );

        let batch_result = make_success_batch_result(sampled_records_per_batch);
        ObservabilityHelper::record_sql_processing(&obs, "per-record-job", &batch_result, 5);

        ObservabilityHelper::record_serialization_success(
            &obs,
            "per-record-job",
            sampled_records_per_batch,
            1,
        );
    }

    let total_spans = get_span_count(&obs_manager).await - initial_count;
    let expected_spans = (batch_count as usize) * sampled_records_per_batch;
    println!(
        "Per-record API: {} spans created across {} batches ({} per batch)",
        total_spans,
        batch_count,
        total_spans as f64 / batch_count as f64
    );

    assert_eq!(
        total_spans, expected_spans,
        "Expected {} record spans ({} per batch * {} batches), got {}",
        expected_spans, sampled_records_per_batch, batch_count, total_spans
    );
}

/// Test per-record API with upstream context: child spans inherit trace ID
#[tokio::test]
#[serial]
async fn test_per_record_api_with_upstream_context() {
    init_logger();

    let obs_manager = create_test_observability_manager("upstream-diagnostic").await;
    let obs = Some(obs_manager.clone());

    println!("\n========== UPSTREAM CONTEXT DIAGNOSTIC ==========");

    // Extract upstream context from well-known traceparent
    let upstream_ctx =
        velostream::velostream::observability::trace_propagation::extract_trace_context(&{
            let mut h = std::collections::HashMap::new();
            h.insert("traceparent".to_string(), UPSTREAM_TRACEPARENT.to_string());
            h
        })
        .expect("Should extract upstream context");

    let batch_count = 5u64;

    for batch_id in 0..batch_count {
        let record_span =
            ObservabilityHelper::start_record_span(&obs, "upstream-job", Some(&upstream_ctx));
        assert!(
            record_span.is_some(),
            "Batch #{}: record span should be Some with upstream context",
            batch_id
        );

        let span = record_span.unwrap();
        let ctx = span.span_context().unwrap();

        // Child span should inherit the parent's trace_id
        assert_eq!(
            format!("{}", ctx.trace_id()),
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "Batch #{}: child should have parent's trace_id",
            batch_id
        );

        drop(span);
    }

    let total_spans = get_span_count(&obs_manager).await;
    println!(
        "Upstream context: {} spans created across {} batches",
        total_spans, batch_count
    );
    assert_eq!(
        total_spans, batch_count as usize,
        "Expected {} spans, got {}",
        batch_count, total_spans
    );
}

/// Test that span creation is consistent across batches (no dropoff)
#[tokio::test]
#[serial]
async fn test_no_span_dropoff_across_batches() {
    init_logger();

    let obs_manager = create_test_observability_manager("span-dropoff-test").await;
    let obs = Some(obs_manager.clone());

    let batch_count = 10u64;

    let mut spans_per_batch: Vec<bool> = Vec::new();

    for batch_id in 0..batch_count {
        let record_span = ObservabilityHelper::start_record_span(&obs, "dropoff-test-job", None);

        let created = record_span.is_some();
        if let Some(span) = &record_span {
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
                "WARNING: Batch #{} - start_record_span returned None!",
                batch_id
            );
        }

        // Also record metrics (4 args)
        ObservabilityHelper::record_deserialization(&obs, "dropoff-test-job", 3, 1);

        // Inject trace context into output
        if let Some(span) = &record_span {
            let mut output = Arc::new(create_test_record());
            ObservabilityHelper::inject_record_trace_context(span, &mut output);

            let injected = output.headers.contains_key("traceparent");
            if !injected {
                println!(
                    "WARNING: Batch #{} - trace context NOT injected into output",
                    batch_id
                );
            }
        }

        drop(record_span);
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
