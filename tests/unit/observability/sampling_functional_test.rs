//! Functional tests for per-record sampling through process_batch().
//!
//! These tests exercise the FULL pipeline: input record → sampling decision →
//! span creation → SQL execution → trace injection into output records.
//!
//! Unlike the unit tests in per_record_sampling_test.rs (which test the
//! trace_propagation functions in isolation), these tests verify that
//! process_batch() correctly wires sampling decisions to output records:
//!
//! 1. First-hop records (no traceparent) → sampling_ratio controls the dice roll
//! 2. Sampled (flag=01) → output gets new span context (flag=01)
//! 3. Not-sampled (flag=00) → output preserves flag=00, no span created
//! 4. NotSampledNew (no traceparent + ratio=0) → output gets flag=00 injected
//! 5. Multi-hop: output from hop 1 feeds hop 2 — trace_id chains correctly

use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::observability::{ObservabilityManager, SharedObservabilityManager};
use velostream::velostream::server::processors::common::process_batch;
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::config::{StreamingConfig, TracingConfig};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Re-use shared helpers
use super::super::observability_test_helpers::extract_trace_id;

/// Create a test record with given symbol (no traceparent)
fn make_record(symbol: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert("price".to_string(), FieldValue::Float(150.0));
    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

/// Create a test record WITH a traceparent header
fn make_record_with_traceparent(symbol: &str, traceparent: &str) -> StreamRecord {
    let mut r = make_record(symbol);
    r.headers
        .insert("traceparent".to_string(), traceparent.to_string());
    r
}

/// Build observability manager with a specific sampling_ratio
async fn make_obs(sampling_ratio: f64) -> SharedObservabilityManager {
    let tracing_config = TracingConfig {
        service_name: "sampling-functional-test".to_string(),
        service_version: "1.0.0".to_string(),
        otlp_endpoint: None,
        sampling_ratio,
        enable_console_output: false,
        max_span_duration_seconds: 300,
        batch_export_timeout_ms: 30000,
    };
    let streaming_config = StreamingConfig::default().with_tracing_config(tracing_config);
    let mut manager = ObservabilityManager::from_streaming_config(streaming_config);
    manager
        .initialize()
        .await
        .expect("Failed to initialize observability manager");
    Arc::new(tokio::sync::RwLock::new(manager))
}

/// Build a passthrough engine + query for "SELECT symbol, price FROM input"
fn make_engine_and_query() -> (
    Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
    velostream::velostream::sql::ast::StreamingQuery,
) {
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT symbol, price FROM input")
        .expect("Failed to parse test query");
    (engine, query)
}

/// Parse a traceparent string: returns (version, trace_id, span_id, flags)
fn parse_traceparent(tp: &str) -> (&str, &str, &str, &str) {
    let parts: Vec<&str> = tp.split('-').collect();
    assert_eq!(parts.len(), 4, "Invalid traceparent: {}", tp);
    (parts[0], parts[1], parts[2], parts[3])
}

// =============================================================================
// Test 1: First-hop, sampling_ratio=1.0 → all outputs get traceparent flag=01
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_first_hop_all_sampled() {
    let obs = make_obs(1.0).await;
    let (engine, query) = make_engine_and_query();

    // 10 records with no traceparent (first hop)
    let batch: Vec<StreamRecord> = (0..10).map(|_| make_record("AAPL")).collect();

    let result = process_batch(batch, &engine, &query, "test-job", &Some(obs)).await;

    assert_eq!(result.records_processed, 10);
    assert_eq!(result.records_failed, 0);

    // Every output should have traceparent with flag=01 (sampled)
    for (i, record) in result.output_records.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Record {} missing traceparent", i));
        let (version, trace_id, span_id, flags) = parse_traceparent(tp);
        assert_eq!(version, "00", "Record {}: version should be 00", i);
        assert_eq!(trace_id.len(), 32, "Record {}: trace_id len", i);
        assert_eq!(span_id.len(), 16, "Record {}: span_id len", i);
        assert_eq!(
            flags, "01",
            "Record {}: first-hop with ratio=1.0 should produce flag=01",
            i
        );
    }
}

// =============================================================================
// Test 2: First-hop, sampling_ratio=0.0 → all outputs get traceparent flag=00
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_first_hop_none_sampled() {
    let obs = make_obs(0.0).await;
    let (engine, query) = make_engine_and_query();

    let batch: Vec<StreamRecord> = (0..10).map(|_| make_record("MSFT")).collect();

    let result = process_batch(batch, &engine, &query, "test-job", &Some(obs)).await;

    assert_eq!(result.records_processed, 10);

    // Every output should have traceparent with flag=00 (NotSampledNew → injected)
    for (i, record) in result.output_records.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Record {} missing traceparent (should be injected)", i));
        let (_version, trace_id, _span_id, flags) = parse_traceparent(tp);
        assert_ne!(trace_id, "00000000000000000000000000000000");
        assert_eq!(
            flags, "00",
            "Record {}: first-hop with ratio=0.0 should produce flag=00",
            i
        );
    }
}

// =============================================================================
// Test 3: Upstream sampled (flag=01) → output continues trace chain
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_upstream_sampled_continues_chain() {
    let obs = make_obs(0.0).await; // ratio=0, but flag=01 overrides
    let (engine, query) = make_engine_and_query();

    let upstream_tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let batch = vec![make_record_with_traceparent("AAPL", upstream_tp)];

    let result = process_batch(batch, &engine, &query, "test-job", &Some(obs)).await;

    assert_eq!(result.records_processed, 1);
    assert_eq!(result.output_records.len(), 1);

    let output = &result.output_records[0];
    let tp = output
        .headers
        .get("traceparent")
        .expect("Output should have traceparent");
    let (_version, trace_id, span_id, flags) = parse_traceparent(tp);

    // Should continue the same trace
    assert_eq!(
        trace_id, "4bf92f3577b34da6a3ce929d0e0e4736",
        "Output should continue the upstream trace_id"
    );
    // But with a NEW span_id (from the RecordSpan)
    assert_ne!(
        span_id, "00f067aa0ba902b7",
        "Output should have new span_id (not upstream's)"
    );
    // And still sampled
    assert_eq!(flags, "01", "Output should remain sampled (flag=01)");
}

// =============================================================================
// Test 4: Upstream not-sampled (flag=00) → output preserves flag=00, no new span
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_upstream_not_sampled_passes_through() {
    let obs = make_obs(1.0).await; // ratio=1.0, but flag=00 overrides
    let (engine, query) = make_engine_and_query();

    let upstream_tp = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-00";
    let batch = vec![make_record_with_traceparent("AAPL", upstream_tp)];

    let result = process_batch(batch, &engine, &query, "test-job", &Some(obs)).await;

    assert_eq!(result.records_processed, 1);
    assert_eq!(result.output_records.len(), 1);

    let output = &result.output_records[0];
    // For NotSampled, no span is created and no injection happens.
    // The original traceparent propagates via FR-090 header propagation.
    // The output should still have the original traceparent with flag=00.
    let tp = output
        .headers
        .get("traceparent")
        .expect("Output should preserve upstream traceparent via header propagation");
    let (_version, _trace_id, _span_id, flags) = parse_traceparent(tp);
    assert_eq!(
        flags, "00",
        "Not-sampled upstream should remain flag=00 in output"
    );
}

// =============================================================================
// Test 5: Multi-hop chain — hop1 output feeds hop2 input, trace_id chains
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_multi_hop_trace_chain() {
    let obs = make_obs(1.0).await;
    let (engine, query) = make_engine_and_query();

    // HOP 1: First-hop record (no traceparent)
    let batch1 = vec![make_record("AAPL")];
    let result1 = process_batch(
        batch1,
        &engine,
        &query,
        "market_data_ts",
        &Some(obs.clone()),
    )
    .await;

    assert_eq!(result1.output_records.len(), 1);
    let hop1_output = &result1.output_records[0];
    let hop1_tp = hop1_output
        .headers
        .get("traceparent")
        .expect("Hop 1 output should have traceparent");
    let (_, hop1_trace_id, hop1_span_id, hop1_flags) = parse_traceparent(hop1_tp);
    assert_eq!(hop1_flags, "01", "Hop 1 should be sampled");

    // HOP 2: Feed hop1 output into hop2 as input
    let mut hop2_input = make_record("AAPL");
    hop2_input
        .headers
        .insert("traceparent".to_string(), hop1_tp.clone());

    let batch2 = vec![hop2_input];
    let result2 = process_batch(
        batch2,
        &engine,
        &query,
        "price_analytics",
        &Some(obs.clone()),
    )
    .await;

    assert_eq!(result2.output_records.len(), 1);
    let hop2_output = &result2.output_records[0];
    let hop2_tp = hop2_output
        .headers
        .get("traceparent")
        .expect("Hop 2 output should have traceparent");
    let (_, hop2_trace_id, hop2_span_id, hop2_flags) = parse_traceparent(hop2_tp);

    // CRITICAL: trace_id chains across hops
    assert_eq!(
        hop2_trace_id, hop1_trace_id,
        "Hop 2 must continue the same trace_id as hop 1"
    );
    // Each hop has its own span_id
    assert_ne!(
        hop2_span_id, hop1_span_id,
        "Hop 2 must have a different span_id than hop 1"
    );
    // Still sampled
    assert_eq!(hop2_flags, "01", "Hop 2 should remain sampled");
}

// =============================================================================
// Test 6: Not-sampled propagation across hops prevents re-rolling
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_not_sampled_propagation_prevents_reroll() {
    // HOP 1: ratio=0 → NotSampledNew → injects flag=00
    let obs1 = make_obs(0.0).await;
    let (engine, query) = make_engine_and_query();

    let batch1 = vec![make_record("GOOGL")];
    let result1 = process_batch(batch1, &engine, &query, "hop1", &Some(obs1)).await;

    assert_eq!(result1.output_records.len(), 1);
    let hop1_output = &result1.output_records[0];
    let hop1_tp = hop1_output
        .headers
        .get("traceparent")
        .expect("Hop 1 output must have traceparent (flag=00 injected by NotSampledNew)");
    let (_, _, _, hop1_flags) = parse_traceparent(hop1_tp);
    assert_eq!(hop1_flags, "00", "Hop 1 should inject flag=00");

    // HOP 2: ratio=1.0, but flag=00 from upstream must prevent re-rolling
    let obs2 = make_obs(1.0).await;
    let mut hop2_input = make_record("GOOGL");
    hop2_input
        .headers
        .insert("traceparent".to_string(), hop1_tp.clone());

    let batch2 = vec![hop2_input];
    let result2 = process_batch(batch2, &engine, &query, "hop2", &Some(obs2)).await;

    assert_eq!(result2.output_records.len(), 1);
    let hop2_output = &result2.output_records[0];
    let hop2_tp = hop2_output
        .headers
        .get("traceparent")
        .expect("Hop 2 output should have traceparent");
    let (_, _, _, hop2_flags) = parse_traceparent(hop2_tp);
    assert_eq!(
        hop2_flags, "00",
        "Hop 2 must NOT re-sample: upstream flag=00 must be honored even with ratio=1.0"
    );
}

// =============================================================================
// Test 7: Mixed batch — sampled and not-sampled records in same batch
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_mixed_sampling_decisions() {
    let obs = make_obs(1.0).await; // ratio doesn't matter for records with existing traceparent
    let (engine, query) = make_engine_and_query();

    let sampled_tp = "00-aaaa000000000000aaaa000000000000-bbbb000000000000-01";
    let not_sampled_tp = "00-cccc000000000000cccc000000000000-dddd000000000000-00";

    let batch = vec![
        make_record_with_traceparent("AAPL", sampled_tp), // Sampled(Some)
        make_record_with_traceparent("MSFT", not_sampled_tp), // NotSampled
        make_record("GOOGL"),                             // Sampled(None) at ratio=1.0
    ];

    let result = process_batch(batch, &engine, &query, "mixed-job", &Some(obs)).await;

    assert_eq!(result.records_processed, 3);
    assert_eq!(result.output_records.len(), 3);

    // Record 0: upstream sampled → output sampled, same trace_id
    let tp0 = result.output_records[0]
        .headers
        .get("traceparent")
        .expect("Record 0 should have traceparent");
    let (_, tid0, sid0, flags0) = parse_traceparent(tp0);
    assert_eq!(tid0, "aaaa000000000000aaaa000000000000");
    assert_ne!(sid0, "bbbb000000000000"); // new span_id
    assert_eq!(flags0, "01");

    // Record 1: upstream not-sampled → output not-sampled, same traceparent propagated
    let tp1 = result.output_records[1]
        .headers
        .get("traceparent")
        .expect("Record 1 should have traceparent");
    let (_, _tid1, _sid1, flags1) = parse_traceparent(tp1);
    assert_eq!(flags1, "00", "Not-sampled should remain flag=00");

    // Record 2: no upstream + ratio=1.0 → new root, sampled
    let tp2 = result.output_records[2]
        .headers
        .get("traceparent")
        .expect("Record 2 should have traceparent");
    let (_, tid2, _, flags2) = parse_traceparent(tp2);
    assert_eq!(flags2, "01", "New root with ratio=1.0 should be sampled");
    // New root → different trace_id than record 0
    assert_ne!(
        tid2, "aaaa000000000000aaaa000000000000",
        "New root should have different trace_id"
    );
}

// =============================================================================
// Test 8: No observability → no traceparent injection
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_no_observability_no_injection() {
    let (engine, query) = make_engine_and_query();

    let batch = vec![make_record("AAPL")];

    // No observability manager
    let result = process_batch(batch, &engine, &query, "test-job", &None).await;

    assert_eq!(result.records_processed, 1);
    assert_eq!(result.output_records.len(), 1);

    // Without observability, no traceparent should be injected
    assert!(
        !result.output_records[0].headers.contains_key("traceparent"),
        "No observability → no traceparent injection"
    );
}

// =============================================================================
// Test 9: Collected spans verify parent-child relationships
// =============================================================================

#[tokio::test]
#[serial]
async fn test_process_batch_collected_spans_have_correct_parentage() {
    let obs = make_obs(1.0).await;
    let (engine, query) = make_engine_and_query();

    // HOP 1: root span
    let batch1 = vec![make_record("AAPL")];
    let result1 = process_batch(batch1, &engine, &query, "producer_job", &Some(obs.clone())).await;

    let hop1_tp = result1.output_records[0]
        .headers
        .get("traceparent")
        .expect("Hop 1 should have traceparent");

    // HOP 2: child span
    let mut hop2_input = make_record("AAPL");
    hop2_input
        .headers
        .insert("traceparent".to_string(), hop1_tp.clone());
    let batch2 = vec![hop2_input];
    let result2 = process_batch(batch2, &engine, &query, "consumer_job", &Some(obs.clone())).await;

    // Give span processor time to collect
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify collected spans
    let obs_lock = obs.read().await;
    let telemetry = obs_lock.telemetry().expect("Telemetry should exist");
    let collected = telemetry.collected_spans();

    let hop1_trace_id = extract_trace_id(hop1_tp);

    // Filter spans belonging to our trace
    let our_spans: Vec<_> = collected
        .iter()
        .filter(|s| s.span_context.trace_id().to_string() == hop1_trace_id)
        .collect();

    assert!(
        our_spans.len() >= 2,
        "Expected at least 2 spans in trace (hop1 + hop2), got {}",
        our_spans.len()
    );

    // Find producer and consumer spans
    let producer_span = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "process:producer_job")
        .expect("Missing producer_job span");
    let consumer_span = our_spans
        .iter()
        .find(|s| s.name.as_ref() == "process:consumer_job")
        .expect("Missing consumer_job span");

    // Verify parent-child: consumer's parent should be producer's span_id
    assert_eq!(
        consumer_span.parent_span_id,
        producer_span.span_context.span_id(),
        "Consumer span's parent should be producer span"
    );
}
