//! Functional tests for per-record sampling in PartitionReceiver::process_batch().
//!
//! PartitionReceiver has its own synchronous process_batch() (partition_receiver.rs:675)
//! with sampling logic independent from common.rs. These tests verify that sampling
//! decisions, trace injection, and not-sampled propagation work correctly through
//! this code path.
//!
//! Test strategy:
//! - Construct a PartitionReceiver with new_with_queue()
//! - Push records to the lock-free queue, signal EOF
//! - Capture output via a MockWriter and verify traceparent headers

use async_trait::async_trait;
use crossbeam_queue::SegQueue;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use velostream::velostream::observability::{ObservabilityManager, SharedObservabilityManager};
use velostream::velostream::server::processors::JobProcessingConfig;
use velostream::velostream::server::v2::PartitionReceiver;
use velostream::velostream::server::v2::metrics::PartitionMetrics;
use velostream::velostream::sql::execution::config::{StreamingConfig, TracingConfig};
use velostream::velostream::sql::execution::engine::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// ── Helpers ──────────────────────────────────────────────────

/// MockWriter that captures output records for assertion
struct MockWriter {
    records: Arc<Mutex<Vec<StreamRecord>>>,
}

impl MockWriter {
    fn new() -> (Self, Arc<Mutex<Vec<StreamRecord>>>) {
        let records = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                records: records.clone(),
            },
            records,
        )
    }
}

#[async_trait]
impl velostream::velostream::datasource::DataWriter for MockWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records.lock().await.push(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut locked = self.records.lock().await;
        for record in records {
            locked.push((*record).clone());
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records.lock().await.push(record);
        Ok(())
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

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

fn make_record_with_traceparent(symbol: &str, traceparent: &str) -> StreamRecord {
    let mut r = make_record(symbol);
    r.headers
        .insert("traceparent".to_string(), traceparent.to_string());
    r
}

async fn make_obs(sampling_ratio: f64) -> SharedObservabilityManager {
    let tracing_config = TracingConfig {
        service_name: "partition-sampling-test".to_string(),
        service_version: "1.0.0".to_string(),
        otlp_endpoint: None,
        sampling_ratio,
        enable_console_output: false,
        max_span_duration_seconds: 300,
        batch_export_timeout_ms: 30000,
        ..TracingConfig::default()
    };
    let streaming_config = StreamingConfig::default().with_tracing_config(tracing_config);
    let mut manager = ObservabilityManager::from_streaming_config(streaming_config);
    manager
        .initialize()
        .await
        .expect("Failed to initialize observability manager");
    Arc::new(tokio::sync::RwLock::new(manager))
}

fn parse_traceparent(tp: &str) -> (&str, &str, &str, &str) {
    let parts: Vec<&str> = tp.split('-').collect();
    assert_eq!(parts.len(), 4, "Invalid traceparent: {}", tp);
    (parts[0], parts[1], parts[2], parts[3])
}

/// Run a PartitionReceiver with given records and observability, return output records
async fn run_partition_receiver(
    records: Vec<StreamRecord>,
    observability: Option<SharedObservabilityManager>,
) -> Vec<StreamRecord> {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let engine = StreamExecutionEngine::new(tx);

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT symbol, price FROM input")
        .expect("Failed to parse query");
    let query = Arc::new(query);

    let queue: Arc<SegQueue<Vec<StreamRecord>>> = Arc::new(SegQueue::new());
    let eof_flag = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(PartitionMetrics::new(0));

    let (writer, output) = MockWriter::new();
    let writer: Box<dyn velostream::velostream::datasource::DataWriter> = Box::new(writer);

    let config = JobProcessingConfig {
        enable_dlq: false,
        max_retries: 0,
        retry_backoff: Duration::from_millis(0),
        ..Default::default()
    };

    let mut receiver = PartitionReceiver::new_with_queue(
        0,
        engine,
        query,
        queue.clone(),
        eof_flag.clone(),
        metrics,
        Some(Arc::new(Mutex::new(writer))),
        config,
        observability,
        None,
    );

    // Push records and signal EOF
    queue.push(records);
    eof_flag.store(true, Ordering::SeqCst);

    receiver.run().await.expect("PartitionReceiver::run failed");

    let locked = output.lock().await;
    locked.clone()
}

// ── Tests ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_partition_receiver_first_hop_all_sampled() {
    let obs = make_obs(1.0).await;
    let batch: Vec<StreamRecord> = (0..5).map(|_| make_record("AAPL")).collect();

    let output = run_partition_receiver(batch, Some(obs)).await;

    assert_eq!(output.len(), 5);
    for (i, record) in output.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Record {} missing traceparent", i));
        let (_ver, trace_id, _span_id, flags) = parse_traceparent(tp);
        assert_eq!(trace_id.len(), 32);
        assert_eq!(
            flags, "01",
            "Record {}: first-hop with ratio=1.0 should produce flag=01",
            i
        );
    }
}

#[tokio::test]
#[serial]
async fn test_partition_receiver_first_hop_none_sampled() {
    let obs = make_obs(0.0).await;
    let batch: Vec<StreamRecord> = (0..5).map(|_| make_record("MSFT")).collect();

    let output = run_partition_receiver(batch, Some(obs)).await;

    assert_eq!(output.len(), 5);
    for (i, record) in output.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Record {} missing traceparent (should be injected)", i));
        let (_ver, _trace_id, _span_id, flags) = parse_traceparent(tp);
        assert_eq!(
            flags, "00",
            "Record {}: first-hop with ratio=0.0 should produce flag=00",
            i
        );
    }
}

#[tokio::test]
#[serial]
async fn test_partition_receiver_upstream_sampled_continues_chain() {
    let obs = make_obs(0.0).await; // ratio doesn't matter, flag=01 overrides
    let upstream_tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let batch = vec![make_record_with_traceparent("AAPL", upstream_tp)];

    let output = run_partition_receiver(batch, Some(obs)).await;

    assert_eq!(output.len(), 1);
    let tp = output[0]
        .headers
        .get("traceparent")
        .expect("Output should have traceparent");
    let (_ver, trace_id, span_id, flags) = parse_traceparent(tp);
    assert_eq!(trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_ne!(span_id, "00f067aa0ba902b7", "Should have new span_id");
    assert_eq!(flags, "01");
}

#[tokio::test]
#[serial]
async fn test_partition_receiver_upstream_not_sampled_passes_through() {
    let obs = make_obs(1.0).await; // ratio=1.0, but flag=00 overrides
    let upstream_tp = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-00";
    let batch = vec![make_record_with_traceparent("AAPL", upstream_tp)];

    let output = run_partition_receiver(batch, Some(obs)).await;

    assert_eq!(output.len(), 1);
    let tp = output[0]
        .headers
        .get("traceparent")
        .expect("Output should preserve traceparent");
    let (_ver, _trace_id, _span_id, flags) = parse_traceparent(tp);
    assert_eq!(flags, "00", "Not-sampled should remain flag=00");
}

#[tokio::test]
#[serial]
async fn test_partition_receiver_no_observability_no_injection() {
    let batch = vec![make_record("AAPL")];

    let output = run_partition_receiver(batch, None).await;

    assert_eq!(output.len(), 1);
    assert!(
        !output[0].headers.contains_key("traceparent"),
        "No observability → no traceparent injection"
    );
}

#[tokio::test]
#[serial]
async fn test_partition_receiver_not_sampled_propagation_prevents_reroll() {
    // HOP 1: ratio=0 → NotSampledNew → injects flag=00
    let obs1 = make_obs(0.0).await;
    let batch1 = vec![make_record("GOOGL")];
    let hop1_output = run_partition_receiver(batch1, Some(obs1)).await;

    assert_eq!(hop1_output.len(), 1);
    let hop1_tp = hop1_output[0]
        .headers
        .get("traceparent")
        .expect("Hop 1 should inject flag=00");
    let (_, _, _, hop1_flags) = parse_traceparent(hop1_tp);
    assert_eq!(hop1_flags, "00");

    // HOP 2: ratio=1.0, but flag=00 from upstream must prevent re-rolling
    let obs2 = make_obs(1.0).await;
    let batch2 = vec![make_record_with_traceparent("GOOGL", hop1_tp)];
    let hop2_output = run_partition_receiver(batch2, Some(obs2)).await;

    assert_eq!(hop2_output.len(), 1);
    let hop2_tp = hop2_output[0]
        .headers
        .get("traceparent")
        .expect("Hop 2 should have traceparent");
    let (_, _, _, hop2_flags) = parse_traceparent(hop2_tp);
    assert_eq!(
        hop2_flags, "00",
        "Upstream flag=00 must be honored even with ratio=1.0"
    );
}
