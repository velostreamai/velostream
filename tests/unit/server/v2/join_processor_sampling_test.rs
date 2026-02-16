//! Functional tests for per-record sampling in JoinJobProcessor::process_join().
//!
//! JoinJobProcessor applies sampling at join_job_processor.rs:367-401 in the
//! output loop of process_join(). These tests verify that:
//! 1. Sampled join outputs get traceparent with flag=01
//! 2. Not-sampled join outputs get traceparent with flag=00
//! 3. No observability → no traceparent injection
//! 4. Upstream traceparent propagates through join correctly

use async_trait::async_trait;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use velostream::velostream::observability::{ObservabilityManager, SharedObservabilityManager};
use velostream::velostream::server::v2::join_job_processor::{JoinJobConfig, JoinJobProcessor};
use velostream::velostream::sql::execution::config::{StreamingConfig, TracingConfig};
use velostream::velostream::sql::execution::join::JoinSide;
use velostream::velostream::sql::execution::processors::IntervalJoinConfig;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

// ── Helpers ──────────────────────────────────────────────────

/// MockReader for join tests
struct MockReader {
    records: Vec<StreamRecord>,
    position: usize,
}

impl MockReader {
    fn new(records: Vec<StreamRecord>) -> Self {
        Self {
            records,
            position: 0,
        }
    }
}

#[async_trait]
impl velostream::velostream::datasource::DataReader for MockReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.position >= self.records.len() {
            return Ok(vec![]);
        }
        let batch_end = (self.position + 5).min(self.records.len());
        let batch = self.records[self.position..batch_end].to_vec();
        self.position = batch_end;
        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.position < self.records.len())
    }
}

/// MockWriter that captures output records
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

async fn make_obs(sampling_ratio: f64) -> SharedObservabilityManager {
    let tracing_config = TracingConfig {
        service_name: "join-sampling-test".to_string(),
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

/// Create matching left/right records with event_time for join matching
fn make_matching_records(
    count: usize,
    traceparent: Option<&str>,
) -> (Vec<StreamRecord>, Vec<StreamRecord>) {
    let left: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("order_id".to_string(), FieldValue::Integer(100 + i as i64));
            fields.insert(
                "customer".to_string(),
                FieldValue::String(format!("Customer{}", i)),
            );
            fields.insert(
                "event_time".to_string(),
                FieldValue::Integer(1000 + i as i64 * 100),
            );
            let mut headers = HashMap::new();
            if let Some(tp) = traceparent {
                headers.insert("traceparent".to_string(), tp.to_string());
            }
            StreamRecord {
                fields,
                timestamp: chrono::Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: 0,
                event_time: None,
                headers,
                topic: None,
                key: None,
            }
        })
        .collect();

    let right: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("order_id".to_string(), FieldValue::Integer(100 + i as i64));
            fields.insert(
                "carrier".to_string(),
                FieldValue::String(format!("Carrier{}", i)),
            );
            fields.insert(
                "event_time".to_string(),
                FieldValue::Integer(1100 + i as i64 * 100),
            );
            let mut headers = HashMap::new();
            if let Some(tp) = traceparent {
                headers.insert("traceparent".to_string(), tp.to_string());
            }
            StreamRecord {
                fields,
                timestamp: chrono::Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: 0,
                event_time: None,
                headers,
                topic: None,
                key: None,
            }
        })
        .collect();

    (left, right)
}

/// Run a join and return the output records
async fn run_join(
    left: Vec<StreamRecord>,
    right: Vec<StreamRecord>,
    observability: Option<SharedObservabilityManager>,
) -> Vec<StreamRecord> {
    let left_reader = Box::new(MockReader::new(left));
    let right_reader = Box::new(MockReader::new(right));
    let (writer, output) = MockWriter::new();

    let join_config = IntervalJoinConfig::new("orders", "shipments")
        .with_key("order_id", "order_id")
        .with_bounds(Duration::ZERO, Duration::from_secs(3600));

    let config = JoinJobConfig {
        join_config,
        timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let processor = JoinJobProcessor::new(config);

    let stats = processor
        .process_join(
            "orders".to_string(),
            left_reader,
            "shipments".to_string(),
            right_reader,
            Some(Box::new(writer)),
            None,
            observability,
            None,
            None,
        )
        .await
        .expect("process_join failed");

    assert!(stats.join_matches > 0, "Expected join matches");

    let locked = output.lock().await;
    locked.clone()
}

// ── Tests ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_join_sampling_all_sampled() {
    let obs = make_obs(1.0).await;
    let (left, right) = make_matching_records(3, None);

    let output = run_join(left, right, Some(obs)).await;

    assert!(!output.is_empty(), "Should have join output");
    for (i, record) in output.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Join output {} missing traceparent", i));
        let (_ver, trace_id, _span_id, flags) = parse_traceparent(tp);
        assert_eq!(trace_id.len(), 32);
        assert_eq!(
            flags, "01",
            "Join output {}: ratio=1.0 should produce flag=01",
            i
        );
    }
}

#[tokio::test]
#[serial]
async fn test_join_sampling_none_sampled() {
    let obs = make_obs(0.0).await;
    let (left, right) = make_matching_records(3, None);

    let output = run_join(left, right, Some(obs)).await;

    assert!(!output.is_empty(), "Should have join output");
    for (i, record) in output.iter().enumerate() {
        let tp = record.headers.get("traceparent").unwrap_or_else(|| {
            panic!("Join output {} missing traceparent (should be injected)", i)
        });
        let (_ver, _trace_id, _span_id, flags) = parse_traceparent(tp);
        assert_eq!(
            flags, "00",
            "Join output {}: ratio=0.0 should produce flag=00",
            i
        );
    }
}

#[tokio::test]
#[serial]
async fn test_join_no_observability_no_injection() {
    let (left, right) = make_matching_records(3, None);

    let output = run_join(left, right, None).await;

    assert!(!output.is_empty(), "Should have join output");
    for (i, record) in output.iter().enumerate() {
        assert!(
            !record.headers.contains_key("traceparent"),
            "Join output {}: no observability → no traceparent injection",
            i
        );
    }
}

#[tokio::test]
#[serial]
async fn test_join_upstream_sampled_continues_chain() {
    let obs = make_obs(0.0).await; // ratio doesn't matter, flag=01 overrides
    let upstream_tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let (left, right) = make_matching_records(3, Some(upstream_tp));

    let output = run_join(left, right, Some(obs)).await;

    assert!(!output.is_empty(), "Should have join output");
    for (i, record) in output.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Join output {} missing traceparent", i));
        let (_ver, _trace_id, _span_id, flags) = parse_traceparent(tp);
        // Join outputs with upstream flag=01 should remain sampled.
        // The trace_id may differ because the join merges headers from left+right
        // and creates a new span. The key check is flags=01.
        assert_eq!(
            flags, "01",
            "Join output {}: upstream flag=01 should continue sampled",
            i
        );
    }
}

#[tokio::test]
#[serial]
async fn test_join_upstream_not_sampled_passes_through() {
    let obs = make_obs(1.0).await; // ratio=1.0, but flag=00 overrides
    let upstream_tp = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-00";
    let (left, right) = make_matching_records(3, Some(upstream_tp));

    let output = run_join(left, right, Some(obs)).await;

    assert!(!output.is_empty(), "Should have join output");
    for (i, record) in output.iter().enumerate() {
        let tp = record
            .headers
            .get("traceparent")
            .unwrap_or_else(|| panic!("Join output {} should have traceparent", i));
        let (_ver, _trace_id, _span_id, flags) = parse_traceparent(tp);
        assert_eq!(
            flags, "00",
            "Join output {}: upstream flag=00 must remain not-sampled",
            i
        );
    }
}
