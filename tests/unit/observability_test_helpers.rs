//! Shared observability test helpers
//!
//! Consolidated helpers for distributed tracing tests to eliminate code duplication.
//! Used by:
//! - tests/integration/processor_trace_patterns_test.rs
//! - tests/unit/observability/distributed_tracing_test.rs
//! - tests/integration/trace_chain_kafka_test.rs

use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::observability::{ObservabilityManager, SharedObservabilityManager};
use velostream::velostream::sql::execution::config::{StreamingConfig, TracingConfig};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Well-known upstream traceparent for testing
pub const UPSTREAM_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// Create a basic test record with no trace headers
pub fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(150.0));
    StreamRecord {
        fields,
        timestamp: 1700000000000,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

/// Create a test record with an upstream traceparent header
pub fn create_test_record_with_traceparent(traceparent: &str) -> StreamRecord {
    let mut record = create_test_record();
    record
        .headers
        .insert("traceparent".to_string(), traceparent.to_string());
    record
}

/// Create a test record with arbitrary headers
pub fn create_test_record_with_headers(headers: HashMap<String, String>) -> StreamRecord {
    let mut record = create_test_record();
    record.headers = headers;
    record
}

/// Create a test observability manager with in-memory span collection
pub async fn create_test_observability_manager(service_name: &str) -> SharedObservabilityManager {
    let tracing_config = TracingConfig {
        service_name: service_name.to_string(),
        otlp_endpoint: None,
        ..Default::default()
    };

    let streaming_config = StreamingConfig::default().with_tracing_config(tracing_config);

    let mut manager = ObservabilityManager::from_streaming_config(streaming_config);
    manager
        .initialize()
        .await
        .expect("Failed to initialize observability manager");

    Arc::new(tokio::sync::RwLock::new(manager))
}

/// Extract the trace_id portion from a traceparent header
pub fn extract_trace_id(traceparent: &str) -> &str {
    traceparent.split('-').nth(1).expect("invalid traceparent")
}

/// Extract the span_id portion from a traceparent header
pub fn extract_span_id(traceparent: &str) -> &str {
    traceparent.split('-').nth(2).expect("invalid traceparent")
}

/// Assert that a record has a valid traceparent header injected
pub fn assert_traceparent_injected(record: &StreamRecord, msg: &str) {
    let traceparent = record
        .headers
        .get("traceparent")
        .unwrap_or_else(|| panic!("{}: record should have traceparent header", msg));
    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4, "{}: traceparent should have 4 parts", msg);
    assert_eq!(parts[0], "00", "{}: version should be 00", msg);
    assert_eq!(
        parts[1].len(),
        32,
        "{}: trace_id should be 32 hex chars",
        msg
    );
    assert_eq!(
        parts[2].len(),
        16,
        "{}: span_id should be 16 hex chars",
        msg
    );
}
