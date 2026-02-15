//! Comprehensive test for OpenTelemetry trace attributes
//!
//! This test validates that all newly added trace attributes are correctly set
//! across different job processor types:
//!
//! Validated attributes:
//! - OpenTelemetry messaging semantic conventions (messaging.*)
//! - Application-specific attributes (velostream.*)
//! - Performance timing breakdown (deser, exec, ser)
//! - Throughput metrics (records/sec)
//! - Error rates (failed vs processed)
//! - Window-specific metrics (for windowed queries)
//! - Join-specific metrics (for join queries)

use opentelemetry_sdk::export::trace::SpanData;
use serial_test::serial;
use std::time::Duration;
use velostream::velostream::server::processors::observability_helper::ObservabilityHelper;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

// Use shared helpers from mod.rs
use super::*;

/// Helper to extract attribute value from span
fn get_attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| format!("{:?}", kv.value))
}

/// Helper to extract numeric attribute value
fn get_numeric_attribute(span: &SpanData, key: &str) -> Option<i64> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::I64(v) => Some(*v),
            _ => None,
        })
}

/// Helper to extract float attribute value
fn get_float_attribute(span: &SpanData, key: &str) -> Option<f64> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .and_then(|kv| match &kv.value {
            opentelemetry::Value::F64(v) => Some(*v),
            _ => None,
        })
}

/// Test that validates all messaging semantic convention attributes
#[tokio::test]
#[serial]
async fn test_messaging_attributes_complete() {
    let obs = create_test_observability_manager("test_messaging_attrs").await;

    // Create a sample record with Kafka metadata
    let mut record = StreamRecord::new(
        vec![("price".to_string(), FieldValue::Float(123.45))]
            .into_iter()
            .collect(),
    );
    record.topic = Some(FieldValue::String("market_data".to_string()));
    record.partition = 5;
    record.offset = 12345;
    record.key = Some(FieldValue::String("AAPL".to_string()));

    // Create batch span
    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "test_job", 1, &[record]);

    // Set messaging attributes
    ObservabilityHelper::set_messaging_attributes(
        &mut batch_span,
        "market_data",
        5,
        12345,
        Some("test_consumer_group"),
        Some("AAPL"),
    );

    // Drop span to trigger collection
    drop(batch_span);

    // Allow time for span export
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Collect spans
    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(
        !collected_spans.is_empty(),
        "Should collect at least 1 span"
    );

    let span = &collected_spans[0];

    // Validate all messaging attributes
    assert_eq!(
        get_attribute(span, "messaging.system"),
        Some("String(\"kafka\")".to_string()),
        "Should have messaging.system=kafka"
    );

    assert_eq!(
        get_attribute(span, "messaging.operation"),
        Some("String(\"process\")".to_string()),
        "Should have messaging.operation=process"
    );

    assert_eq!(
        get_attribute(span, "messaging.destination.name"),
        Some("String(\"market_data\")".to_string()),
        "Should have messaging.destination.name (topic)"
    );

    assert_eq!(
        get_numeric_attribute(span, "messaging.kafka.partition"),
        Some(5),
        "Should have messaging.kafka.partition"
    );

    assert_eq!(
        get_numeric_attribute(span, "messaging.kafka.offset"),
        Some(12345),
        "Should have messaging.kafka.offset"
    );

    assert_eq!(
        get_attribute(span, "messaging.kafka.consumer_group"),
        Some("String(\"test_consumer_group\")".to_string()),
        "Should have messaging.kafka.consumer_group"
    );

    assert_eq!(
        get_attribute(span, "messaging.kafka.message_key"),
        Some("String(\"AAPL\")".to_string()),
        "Should have messaging.kafka.message_key"
    );

    println!("✅ All messaging semantic convention attributes validated");
}

/// Test that validates all application-specific attributes
#[tokio::test]
#[serial]
async fn test_application_attributes_complete() {
    let obs = create_test_observability_manager("test_app_attrs").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "test_job", 1, &[record]);

    // Set application attributes
    ObservabilityHelper::set_application_attributes(
        &mut batch_span,
        "market_data_processor",
        Some("trading_platform"),
        Some("1.2.3"),
        Some("simple"),
    );

    drop(batch_span);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Validate application attributes
    assert_eq!(
        get_attribute(span, "velostream.job_name"),
        Some("String(\"market_data_processor\")".to_string()),
        "Should have velostream.job_name"
    );

    assert_eq!(
        get_attribute(span, "velostream.app_name"),
        Some("String(\"trading_platform\")".to_string()),
        "Should have velostream.app_name"
    );

    assert_eq!(
        get_attribute(span, "velostream.app_version"),
        Some("String(\"1.2.3\")".to_string()),
        "Should have velostream.app_version"
    );

    assert_eq!(
        get_attribute(span, "velostream.job_mode"),
        Some("String(\"simple\")".to_string()),
        "Should have velostream.job_mode"
    );

    println!("✅ All application-specific attributes validated");
}

/// Test that validates performance timing breakdown attributes
#[tokio::test]
#[serial]
async fn test_performance_timing_attributes() {
    let obs = create_test_observability_manager("test_perf_attrs").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "test_job", 1, &[record]);

    // Set performance timings: 10ms deser, 50ms exec, 5ms ser
    ObservabilityHelper::set_performance_timings(&mut batch_span, 10, 50, 5);

    drop(batch_span);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Validate timing attributes
    assert_eq!(
        get_numeric_attribute(span, "velostream.timing.deserialization_ms"),
        Some(10),
        "Should have deserialization_ms=10"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.timing.execution_ms"),
        Some(50),
        "Should have execution_ms=50"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.timing.serialization_ms"),
        Some(5),
        "Should have serialization_ms=5"
    );

    // Validate percentage breakdown (total = 10 + 50 + 5 = 65ms)
    let deser_pct = get_float_attribute(span, "velostream.timing.deserialization_percent");
    let exec_pct = get_float_attribute(span, "velostream.timing.execution_percent");
    let ser_pct = get_float_attribute(span, "velostream.timing.serialization_percent");

    assert!(deser_pct.is_some(), "Should have deserialization_percent");
    assert!(exec_pct.is_some(), "Should have execution_percent");
    assert!(ser_pct.is_some(), "Should have serialization_percent");

    // Check percentages roughly match expected values
    assert!(
        (deser_pct.unwrap() - 15.38).abs() < 0.1,
        "Deser should be ~15.38% (10/65)"
    );
    assert!(
        (exec_pct.unwrap() - 76.92).abs() < 0.1,
        "Exec should be ~76.92% (50/65)"
    );
    assert!(
        (ser_pct.unwrap() - 7.69).abs() < 0.1,
        "Ser should be ~7.69% (5/65)"
    );

    println!("✅ All performance timing attributes validated");
}

/// Test that validates throughput metrics
#[tokio::test]
#[serial]
async fn test_throughput_metrics() {
    let obs = create_test_observability_manager("test_throughput").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "test_job", 1, &[record]);

    // Set throughput: 10,000 records/sec, 1MB/sec
    ObservabilityHelper::set_throughput_metrics(&mut batch_span, 10000.0, Some(1_000_000.0));

    drop(batch_span);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Validate throughput attributes
    assert_eq!(
        get_float_attribute(span, "velostream.throughput.records_per_sec"),
        Some(10000.0),
        "Should have throughput.records_per_sec=10000"
    );

    assert_eq!(
        get_float_attribute(span, "velostream.throughput.bytes_per_sec"),
        Some(1_000_000.0),
        "Should have throughput.bytes_per_sec=1000000"
    );

    println!("✅ All throughput metrics validated");
}

/// Test that validates error rate metrics
#[tokio::test]
#[serial]
async fn test_error_rate_metrics() {
    let obs = create_test_observability_manager("test_errors").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "test_job", 1, &[record]);

    // Set error rate: 5 errors out of 100 records = 5%
    ObservabilityHelper::set_error_rate(&mut batch_span, 5, 100);

    drop(batch_span);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Validate error attributes
    assert_eq!(
        get_numeric_attribute(span, "velostream.errors.count"),
        Some(5),
        "Should have errors.count=5"
    );

    assert_eq!(
        get_float_attribute(span, "velostream.errors.rate_percent"),
        Some(5.0),
        "Should have errors.rate_percent=5.0"
    );

    println!("✅ All error rate metrics validated");
}

/// Test that validates window-specific metrics
#[tokio::test]
#[serial]
async fn test_window_metrics() {
    let obs = create_test_observability_manager("test_windows").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "windowed_query", 1, &[record]);

    // Set window metrics: 1000ms window with 50 records, 3 windows emitted
    let window_start = 1707849600000i64; // Jan 13, 2024 12:00:00 UTC
    let window_end = window_start + 1000; // +1 second
    ObservabilityHelper::set_window_metrics(&mut batch_span, window_start, window_end, 50, 3);

    drop(batch_span);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Validate window attributes
    assert_eq!(
        get_numeric_attribute(span, "velostream.window.start_ms"),
        Some(window_start),
        "Should have window.start_ms"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.window.end_ms"),
        Some(window_end),
        "Should have window.end_ms"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.window.records"),
        Some(50),
        "Should have window.records=50"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.window.count"),
        Some(3),
        "Should have window.count=3 (windows emitted)"
    );

    println!("✅ All window metrics validated");
}

/// Test that validates join-specific metrics
#[tokio::test]
#[serial]
async fn test_join_metrics() {
    let obs = create_test_observability_manager("test_joins").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    let mut batch_span =
        ObservabilityHelper::start_batch_span(&Some(obs.clone()), "join_query", 1, &[record]);

    // Set join metrics: 1000 left, 800 right, 750 joined = 93.75% hit rate
    ObservabilityHelper::set_join_metrics(&mut batch_span, 1000, 800, 750, 0.9375);

    drop(batch_span);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert!(!collected_spans.is_empty());
    let span = &collected_spans[0];

    // Validate join attributes
    assert_eq!(
        get_numeric_attribute(span, "velostream.join.left_records"),
        Some(1000),
        "Should have join.left_records=1000"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.join.right_records"),
        Some(800),
        "Should have join.right_records=800"
    );

    assert_eq!(
        get_numeric_attribute(span, "velostream.join.joined_records"),
        Some(750),
        "Should have join.joined_records=750"
    );

    assert_eq!(
        get_float_attribute(span, "velostream.join.hit_rate"),
        Some(0.9375),
        "Should have join.hit_rate=0.9375 (93.75%)"
    );

    println!("✅ All join metrics validated");
}

/// Test that validates all processor modes set correct job_mode attribute
#[tokio::test]
#[serial]
async fn test_job_mode_across_processors() {
    let obs = create_test_observability_manager("test_job_modes").await;

    let record = StreamRecord::new(
        vec![("value".to_string(), FieldValue::Integer(100))]
            .into_iter()
            .collect(),
    );

    // Test each processor mode
    let modes = vec!["simple", "transactional", "partition", "join"];

    for mode in &modes {
        let mut batch_span = ObservabilityHelper::start_batch_span(
            &Some(obs.clone()),
            &format!("test_{}", mode),
            1,
            &[record.clone()],
        );

        ObservabilityHelper::set_application_attributes(
            &mut batch_span,
            &format!("test_{}", mode),
            None,
            None,
            Some(mode),
        );

        drop(batch_span);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let obs_lock = obs.read().await;
    let collected_spans = obs_lock.telemetry().unwrap().collected_spans();
    drop(obs_lock);

    assert_eq!(
        collected_spans.len(),
        4,
        "Should collect 4 spans (one per mode)"
    );

    // Verify each span has the correct job_mode
    for (idx, mode) in modes.iter().enumerate() {
        let span = &collected_spans[idx];
        assert_eq!(
            get_attribute(span, "velostream.job_mode"),
            Some(format!("String(\"{}\")", mode)),
            "Span {} should have job_mode={}",
            idx,
            mode
        );
    }

    println!("✅ All processor job_mode attributes validated");
}
