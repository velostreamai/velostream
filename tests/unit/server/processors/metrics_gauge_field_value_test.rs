//! Tests that gauge and histogram metric emission works correctly for every FieldValue variant.
//!
//! Ensures `emit_metrics_generic` properly handles Float, Integer, ScaledInteger,
//! Decimal, Timestamp, Date, Null, String, and Boolean field values — emitting numeric values and
//! gracefully skipping non-numeric ones. Both gauge (set) and histogram (observe)
//! batch functions are exercised.

use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use velostream::velostream::server::config::StreamJobServerConfig;
use velostream::velostream::server::observability_config_extractor::ObservabilityConfigExtractor;
use velostream::velostream::server::processors::observability_wrapper::ObservabilityWrapper;
use velostream::velostream::server::stream_job_server::StreamJobServer;
use velostream::velostream::sql::execution::config::{PrometheusConfig, StreamingConfig};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn prometheus_config() -> PrometheusConfig {
    PrometheusConfig {
        metrics_path: "/metrics".to_string(),
        bind_address: "0.0.0.0".to_string(),
        port: 0,
        enable_histograms: true,
        enable_query_metrics: true,
        enable_streaming_metrics: true,
        collection_interval_seconds: 15,
        max_labels_per_metric: 10,
        ..PrometheusConfig::default()
    }
}

/// SQL template with a single histogram metric on a configurable field.
fn histogram_sql() -> &'static str {
    r#"-- @observability.metrics.enabled: true
-- @metric: velo_test_histogram_value
-- @metric_type: histogram
-- @metric_help: "Test histogram"
-- @metric_field: value
CREATE STREAM test_hist_stream AS
SELECT symbol PRIMARY KEY, value FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'input_stream',
    'test_hist_stream.type' = 'kafka_sink',
    'test_hist_stream.topic.name' = 'test_hist_stream'
)"#
}

/// Helper: set up server + wrapper + register histogram.
async fn setup_histogram_test() -> (
    StreamJobServer,
    ObservabilityWrapper,
    velostream::velostream::sql::ast::StreamingQuery,
    String,
) {
    let streaming_config = StreamingConfig::default().with_prometheus_config(prometheus_config());
    let server_config =
        StreamJobServerConfig::new("localhost:9092".to_string(), "test".to_string());
    let server =
        StreamJobServer::with_config_and_observability(server_config, streaming_config).await;

    let parser = StreamingSqlParser::new();
    let sql = histogram_sql();
    let query = parser.parse(sql).expect("SQL should parse");

    let _annotation_config = ObservabilityConfigExtractor::extract_from_sql_string(sql)
        .expect("Config extraction should succeed");

    let server_obs = server.observability().cloned();
    let wrapper = ObservabilityWrapper::with_observability(server_obs.clone());
    let job_name = "test_hist_stream".to_string();

    wrapper
        .metrics_helper()
        .register_histogram_metrics(&query, &server_obs, &job_name)
        .await
        .expect("Histogram registration should succeed");

    (server, wrapper, query, job_name)
}

/// SQL template with a single gauge metric on a configurable field.
fn gauge_sql() -> &'static str {
    r#"-- @observability.metrics.enabled: true
-- @metric: velo_test_gauge_value
-- @metric_type: gauge
-- @metric_help: "Test gauge"
-- @metric_field: value
CREATE STREAM test_stream AS
SELECT symbol PRIMARY KEY, value FROM input_stream
EMIT CHANGES
WITH (
    'input_stream.type' = 'kafka_source',
    'input_stream.topic.name' = 'input_stream',
    'test_stream.type' = 'kafka_sink',
    'test_stream.topic.name' = 'test_stream'
)"#
}

/// Helper: set up server + wrapper + register gauge, return (server, wrapper, query, job_name).
async fn setup_gauge_test() -> (
    StreamJobServer,
    ObservabilityWrapper,
    velostream::velostream::sql::ast::StreamingQuery,
    String,
) {
    let streaming_config = StreamingConfig::default().with_prometheus_config(prometheus_config());
    let server_config =
        StreamJobServerConfig::new("localhost:9092".to_string(), "test".to_string());
    let server =
        StreamJobServer::with_config_and_observability(server_config, streaming_config).await;

    let parser = StreamingSqlParser::new();
    let sql = gauge_sql();
    let query = parser.parse(sql).expect("SQL should parse");

    let _annotation_config = ObservabilityConfigExtractor::extract_from_sql_string(sql)
        .expect("Config extraction should succeed");

    let server_obs = server.observability().cloned();
    let wrapper = ObservabilityWrapper::with_observability(server_obs.clone());
    let job_name = "test_stream".to_string();

    wrapper
        .metrics_helper()
        .register_gauge_metrics(&query, &server_obs, &job_name)
        .await
        .expect("Gauge registration should succeed");

    // Also register counter so we can verify counters still work when gauge is skipped
    wrapper
        .metrics_helper()
        .register_counter_metrics(&query, &server_obs, &job_name)
        .await
        .ok(); // counter may not exist in this SQL, that's fine

    (server, wrapper, query, job_name)
}

fn make_record(value: FieldValue) -> Vec<Arc<StreamRecord>> {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("value".to_string(), value);
    vec![Arc::new(StreamRecord::new(fields))]
}

#[tokio::test]
async fn test_gauge_with_float_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Float(42.5));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for Float.\nMetrics:\n{}",
        metrics
    );
    assert!(
        metrics.contains("42.5"),
        "Gauge value should be 42.5.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_integer_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Integer(100));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for Integer.\nMetrics:\n{}",
        metrics
    );
    // Integer 100 → f64 100.0, Prometheus renders as "100"
    assert!(
        metrics.contains("100"),
        "Gauge value should contain 100.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_scaled_integer_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::ScaledInteger(12345, 2));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for ScaledInteger.\nMetrics:\n{}",
        metrics
    );
    assert!(
        metrics.contains("123.45"),
        "Gauge value should be 123.45.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_decimal_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let decimal = Decimal::from_str("99.99").expect("Valid decimal");
    let records = make_record(FieldValue::Decimal(decimal));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for Decimal.\nMetrics:\n{}",
        metrics
    );
    assert!(
        metrics.contains("99.99"),
        "Gauge value should be ~99.99.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_null_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Null);

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    // Gauge should NOT be emitted for Null — but no crash
    assert!(
        !metrics.contains("velo_test_gauge_value"),
        "Gauge should NOT be emitted for Null.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_non_numeric_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::String("not_a_number".to_string()));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        !metrics.contains("velo_test_gauge_value"),
        "Gauge should NOT be emitted for String.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_boolean_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Boolean(true));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        !metrics.contains("velo_test_gauge_value"),
        "Gauge should NOT be emitted for Boolean.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_timestamp_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    // 2024-01-15 12:30:45 UTC
    let ts = NaiveDateTime::parse_from_str("2024-01-15 12:30:45", "%Y-%m-%d %H:%M:%S")
        .expect("Valid datetime");
    let expected_millis = ts.and_utc().timestamp_millis();
    let records = make_record(FieldValue::Timestamp(ts));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for Timestamp.\nMetrics:\n{}",
        metrics
    );
    // Verify it contains the expected epoch milliseconds value
    assert!(
        metrics.contains(&expected_millis.to_string()),
        "Gauge value should be epoch milliseconds ({}).\nMetrics:\n{}",
        expected_millis,
        metrics
    );
}

#[tokio::test]
async fn test_gauge_with_date_field() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();
    // 2024-01-15 → 1705276800 seconds since epoch (midnight UTC)
    let date = NaiveDate::from_ymd_opt(2024, 1, 15).expect("Valid date");
    let records = make_record(FieldValue::Date(date));

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for Date.\nMetrics:\n{}",
        metrics
    );
    // Verify it's a large number (epoch seconds for midnight)
    assert!(
        metrics.contains("1705276800"),
        "Gauge value should be epoch seconds (1705276800).\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_gauge_mixed_null_and_valid() {
    let (server, wrapper, query, job_name) = setup_gauge_test().await;
    let server_obs = server.observability().cloned();

    let records: Vec<Arc<StreamRecord>> = vec![
        {
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert("value".to_string(), FieldValue::Null);
            Arc::new(StreamRecord::new(fields))
        },
        {
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert("value".to_string(), FieldValue::Float(77.7));
            Arc::new(StreamRecord::new(fields))
        },
        {
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert("value".to_string(), FieldValue::Null);
            Arc::new(StreamRecord::new(fields))
        },
    ];

    wrapper
        .metrics_helper()
        .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    // The Float record should cause the gauge to be emitted
    assert!(
        metrics.contains("velo_test_gauge_value"),
        "Gauge should be emitted for the Float record among Nulls.\nMetrics:\n{}",
        metrics
    );
    assert!(
        metrics.contains("77.7"),
        "Gauge value should be 77.7 (last valid observation).\nMetrics:\n{}",
        metrics
    );
}

// ---------------------------------------------------------------------------
// Histogram tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_histogram_with_float_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Float(42.5));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for Float.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_integer_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Integer(100));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for Integer.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_scaled_integer_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::ScaledInteger(12345, 2));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for ScaledInteger.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_decimal_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let decimal = Decimal::from_str("99.99").expect("Valid decimal");
    let records = make_record(FieldValue::Decimal(decimal));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for Decimal.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_null_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Null);

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        !metrics.contains("velo_test_histogram_value_bucket"),
        "Histogram should NOT be emitted for Null.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_non_numeric_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::String("not_a_number".to_string()));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        !metrics.contains("velo_test_histogram_value_bucket"),
        "Histogram should NOT be emitted for String.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_boolean_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    let records = make_record(FieldValue::Boolean(true));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        !metrics.contains("velo_test_histogram_value_bucket"),
        "Histogram should NOT be emitted for Boolean.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_timestamp_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    // 2024-01-15 12:30:45 UTC → 1705322445000 milliseconds since epoch
    let ts = NaiveDateTime::parse_from_str("2024-01-15 12:30:45", "%Y-%m-%d %H:%M:%S")
        .expect("Valid datetime");
    let records = make_record(FieldValue::Timestamp(ts));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for Timestamp.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_with_date_field() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();
    // 2024-01-15 → 1705276800 seconds since epoch (midnight UTC)
    let date = NaiveDate::from_ymd_opt(2024, 1, 15).expect("Valid date");
    let records = make_record(FieldValue::Date(date));

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for Date.\nMetrics:\n{}",
        metrics
    );
}

#[tokio::test]
async fn test_histogram_mixed_null_and_valid() {
    let (server, wrapper, query, job_name) = setup_histogram_test().await;
    let server_obs = server.observability().cloned();

    let records: Vec<Arc<StreamRecord>> = vec![
        {
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert("value".to_string(), FieldValue::Null);
            Arc::new(StreamRecord::new(fields))
        },
        {
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert("value".to_string(), FieldValue::Float(55.5));
            Arc::new(StreamRecord::new(fields))
        },
        {
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert("value".to_string(), FieldValue::Null);
            Arc::new(StreamRecord::new(fields))
        },
    ];

    wrapper
        .metrics_helper()
        .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
        .await;

    let metrics = server
        .get_performance_metrics()
        .expect("Should return metrics");
    assert!(
        metrics.contains("velo_test_histogram_value"),
        "Histogram should be emitted for the Float record among Nulls.\nMetrics:\n{}",
        metrics
    );
}
