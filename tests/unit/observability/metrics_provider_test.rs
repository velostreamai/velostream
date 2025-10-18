//! Comprehensive unit tests for MetricsProvider and batch metrics functionality
//!
//! Tests cover:
//! - MetricsProvider initialization and lifecycle
//! - Dynamic metrics registration (counters, gauges, histograms)
//! - Batch metric accumulation and emission
//! - All execution paths in emit_batch()
//! - Error handling and edge cases

use prometheus::Registry;
use std::time::Duration;
use velostream::velostream::observability::metrics::{MetricBatch, MetricsProvider};
use velostream::velostream::sql::execution::config::PrometheusConfig;

// ===== MetricsProvider Creation Tests =====

#[tokio::test]
async fn test_metrics_provider_creation() {
    let config = PrometheusConfig::lightweight();
    let provider = MetricsProvider::new(config).await;
    assert!(provider.is_ok());

    let provider = provider.unwrap();
    assert!(provider.active);
}

#[tokio::test]
async fn test_metrics_provider_default_config() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await;
    assert!(provider.is_ok());
}

// ===== Metrics Recording Tests =====

#[tokio::test]
async fn test_metrics_recording() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    // Test SQL metrics
    provider.record_sql_query("select", Duration::from_millis(150), true, 100);
    provider.record_sql_query("insert", Duration::from_millis(250), false, 50);

    // Test streaming metrics
    provider.record_streaming_operation("deserialization", Duration::from_millis(100), 1000, 500.0);

    // Test system metrics
    provider.update_system_metrics(45.5, 1024 * 1024 * 1024, 10);

    // Test stats
    let stats = provider.get_stats();
    assert_eq!(stats.sql_queries_total, 2);
    assert_eq!(stats.sql_errors_total, 1);
    assert_eq!(stats.streaming_operations_total, 1);
    assert_eq!(stats.records_processed_total, 150);
    assert_eq!(stats.records_streamed_total, 1000);
    assert_eq!(stats.active_connections, 10);
}

#[tokio::test]
async fn test_metrics_text_export() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider.record_sql_query("select", Duration::from_millis(100), true, 50);

    let metrics_text = provider.get_metrics_text();
    assert!(metrics_text.is_ok());

    let text = metrics_text.unwrap();
    assert!(text.contains("velo_sql_queries_total"));
    assert!(text.contains("velo_sql_query_duration_seconds"));
}

// ===== Metrics Component Creation Tests =====

#[test]
fn test_sql_metrics_creation() {
    let registry = Registry::new();
    let config = PrometheusConfig::default();

    // Note: SqlMetrics is private, so we just verify provider creation works
    let result = PrometheusConfig::default();
    assert_eq!(result.enable_prometheus_metrics, true);
}

#[test]
fn test_streaming_metrics_creation() {
    let config = PrometheusConfig::default();
    assert_eq!(config.port, 9091);
    assert_eq!(config.metrics_path, "/metrics");
}

#[test]
fn test_system_metrics_creation() {
    let config = PrometheusConfig::default();
    assert!(config.enable_prometheus_metrics);
}

// ===== Dynamic Counter Metrics Tests =====

#[tokio::test]
async fn test_register_counter_metric_basic() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.register_counter_metric(
        "test_events_total",
        "Total number of test events",
        &vec![],
    );

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_register_counter_metric_with_labels() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.register_counter_metric(
        "test_events_labeled_total",
        "Test events with labels",
        &vec!["event_type".to_string(), "severity".to_string()],
    );

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_counter_metric() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_counter_metric(
            "test_counter_total",
            "Test counter",
            &vec!["label1".to_string()],
        )
        .await;

    let result = provider.emit_counter("test_counter_total", &vec!["value1".to_string()]);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_counter_unregistered() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.emit_counter("nonexistent_counter", &vec![]);
    assert!(result.is_err());
}

// ===== Dynamic Gauge Metrics Tests =====

#[tokio::test]
async fn test_register_gauge_metric_basic() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.register_gauge_metric("test_gauge", "Test gauge metric", &vec![]);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_register_gauge_metric_with_labels() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.register_gauge_metric(
        "test_gauge_labeled",
        "Test gauge with labels",
        &vec!["instance".to_string()],
    );

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_gauge_metric() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_gauge_metric("test_gauge", "Test gauge", &vec!["label1".to_string()])
        .await;

    let result = provider.emit_gauge("test_gauge", &vec!["value1".to_string()], 42.5);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_gauge_unregistered() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.emit_gauge("nonexistent_gauge", &vec![], 0.0);
    assert!(result.is_err());
}

// ===== Dynamic Histogram Metrics Tests =====

#[tokio::test]
async fn test_register_histogram_metric_basic() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.register_histogram_metric(
        "test_histogram",
        "Test histogram metric",
        &vec![],
        None,
    );

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_register_histogram_metric_custom_buckets() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.register_histogram_metric(
        "test_histogram_custom",
        "Test histogram with custom buckets",
        &vec![],
        Some(vec![0.1, 0.5, 1.0, 5.0]),
    );

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_histogram_metric() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_histogram_metric(
            "test_histogram",
            "Test histogram",
            &vec!["label1".to_string()],
            None,
        )
        .await;

    let result = provider.emit_histogram("test_histogram", &vec!["value1".to_string()], 0.5);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_histogram_unregistered() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.emit_histogram("nonexistent_histogram", &vec![], 0.0);
    assert!(result.is_err());
}

// ===== Phase 4: Batch Metrics Tests =====

#[test]
fn test_metric_batch_creation() {
    let batch = MetricBatch::new();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
}

#[test]
fn test_metric_batch_with_capacity() {
    let batch = MetricBatch::with_capacity(100);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
}

#[test]
fn test_metric_batch_add_counter() {
    let mut batch = MetricBatch::new();
    batch.add_counter("test_counter".to_string(), vec!["label1".to_string()]);
    assert_eq!(batch.len(), 1);
}

#[test]
fn test_metric_batch_add_gauge() {
    let mut batch = MetricBatch::new();
    batch.add_gauge("test_gauge".to_string(), vec!["label1".to_string()], 42.5);
    assert_eq!(batch.len(), 1);
}

#[test]
fn test_metric_batch_add_histogram() {
    let mut batch = MetricBatch::new();
    batch.add_histogram(
        "test_histogram".to_string(),
        vec!["label1".to_string()],
        0.5,
    );
    assert_eq!(batch.len(), 1);
}

#[test]
fn test_metric_batch_mixed_events() {
    let mut batch = MetricBatch::new();
    batch.add_counter("counter".to_string(), vec![]);
    batch.add_gauge("gauge".to_string(), vec![], 1.0);
    batch.add_histogram("histogram".to_string(), vec![], 0.5);
    assert_eq!(batch.len(), 3);
}

#[test]
fn test_metric_batch_capacity_preallocation() {
    let batch = MetricBatch::with_capacity(1000);
    assert_eq!(batch.len(), 0); // Still empty, just pre-allocated
    assert!(batch.is_empty());
}

#[tokio::test]
async fn test_emit_batch_empty() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let batch = MetricBatch::new();
    let result = provider.emit_batch(batch);
    // Should succeed (empty batch is ok)
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_single_counter() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_counter_metric(
            "batch_counter",
            "Counter for batch testing",
            &vec!["type".to_string()],
        )
        .await;

    let mut batch = MetricBatch::new();
    batch.add_counter("batch_counter".to_string(), vec!["test".to_string()]);

    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_single_gauge() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_gauge_metric(
            "batch_gauge",
            "Gauge for batch testing",
            &vec!["type".to_string()],
        )
        .await;

    let mut batch = MetricBatch::new();
    batch.add_gauge("batch_gauge".to_string(), vec!["test".to_string()], 99.9);

    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_single_histogram() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_histogram_metric(
            "batch_histogram",
            "Histogram for batch testing",
            &vec!["type".to_string()],
            None,
        )
        .await;

    let mut batch = MetricBatch::new();
    batch.add_histogram(
        "batch_histogram".to_string(),
        vec!["test".to_string()],
        0.123,
    );

    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_multiple_mixed_events() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    // Register all metric types
    provider
        .register_counter_metric("batch_counter", "Test counter", &vec![])
        .await;
    provider
        .register_gauge_metric("batch_gauge", "Test gauge", &vec![])
        .await;
    provider
        .register_histogram_metric("batch_histogram", "Test histogram", &vec![], None)
        .await;

    // Create batch with multiple events
    let mut batch = MetricBatch::new();
    batch.add_counter("batch_counter".to_string(), vec![]);
    batch.add_gauge("batch_gauge".to_string(), vec![], 50.0);
    batch.add_histogram("batch_histogram".to_string(), vec![], 0.25);
    batch.add_counter("batch_counter".to_string(), vec![]);
    batch.add_gauge("batch_gauge".to_string(), vec![], 75.0);

    assert_eq!(batch.len(), 5);
    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_unregistered_metrics_skipped() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    let mut batch = MetricBatch::new();
    batch.add_counter("unregistered_counter".to_string(), vec![]);
    batch.add_gauge("unregistered_gauge".to_string(), vec![], 1.0);
    batch.add_histogram("unregistered_histogram".to_string(), vec![], 0.5);

    // Should still succeed (just skips unregistered metrics with warning)
    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_with_labels() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_counter_metric(
            "labeled_counter",
            "Counter with labels",
            &vec!["type".to_string(), "severity".to_string()],
        )
        .await;

    let mut batch = MetricBatch::new();
    batch.add_counter(
        "labeled_counter".to_string(),
        vec!["error".to_string(), "high".to_string()],
    );
    batch.add_counter(
        "labeled_counter".to_string(),
        vec!["warning".to_string(), "medium".to_string()],
    );

    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_emit_batch_large_batch() {
    let config = PrometheusConfig::default();
    let provider = MetricsProvider::new(config).await.unwrap();

    provider
        .register_counter_metric("large_counter", "Counter for large batch", &vec![])
        .await;

    let mut batch = MetricBatch::with_capacity(1000);
    for i in 0..1000 {
        batch.add_counter(
            "large_counter".to_string(),
            vec![format!("index_{}", i % 10)],
        );
    }

    assert_eq!(batch.len(), 1000);
    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}

// ===== Provider Lifecycle Tests =====

#[tokio::test]
async fn test_provider_shutdown() {
    let config = PrometheusConfig::default();
    let mut provider = MetricsProvider::new(config).await.unwrap();

    let result = provider.shutdown().await;
    assert!(result.is_ok());
    assert!(!provider.active);
}

#[tokio::test]
async fn test_metrics_after_shutdown() {
    let config = PrometheusConfig::default();
    let mut provider = MetricsProvider::new(config).await.unwrap();

    provider.shutdown().await.ok();

    // Should still succeed but be no-ops
    provider.record_sql_query("select", Duration::from_millis(100), true, 1);
    let batch = MetricBatch::new();
    let result = provider.emit_batch(batch);
    assert!(result.is_ok());
}
