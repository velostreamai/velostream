use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use velostream::velostream::observability::{
    ObservabilityManager, PrometheusConfig, TelemetryConfig,
};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::StreamingQuery;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_record(symbol: String, volume: i64, avg_volume: i64) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("volume".to_string(), FieldValue::Integer(volume));
        fields.insert("avg_volume".to_string(), FieldValue::Integer(avg_volume));
        fields.insert("event_time".to_string(), FieldValue::Integer(1000));

        StreamRecord {
            fields,
            timestamp: 1000,
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_counter_metric_registration_and_emission() {
        // Create observability manager with metrics enabled
        let prometheus_config = PrometheusConfig {
            port: 0, // Use dynamic port for testing
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Parse SQL query with metric annotations
        let parser = StreamingSqlParser::new();
        let sql = r#"
            -- @metric: test_volume_spikes_total
            -- @metric_type: counter
            -- @metric_help: "Total number of volume spikes detected"
            -- @metric_labels: symbol
            CREATE STREAM volume_spikes AS
            SELECT symbol, volume, avg_volume
            FROM market_data
            WHERE volume > avg_volume * 2
        "#;

        let query = parser.parse(sql).expect("Failed to parse SQL");

        // Extract metric annotations
        let annotations = match &query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations.clone(),
            _ => panic!("Expected CreateStream query"),
        };

        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations[0].name, "test_volume_spikes_total");
        assert_eq!(annotations[0].labels, vec!["symbol"]);

        // Register counter metric
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .register_counter_metric(
                    &annotations[0].name,
                    &annotations[0].help.as_deref().unwrap_or("Test metric"),
                    &annotations[0].labels,
                )
                .expect("Failed to register counter metric");
        }

        // Create test records that would match the query
        let records = vec![
            create_test_record("AAPL".to_string(), 1000, 400), // volume > avg * 2
            create_test_record("GOOGL".to_string(), 2000, 800), // volume > avg * 2
            create_test_record("AAPL".to_string(), 1500, 600), // volume > avg * 2
        ];

        // Emit counter metrics for each record
        if let Some(metrics) = obs_manager.metrics() {
            for record in &records {
                let symbol = record
                    .fields
                    .get("symbol")
                    .expect("Symbol field missing")
                    .to_display_string();

                metrics
                    .emit_counter(&annotations[0].name, &vec![symbol])
                    .expect("Failed to emit counter");
            }
        }

        // Verify metrics were recorded by checking the Prometheus registry
        if let Some(metrics) = obs_manager.metrics() {
            let metrics_text = metrics
                .get_metrics_text()
                .expect("Failed to get Prometheus metrics");

            // Verify the metric appears in the output
            assert!(
                metrics_text.contains("test_volume_spikes_total"),
                "Metric name should appear in Prometheus output"
            );

            // Verify we have counters for both symbols
            assert!(
                metrics_text.contains(r#"symbol="AAPL""#),
                "AAPL label should appear in metrics"
            );
            assert!(
                metrics_text.contains(r#"symbol="GOOGL""#),
                "GOOGL label should appear in metrics"
            );
        }
    }

    #[tokio::test]
    async fn test_counter_metric_with_multiple_labels() {
        // Create observability manager
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Register counter with multiple labels
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .register_counter_metric(
                    "test_events_total",
                    "Total events",
                    &vec!["symbol".to_string(), "exchange".to_string()],
                )
                .expect("Failed to register counter metric");

            // Emit with different label combinations
            metrics
                .emit_counter(
                    "test_events_total",
                    &vec!["AAPL".to_string(), "NYSE".to_string()],
                )
                .expect("Failed to emit counter");

            metrics
                .emit_counter(
                    "test_events_total",
                    &vec!["GOOGL".to_string(), "NASDAQ".to_string()],
                )
                .expect("Failed to emit counter");

            metrics
                .emit_counter(
                    "test_events_total",
                    &vec!["AAPL".to_string(), "NYSE".to_string()],
                )
                .expect("Failed to emit counter");

            // Verify metrics
            let metrics_text = metrics.get_metrics_text().expect("Failed to get metrics");

            assert!(metrics_text.contains("test_events_total"));
            // Check labels individually - Prometheus may format with spaces
            assert!(metrics_text.contains(r#"symbol="AAPL""#));
            assert!(metrics_text.contains(r#"exchange="NYSE""#));
            assert!(metrics_text.contains(r#"symbol="GOOGL""#));
            assert!(metrics_text.contains(r#"exchange="NASDAQ""#));
        }
    }

    #[tokio::test]
    async fn test_multiple_counter_metrics() {
        // Create observability manager
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Parse SQL with multiple metric annotations
        let parser = StreamingSqlParser::new();
        let sql = r#"
            -- @metric: test_total_events
            -- @metric_type: counter
            -- @metric: test_high_volume_events
            -- @metric_type: counter
            -- @metric_labels: symbol
            CREATE STREAM events AS
            SELECT symbol, volume
            FROM market_data
        "#;

        let query = parser.parse(sql).expect("Failed to parse SQL");

        let annotations = match &query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations.clone(),
            _ => panic!("Expected CreateStream query"),
        };

        assert_eq!(annotations.len(), 2);

        // Register both metrics
        if let Some(metrics) = obs_manager.metrics() {
            for annotation in &annotations {
                metrics
                    .register_counter_metric(&annotation.name, "Test metric", &annotation.labels)
                    .expect("Failed to register counter");
            }

            // Emit to both metrics
            metrics
                .emit_counter("test_total_events", &vec![])
                .expect("Failed to emit");
            metrics
                .emit_counter("test_high_volume_events", &vec!["AAPL".to_string()])
                .expect("Failed to emit");

            // Verify both metrics exist
            let metrics_text = metrics.get_metrics_text().expect("Failed to get metrics");
            assert!(metrics_text.contains("test_total_events"));
            assert!(metrics_text.contains("test_high_volume_events"));
        }
    }

    #[tokio::test]
    async fn test_gauge_metric_registration_and_emission() {
        // Create observability manager with metrics enabled
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Parse SQL query with gauge metric annotations
        let parser = StreamingSqlParser::new();
        let sql = r#"
            -- @metric: test_current_volume
            -- @metric_type: gauge
            -- @metric_help: "Current volume for symbol"
            -- @metric_labels: symbol
            -- @metric_field: volume
            CREATE STREAM volume_monitor AS
            SELECT symbol, volume, avg_volume
            FROM market_data
        "#;

        let query = parser.parse(sql).expect("Failed to parse SQL");

        // Extract metric annotations
        let annotations = match &query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations.clone(),
            _ => panic!("Expected CreateStream query"),
        };

        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations[0].name, "test_current_volume");
        assert_eq!(annotations[0].labels, vec!["symbol"]);

        // Register gauge metric
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .register_gauge_metric(
                    &annotations[0].name,
                    &annotations[0].help.as_deref().unwrap_or("Test metric"),
                    &annotations[0].labels,
                )
                .expect("Failed to register gauge metric");
        }

        // Emit gauge values
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .emit_gauge("test_current_volume", &vec!["AAPL".to_string()], 1000.5)
                .expect("Failed to emit gauge");
            metrics
                .emit_gauge("test_current_volume", &vec!["GOOGL".to_string()], 2500.75)
                .expect("Failed to emit gauge");

            // Verify metrics were recorded
            let metrics_text = metrics
                .get_metrics_text()
                .expect("Failed to get Prometheus metrics");

            assert!(
                metrics_text.contains("test_current_volume"),
                "Gauge metric name should appear in Prometheus output"
            );
            assert!(
                metrics_text.contains(r#"symbol="AAPL""#),
                "AAPL label should appear in metrics"
            );
            assert!(
                metrics_text.contains(r#"symbol="GOOGL""#),
                "GOOGL label should appear in metrics"
            );
        }
    }

    #[tokio::test]
    async fn test_gauge_metric_with_multiple_labels() {
        // Create observability manager
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Register gauge with multiple labels
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .register_gauge_metric(
                    "test_current_price",
                    "Current price per symbol and exchange",
                    &vec!["symbol".to_string(), "exchange".to_string()],
                )
                .expect("Failed to register gauge metric");

            // Emit with different label combinations
            metrics
                .emit_gauge(
                    "test_current_price",
                    &vec!["AAPL".to_string(), "NYSE".to_string()],
                    150.25,
                )
                .expect("Failed to emit gauge");

            metrics
                .emit_gauge(
                    "test_current_price",
                    &vec!["GOOGL".to_string(), "NASDAQ".to_string()],
                    2800.50,
                )
                .expect("Failed to emit gauge");

            // Update existing gauge
            metrics
                .emit_gauge(
                    "test_current_price",
                    &vec!["AAPL".to_string(), "NYSE".to_string()],
                    151.75,
                )
                .expect("Failed to emit gauge");

            // Verify metrics
            let metrics_text = metrics.get_metrics_text().expect("Failed to get metrics");

            assert!(metrics_text.contains("test_current_price"));
            // Check labels individually - Prometheus may format with spaces
            assert!(metrics_text.contains(r#"symbol="AAPL""#));
            assert!(metrics_text.contains(r#"exchange="NYSE""#));
            assert!(metrics_text.contains(r#"symbol="GOOGL""#));
            assert!(metrics_text.contains(r#"exchange="NASDAQ""#));
        }
    }

    #[tokio::test]
    async fn test_histogram_metric_registration_and_emission() {
        // Create observability manager with metrics enabled
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Parse SQL query with histogram metric annotations
        let parser = StreamingSqlParser::new();
        let sql = r#"
            -- @metric: test_trade_volume_distribution
            -- @metric_type: histogram
            -- @metric_help: "Distribution of trade volumes"
            -- @metric_labels: symbol
            -- @metric_field: volume
            -- @metric_buckets: 100,500,1000,5000,10000
            CREATE STREAM volume_distribution AS
            SELECT symbol, volume
            FROM market_data
        "#;

        let query = parser.parse(sql).expect("Failed to parse SQL");

        // Extract metric annotations
        let annotations = match &query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations.clone(),
            _ => panic!("Expected CreateStream query"),
        };

        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations[0].name, "test_trade_volume_distribution");
        assert_eq!(annotations[0].labels, vec!["symbol"]);
        assert!(annotations[0].buckets.is_some());

        // Register histogram metric
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .register_histogram_metric(
                    &annotations[0].name,
                    &annotations[0].help.as_deref().unwrap_or("Test metric"),
                    &annotations[0].labels,
                    annotations[0].buckets.clone(),
                )
                .expect("Failed to register histogram metric");
        }

        // Emit histogram observations
        if let Some(metrics) = obs_manager.metrics() {
            // Observe multiple values for AAPL
            metrics
                .emit_histogram(
                    "test_trade_volume_distribution",
                    &vec!["AAPL".to_string()],
                    250.0,
                )
                .expect("Failed to emit histogram");
            metrics
                .emit_histogram(
                    "test_trade_volume_distribution",
                    &vec!["AAPL".to_string()],
                    750.0,
                )
                .expect("Failed to emit histogram");
            metrics
                .emit_histogram(
                    "test_trade_volume_distribution",
                    &vec!["AAPL".to_string()],
                    1500.0,
                )
                .expect("Failed to emit histogram");

            // Observe values for GOOGL
            metrics
                .emit_histogram(
                    "test_trade_volume_distribution",
                    &vec!["GOOGL".to_string()],
                    3000.0,
                )
                .expect("Failed to emit histogram");

            // Verify metrics were recorded
            let metrics_text = metrics
                .get_metrics_text()
                .expect("Failed to get Prometheus metrics");

            assert!(
                metrics_text.contains("test_trade_volume_distribution"),
                "Histogram metric name should appear in Prometheus output"
            );
            assert!(
                metrics_text.contains(r#"symbol="AAPL""#),
                "AAPL label should appear in metrics"
            );
            assert!(
                metrics_text.contains(r#"symbol="GOOGL""#),
                "GOOGL label should appear in metrics"
            );
            // Histogram metrics include _bucket, _sum, and _count suffixes
            assert!(
                metrics_text.contains("test_trade_volume_distribution_bucket"),
                "Histogram bucket metrics should appear"
            );
        }
    }

    #[tokio::test]
    async fn test_histogram_with_default_buckets() {
        // Create observability manager
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Register histogram without custom buckets (use defaults)
        if let Some(metrics) = obs_manager.metrics() {
            metrics
                .register_histogram_metric(
                    "test_default_buckets",
                    "Test histogram with default buckets",
                    &vec!["category".to_string()],
                    None, // No custom buckets
                )
                .expect("Failed to register histogram metric");

            // Emit observations
            metrics
                .emit_histogram("test_default_buckets", &vec!["test".to_string()], 0.5)
                .expect("Failed to emit histogram");
            metrics
                .emit_histogram("test_default_buckets", &vec!["test".to_string()], 1.5)
                .expect("Failed to emit histogram");
            metrics
                .emit_histogram("test_default_buckets", &vec!["test".to_string()], 5.0)
                .expect("Failed to emit histogram");

            // Verify metrics
            let metrics_text = metrics.get_metrics_text().expect("Failed to get metrics");

            assert!(metrics_text.contains("test_default_buckets"));
            assert!(metrics_text.contains("test_default_buckets_bucket"));
        }
    }

    #[tokio::test]
    async fn test_mixed_metric_types() {
        // Create observability manager
        let prometheus_config = PrometheusConfig {
            port: 0,
            enabled: true,
        };

        let telemetry_config = TelemetryConfig::default();

        let obs_manager = ObservabilityManager::new(prometheus_config, telemetry_config)
            .await
            .expect("Failed to create observability manager");

        // Parse SQL with multiple metric types
        let parser = StreamingSqlParser::new();
        let sql = r#"
            -- @metric: test_event_count
            -- @metric_type: counter
            -- @metric_labels: symbol
            -- @metric: test_current_value
            -- @metric_type: gauge
            -- @metric_field: value
            -- @metric_labels: symbol
            -- @metric: test_value_distribution
            -- @metric_type: histogram
            -- @metric_field: value
            -- @metric_labels: symbol
            CREATE STREAM mixed_metrics AS
            SELECT symbol, value
            FROM market_data
        "#;

        let query = parser.parse(sql).expect("Failed to parse SQL");

        let annotations = match &query {
            StreamingQuery::CreateStream {
                metric_annotations, ..
            } => metric_annotations.clone(),
            _ => panic!("Expected CreateStream query"),
        };

        assert_eq!(annotations.len(), 3);

        // Register all metrics
        if let Some(metrics) = obs_manager.metrics() {
            for annotation in &annotations {
                match annotation.metric_type {
                    velostream::velostream::sql::parser::annotations::MetricType::Counter => {
                        metrics
                            .register_counter_metric(
                                &annotation.name,
                                "Test counter",
                                &annotation.labels,
                            )
                            .expect("Failed to register counter");
                    }
                    velostream::velostream::sql::parser::annotations::MetricType::Gauge => {
                        metrics
                            .register_gauge_metric(
                                &annotation.name,
                                "Test gauge",
                                &annotation.labels,
                            )
                            .expect("Failed to register gauge");
                    }
                    velostream::velostream::sql::parser::annotations::MetricType::Histogram => {
                        metrics
                            .register_histogram_metric(
                                &annotation.name,
                                "Test histogram",
                                &annotation.labels,
                                annotation.buckets.clone(),
                            )
                            .expect("Failed to register histogram");
                    }
                }
            }

            // Emit to all metrics
            metrics
                .emit_counter("test_event_count", &vec!["AAPL".to_string()])
                .expect("Failed to emit counter");
            metrics
                .emit_gauge("test_current_value", &vec!["AAPL".to_string()], 100.5)
                .expect("Failed to emit gauge");
            metrics
                .emit_histogram("test_value_distribution", &vec!["AAPL".to_string()], 100.5)
                .expect("Failed to emit histogram");

            // Verify all metrics exist
            let metrics_text = metrics.get_metrics_text().expect("Failed to get metrics");
            assert!(metrics_text.contains("test_event_count"));
            assert!(metrics_text.contains("test_current_value"));
            assert!(metrics_text.contains("test_value_distribution"));
        }
    }
}
