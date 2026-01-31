//! Tests that @metric annotations from ALL CREATE STREAM statements in a multi-statement
//! SQL app are registered and emitted via a single shared ObservabilityManager.
//!
//! Exercises the exact same code path as `deploy_sql_application_with_filename`:
//!   1. Parse multi-statement SQL via SqlApplicationParser
//!   2. For each statement: inject @observability.metrics.enabled, re-parse, extract config
//!   3. Create processor with shared observability, register + emit metrics
//!   4. Verify server.get_performance_metrics() returns ALL metrics from ALL statements

use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::server::config::StreamJobServerConfig;
use velostream::velostream::server::observability_config_extractor::ObservabilityConfigExtractor;
use velostream::velostream::server::processors::observability_wrapper::ObservabilityWrapper;
use velostream::velostream::server::stream_job_server::StreamJobServer;
use velostream::velostream::sql::app_parser::SqlApplicationParser;
use velostream::velostream::sql::execution::config::{PrometheusConfig, StreamingConfig};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Single-app deployment test: parse one SQL file with 3 CREATE STREAM statements,
/// walk through the exact deploy_sql_application code path, and verify all metrics
/// from all statements are scrapable via get_performance_metrics().
#[tokio::test]
async fn test_single_app_deploy_all_metrics_scrapable() {
    // -- 1. Create a StreamJobServer with observability (same as velo-sql CLI) --
    let prometheus_config = PrometheusConfig {
        metrics_path: "/metrics".to_string(),
        bind_address: "0.0.0.0".to_string(),
        port: 0,
        enable_histograms: true,
        enable_query_metrics: true,
        enable_streaming_metrics: true,
        collection_interval_seconds: 15,
        max_labels_per_metric: 10,
    };
    let streaming_config = StreamingConfig::default().with_prometheus_config(prometheus_config);
    let server_config =
        StreamJobServerConfig::new("localhost:9092".to_string(), "test".to_string());
    let server =
        StreamJobServer::with_config_and_observability(server_config, streaming_config).await;

    assert!(
        server.has_performance_monitoring(),
        "Server must have observability enabled"
    );

    // -- 2. The SQL app: 3 statements, 5 total @metric annotations --
    let sql = r#"-- SQL Application: app_market_data
-- Version: 1.0.0
-- Description: Market data pipeline

-- @observability.metrics.enabled: true

-- @metric: velo_market_data_ts_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by market_data_ts"
--
-- @metric: velo_market_data_current_price
-- @metric_type: gauge
-- @metric_help: "Current price value"
-- @metric_field: price
CREATE STREAM market_data_ts AS
SELECT symbol PRIMARY KEY, price FROM in_market_data_stream
EMIT CHANGES
WITH (
    'in_market_data_stream.type' = 'kafka_source',
    'in_market_data_stream.topic.name' = 'in_market_data_stream',
    'market_data_ts.type' = 'kafka_sink',
    'market_data_ts.topic.name' = 'market_data_ts'
);

-- @metric: velo_tick_buckets_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by tick_buckets"
-- @metric_labels: symbol
--
-- @metric: velo_tick_buckets_avg_price
-- @metric_type: gauge
-- @metric_help: "Average price in bucket"
-- @metric_labels: symbol
-- @metric_field: avg_price
CREATE STREAM tick_buckets AS
SELECT
    symbol PRIMARY KEY,
    AVG(price) as avg_price,
    COUNT(*) as trade_count
FROM market_data_ts
WINDOW TUMBLING(timestamp, INTERVAL '1' SECOND)
GROUP BY symbol
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'tick_buckets.type' = 'kafka_sink',
    'tick_buckets.topic.name' = 'tick_buckets'
);

-- @metric: velo_enriched_market_data_records_total
-- @metric_type: counter
-- @metric_help: "Total records processed by enriched_market_data"
CREATE STREAM enriched_market_data AS
SELECT m.symbol PRIMARY KEY, m.price
FROM market_data_ts m
EMIT CHANGES
WITH (
    'market_data_ts.type' = 'kafka_source',
    'market_data_ts.topic.name' = 'market_data_ts',
    'enriched_market_data.type' = 'kafka_sink',
    'enriched_market_data.topic.name' = 'enriched_market_data'
)"#;

    // -- 3. Parse via SqlApplicationParser (same as server) --
    let app_parser = SqlApplicationParser::new();
    let app = app_parser
        .parse_application(sql)
        .expect("Should parse application");
    assert_eq!(app.statements.len(), 3, "App should have 3 statements");
    assert_eq!(
        app.metadata.observability_metrics_enabled,
        Some(true),
        "App should have @observability.metrics.enabled: true"
    );

    // -- 4. For each statement, walk the deploy_job code path --
    let parser = StreamingSqlParser::new();
    let server_obs = server.observability().cloned();
    assert!(server_obs.is_some(), "Server should have observability");

    for (i, stmt) in app.statements.iter().enumerate() {
        // 4a. Inject observability (stream_job_server.rs lines 1885-1891)
        let mut merged_sql = stmt.sql.clone();
        if let Some(true) = app.metadata.observability_metrics_enabled {
            if !merged_sql.contains("'observability.metrics.enabled'")
                && !merged_sql.contains("@observability.metrics.enabled")
            {
                merged_sql.push_str("\n-- App-level observability injection");
                merged_sql.push_str("\n-- @observability.metrics.enabled: true");
            }
        }

        // 4b. Re-parse the statement (deploy_job line 544)
        let query = parser
            .parse(&merged_sql)
            .unwrap_or_else(|e| panic!("Statement {}: parse failed: {:?}", i, e));

        // 4c. Extract observability config from annotations (deploy_job lines 728-730)
        let annotation_config = ObservabilityConfigExtractor::extract_from_sql_string(&merged_sql)
            .unwrap_or_else(|e| panic!("Statement {}: config extraction failed: {:?}", i, e));

        assert!(
            annotation_config.enable_prometheus_metrics,
            "Statement {}: enable_prometheus_metrics should be true after injection",
            i
        );

        // 4d. Create processor with shared observability (create_processor_for_job)
        let job_name = match &query {
            velostream::velostream::sql::ast::StreamingQuery::CreateStream { name, .. } => {
                name.clone()
            }
            _ => format!("stmt_{}", i),
        };

        let wrapper = ObservabilityWrapper::with_observability(server_obs.clone());

        // 4e. Register all metrics (process_multi_job line 412)
        wrapper
            .metrics_helper()
            .register_counter_metrics(&query, &server_obs, &job_name)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Statement {} ({}): counter registration failed: {:?}",
                    i, job_name, e
                )
            });
        wrapper
            .metrics_helper()
            .register_gauge_metrics(&query, &server_obs, &job_name)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Statement {} ({}): gauge registration failed: {:?}",
                    i, job_name, e
                )
            });
        wrapper
            .metrics_helper()
            .register_histogram_metrics(&query, &server_obs, &job_name)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Statement {} ({}): histogram registration failed: {:?}",
                    i, job_name, e
                )
            });

        // 4f. Emit records (simulating batch processing — metrics need at least one observation)
        let records: Vec<Arc<StreamRecord>> = (0..3)
            .map(|j| {
                let mut fields = HashMap::new();
                fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
                fields.insert("price".to_string(), FieldValue::Float(150.0 + j as f64));
                fields.insert("avg_price".to_string(), FieldValue::Float(150.0));
                fields.insert("trade_count".to_string(), FieldValue::Integer(10));
                Arc::new(StreamRecord::new(fields))
            })
            .collect();

        wrapper
            .metrics_helper()
            .emit_counter_metrics(&query, &records, &server_obs, &job_name)
            .await;
        wrapper
            .metrics_helper()
            .emit_gauge_metrics(&query, &records, &server_obs, &job_name)
            .await;
        wrapper
            .metrics_helper()
            .emit_histogram_metrics(&query, &records, &server_obs, &job_name)
            .await;
    }

    // -- 5. Verify: get_performance_metrics() returns ALL 5 metrics --
    let metrics_text = server
        .get_performance_metrics()
        .expect("Server should return metrics");

    // Statement 1: market_data_ts (counter + gauge)
    assert!(
        metrics_text.contains("velo_market_data_ts_records_total"),
        "Statement 1 counter missing.\nMetrics:\n{}",
        metrics_text
    );
    assert!(
        metrics_text.contains("velo_market_data_current_price"),
        "Statement 1 gauge missing.\nMetrics:\n{}",
        metrics_text
    );

    // Statement 2: tick_buckets (counter + gauge)
    assert!(
        metrics_text.contains("velo_tick_buckets_records_total"),
        "Statement 2 counter missing.\nMetrics:\n{}",
        metrics_text
    );
    assert!(
        metrics_text.contains("velo_tick_buckets_avg_price"),
        "Statement 2 gauge missing.\nMetrics:\n{}",
        metrics_text
    );

    // Statement 3: enriched_market_data (counter)
    assert!(
        metrics_text.contains("velo_enriched_market_data_records_total"),
        "Statement 3 counter missing.\nMetrics:\n{}",
        metrics_text
    );
}
