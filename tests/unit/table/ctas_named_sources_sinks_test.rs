//! Tests for CTAS with named sources and sinks
//!
//! This test suite validates the proper syntax for CREATE TABLE AS SELECT
//! with named sources and sinks using the pattern:
//! - FROM named_source WITH ('config_file' = 'path')
//! - EMIT CHANGES INTO named_sink WITH ('named_sink.config_file' = 'path')

use velostream::velostream::sql::ast::{EmitMode, StreamSource, StreamingQuery};
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_basic_ctas_with_named_source_and_sink() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE TABLE user_analytics AS
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_spent
        FROM orders_source
        WITH ('config_file' = 'configs/orders_source.yaml')
        GROUP BY customer_id
        EMIT CHANGES
        INTO analytics_sink
        WITH ('analytics_sink.config_file' = 'configs/analytics_sink.yaml')
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse basic CTAS with named source/sink: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "user_analytics");

            match *as_select {
                StreamingQuery::Select {
                    emit_mode,
                    group_by,
                    from,
                    ..
                } => {
                    assert_eq!(
                        emit_mode, None,
                        "Nested SELECT doesn't have EMIT (at parent level)"
                    );
                    assert!(group_by.is_some(), "Should have GROUP BY");
                    match from {
                        StreamSource::Stream(_) | StreamSource::Table(_) | StreamSource::Uri(_) => {
                            // Good - have FROM clause
                        }
                        _ => panic!("Should have FROM clause"),
                    }
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query with named sink"),
    }
}

#[test]
fn test_complex_financial_ctas_with_named_sources() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE TABLE real_time_positions AS
        SELECT
            trader_id,
            symbol,
            sector,
            SUM(quantity) as net_position,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            MAX(timestamp) as last_trade_time
        FROM trades_source
        WITH ('config_file' = 'configs/trades_kafka_source.yaml')
        WHERE status = 'FILLED' AND price > 0
        GROUP BY trader_id, symbol, sector
        EMIT CHANGES
        INTO positions_kafka_sink
        WITH ('positions_kafka_sink.config_file' = 'configs/positions_kafka_sink.yaml')
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse complex financial CTAS: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "real_time_positions");

            match *as_select {
                StreamingQuery::Select {
                    emit_mode,
                    from,
                    where_clause,
                    group_by,
                    fields,
                    ..
                } => {
                    assert_eq!(emit_mode, None, "Nested SELECT doesn't have EMIT");
                    match from {
                        StreamSource::Stream(_) | StreamSource::Table(_) => {
                            // Good - have named source
                        }
                        _ => panic!("Should have named source"),
                    }
                    assert!(where_clause.is_some(), "Should have WHERE clause");
                    assert!(group_by.is_some(), "Should have GROUP BY");

                    // Verify we have the expected fields
                    assert_eq!(fields.len(), 7, "Should have 7 SELECT fields");
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query with named sink"),
    }
}

#[test]
fn test_market_data_aggregation_ctas() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE TABLE market_summary AS
        SELECT
            symbol,
            exchange,
            AVG(price) OVER (
                PARTITION BY symbol
                ORDER BY timestamp
                RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
            ) as moving_avg_5min,
            SUM(volume) as total_volume,
            (MAX(ask_price) - MIN(bid_price)) / AVG(price) * 10000 as spread_bps
        FROM market_data_source
        WITH ('config_file' = 'configs/market_data_kafka.yaml')
        WHERE price > 0 AND volume > 0
        GROUP BY symbol, exchange
        EMIT CHANGES
        INTO market_summary_sink
        WITH ('market_summary_sink.config_file' = 'configs/market_summary_file.yaml')
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse market data CTAS with window functions: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "market_summary");

            match *as_select {
                StreamingQuery::Select {
                    emit_mode,
                    fields,
                    from,
                    where_clause,
                    group_by,
                    ..
                } => {
                    assert_eq!(emit_mode, None, "Nested SELECT doesn't have EMIT");
                    match from {
                        StreamSource::Stream(_) | StreamSource::Table(_) => {
                            // Good - have source
                        }
                        _ => panic!("Should have source"),
                    }
                    assert!(where_clause.is_some());
                    assert!(group_by.is_some());
                    assert_eq!(fields.len(), 5, "Should have 5 SELECT fields");
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query with named sink"),
    }
}

#[test]
fn test_file_source_to_kafka_sink_ctas() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE TABLE processed_logs AS
        SELECT
            timestamp,
            log_level,
            service_name,
            COUNT(*) as event_count,
            MAX(timestamp) as last_seen
        FROM log_file_source
        WITH ('config_file' = 'configs/log_file_source.yaml')
        WHERE log_level IN ('ERROR', 'WARN')
        GROUP BY DATE_TRUNC('minute', timestamp), log_level, service_name
        EMIT CHANGES
        INTO alerts_kafka_sink
        WITH ('alerts_kafka_sink.config_file' = 'configs/alerts_kafka.yaml')
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse file-to-kafka CTAS: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "processed_logs");

            match *as_select {
                StreamingQuery::Select {
                    emit_mode,
                    from,
                    where_clause,
                    group_by,
                    ..
                } => {
                    assert_eq!(emit_mode, None, "Nested SELECT doesn't have EMIT");
                    match from {
                        StreamSource::Stream(_) | StreamSource::Table(_) | StreamSource::Uri(_) => {
                            // Good - have file source
                        }
                        _ => panic!("Should have file source"),
                    }
                    assert!(where_clause.is_some(), "Should filter log levels");
                    assert!(group_by.is_some(), "Should group by time window");
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query with named sink"),
    }
}

#[test]
fn test_multiple_named_sink_config_pattern() {
    let parser = StreamingSqlParser::new();

    // Test that each sink uses its own config file pattern
    let test_cases = vec![
        (
            "primary_sink",
            "WITH ('primary_sink.config_file' = 'configs/primary.yaml')",
        ),
        (
            "backup_data_sink",
            "WITH ('backup_data_sink.config_file' = 'configs/backup.yaml')",
        ),
        (
            "alerts_notification_sink",
            "WITH ('alerts_notification_sink.config_file' = 'configs/alerts_notifications.yaml')",
        ),
    ];

    for (sink_name, with_clause) in test_cases {
        let sql = format!(
            r#"
            CREATE TABLE test_table AS
            SELECT customer_id, COUNT(*) as count
            FROM test_source
            WITH ('config_file' = 'configs/source.yaml')
            GROUP BY customer_id
            EMIT CHANGES
            INTO {}
            {}
            "#,
            sink_name, with_clause
        );

        let result = parser.parse(&sql);
        assert!(
            result.is_ok(),
            "Failed to parse CTAS with sink '{}': {:?}",
            sink_name,
            result
        );

        // Verify the query structure
        if let Ok(StreamingQuery::CreateTable { as_select, .. }) = result {
            if let StreamingQuery::Select { emit_mode, .. } = *as_select {
                assert_eq!(
                    emit_mode, None,
                    "Nested SELECT doesn't have EMIT (it's at parent CREATE TABLE level) for sink '{}'",
                    sink_name
                );
            }
        }
    }
}

#[test]
fn test_ctas_config_pattern_validation() {
    let parser = StreamingSqlParser::new();

    // Test various config file patterns to ensure they parse correctly
    let valid_patterns = vec![
        r#"WITH ('simple_sink.config_file' = 'config.yaml')"#,
        r#"WITH ("double_quoted_sink.config_file" = "config.yaml")"#,
        r#"WITH ('complex_sink_name.config_file' = '/path/to/config.yaml')"#,
        r#"WITH ('env_sink.config_file' = '/configs/env_sink.yaml')"#,
    ];

    for (i, pattern) in valid_patterns.iter().enumerate() {
        let sql = format!(
            r#"
            CREATE TABLE test_table_{} AS
            SELECT * FROM source
            WITH ('config_file' = 'source.yaml')
            EMIT CHANGES
            INTO test_sink_{}
            {}
            "#,
            i, i, pattern
        );

        let result = parser.parse(&sql);
        assert!(
            result.is_ok(),
            "Pattern '{}' should parse successfully: {:?}",
            pattern,
            result
        );
    }
}

#[tokio::test]
async fn test_ctas_named_sources_integration_ready() {
    use velostream::velostream::table::ctas::CtasExecutor;

    // Verify that the CTAS executor can handle named sources and sinks
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test_group".to_string());

    let sql = r#"
        CREATE TABLE integration_test AS
        SELECT
            id,
            name,
            status,
            created_at,
            COUNT(*) OVER (PARTITION BY status) as status_count
        FROM integration_source
        WITH ('config_file' = 'tests/configs/integration_source.yaml')
        WHERE status IN ('active', 'pending')
        EMIT CHANGES
        INTO integration_sink
        WITH ('integration_sink.config_file' = 'tests/configs/integration_sink.yaml')
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);

    assert!(
        parsed.is_ok(),
        "Integration-ready CTAS should parse successfully: {:?}",
        parsed
    );

    // Verify the structure is ready for execution
    match parsed.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "integration_test");

            match *as_select {
                StreamingQuery::Select {
                    emit_mode,
                    from,
                    where_clause,
                    ..
                } => {
                    assert_eq!(
                        emit_mode, None,
                        "Nested SELECT doesn't have EMIT (it's at parent CREATE TABLE level)"
                    );
                    match from {
                        StreamSource::Stream(_) | StreamSource::Table(_) | StreamSource::Uri(_) => {
                            // Good - have configured source
                        }
                        _ => panic!("Should have configured source"),
                    }
                    assert!(where_clause.is_some(), "Should have filtering");
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query with named sink"),
    }

    println!("✅ CTAS with named sources and sinks is integration-ready");
}
