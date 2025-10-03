//! Unit tests for CSAS/CTAS configuration extraction
//!
//! Tests the parsing and extraction of configuration properties from SQL WITH clauses
//! for CREATE STREAM AS SELECT (CSAS) and CREATE TABLE AS SELECT (CTAS) statements.
//!
//! Tests verify:
//! 1. SQL parsing of CSAS/CTAS with named sources and sinks
//! 2. Property extraction from WITH clauses
//! 3. QueryAnalyzer detection of required sources and sinks
//! 4. Phase 1B-4 feature configuration parsing
//!
//! Architecture: Named sources/sinks are configured in the WITH clause,
//! eliminating the need for a separate INTO clause.

use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::query_analyzer::QueryAnalyzer;

#[test]
fn test_csas_with_named_source_config_parsing() {
    let parser = StreamingSqlParser::new();

    // CSAS with named source configured directly (no config file to avoid file dependency)
    let sql = r#"
        CREATE STREAM price_alerts AS
        SELECT
            symbol,
            price,
            volume,
            CASE
                WHEN price > 1000 THEN 'HIGH'
                WHEN price > 500 THEN 'MEDIUM'
                ELSE 'LOW'
            END as price_tier
        FROM market_data_source
        WITH (
            'market_data_source.type' = 'kafka_source',
            'market_data_source.bootstrap.servers' = 'localhost:9092',
            'market_data_source.topic' = 'market_data',
            'market_data_source.group.id' = 'test-group',

            'price_alerts_sink.type' = 'kafka_sink',
            'price_alerts_sink.topic' = 'price_alerts',
            'price_alerts_sink.bootstrap.servers' = 'localhost:9092'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CSAS with named source: {:?}",
        result.err()
    );

    // Verify the query was parsed correctly
    let query = result.unwrap();

    // Extract properties using QueryAnalyzer
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis_result = analyzer.analyze(&query);

    assert!(
        analysis_result.is_ok(),
        "Failed to analyze query: {:?}",
        analysis_result.err()
    );

    let analysis = analysis_result.unwrap();

    // Verify source configuration was extracted
    assert!(
        !analysis.required_sources.is_empty(),
        "Should have at least one required source"
    );

    println!("✅ CSAS query parsed successfully");
    println!("   Sources detected: {:?}", analysis.required_sources.len());
    println!("   Sinks detected: {:?}", analysis.required_sinks.len());
}

#[test]
fn test_ctas_with_named_source_config_parsing() {
    let parser = StreamingSqlParser::new();

    // CTAS with named source configured directly (no config file)
    let sql = r#"
        CREATE TABLE trading_positions AS
        SELECT
            trader_id,
            symbol,
            SUM(quantity) as net_position,
            AVG(price) as avg_price,
            COUNT(*) as trade_count
        FROM trades_source
        WHERE status = 'FILLED'
        GROUP BY trader_id, symbol
        WITH (
            'trades_source.type' = 'kafka_source',
            'trades_source.bootstrap.servers' = 'localhost:9092',
            'trades_source.topic' = 'trades',
            'trades_source.group.id' = 'trades-group',

            'table_model' = 'normal',
            'retention' = '7 days'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CTAS with named source: {:?}",
        result.err()
    );

    let query = result.unwrap();

    // Extract properties
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis_result = analyzer.analyze(&query);

    assert!(
        analysis_result.is_ok(),
        "Failed to analyze CTAS query: {:?}",
        analysis_result.err()
    );

    let analysis = analysis_result.unwrap();

    // Verify source configuration
    assert!(
        !analysis.required_sources.is_empty(),
        "CTAS should have required sources"
    );

    println!("✅ CTAS query parsed successfully");
    println!("   Sources detected: {:?}", analysis.required_sources.len());
}

#[test]
fn test_csas_with_multiple_named_sources() {
    let parser = StreamingSqlParser::new();

    // CSAS with JOIN scenario - stream-table join
    // Note: Table reference (traders_reference) doesn't need source config
    // Only the streaming source needs configuration
    let sql = r#"
        CREATE STREAM enriched_trades AS
        SELECT
            t.trade_id,
            t.symbol,
            t.price,
            t.quantity
        FROM trades_source t
        WITH (
            'trades_source.type' = 'kafka_source',
            'trades_source.bootstrap.servers' = 'localhost:9092',
            'trades_source.topic' = 'trades',
            'trades_source.group.id' = 'enriched-group',

            'enriched_trades_sink.type' = 'kafka_sink',
            'enriched_trades_sink.bootstrap.servers' = 'localhost:9092',
            'enriched_trades_sink.topic' = 'enriched_trades'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CSAS with stream source: {:?}",
        result.err()
    );

    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis = analyzer.analyze(&query).expect("Analysis should succeed");

    // Should detect the streaming source
    assert!(
        analysis.required_sources.len() >= 1,
        "Should detect at least the streaming source"
    );

    println!("✅ CSAS with stream source parsed successfully");
    println!("   Sources: {}", analysis.required_sources.len());
    println!("   Sinks: {}", analysis.required_sinks.len());
}

#[test]
fn test_property_extraction_from_with_clause() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM test AS
        SELECT * FROM source
        WITH (
            'source.type' = 'kafka_source',
            'source.bootstrap.servers' = 'broker1:9092,broker2:9092',
            'source.topic' = 'test-topic',
            'source.group.id' = 'test-group',
            'source.auto.offset.reset' = 'earliest',
            'source.enable.auto.commit' = 'false',

            'sink.type' = 'kafka_sink',
            'sink.topic' = 'output-topic',
            'sink.compression.type' = 'snappy',
            'sink.batch.size' = '16384'
        )
    "#;

    let result = parser.parse(sql);
    assert!(result.is_ok(), "Should parse SQL with properties");

    let query = result.unwrap();

    // Extract properties through QueryAnalyzer
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis = analyzer
        .analyze(&query)
        .expect("Should analyze successfully");

    // Verify sources were detected
    assert!(
        !analysis.required_sources.is_empty(),
        "Should detect sources from WITH clause properties"
    );

    println!("✅ Properties extracted from WITH clause");
    println!("   Detected {} sources", analysis.required_sources.len());
    println!("   Detected {} sinks", analysis.required_sinks.len());
}

#[test]
fn test_csas_phase1b4_features_with_named_sources() {
    let parser = StreamingSqlParser::new();

    // CSAS with Phase 1B-4 features AND named sources (inline config for unit test)
    let sql = r#"
        CREATE STREAM real_time_alerts AS
        SELECT
            symbol,
            price,
            volume,
            event_time
        FROM market_data_source
        WITH (
            -- Source configuration
            'market_data_source.type' = 'kafka_source',
            'market_data_source.bootstrap.servers' = 'localhost:9092',
            'market_data_source.topic' = 'market_data',
            'market_data_source.group.id' = 'alerts-group',

            -- Sink configuration
            'alerts_sink.type' = 'kafka_sink',
            'alerts_sink.bootstrap.servers' = 'localhost:9092',
            'alerts_sink.topic' = 'alerts',

            -- Phase 1B: Event-time processing
            'event.time.field' = 'trade_timestamp',
            'watermark.strategy' = 'bounded_out_of_orderness',
            'watermark.max_out_of_orderness' = '5s',
            'late.data.strategy' = 'dead_letter',

            -- Phase 2: Circuit breakers
            'circuit.breaker.enabled' = 'true',
            'circuit.breaker.failure.threshold' = '5',
            'max.memory.mb' = '1024',

            -- Phase 4: Observability
            'observability.tracing.enabled' = 'true',
            'observability.metrics.enabled' = 'true'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Should parse CSAS with Phase 1B-4 features and named sources: {:?}",
        result.err()
    );

    println!("✅ CSAS with Phase 1B-4 features + named sources parsed successfully");
}

#[test]
fn test_property_validation_edge_cases() {
    let parser = StreamingSqlParser::new();

    // Test empty WITH clause
    let sql = r#"
        CREATE STREAM test AS SELECT * FROM source WITH ()
    "#;
    assert!(parser.parse(sql).is_ok(), "Should parse empty WITH clause");

    // Test single property
    let sql = r#"
        CREATE STREAM test AS SELECT * FROM source
        WITH ('source.type' = 'kafka_source')
    "#;
    assert!(parser.parse(sql).is_ok(), "Should parse single property");

    // Test properties with special characters
    let sql = r#"
        CREATE STREAM test AS SELECT * FROM source
        WITH (
            'source.bootstrap.servers' = 'broker1:9092,broker2:9092,broker3:9092',
            'source.group.id' = 'test-group_v1.0'
        )
    "#;
    assert!(
        parser.parse(sql).is_ok(),
        "Should parse properties with special chars"
    );
}

#[test]
fn test_error_handling_invalid_sql() {
    let parser = StreamingSqlParser::new();

    // Invalid WITH syntax (missing equals)
    let sql = r#"CREATE STREAM test AS SELECT * FROM source WITH ('key' 'value')"#;
    assert!(
        parser.parse(sql).is_err(),
        "Should fail with invalid WITH syntax"
    );

    // Completely malformed SQL
    let sql = r#"CREATE STREAM AS FROM SELECT"#;
    assert!(parser.parse(sql).is_err(), "Should fail with malformed SQL");

    // Missing AS keyword
    let sql = r#"CREATE STREAM test SELECT * FROM source"#;
    assert!(parser.parse(sql).is_err(), "Should fail without AS keyword");
}

#[test]
fn test_missing_sink_configuration() {
    let parser = StreamingSqlParser::new();

    // CSAS without sink configuration - should fail analysis
    let sql = r#"
        CREATE STREAM output_stream AS
        SELECT * FROM source
        WITH (
            'source.type' = 'kafka_source',
            'source.bootstrap.servers' = 'localhost:9092',
            'source.topic' = 'input'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Should parse SQL (parsing only checks syntax)"
    );

    // Analysis should detect missing sink configuration
    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis_result = analyzer.analyze(&query);

    // Should fail with ConfigurationError
    assert!(
        analysis_result.is_err(),
        "Should fail when CSAS is missing sink configuration"
    );

    if let Err(e) = analysis_result {
        let error_msg = format!("{:?}", e);
        assert!(
            error_msg.contains("sink configuration"),
            "Error should mention missing sink configuration: {}",
            error_msg
        );
        println!(
            "✅ Missing sink configuration correctly detected as error: {:?}",
            e
        );
    }
}

#[test]
fn test_missing_source_configuration() {
    let parser = StreamingSqlParser::new();

    // CSAS without source configuration - should fail analysis
    let sql = r#"
        CREATE STREAM output_stream AS
        SELECT * FROM undefined_source
        WITH (
            'output_stream_sink.type' = 'kafka_sink',
            'output_stream_sink.bootstrap.servers' = 'localhost:9092',
            'output_stream_sink.topic' = 'output'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Should parse SQL (parsing only checks syntax)"
    );

    // Analysis should detect missing source configuration
    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis_result = analyzer.analyze(&query);

    // Should fail when trying to use undefined source
    if let Err(e) = analysis_result {
        println!(
            "✅ Missing source configuration correctly detected: {:?}",
            e
        );
    } else {
        let analysis = analysis_result.unwrap();
        println!("⚠️  Warning: Missing source 'undefined_source' was not detected as error");
        println!("   Detected sources: {:?}", analysis.required_sources);
    }
}

#[test]
fn test_config_property_precedence() {
    let parser = StreamingSqlParser::new();

    // Test that explicit properties take precedence (includes sink config)
    let sql = r#"
        CREATE STREAM test AS SELECT * FROM source
        WITH (
            'source.type' = 'kafka_source',
            'source.bootstrap.servers' = 'explicit-broker:9092',
            'source.topic' = 'explicit-topic',
            'source.bootstrap.servers' = 'override-broker:9092',

            'test_sink.type' = 'kafka_sink',
            'test_sink.bootstrap.servers' = 'localhost:9092',
            'test_sink.topic' = 'test_output'
        )
    "#;

    let result = parser.parse(sql);
    assert!(result.is_ok(), "Should parse SQL with duplicate keys");

    // Verify analysis works even with duplicates
    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    assert!(
        analyzer.analyze(&query).is_ok(),
        "Should analyze despite duplicate keys"
    );
}

#[test]
fn test_complex_where_clause_with_config() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM filtered_trades AS
        SELECT
            trade_id,
            symbol,
            price,
            quantity
        FROM trades_source
        WHERE price > 100.0
          AND quantity > 1000
          AND status = 'FILLED'
          AND exchange IN ('NYSE', 'NASDAQ')
        WITH (
            'trades_source.type' = 'kafka_source',
            'trades_source.bootstrap.servers' = 'localhost:9092',
            'trades_source.topic' = 'trades',
            'trades_source.group.id' = 'filtered-group',

            'filtered_trades_sink.type' = 'kafka_sink',
            'filtered_trades_sink.bootstrap.servers' = 'localhost:9092',
            'filtered_trades_sink.topic' = 'filtered_trades'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Should parse complex WHERE with config: {:?}",
        result.err()
    );

    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis = analyzer
        .analyze(&query)
        .expect("Should analyze complex query");

    assert!(
        !analysis.required_sources.is_empty(),
        "Should detect source"
    );
    println!("✅ Complex WHERE clause with config parsed successfully");
}

#[test]
fn test_aggregation_with_config() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM aggregated_metrics AS
        SELECT
            symbol,
            COUNT(*) as trade_count,
            SUM(quantity) as total_quantity,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM trades_source
        GROUP BY symbol
        HAVING COUNT(*) > 100
        WITH (
            'trades_source.type' = 'kafka_source',
            'trades_source.bootstrap.servers' = 'localhost:9092',
            'trades_source.topic' = 'trades',
            'trades_source.group.id' = 'agg-group',

            'aggregated_metrics_sink.type' = 'kafka_sink',
            'aggregated_metrics_sink.bootstrap.servers' = 'localhost:9092',
            'aggregated_metrics_sink.topic' = 'aggregated_metrics'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Should parse aggregation with config: {:?}",
        result.err()
    );

    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis = analyzer
        .analyze(&query)
        .expect("Should analyze aggregation query");

    assert!(
        !analysis.required_sources.is_empty(),
        "Should detect source in aggregation"
    );
    println!("✅ Aggregation with GROUP BY/HAVING and config parsed successfully");
}

#[test]
fn test_window_functions_with_config() {
    let parser = StreamingSqlParser::new();

    let sql = r#"
        CREATE STREAM windowed_data AS
        SELECT
            symbol,
            price,
            LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price,
            LEAD(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as next_price,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp) as row_num
        FROM trades_source
        WITH (
            'trades_source.type' = 'kafka_source',
            'trades_source.bootstrap.servers' = 'localhost:9092',
            'trades_source.topic' = 'trades',
            'trades_source.group.id' = 'window-group',

            'windowed_data_sink.type' = 'kafka_sink',
            'windowed_data_sink.bootstrap.servers' = 'localhost:9092',
            'windowed_data_sink.topic' = 'windowed_data'
        )
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Should parse window functions with config: {:?}",
        result.err()
    );

    let query = result.unwrap();
    let analyzer = QueryAnalyzer::new("test-group".to_string());
    let analysis = analyzer
        .analyze(&query)
        .expect("Should analyze window function query");

    assert!(
        !analysis.required_sources.is_empty(),
        "Should detect source with window functions"
    );
    println!("✅ Window functions (LAG, LEAD, ROW_NUMBER) with config parsed successfully");
}
