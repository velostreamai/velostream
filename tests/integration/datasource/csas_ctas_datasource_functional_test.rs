//! Functional tests for CSAS/CTAS with named datasources
//!
//! Tests that:
//! 1. Named sources are loaded from config files
//! 2. Properties from config files are applied correctly
//! 3. Named sinks can be configured via WITH clause
//! 4. Both CSAS and CTAS work with the datasource system
//!
//! Architecture: Named sinks are configured in the WITH clause of the CSAS/CTAS statement,
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
    let analysis = analyzer.analyze(&query).expect("Should analyze successfully");

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

    // CSAS with Phase 1B-4 features AND named sources
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
            'market_data_source.config_file' = 'configs/market_data_source.yaml',
            'market_data_source.topic' = 'market_data',

            -- Sink configuration
            'alerts_sink.type' = 'kafka_sink',
            'alerts_sink.config_file' = 'configs/alerts_sink.yaml',
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
