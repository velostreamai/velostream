use velostream::velostream::sql::ast::{EmitMode, StreamSource, StreamingQuery};
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_create_table_with_emit_changes_parsing() {
    let parser = StreamingSqlParser::new();

    // Test CREATE TABLE AS SELECT with EMIT CHANGES
    let sql = r#"
        CREATE TABLE real_time_orders AS
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_spent
        FROM orders_stream
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CREATE TABLE with EMIT CHANGES: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name,
            as_select,
            emit_mode,
            ..
        } => {
            assert_eq!(name, "real_time_orders");

            // CREATE TABLE doesn't parse EMIT CHANGES at top level currently
            // It's parsed in the nested SELECT query
            assert_eq!(
                emit_mode, None,
                "CREATE TABLE doesn't have EMIT CHANGES at top level"
            );

            // Verify the nested SELECT has EMIT CHANGES
            match *as_select {
                StreamingQuery::Select {
                    emit_mode: nested_emit,
                    ..
                } => {
                    assert_eq!(
                        nested_emit,
                        Some(EmitMode::Changes),
                        "Nested SELECT should also have EMIT CHANGES"
                    );
                }
                _ => panic!("Expected nested SELECT query"),
            }
        }
        other => panic!("Expected CreateTable query, got: {:?}", other),
    }
}

#[test]
fn test_create_table_into_with_emit_changes() {
    let parser = StreamingSqlParser::new();

    // Test simpler CREATE TABLE AS SELECT with EMIT CHANGES
    let sql = r#"
        CREATE TABLE analytics_summary AS
        SELECT
            category,
            COUNT(*) as event_count,
            AVG(value) as avg_value
        FROM events_stream
        GROUP BY category
        EMIT CHANGES
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CREATE TABLE with EMIT CHANGES: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "analytics_summary");

            // Verify nested SELECT has EMIT CHANGES
            match *as_select {
                StreamingQuery::Select {
                    emit_mode: nested_emit,
                    ..
                } => {
                    assert_eq!(
                        nested_emit,
                        Some(EmitMode::Changes),
                        "Nested SELECT should have EMIT CHANGES"
                    );
                }
                _ => panic!("Expected nested SELECT"),
            }
        }
        other => panic!("Expected CreateTable query, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_ctas_executor_with_emit_changes() {
    use velostream::velostream::table::ctas::CtasExecutor;

    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test that CTAS executor preserves EMIT CHANGES with named sources/sinks
    let sql = r#"
        CREATE TABLE positions AS
        SELECT
            trader_id,
            symbol,
            SUM(quantity) as net_position,
            COUNT(*) as trade_count
        FROM trades_source
        WITH ('config_file' = 'configs/trades_source.yaml')
        GROUP BY trader_id, symbol
        EMIT CHANGES
        INTO positions_sink
        WITH ('positions_sink.config_file' = 'configs/positions_sink.yaml')
    "#;

    // Parse to verify structure
    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);

    assert!(
        parsed.is_ok(),
        "Failed to parse CTAS with EMIT CHANGES and named sources/sinks"
    );

    match parsed.unwrap() {
        StreamingQuery::CreateTable { as_select, .. } => {
            // Check nested SELECT for EMIT CHANGES
            match *as_select {
                StreamingQuery::Select { emit_mode, .. } => {
                    assert_eq!(
                        emit_mode,
                        Some(EmitMode::Changes),
                        "SELECT in CTAS should have EMIT CHANGES"
                    );
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query"),
    }

    // Note: Actual execution would require Kafka setup
    // This test verifies the SQL is valid and EMIT CHANGES is preserved with named sources/sinks
}

#[test]
fn test_create_table_emit_final_with_window() {
    let parser = StreamingSqlParser::new();

    // Test CREATE TABLE with EMIT FINAL (requires WINDOW)
    let sql = r#"
        CREATE TABLE hourly_summary AS
        SELECT
            product_id,
            COUNT(*) as sales_count,
            SUM(amount) as revenue
        FROM sales_stream
        GROUP BY product_id
        WINDOW TUMBLING(1h)
        EMIT FINAL
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CREATE TABLE with EMIT FINAL"
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name,
            emit_mode,
            as_select,
            ..
        } => {
            assert_eq!(name, "hourly_summary");
            assert_eq!(
                emit_mode, None,
                "CREATE TABLE doesn't have EMIT at top level"
            );

            // Verify window exists in nested SELECT
            match *as_select {
                StreamingQuery::Select {
                    window,
                    emit_mode: nested_emit,
                    ..
                } => {
                    assert!(window.is_some(), "EMIT FINAL requires WINDOW clause");
                    assert_eq!(
                        nested_emit,
                        Some(EmitMode::Final),
                        "Nested SELECT should have EMIT FINAL"
                    );
                }
                _ => panic!("Expected nested SELECT"),
            }
        }
        _ => panic!("Expected CreateTable query"),
    }
}

#[test]
fn test_create_table_default_emit_mode() {
    let parser = StreamingSqlParser::new();

    // Test CREATE TABLE without explicit EMIT mode
    let sql = r#"
        CREATE TABLE customer_stats AS
        SELECT
            customer_id,
            COUNT(*) as order_count
        FROM orders
        GROUP BY customer_id
    "#;

    let result = parser.parse(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        StreamingQuery::CreateTable {
            emit_mode,
            as_select,
            ..
        } => {
            // When no EMIT mode specified, it should be None at CREATE TABLE level
            assert_eq!(
                emit_mode, None,
                "CREATE TABLE without EMIT clause should have None"
            );

            // The nested SELECT with GROUP BY defaults to EMIT CHANGES behavior
            match *as_select {
                StreamingQuery::Select {
                    group_by,
                    emit_mode: nested_emit,
                    ..
                } => {
                    assert!(group_by.is_some());
                    // Parser doesn't set default - execution engine does
                    assert_eq!(nested_emit, None);
                }
                _ => panic!("Expected nested SELECT"),
            }
        }
        _ => panic!("Expected CreateTable query"),
    }
}

#[test]
fn test_create_stream_with_emit_changes() {
    let parser = StreamingSqlParser::new();

    // Test CREATE STREAM AS SELECT with EMIT CHANGES
    let sql = r#"
        CREATE STREAM high_value_orders AS
        SELECT
            order_id,
            customer_id,
            amount
        FROM orders
        WHERE amount > 1000
        EMIT CHANGES
    "#;

    let result = parser.parse(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        StreamingQuery::CreateStream {
            name, emit_mode, ..
        } => {
            assert_eq!(name, "high_value_orders");
            assert_eq!(
                emit_mode, None,
                "CREATE STREAM doesn't have EMIT at top level"
            );
        }
        _ => panic!("Expected CreateStream query"),
    }
}

#[test]
fn test_ctas_with_named_sources_and_sinks() {
    let parser = StreamingSqlParser::new();

    // Test CREATE TABLE with named sources and sinks
    let sql = r#"
        CREATE TABLE market_analytics AS
        SELECT
            symbol,
            AVG(price) as avg_price,
            SUM(volume) as total_volume,
            COUNT(*) as trade_count
        FROM market_data_source
        WITH ('config_file' = 'configs/market_data.yaml')
        WHERE price > 0 AND volume > 0
        GROUP BY symbol
        EMIT CHANGES
        INTO analytics_sink
        WITH ('analytics_sink.config_file' = 'configs/analytics_sink.yaml')
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse CTAS with named sources and sinks: {:?}",
        result
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name, as_select, ..
        } => {
            assert_eq!(name, "market_analytics");

            // Verify the SELECT uses named source and has EMIT CHANGES
            match *as_select {
                StreamingQuery::Select {
                    from,
                    emit_mode,
                    where_clause,
                    group_by,
                    ..
                } => {
                    // Should use named source
                    match from {
                        StreamSource::Stream(_) | StreamSource::Table(_) => {
                            // Good - using named source
                        }
                        _ => panic!("Should have FROM clause with named source"),
                    }

                    // Should have EMIT CHANGES
                    assert_eq!(
                        emit_mode,
                        Some(EmitMode::Changes),
                        "Should have EMIT CHANGES for real-time analytics"
                    );

                    // Should have WHERE and GROUP BY
                    assert!(where_clause.is_some(), "Should have WHERE clause");
                    assert!(group_by.is_some(), "Should have GROUP BY for aggregation");
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query"),
    }
}

#[test]
fn test_ctas_emit_changes_cdc_semantics() {
    let parser = StreamingSqlParser::new();

    // Test that CREATE TABLE with EMIT CHANGES represents CDC semantics
    let sql = r#"
        CREATE TABLE real_time_positions AS
        SELECT
            trader_id,
            symbol,
            SUM(quantity) as net_position,
            COUNT(*) as trade_count
        FROM trades
        WHERE status = 'FILLED'
        GROUP BY trader_id, symbol
        EMIT CHANGES
    "#;

    let result = parser.parse(sql);
    assert!(
        result.is_ok(),
        "Failed to parse complex CTAS with EMIT CHANGES"
    );

    match result.unwrap() {
        StreamingQuery::CreateTable {
            name,
            emit_mode,
            as_select,
            ..
        } => {
            assert_eq!(name, "real_time_positions");
            assert_eq!(
                emit_mode, None,
                "CREATE TABLE doesn't have EMIT at top level"
            );

            // This represents a CDC table that updates in real-time
            // Each trade immediately updates the position
            match *as_select {
                StreamingQuery::Select {
                    group_by,
                    emit_mode: nested_emit,
                    where_clause,
                    ..
                } => {
                    assert!(group_by.is_some(), "Should have GROUP BY");
                    assert!(where_clause.is_some(), "Should have WHERE clause");
                    assert_eq!(
                        nested_emit,
                        Some(EmitMode::Changes),
                        "Should emit changes immediately for real-time position tracking"
                    );
                }
                _ => panic!("Expected SELECT query"),
            }
        }
        _ => panic!("Expected CreateTable query"),
    }
}
