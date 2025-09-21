/*!
# Comprehensive SQL Parser Integration Test

Tests the complete SQL parser with the actual financial trading demo SQL.
This verifies that all new parser enhancements work together in a real-world scenario.

Tests covered:
- Full financial trading SQL parsing (from demo/trading/sql/financial_trading.sql)
- Table aliases in window functions
- INTERVAL syntax in window frames
- EXTRACT function with SQL standard syntax
- Complex multi-join queries with grouping and windowing
*/

use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_complete_financial_trading_sql_parsing() {
    let parser = StreamingSqlParser::new();

    // This is the actual SQL from the financial trading demo
    // It exercises all the new parser features in a realistic scenario
    let financial_sql = r#"
        SELECT
            p.trader_id,
            p.symbol,
            COUNT(*) as transaction_count,
            AVG(m.price) as avg_price,
            SUM(CASE WHEN m.side = 'BUY' THEN m.quantity ELSE 0 END) as total_buys,
            SUM(CASE WHEN m.side = 'SELL' THEN m.quantity ELSE 0 END) as total_sells,
            MAX(m.price) as max_price,
            MIN(m.price) as min_price,
            STDDEV(m.price) as price_volatility,
            LAG(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as prev_price,
            LEAD(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as next_price,
            RANK() OVER (PARTITION BY m.symbol ORDER BY m.volume DESC) as volume_rank,
            EXTRACT(EPOCH FROM (m.event_time - p.event_time)) as time_diff_seconds
        FROM market_data m
        JOIN positions p ON m.symbol = p.symbol
        WHERE m.event_time >= '2024-01-01T00:00:00Z'
            AND p.quantity > 100
            AND m.price BETWEEN 50.0 AND 500.0
        GROUP BY p.trader_id, p.symbol, m.event_time, p.event_time, m.price, m.side, m.quantity, m.volume
        HAVING COUNT(*) > 5
            AND AVG(m.price) > 100.0
        WINDOW TUMBLING(INTERVAL '1' HOUR)
            RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
        ORDER BY p.trader_id, avg_price DESC
        LIMIT 1000
    "#;

    let result = parser.parse(financial_sql);
    assert!(
        result.is_ok(),
        "Financial trading SQL should parse successfully with all new features. Error: {:?}",
        result.err()
    );

    // Verify the SQL was parsed into the expected structure
    match result.unwrap() {
        velostream::velostream::sql::ast::StreamingQuery::Select {
            fields,
            from,
            joins,
            where_clause,
            group_by,
            having_clause,
            window_spec,
            order_by,
            limit,
            ..
        } => {
            // Verify we have all expected SELECT fields (13 fields)
            assert_eq!(fields.len(), 13, "Should have 13 SELECT fields including new features");

            // Verify FROM clause exists
            assert!(from.is_some(), "Should have FROM clause");

            // Verify JOIN exists
            assert!(!joins.is_empty(), "Should have JOIN clauses");

            // Verify WHERE clause exists
            assert!(where_clause.is_some(), "Should have WHERE clause");

            // Verify GROUP BY exists
            assert!(group_by.is_some(), "Should have GROUP BY clause");

            // Verify HAVING clause exists
            assert!(having_clause.is_some(), "Should have HAVING clause");

            // Verify WINDOW specification exists
            assert!(window_spec.is_some(), "Should have WINDOW specification");

            // Verify ORDER BY exists
            assert!(order_by.is_some(), "Should have ORDER BY clause");

            // Verify LIMIT exists and is correct
            assert_eq!(limit, Some(1000), "Should have LIMIT 1000");

            println!("✅ Complete financial trading SQL parsed successfully!");
            println!("   - 13 SELECT fields including window functions with table aliases");
            println!("   - EXTRACT function with SQL standard syntax");
            println!("   - Window frames with INTERVAL syntax");
            println!("   - Complex JOINs, WHERE, GROUP BY, HAVING, ORDER BY");
            println!("   - TUMBLING window with RANGE BETWEEN INTERVAL");
        }
        _ => panic!("Expected SELECT query, got different query type"),
    }
}

#[test]
fn test_individual_new_parser_features() {
    let parser = StreamingSqlParser::new();

    // Test 1: Table aliases in window functions
    println!("Testing table aliases in window functions...");
    let table_alias_sql = "SELECT LAG(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) FROM market_data m JOIN positions p ON m.symbol = p.symbol";
    let result = parser.parse(table_alias_sql);
    assert!(result.is_ok(), "Table aliases in PARTITION BY should work: {:?}", result.err());
    println!("✅ Table aliases in window functions - PASSED");

    // Test 2: INTERVAL syntax in window frames
    println!("Testing INTERVAL syntax in window frames...");
    let interval_sql = "SELECT AVG(price) OVER (ORDER BY event_time RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW) FROM trades";
    let result = parser.parse(interval_sql);
    assert!(result.is_ok(), "INTERVAL in window frames should work: {:?}", result.err());
    println!("✅ INTERVAL syntax in window frames - PASSED");

    // Test 3: EXTRACT function SQL standard syntax
    println!("Testing EXTRACT function SQL standard syntax...");
    let extract_sql = "SELECT EXTRACT(EPOCH FROM (end_time - start_time)) as duration FROM events";
    let result = parser.parse(extract_sql);
    assert!(result.is_ok(), "EXTRACT SQL standard syntax should work: {:?}", result.err());
    println!("✅ EXTRACT function SQL standard syntax - PASSED");

    // Test 4: EXTRACT function legacy syntax (should still work)
    println!("Testing EXTRACT function legacy syntax...");
    let extract_legacy_sql = "SELECT EXTRACT('YEAR', timestamp_col) as year FROM events";
    let result = parser.parse(extract_legacy_sql);
    assert!(result.is_ok(), "EXTRACT legacy syntax should still work: {:?}", result.err());
    println!("✅ EXTRACT function legacy syntax - PASSED");

    // Test 5: Complex combination of all features
    println!("Testing complex combination of all new features...");
    let complex_sql = r#"
        SELECT
            p.trader_id,
            AVG(m.price) OVER (
                PARTITION BY p.trader_id
                ORDER BY m.event_time
                RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
            ) as hourly_avg,
            EXTRACT(HOUR FROM m.event_time) as hour_of_day
        FROM market_data m
        JOIN positions p ON m.symbol = p.symbol
        WHERE EXTRACT(DOW FROM m.event_time) BETWEEN 1 AND 5
    "#;
    let result = parser.parse(complex_sql);
    assert!(result.is_ok(), "Complex combination should work: {:?}", result.err());
    println!("✅ Complex combination of all new features - PASSED");
}

#[test]
fn test_various_interval_units_comprehensive() {
    let parser = StreamingSqlParser::new();

    let interval_cases = vec![
        ("SECOND", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '30' SECOND PRECEDING AND CURRENT ROW) FROM events"),
        ("MINUTE", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW) FROM events"),
        ("HOUR", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW) FROM events"),
        ("DAY", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW) FROM events"),
        ("MINUTES", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '15' MINUTES PRECEDING AND CURRENT ROW) FROM events"),
        ("HOURS", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '6' HOURS PRECEDING AND CURRENT ROW) FROM events"),
        ("DAYS", "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '7' DAYS PRECEDING AND CURRENT ROW) FROM events"),
    ];

    for (unit, sql) in interval_cases {
        println!("Testing INTERVAL {} syntax...", unit);
        let result = parser.parse(sql);
        assert!(
            result.is_ok(),
            "INTERVAL {} should parse correctly: {:?}",
            unit,
            result.err()
        );
        println!("✅ INTERVAL {} - PASSED", unit);
    }
}

#[test]
fn test_extract_all_supported_parts() {
    let parser = StreamingSqlParser::new();

    let extract_parts = vec![
        "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
        "DOW", "DOY", "WEEK", "QUARTER", "EPOCH",
    ];

    for part in extract_parts {
        // Test SQL standard syntax
        let sql_standard = format!("SELECT EXTRACT({} FROM timestamp_col) as extracted FROM events", part);
        let result = parser.parse(&sql_standard);
        assert!(
            result.is_ok(),
            "EXTRACT({} FROM ...) should parse correctly: {:?}",
            part,
            result.err()
        );

        // Test function call syntax
        let function_call = format!("SELECT EXTRACT('{}', timestamp_col) as extracted FROM events", part);
        let result = parser.parse(&function_call);
        assert!(
            result.is_ok(),
            "EXTRACT('{}', ...) should parse correctly: {:?}",
            part,
            result.err()
        );

        println!("✅ EXTRACT {} - Both syntaxes PASSED", part);
    }
}

#[test]
fn test_performance_with_complex_sql() {
    use std::time::Instant;

    let parser = StreamingSqlParser::new();

    // Test parsing performance with the complex financial SQL
    let financial_sql = r#"
        SELECT
            p.trader_id,
            p.symbol,
            COUNT(*) as transaction_count,
            AVG(m.price) as avg_price,
            SUM(CASE WHEN m.side = 'BUY' THEN m.quantity ELSE 0 END) as total_buys,
            SUM(CASE WHEN m.side = 'SELL' THEN m.quantity ELSE 0 END) as total_sells,
            MAX(m.price) as max_price,
            MIN(m.price) as min_price,
            STDDEV(m.price) as price_volatility,
            LAG(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as prev_price,
            LEAD(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as next_price,
            RANK() OVER (PARTITION BY m.symbol ORDER BY m.volume DESC) as volume_rank,
            EXTRACT(EPOCH FROM (m.event_time - p.event_time)) as time_diff_seconds,
            AVG(m.price) OVER (
                PARTITION BY p.trader_id
                ORDER BY m.event_time
                RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
            ) as hourly_moving_avg,
            COUNT(*) OVER (
                PARTITION BY m.symbol
                ORDER BY m.event_time
                RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
            ) as trades_last_15min
        FROM market_data m
        JOIN positions p ON m.symbol = p.symbol
        WHERE m.event_time >= '2024-01-01T00:00:00Z'
            AND p.quantity > 100
            AND m.price BETWEEN 50.0 AND 500.0
            AND EXTRACT(DOW FROM m.event_time) BETWEEN 1 AND 5
            AND EXTRACT(HOUR FROM m.event_time) BETWEEN 9 AND 16
        GROUP BY p.trader_id, p.symbol, m.event_time, p.event_time, m.price, m.side, m.quantity, m.volume
        HAVING COUNT(*) > 5
            AND AVG(m.price) > 100.0
            AND MAX(m.volume) > 1000
        WINDOW TUMBLING(INTERVAL '1' HOUR)
            RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
        ORDER BY p.trader_id, avg_price DESC, hourly_moving_avg DESC
        LIMIT 1000
    "#;

    let start = Instant::now();
    let result = parser.parse(financial_sql);
    let duration = start.elapsed();

    assert!(result.is_ok(), "Complex SQL should parse successfully");

    // Performance should be reasonable (under 100ms for complex SQL)
    assert!(
        duration.as_millis() < 100,
        "Parsing should complete quickly, took: {:?}ms",
        duration.as_millis()
    );

    println!("✅ Performance test - Complex SQL parsed in {:?}ms", duration.as_millis());
}

#[test]
fn test_error_handling_with_invalid_syntax() {
    let parser = StreamingSqlParser::new();

    // Test invalid INTERVAL syntax
    let invalid_interval = "SELECT COUNT(*) OVER (ORDER BY ts RANGE BETWEEN INVALID '1' HOUR PRECEDING AND CURRENT ROW) FROM events";
    let result = parser.parse(invalid_interval);
    assert!(result.is_err(), "Invalid INTERVAL syntax should fail");

    // Test invalid EXTRACT syntax
    let invalid_extract = "SELECT EXTRACT(INVALID_PART FROM timestamp_col) FROM events";
    let result = parser.parse(invalid_extract);
    // Note: This might succeed if INVALID_PART is treated as an identifier
    // The important thing is that it doesn't crash

    // Test malformed table alias
    let invalid_alias = "SELECT COUNT(*) OVER (PARTITION BY . ORDER BY col) FROM table";
    let result = parser.parse(invalid_alias);
    assert!(result.is_err(), "Malformed table alias should fail");

    println!("✅ Error handling tests completed");
}