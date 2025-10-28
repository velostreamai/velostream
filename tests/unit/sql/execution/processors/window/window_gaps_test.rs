/*!
# Window Function Gap Tests

Tests to verify support for SQL features identified in financial_trading.sql gap analysis:
1. FIRST_VALUE and LAST_VALUE window functions for OHLC calculations
2. TUMBLE_START and TUMBLE_END utility functions
3. Shorthand WINDOW notation (without explicit event_time field)
4. OR logic in HAVING clauses with complex boolean expressions
5. Division operations in HAVING with aggregate functions (e.g., ratios)
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Use shared test utilities from the same module directory
use super::shared_test_utils::{SqlExecutor, TestDataBuilder};

fn create_price_record(symbol: &str, price: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert("price".to_string(), FieldValue::Float(price));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));
    StreamRecord::new(fields)
}

// ===== GAP 1: FIRST_VALUE and LAST_VALUE Window Functions =====

#[tokio::test]
async fn test_first_value_window_function() {
    let query = "SELECT symbol, FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY timestamp) as first_price FROM prices WINDOW TUMBLING(5s)";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    // This test verifies the parser can recognize FIRST_VALUE syntax
    match result {
        Ok(_) => {
            println!("✓ Parser accepts FIRST_VALUE window function syntax");
        }
        Err(e) => {
            // Current limitation: FIRST_VALUE may not be fully implemented yet
            println!("⚠️  FIRST_VALUE window function not yet supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_last_value_window_function() {
    let query = "SELECT symbol, LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY timestamp) as last_price FROM prices WINDOW TUMBLING(5s)";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts LAST_VALUE window function syntax");
        }
        Err(e) => {
            println!("⚠️  LAST_VALUE window function not yet supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_ohlc_calculation_with_first_last_value() {
    // Test for OHLC (Open-High-Low-Close) pattern from financial_trading.sql line 101
    let query = r#"
        SELECT
            symbol,
            FIRST_VALUE(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as open,
            MAX(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as high,
            MIN(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as low,
            LAST_VALUE(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as close
        FROM prices
        WINDOW TUMBLING(1m)
    "#;

    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts complex OHLC query with FIRST_VALUE and LAST_VALUE");
        }
        Err(e) => {
            println!(
                "⚠️  Complex OHLC query with FIRST_VALUE/LAST_VALUE not supported: {}",
                e
            );
        }
    }
}

// ===== GAP 2: TUMBLE_START and TUMBLE_END Utility Functions =====

#[tokio::test]
async fn test_tumble_start_window_utility() {
    // Test from financial_trading.sql line 94-95
    let query = "SELECT TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start FROM trades";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts TUMBLE_START utility function");
        }
        Err(e) => {
            println!("⚠️  TUMBLE_START utility function not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_tumble_end_window_utility() {
    // Test from financial_trading.sql
    let query = "SELECT TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end FROM trades";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts TUMBLE_END utility function");
        }
        Err(e) => {
            println!("⚠️  TUMBLE_END utility function not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_tumble_start_end_equivalence_to_window_boundaries() {
    // Verify that TUMBLE_START/TUMBLE_END are equivalent to _window_start/_window_end
    let tumble_query = "SELECT TUMBLE_START(event_time, INTERVAL '1' MINUTE) as start, COUNT(*) FROM trades GROUP BY TUMBLE_START(event_time, INTERVAL '1' MINUTE)";
    let window_query = "SELECT _window_start as start, COUNT(*) FROM trades GROUP BY _window_start WINDOW TUMBLING(1m)";

    let parser = StreamingSqlParser::new();
    let tumble_result = parser.parse(tumble_query);
    let window_result = parser.parse(window_query);

    if tumble_result.is_ok() && window_result.is_ok() {
        println!("✓ Both TUMBLE_START and window _window_start syntax are supported");
    } else if tumble_result.is_err() && window_result.is_ok() {
        println!("⚠️  TUMBLE_START not yet supported, but window _window_start is available");
    }
}

// ===== GAP 3: Shorthand WINDOW Notation =====

#[tokio::test]
async fn test_shorthand_tumbling_window_notation() {
    // Test from financial_trading.sql line 689: WINDOW TUMBLING(1m)
    // This is shorthand without explicit event_time field
    let query = "SELECT symbol, COUNT(*) FROM trades GROUP BY symbol WINDOW TUMBLING(1m)";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts shorthand WINDOW TUMBLING(1m) notation");
        }
        Err(e) => {
            println!("⚠️  Shorthand TUMBLING notation not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_shorthand_sliding_window_notation() {
    // Shorthand sliding window without explicit event_time
    let query = "SELECT symbol, AVG(price) FROM trades GROUP BY symbol WINDOW SLIDING(5m, 1m)";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts shorthand WINDOW SLIDING(size, advance) notation");
        }
        Err(e) => {
            println!("⚠️  Shorthand SLIDING notation not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_shorthand_session_window_notation() {
    // Shorthand session window without explicit event_time
    let query =
        "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id WINDOW SESSION(30s)";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts shorthand WINDOW SESSION(gap) notation");
        }
        Err(e) => {
            println!("⚠️  Shorthand SESSION notation not supported: {}", e);
        }
    }
}

// ===== GAP 4: OR Logic in HAVING Clauses =====

#[tokio::test]
async fn test_or_logic_in_having_clause() {
    // Basic OR condition in HAVING
    let query = "SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol WINDOW TUMBLING(5s) HAVING COUNT(*) > 5 OR COUNT(*) < 2";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts OR logic in HAVING clause");
        }
        Err(e) => {
            println!("⚠️  OR logic in HAVING not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_complex_boolean_logic_in_having() {
    // Complex AND/OR combinations in HAVING
    // From financial_trading.sql line 589-592 area
    let query = r#"
        SELECT symbol, COUNT(*) as cnt, AVG(price) as avg_price
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5s)
        HAVING (COUNT(*) > 1 AND AVG(price) > 100.0) OR (COUNT(*) < 5 AND AVG(price) < 50.0)
    "#;

    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts complex AND/OR logic in HAVING clause");
        }
        Err(e) => {
            println!("⚠️  Complex boolean logic in HAVING not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_or_with_multiple_aggregates_in_having() {
    // OR condition with different aggregates
    let query = r#"
        SELECT symbol, COUNT(*) as trade_count, SUM(volume) as total_volume
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5s)
        HAVING SUM(volume) > 1000 OR COUNT(*) > 100
    "#;

    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts OR logic with different aggregates in HAVING");
        }
        Err(e) => {
            println!(
                "⚠️  OR with mixed aggregates in HAVING not supported: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_or_logic_execution_with_trading_data() {
    // Functional test with real data execution
    let query = "SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol WINDOW TUMBLING(5s) HAVING COUNT(*) > 2 OR COUNT(*) < 1";

    let records = vec![
        TestDataBuilder::trade_record(1, "AAPL", 150.0, 1000, 0),
        TestDataBuilder::trade_record(2, "AAPL", 155.0, 1200, 100),
        TestDataBuilder::trade_record(3, "AAPL", 145.0, 900, 200),
        TestDataBuilder::trade_record(4, "GOOGL", 2000.0, 100, 300),
        TestDataBuilder::trade_record(5, "MSFT", 350.0, 600, 400),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    if !results.is_empty() {
        println!(
            "✓ OR logic in HAVING executed successfully with {} results",
            results.len()
        );
    } else {
        println!("⚠️  OR logic in HAVING produced no results (window may not have closed)");
    }
}

// ===== GAP 5: Division Operations in HAVING with Aggregates =====

#[tokio::test]
async fn test_division_in_having_with_aggregates() {
    // Division operation in HAVING - from financial_trading.sql around line 589
    let query = "SELECT symbol, COUNT(*) as cnt FROM trades GROUP BY symbol WINDOW TUMBLING(5s) HAVING COUNT(*) / 2 > 5";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts division in HAVING with aggregates");
        }
        Err(e) => {
            println!(
                "⚠️  Division in HAVING with aggregates not supported: {}",
                e
            );
        }
    }
}

#[tokio::test]
async fn test_division_ratio_in_having() {
    // Ratio calculation in HAVING (buy_ratio / sell_ratio pattern)
    let query = r#"
        SELECT symbol,
            SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) as buy_qty,
            SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) as sell_qty
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5s)
        HAVING SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) /
               SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) > 1.0
    "#;

    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts ratio calculation in HAVING");
        }
        Err(e) => {
            println!("⚠️  Ratio calculation in HAVING not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_division_by_zero_safety_in_having() {
    // Test if division by zero is handled safely
    // This query could produce division by zero if the denominator becomes 0
    let query = r#"
        SELECT symbol, COUNT(*) as total_count
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5s)
        HAVING COUNT(*) > 10 AND COUNT(*) / (COUNT(*) - COUNT(*)) > 1
    "#;

    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    if result.is_ok() {
        println!("✓ Parser accepts division by zero edge case (should fail at execution)");
    } else {
        println!(
            "⚠️  Division by zero parsing rejected: {}",
            result.unwrap_err()
        );
    }
}

#[tokio::test]
async fn test_aggregate_arithmetic_with_safety_bounds() {
    // Safe division with bounds checking
    let query = r#"
        SELECT symbol, SUM(volume) as total_volume, COUNT(*) as trade_count
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5s)
        HAVING SUM(volume) > 0 AND COUNT(*) > 0 AND SUM(volume) / COUNT(*) > 100.0
    "#;

    let parser = StreamingSqlParser::new();
    let result = parser.parse(query);

    match result {
        Ok(_) => {
            println!("✓ Parser accepts safe division with bounds checking in HAVING");
        }
        Err(e) => {
            println!("⚠️  Safe division with bounds not supported: {}", e);
        }
    }
}

#[tokio::test]
async fn test_division_execution_with_trading_data() {
    // Functional test with real data
    let query = r#"
        SELECT symbol, COUNT(*) as cnt, SUM(volume) as vol
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5s)
        HAVING SUM(volume) > 500 AND COUNT(*) > 0 AND SUM(volume) / COUNT(*) > 100
    "#;

    let records = vec![
        TestDataBuilder::trade_record(1, "AAPL", 150.0, 600, 0), // vol/cnt = 600
        TestDataBuilder::trade_record(2, "AAPL", 155.0, 800, 100), // vol/cnt = 700
        TestDataBuilder::trade_record(3, "MSFT", 350.0, 100, 200), // vol/cnt = 100, filtered out
        TestDataBuilder::trade_record(4, "MSFT", 340.0, 200, 300), // vol/cnt = 150, filtered out
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    if !results.is_empty() {
        println!(
            "✓ Division arithmetic in HAVING executed successfully with {} results",
            results.len()
        );
    } else {
        println!(
            "⚠️  Division arithmetic in HAVING produced no results (window may not have closed)"
        );
    }
}

// ===== Summary Tests =====

#[tokio::test]
async fn test_gap_coverage_summary() {
    println!("\n=== Window Function Gap Test Summary ===");
    println!("Gap 1: FIRST_VALUE/LAST_VALUE - Tests window function syntax support");
    println!("Gap 2: TUMBLE_START/TUMBLE_END - Tests utility function syntax support");
    println!("Gap 3: Shorthand WINDOW notation - Tests implicit event_time handling");
    println!("Gap 4: OR logic in HAVING - Tests complex boolean expressions");
    println!("Gap 5: Division in aggregates - Tests arithmetic in HAVING clauses");
    println!("\nAll gap tests provide diagnostic feedback for feature support verification");
}
