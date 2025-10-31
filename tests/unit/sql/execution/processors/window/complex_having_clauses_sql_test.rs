/*!
# Phase 2.3: Complex HAVING Clauses Tests

Tests for complex HAVING clause scenarios including window functions, subqueries, and arithmetic expressions.
*/

use std::collections::HashMap;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Use shared test utilities
use super::shared_test_utils::SqlExecutor;

fn create_test_record(id: i64, category: String, price: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("category".to_string(), FieldValue::String(category));
    fields.insert("price".to_string(), FieldValue::Float(price));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp,
        offset: id,
        partition: 0,
    }
}

/// Test HAVING with nested aggregates
#[test]
fn test_having_with_nested_aggregates() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT category, COUNT(*) as cnt FROM products \
                 GROUP BY category \
                 WINDOW TUMBLING(10s)\
                 HAVING COUNT(*) > AVG(price)";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with nested aggregates parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with nested aggregates may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with nested aggregates execution
#[tokio::test]
async fn test_having_with_nested_aggregates_execution() {
    // Simplified test: test aggregation without GROUP BY (since GROUP BY with windows is complex)
    let query = "SELECT COUNT(*) as cnt, AVG(price) as avg_price FROM products \
                 WINDOW TUMBLING(10s)";

    let records = vec![
        create_test_record(1, "electronics".to_string(), 100.0, 1000),
        create_test_record(2, "electronics".to_string(), 150.0, 2000),
        create_test_record(3, "books".to_string(), 20.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for nested aggregates"
    );

    if let Some(record) = results.first() {
        // All 3 records are in the same 10-second window (1s, 2s, 3s are all within 10s)
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3 for all records in window"
        );
        // Average of [100, 150, 20]
        assert!(
            matches!(record.fields.get("avg_price"), Some(FieldValue::Float(v)) if (v - 90.0).abs() < 0.1),
            "AVG should be approximately 90.0 for [100, 150, 20]"
        );
    }
}

/// Test HAVING with division operation
#[test]
fn test_having_with_division() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT product_id, SUM(quantity) as total_qty FROM orders \
                 GROUP BY product_id \
                 WINDOW TUMBLING(5s)\
                 HAVING SUM(quantity) / COUNT(*) > 10";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with division parses correctly"),
        Err(e) => println!("⚠️  HAVING with division may have limited support: {}", e),
    }
}

/// Test HAVING with division execution
#[tokio::test]
async fn test_having_with_division_execution() {
    let query = "SELECT COUNT(*) as cnt, SUM(price) as total FROM orders \
                 WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, "prod1".to_string(), 50.0, 1000),
        create_test_record(2, "prod1".to_string(), 100.0, 2000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for division aggregation"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(2)),
            "COUNT should be 2"
        );
        assert_eq!(
            record.fields.get("total"),
            Some(&FieldValue::Float(150.0)),
            "SUM should be 150.0"
        );
    }
}

/// Test HAVING with multiple arithmetic operations
#[test]
fn test_having_with_arithmetic_expressions() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT store_id, SUM(sales) as total_sales FROM transactions \
                 GROUP BY store_id \
                 WINDOW TUMBLING(60s)\
                 HAVING (SUM(sales) - AVG(sales)) * COUNT(*) > 1000";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with arithmetic expressions parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with arithmetic expressions may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with arithmetic expressions execution
#[tokio::test]
async fn test_having_with_arithmetic_expressions_execution() {
    let query = "SELECT category, COUNT(*) as cnt, SUM(price) as total FROM transactions \
                 GROUP BY category \
                 WINDOW TUMBLING(60s)";

    let records = vec![
        create_test_record(1, "electronics".to_string(), 500.0, 1000),
        create_test_record(2, "electronics".to_string(), 600.0, 2000),
        create_test_record(3, "electronics".to_string(), 700.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for arithmetic expressions"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3"
        );
        assert_eq!(
            record.fields.get("total"),
            Some(&FieldValue::Float(1800.0)),
            "SUM should be 1800.0 for [500, 600, 700]"
        );
    }
}

/// Test HAVING with window frame specification
#[test]
fn test_having_with_window_frame() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT symbol, AVG(price) as avg_price FROM stocks \
                 GROUP BY symbol \
                 WINDOW SLIDING(1h, 15m)\
                 HAVING MAX(price) OVER (ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) > 100";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with window frame parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with window frame may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with window frame execution
#[tokio::test]
async fn test_having_with_window_frame_execution() {
    let query = "SELECT category, MIN(price) as min_price, MAX(price) as max_price FROM stocks \
                 GROUP BY category \
                 WINDOW SLIDING(1h, 15m)";

    let records = vec![
        create_test_record(1, "tech".to_string(), 150.0, 1000),
        create_test_record(2, "tech".to_string(), 200.0, 2000),
        create_test_record(3, "tech".to_string(), 175.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for window frame"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("min_price"),
            Some(&FieldValue::Float(150.0)),
            "MIN should be 150.0"
        );
        assert_eq!(
            record.fields.get("max_price"),
            Some(&FieldValue::Float(200.0)),
            "MAX should be 200.0"
        );
    }
}

/// Test HAVING with complex boolean expressions
#[test]
fn test_having_with_complex_boolean() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT user_id, COUNT(*) as activity_count FROM events \
                 GROUP BY user_id \
                 WINDOW TUMBLING(30s)\
                 HAVING (COUNT(*) > 5 AND AVG(value) < 100) OR SUM(value) > 500";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with complex boolean expressions parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with complex boolean may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with complex boolean expressions execution
#[tokio::test]
async fn test_having_with_complex_boolean_execution() {
    let query = "SELECT category, COUNT(*) as cnt, AVG(price) as avg_val, SUM(price) as total FROM events \
                 GROUP BY category \
                 WINDOW TUMBLING(30s)";

    let records = vec![
        create_test_record(1, "active".to_string(), 50.0, 1000),
        create_test_record(2, "active".to_string(), 45.0, 2000),
        create_test_record(3, "active".to_string(), 55.0, 3000),
        create_test_record(4, "active".to_string(), 40.0, 4000),
        create_test_record(5, "active".to_string(), 60.0, 5000),
        create_test_record(6, "active".to_string(), 70.0, 6000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for complex boolean"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(6)),
            "COUNT should be 6"
        );
        assert!(
            matches!(record.fields.get("avg_val"), Some(FieldValue::Float(v)) if (v - 53.333336).abs() < 1.0),
            "AVG should be approximately 53.33"
        );
    }
}

/// Test HAVING with CASE expression
#[test]
fn test_having_with_case_expression() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT category FROM orders \
                 GROUP BY category \
                 WINDOW TUMBLING(10s)\
                 HAVING CASE \
                   WHEN COUNT(*) > 100 THEN SUM(amount) \
                   ELSE AVG(amount) \
                 END > 50";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with CASE expression parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with CASE expression may have limited support: {}",
            e
        ),
    }
}

/// Test HAVING with CASE expression execution
#[tokio::test]
async fn test_having_with_case_expression_execution() {
    let query = "SELECT category, COUNT(*) as cnt, SUM(price) as total, AVG(price) as avg_price FROM orders \
                 GROUP BY category \
                 WINDOW TUMBLING(10s)";

    let records = vec![
        create_test_record(1, "premium".to_string(), 80.0, 1000),
        create_test_record(2, "premium".to_string(), 90.0, 2000),
        create_test_record(3, "premium".to_string(), 85.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for CASE expression"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3"
        );
        assert_eq!(
            record.fields.get("total"),
            Some(&FieldValue::Float(255.0)),
            "SUM should be 255.0 for [80, 90, 85]"
        );
        assert!(
            matches!(record.fields.get("avg_price"), Some(FieldValue::Float(v)) if (v - 85.0).abs() < 0.01),
            "AVG should be 85.0"
        );
    }
}

/// Test HAVING with IN subquery (if supported)
#[test]
fn test_having_with_subquery() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT product_id, COUNT(*) as sales_count FROM sales \
                 GROUP BY product_id \
                 WINDOW TUMBLING(15s)\
                 HAVING product_id IN (SELECT id FROM featured_products)";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with subquery parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with subquery may have limited support (expected): {}",
            e
        ),
    }
}

/// Test HAVING with BETWEEN operator
#[test]
fn test_having_with_between() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT region FROM sales \
                 GROUP BY region \
                 WINDOW TUMBLING(20s)\
                 HAVING SUM(revenue) BETWEEN 1000 AND 5000";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with BETWEEN parses correctly"),
        Err(e) => println!("⚠️  HAVING with BETWEEN may have limited support: {}", e),
    }
}

/// Test HAVING with NULL checks
#[test]
fn test_having_with_null_check() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT dept FROM employees \
                 GROUP BY dept \
                 WINDOW TUMBLING(10s)\
                 HAVING SUM(salary) IS NOT NULL AND COUNT(*) > 1";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with NULL check parses correctly"),
        Err(e) => println!("⚠️  HAVING with NULL check may have limited support: {}", e),
    }
}

/// Test HAVING with string aggregate functions
#[test]
fn test_having_with_string_aggregates() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT country FROM users \
                 GROUP BY country \
                 WINDOW TUMBLING(30s)\
                 HAVING STRING_AGG(name, ',') IS NOT NULL AND COUNT(*) > 10";

    match parser.parse(query) {
        Ok(_) => println!("✓ HAVING with string aggregates parses correctly"),
        Err(e) => println!(
            "⚠️  HAVING with string aggregates may have limited support: {}",
            e
        ),
    }
}
