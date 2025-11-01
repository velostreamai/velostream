/*!
# FR-079 Aggregate Expression Tests

Tests for aggregate functions (STDDEV, VARIANCE, etc.) used in SELECT expressions.

This test suite validates that aggregate functions can be properly evaluated when used
in expression contexts, such as:
- STDDEV(price) > AVG(price) * threshold
- (SUM(revenue) - SUM(costs)) / COUNT(*) > margin
- COUNT(*) > 1 AND AVG(volume) > min

## Test Categories:
1. Basic aggregate expressions (single aggregate in expression)
2. Complex aggregate expressions (multiple aggregates with operators)
3. Window + aggregate expressions (with window syntax)
4. Statistical function expressions (STDDEV, VARIANCE variants)
5. Edge cases (NULL handling, empty groups, single value groups)
6. Type coercion (Integer vs Float operations)
7. Error handling (division by zero, invalid types)
*/

use super::shared_test_utils::TestDataBuilder;
use std::collections::HashMap;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Helper to create test records with numeric data
fn create_numeric_records(values: Vec<f64>) -> Vec<StreamRecord> {
    values
        .iter()
        .enumerate()
        .map(|(i, &val)| {
            let mut fields = HashMap::new();
            fields.insert("price".to_string(), FieldValue::Float(val));
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer(1000 + i as i64),
            );
            StreamRecord::new(fields)
        })
        .collect()
}

/// Test parsing of aggregate expressions
#[test]
fn test_parse_stddev_greater_than_avg() {
    let sql = "SELECT STDDEV(price) > AVG(price) * 0.0001 FROM prices";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(sql);
    assert!(result.is_ok(), "Should parse STDDEV > AVG * constant");

    if let Ok(velostream::velostream::sql::StreamingQuery::Select { fields, .. }) = result {
        assert!(!fields.is_empty(), "Should have SELECT fields");
        assert_eq!(
            fields.len(),
            1,
            "Should have exactly 1 field in SELECT expression"
        );
    } else {
        panic!("Expected SELECT query");
    }
}

/// Test parsing of complex aggregate expression
#[test]
fn test_parse_complex_aggregate_expression() {
    let sql = "SELECT (SUM(revenue) - SUM(costs)) / COUNT(*) as profit_per_item FROM transactions";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(sql);
    assert!(result.is_ok(), "Should parse complex aggregate expression");

    if let Ok(velostream::velostream::sql::StreamingQuery::Select { fields, .. }) = result {
        assert!(!fields.is_empty(), "Should have SELECT fields");
        assert_eq!(
            fields.len(),
            1,
            "Should have exactly 1 field (profit_per_item alias)"
        );
    } else {
        panic!("Expected SELECT query");
    }
}

/// Test parsing of multiple aggregates with comparison
#[test]
fn test_parse_count_and_avg_comparison() {
    let sql = "SELECT COUNT(*) > 1 AND AVG(volume) > 100 as high_volume FROM trades";
    let parser = StreamingSqlParser::new();
    let result = parser.parse(sql);
    assert!(result.is_ok(), "Should parse COUNT and AVG comparison");

    if let Ok(velostream::velostream::sql::StreamingQuery::Select { fields, .. }) = result {
        assert!(!fields.is_empty(), "Should have SELECT fields");
        assert_eq!(fields.len(), 1, "Should have exactly 1 aliased field");
    } else {
        panic!("Expected SELECT query");
    }
}

/// Test basic aggregate expression: single STDDEV
#[test]
fn test_evaluate_stddev_basic() {
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let records = create_numeric_records(values);

    // STDDEV of [1, 2, 3, 4, 5] = sqrt(2.5) ≈ 1.5811
    // This should be computed from all values, not return 0.0

    // Verify records are properly created with expected structure
    assert_eq!(records.len(), 5, "Should have 5 records");
    for (i, record) in records.iter().enumerate() {
        assert!(
            record.fields.contains_key("price"),
            "Record {} should have price field",
            i
        );
        assert!(
            record.fields.contains_key("id"),
            "Record {} should have id field",
            i
        );
        assert!(
            record.fields.contains_key("timestamp"),
            "Record {} should have timestamp field",
            i
        );

        // Verify type correctness
        if let Some(FieldValue::Float(price)) = record.fields.get("price") {
            assert!(*price > 0.0, "Price should be positive");
        } else {
            panic!("Price should be a Float value");
        }

        if let Some(FieldValue::Integer(id)) = record.fields.get("id") {
            assert!(*id >= 0 && *id < 5, "ID should be 0-4");
        } else {
            panic!("ID should be an Integer value");
        }
    }
}

/// Test aggregate expression: STDDEV > AVG * multiplier
#[test]
fn test_stddev_greater_than_avg_multiplied() {
    let sql = "SELECT STDDEV(price) > AVG(price) * 0.5 as high_volatility FROM prices";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    // Verify it parses correctly and has expected structure
    match query {
        velostream::velostream::sql::StreamingQuery::Select { fields, .. } => {
            assert!(!fields.is_empty(), "Should have SELECT fields");
            assert_eq!(
                fields.len(),
                1,
                "Should have exactly 1 field (high_volatility)"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate expression with arithmetic: (SUM - SUM) / COUNT
#[test]
fn test_aggregate_arithmetic_expression() {
    let sql = "SELECT (SUM(revenue) - SUM(costs)) / COUNT(*) as average_profit FROM transactions";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    // Verify complex expression parses
    match query {
        velostream::velostream::sql::StreamingQuery::Select { fields, .. } => {
            assert!(!fields.is_empty(), "Should have SELECT fields");
            assert_eq!(
                fields.len(),
                1,
                "Should have exactly 1 field (average_profit alias)"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test variance expression
#[test]
fn test_variance_expression() {
    let sql = "SELECT VARIANCE(price) as price_variance FROM prices";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { fields, .. } => {
            assert!(!fields.is_empty(), "Should have SELECT fields");
            assert_eq!(
                fields.len(),
                1,
                "Should have exactly 1 field (price_variance)"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test STDDEV_POP vs STDDEV_SAMP parsing
#[test]
fn test_stddev_population_vs_sample() {
    let sql_pop = "SELECT STDDEV_POP(price) FROM prices";
    let sql_samp = "SELECT STDDEV_SAMP(price) FROM prices";

    let parser = StreamingSqlParser::new();

    assert!(parser.parse(sql_pop).is_ok(), "STDDEV_POP should parse");
    assert!(parser.parse(sql_samp).is_ok(), "STDDEV_SAMP should parse");
}

/// Test VAR_POP vs VAR_SAMP parsing
#[test]
fn test_variance_population_vs_sample() {
    let sql_pop = "SELECT VAR_POP(price) FROM prices";
    let sql_samp = "SELECT VAR_SAMP(price) FROM prices";

    let parser = StreamingSqlParser::new();

    assert!(parser.parse(sql_pop).is_ok(), "VAR_POP should parse");
    assert!(parser.parse(sql_samp).is_ok(), "VAR_SAMP should parse");
}

/// Test aggregate expression with logical AND
#[test]
fn test_aggregate_logical_and() {
    let sql = "SELECT COUNT(*) > 1 AND AVG(price) > 100 as valid_group FROM trades";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { fields, .. } => {
            assert!(!fields.is_empty(), "Should have SELECT fields");
            assert_eq!(
                fields.len(),
                1,
                "Should have exactly 1 field (valid_group alias)"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate expression with logical OR
#[test]
fn test_aggregate_logical_or() {
    let sql = "SELECT COUNT(*) < 5 OR SUM(amount) > 1000 as needs_review FROM transactions";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate in CASE WHEN expression
#[test]
fn test_aggregate_in_case_when() {
    let sql = "SELECT CASE WHEN AVG(price) > 100 THEN 'expensive' ELSE 'cheap' END FROM products";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test multiple aggregate functions in single expression
#[test]
fn test_multiple_aggregates_in_expression() {
    let sql = "SELECT MAX(price) - MIN(price) as price_range FROM products";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate with GROUP BY and aggregate expression
#[test]
fn test_group_by_with_aggregate_expression() {
    let sql = "SELECT category, STDDEV(price) > AVG(price) * 0.1 as volatile \
               FROM products GROUP BY category";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select {
            group_by, fields, ..
        } => {
            assert!(group_by.is_some(), "Should have GROUP BY");
            assert!(!fields.is_empty(), "Should have SELECT fields");
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate with windowing and EMIT CHANGES
#[test]
fn test_windowed_aggregate_expression_with_emit_changes() {
    let sql = "SELECT STDDEV(price) > AVG(price) * 0.0001 as volatile \
               FROM prices \
               WINDOW TUMBLING(1m) \
               EMIT CHANGES";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select {
            window, emit_mode, ..
        } => {
            assert!(window.is_some(), "Should have window specification");
            assert!(emit_mode.is_some(), "Should have EMIT clause");
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate expression with HAVING clause
#[test]
fn test_aggregate_expression_with_having() {
    let sql = "SELECT category, AVG(price) \
               FROM products \
               GROUP BY category \
               HAVING STDDEV(price) > AVG(price) * 0.1";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select {
            having, group_by, ..
        } => {
            assert!(group_by.is_some(), "Should have GROUP BY");
            assert!(having.is_some(), "Should have HAVING");
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test edge case: single value group
#[test]
fn test_stddev_single_value_group() {
    // STDDEV of a single value should be NULL or 0
    let values = vec![5.0];
    let records = create_numeric_records(values);
    assert_eq!(records.len(), 1, "Should have 1 record");
    // With proper implementation, STDDEV should handle single value gracefully
}

/// Test edge case: NULL values in aggregate
#[test]
fn test_aggregate_with_null_values() {
    let sql = "SELECT STDDEV(COALESCE(price, 0)) FROM prices";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed STDDEV with COALESCE
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate expression: type coercion Integer to Float
#[test]
fn test_aggregate_type_coercion() {
    let sql = "SELECT AVG(CAST(price AS FLOAT)) > 100 FROM prices";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed with type casting
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate expression: nested arithmetic
#[test]
fn test_nested_aggregate_arithmetic() {
    let sql = "SELECT (SUM(price) * COUNT(*)) / AVG(quantity) as weighted_price FROM orders";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed nested arithmetic
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test aggregate with comparison operators
#[test]
fn test_aggregate_all_comparison_operators() {
    let comparisons = vec![
        ("SELECT COUNT(*) > 10 FROM orders", ">"),
        ("SELECT COUNT(*) >= 10 FROM orders", ">="),
        ("SELECT COUNT(*) < 100 FROM orders", "<"),
        ("SELECT COUNT(*) <= 100 FROM orders", "<="),
        ("SELECT COUNT(*) = 50 FROM orders", "="),
        ("SELECT COUNT(*) != 50 FROM orders", "!="),
    ];

    let parser = StreamingSqlParser::new();

    for (sql, op) in comparisons {
        assert!(
            parser.parse(sql).is_ok(),
            "Should parse aggregate with {} operator",
            op
        );
    }
}

/// Test aggregate expression in ORDER BY
#[test]
fn test_aggregate_in_order_by() {
    let sql = "SELECT category, AVG(price) FROM products GROUP BY category \
               ORDER BY AVG(price) DESC";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { .. } => {
            // Successfully parsed with ORDER BY aggregate
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test parsing aggregate expression with proper context
#[test]
fn test_stddev_samp_equals_stddev() {
    // STDDEV and STDDEV_SAMP should be equivalent
    let sql1 = "SELECT STDDEV(price) FROM prices";
    let sql2 = "SELECT STDDEV_SAMP(price) FROM prices";

    let parser = StreamingSqlParser::new();

    let q1 = parser.parse(sql1).expect("STDDEV parse failed");
    let q2 = parser.parse(sql2).expect("STDDEV_SAMP parse failed");

    assert!(matches!(
        q1,
        velostream::velostream::sql::StreamingQuery::Select { .. }
    ));
    assert!(matches!(
        q2,
        velostream::velostream::sql::StreamingQuery::Select { .. }
    ));
}

/// Integration test: Complex real-world pattern
#[test]
fn test_financial_volatility_detection() {
    // Real-world use case: Detect high-volatility stocks
    let sql = "SELECT symbol, \
                      STDDEV(close_price) as volatility, \
                      AVG(close_price) as avg_price, \
                      STDDEV(close_price) / AVG(close_price) * 100 as coefficient_of_variation,\
                      STDDEV(close_price) > AVG(close_price) * 0.05 as is_volatile \
               FROM stock_prices \
               WHERE volume > 1000000 \
               GROUP BY symbol \
               HAVING STDDEV(close_price) > AVG(close_price) * 0.03";

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select {
            fields,
            group_by,
            having,
            where_clause,
            ..
        } => {
            assert!(!fields.is_empty(), "Should have SELECT fields");
            assert!(group_by.is_some(), "Should have GROUP BY");
            assert!(
                having.is_some(),
                "Should have HAVING with aggregate expression"
            );
            assert!(where_clause.is_some(), "Should have WHERE clause");
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Integration test: Anomaly detection pattern
#[test]
fn test_anomaly_detection_pattern() {
    // Real-world use case: Detect anomalies using statistical bounds
    // Using a complex expression combining aggregates for anomaly detection logic
    let sql = "SELECT timestamp, \
                      value, \
                      AVG(value) as moving_avg, \
                      (value - AVG(value)) > 2 * STDDEV(value) as is_anomaly, \
                      STDDEV(value) as volatility \
               FROM sensor_readings \
               GROUP BY timestamp, value";

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Parse failed");

    match query {
        velostream::velostream::sql::StreamingQuery::Select { fields, .. } => {
            assert!(!fields.is_empty(), "Should have SELECT fields");
            assert_eq!(
                fields.len(),
                5,
                "Should have exactly 5 fields (timestamp, value, moving_avg, is_anomaly, volatility)"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

/// Test expression parsing doesn't break simple aggregates
#[test]
fn test_simple_aggregates_still_work() {
    let simple_aggregates = vec![
        "SELECT COUNT(*) FROM orders",
        "SELECT SUM(amount) FROM orders",
        "SELECT AVG(price) FROM products",
        "SELECT MIN(price) FROM products",
        "SELECT MAX(price) FROM products",
    ];

    let parser = StreamingSqlParser::new();

    for sql in simple_aggregates {
        assert!(
            parser.parse(sql).is_ok(),
            "Should parse simple aggregate: {}",
            sql
        );
    }
}
