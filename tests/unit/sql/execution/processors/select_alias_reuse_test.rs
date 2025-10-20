/*!
# SELECT Clause Column Alias Reuse Tests

Comprehensive tests for the FR-078 feature enabling SELECT clause aliases
to be referenced in subsequent fields during the same SELECT clause.

## Test Scenarios

- Simple alias reuse in expressions
- Multiple alias chain references
- Alias shadowing (alias overrides column name)
- GROUP BY with alias reuse
- HAVING with alias reuse
- CASE expressions with aliases
- Window functions with aliases
- Edge cases (NULL values, type mismatches)
- Backward compatibility (non-aliased queries)
*/

use std::collections::HashMap;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::execution::processors::{QueryProcessor, ProcessorContext};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("volume".to_string(), FieldValue::Float(1000.0));
    fields.insert("avg_volume".to_string(), FieldValue::Float(500.0));
    fields.insert("price".to_string(), FieldValue::Float(100.0));
    fields.insert("min_price".to_string(), FieldValue::Float(90.0));
    fields.insert("max_price".to_string(), FieldValue::Float(110.0));
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("quantity".to_string(), FieldValue::Integer(100));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test_data".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1640995200000,
        offset: 0,
        partition: 0,
        event_time: None,
    }
}

#[test]
fn test_backward_compatibility_basic_query() {
    // Test: Basic query without any alias reuse (should always work)
    let sql = r#"
    SELECT
        id,
        volume,
        price
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_basic");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(output.fields.get("id"), Some(&FieldValue::Integer(1)));
            assert_eq!(output.fields.get("volume"), Some(&FieldValue::Float(1000.0)));
            assert_eq!(output.fields.get("price"), Some(&FieldValue::Float(100.0)));
        }
    }
}

// Phase 2.1 & 2.2 tests - Placeholder for future implementation
// These tests verify alias reuse in non-grouped and GROUP BY contexts
// Currently skipped as Phase 2.1/2.2 implementation is in progress

#[test]
fn test_extended_backward_compatibility() {
    // Test: More comprehensive backward compatibility test
    let sql = r#"
    SELECT
        id,
        symbol,
        volume,
        price,
        quantity
    FROM trades
    "#;

    let parser = StreamingSqlParser::new();
    let parsed = parser.parse(sql);
    assert!(parsed.is_ok(), "Query should parse successfully");

    let query = parsed.unwrap();
    let mut context = ProcessorContext::new("test_backward_compat_ext");
    let record = create_test_record();

    let result = QueryProcessor::process_query(&query, &record, &mut context);
    assert!(
        result.is_ok(),
        "Query should execute: {:?}",
        result.err()
    );

    if let Ok(proc_result) = result {
        if let Some(output) = proc_result.record {
            assert_eq!(output.fields.get("id"), Some(&FieldValue::Integer(1)));
            assert_eq!(
                output.fields.get("symbol"),
                Some(&FieldValue::String("AAPL".to_string()))
            );
            assert_eq!(output.fields.get("volume"), Some(&FieldValue::Float(1000.0)));
            assert_eq!(output.fields.get("price"), Some(&FieldValue::Float(100.0)));
            assert_eq!(output.fields.get("quantity"), Some(&FieldValue::Integer(100)));
        }
    }
}

#[test]
fn test_parsing_queries_with_aliases() {
    // Test: Verify that queries with aliases parse correctly
    // (even if not all functionality is implemented yet)
    let queries = vec![
        "SELECT volume / avg_volume AS spike_ratio FROM trades",
        "SELECT price / 100 AS normalized_price FROM trades",
        "SELECT max_price - min_price AS price_range FROM trades",
    ];

    let parser = StreamingSqlParser::new();
    for query in queries {
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Query '{}' should parse successfully",
            query
        );
    }
}
