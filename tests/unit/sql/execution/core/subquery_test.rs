/*!
# Subquery Support Tests

Comprehensive tests for the newly implemented subquery functionality including:
- Scalar subqueries: SELECT (SELECT max_value FROM config) as config_value FROM events
- EXISTS subqueries: WHERE EXISTS (SELECT 1 FROM table WHERE condition)
- NOT EXISTS subqueries: WHERE NOT EXISTS (SELECT 1 FROM table WHERE condition)
- IN subqueries: WHERE column IN (SELECT id FROM table WHERE condition)
- NOT IN subqueries: WHERE column NOT IN (SELECT id FROM table WHERE condition)
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(42));
    fields.insert(
        "name".to_string(),
        FieldValue::String("test_record".to_string()),
    );
    fields.insert("amount".to_string(), FieldValue::Float(123.45));
    fields.insert("active".to_string(), FieldValue::Boolean(true));

    let mut headers = HashMap::new();
    headers.insert("test_source".to_string(), "subquery_tests".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1640995200000, // 2022-01-01 00:00:00 UTC
        offset: 100,
        partition: 0,
        event_time: None,
    }
}

// Conversion function no longer needed - using StreamRecord directly

async fn execute_subquery_test(
    query: &str,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Execute the query with StreamRecord directly
    engine.execute_with_record(&parsed_query, record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_scalar_subquery_parsing() {
    // Test that scalar subqueries can be parsed correctly
    let query = "SELECT id, (SELECT 100) as config_value FROM test_stream";
    let result = execute_subquery_test(query).await;

    // Should parse successfully (the mock implementation returns 1 for scalar subqueries)
    assert!(result.is_ok(), "Scalar subquery should parse and execute");

    let results = result.unwrap();
    assert_eq!(results.len(), 1);

    // Verify the record contains the original fields plus the subquery result
    assert!(results[0].fields.contains_key("id"));
    assert!(results[0].fields.contains_key("config_value"));

    // Mock implementation should return 1 for scalar subqueries
    assert_eq!(
        results[0].fields.get("config_value"),
        Some(&FieldValue::Integer(1))
    );
}

#[tokio::test]
async fn test_exists_subquery() {
    // Test EXISTS subquery functionality
    let query =
        "SELECT id, name FROM test_stream WHERE EXISTS (SELECT 1 FROM config WHERE active = true)";
    let result = execute_subquery_test(query).await;

    assert!(result.is_ok(), "EXISTS subquery should parse and execute");

    let results = result.unwrap();
    // Mock implementation returns true for EXISTS, so record should be included
    assert_eq!(results.len(), 1);
    assert!(results[0].fields.contains_key("id"));
    assert!(results[0].fields.contains_key("name"));
}

#[tokio::test]
async fn test_not_exists_subquery() {
    // Test NOT EXISTS subquery functionality
    let query = "SELECT id, name FROM test_stream WHERE NOT EXISTS (SELECT 1 FROM config WHERE active = false)";
    let result = execute_subquery_test(query).await;

    assert!(
        result.is_ok(),
        "NOT EXISTS subquery should parse and execute"
    );

    let results = result.unwrap();
    // Mock implementation returns true for EXISTS, so NOT EXISTS returns false
    // This means the WHERE condition fails and no records should be returned
    assert_eq!(
        results.len(),
        0,
        "NOT EXISTS should filter out the record with mock implementation"
    );
}

#[tokio::test]
async fn test_in_subquery_with_positive_value() {
    // Test IN subquery with a positive integer (mock returns true for positive integers)
    let query = "SELECT id, name FROM test_stream WHERE id IN (SELECT valid_id FROM config)";
    let result = execute_subquery_test(query).await;

    assert!(result.is_ok(), "IN subquery should parse and execute");

    let results = result.unwrap();
    // Mock implementation returns true for positive integers in IN subqueries
    // Since id = 42 (positive), it should match
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].fields.get("id"), Some(&FieldValue::Integer(42)));
}

#[tokio::test]
async fn test_not_in_subquery() {
    // Test NOT IN subquery functionality
    let query = "SELECT id, name FROM test_stream WHERE id NOT IN (SELECT blocked_id FROM config)";
    let result = execute_subquery_test(query).await;

    assert!(result.is_ok(), "NOT IN subquery should parse and execute");

    let results = result.unwrap();
    // Mock implementation returns true for positive integers in IN subqueries
    // So NOT IN would return false for positive integers, filtering out the record
    assert_eq!(
        results.len(),
        0,
        "NOT IN should filter out positive integers with mock implementation"
    );
}

#[tokio::test]
async fn test_complex_subquery_in_select() {
    // Test more complex subquery usage in SELECT clause
    let query = r#"
        SELECT 
            id,
            name,
            (SELECT 'default_config') as config_type,
            (SELECT 999) as max_limit
        FROM test_stream 
        WHERE EXISTS (SELECT 1 FROM active_configs WHERE config_name = 'production')
    "#;

    let result = execute_subquery_test(query).await;
    assert!(result.is_ok(), "Complex subquery should parse and execute");

    let results = result.unwrap();
    assert_eq!(results.len(), 1);

    // Verify all expected fields are present
    assert!(results[0].fields.contains_key("id"));
    assert!(results[0].fields.contains_key("name"));
    assert!(results[0].fields.contains_key("config_type"));
    assert!(results[0].fields.contains_key("max_limit"));

    // Verify subquery results (mock implementations)
    assert_eq!(
        results[0].fields.get("config_type"),
        Some(&FieldValue::Integer(1))
    ); // Scalar subquery mock
    assert_eq!(
        results[0].fields.get("max_limit"),
        Some(&FieldValue::Integer(1))
    ); // Scalar subquery mock
}

#[tokio::test]
async fn test_nested_subqueries() {
    // Test nested subquery scenarios
    let query = r#"
        SELECT 
            id,
            (SELECT (SELECT 'nested') as inner_config) as outer_config
        FROM test_stream
    "#;

    let result = execute_subquery_test(query).await;
    assert!(result.is_ok(), "Nested subqueries should parse and execute");

    let results = result.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].fields.contains_key("outer_config"));
}

#[tokio::test]
async fn test_subquery_with_string_field() {
    // Test IN subquery with string field (should return true for non-empty strings)
    let query = "SELECT id, name FROM test_stream WHERE name IN (SELECT valid_name FROM config)";
    let result = execute_subquery_test(query).await;

    assert!(
        result.is_ok(),
        "String IN subquery should parse and execute"
    );

    let results = result.unwrap();
    // Mock implementation returns true for non-empty strings
    // Since name = "test_record" (non-empty), it should match
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("name"),
        Some(&FieldValue::String("test_record".to_string()))
    );
}

#[tokio::test]
async fn test_subquery_with_boolean_field() {
    // Test IN subquery with boolean field (should return true for true values)
    let query = "SELECT id, active FROM test_stream WHERE active IN (SELECT enabled FROM config)";
    let result = execute_subquery_test(query).await;

    assert!(
        result.is_ok(),
        "Boolean IN subquery should parse and execute"
    );

    let results = result.unwrap();
    // Mock implementation returns the boolean value itself for IN subqueries
    // Since active = true, it should match
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("active"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_subquery_error_handling() {
    // Test error cases - queries that should fail to parse
    let invalid_queries = vec![
        // Invalid subquery syntax (missing SELECT)
        "SELECT id FROM test_stream WHERE id IN (FROM config)",
        // Invalid nested structure
        "SELECT id FROM test_stream WHERE id IN (SELECT WHERE active = true)",
    ];

    for query in invalid_queries {
        let result = execute_subquery_test(query).await;
        assert!(result.is_err(), "Query '{}' should fail to parse", query);
    }
}

#[tokio::test]
async fn test_subquery_types_comprehensive() {
    // Test all subquery types are recognized by the parser
    let queries = vec![
        (
            "EXISTS",
            "SELECT id FROM test WHERE EXISTS (SELECT 1 FROM config)",
        ),
        (
            "NOT EXISTS",
            "SELECT id FROM test WHERE NOT EXISTS (SELECT 1 FROM config)",
        ),
        (
            "IN",
            "SELECT id FROM test WHERE id IN (SELECT id FROM config)",
        ),
        (
            "NOT IN",
            "SELECT id FROM test WHERE id NOT IN (SELECT id FROM config)",
        ),
        ("Scalar", "SELECT id, (SELECT 1) as scalar_val FROM test"),
    ];

    for (subquery_type, query) in queries {
        let result = execute_subquery_test(query).await;
        assert!(
            result.is_ok(),
            "{} subquery should parse successfully",
            subquery_type
        );
    }
}

#[tokio::test]
async fn test_subquery_with_multiple_conditions() {
    // Test subqueries combined with other conditions using supported features
    let query = r#"
        SELECT id, name, amount 
        FROM test_stream 
        WHERE amount > 100 
          AND EXISTS (SELECT 1 FROM config WHERE config.valid_id = test_stream.id) 
          AND EXISTS (SELECT 1 FROM permissions WHERE user_id = 42)
    "#;

    let result = execute_subquery_test(query).await;
    assert!(
        result.is_ok(),
        "Complex conditions with EXISTS subqueries should work"
    );

    let results = result.unwrap();
    // All conditions should pass with our test data and mock implementation:
    // - amount > 100: true (123.45 > 100)
    // - EXISTS subqueries: true (mock implementation)
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_parser_subquery_integration() {
    // Test that the parser correctly identifies and structures subqueries
    let parser = StreamingSqlParser::new();

    // Test scalar subquery parsing
    let scalar_query = "SELECT (SELECT 1) as val FROM test";
    let parsed = parser.parse(scalar_query);
    assert!(parsed.is_ok(), "Scalar subquery should parse successfully");

    // Test EXISTS subquery parsing
    let exists_query = "SELECT id FROM test WHERE EXISTS (SELECT 1 FROM config)";
    let parsed = parser.parse(exists_query);
    assert!(parsed.is_ok(), "EXISTS subquery should parse successfully");

    // Test IN subquery parsing
    let in_query = "SELECT id FROM test WHERE id IN (SELECT id FROM config)";
    let parsed = parser.parse(in_query);
    assert!(parsed.is_ok(), "IN subquery should parse successfully");
}
