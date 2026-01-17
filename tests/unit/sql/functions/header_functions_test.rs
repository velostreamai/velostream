/*!
# Tests for Header Functions

Comprehensive test suite for header writing functions including SET_HEADER and REMOVE_HEADER.
Tests both functionality and error handling for Kafka message header manipulation.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));
    fields.insert("amount".to_string(), FieldValue::Float(123.45));
    fields.insert("active".to_string(), FieldValue::Boolean(true));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test-system".to_string());
    headers.insert("version".to_string(), "1.0.0".to_string());
    headers.insert("existing_header".to_string(), "existing_value".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1734652800000,
        offset: 100,
        partition: 0,
        event_time: None,
        topic: None,
        key: None,
    }
}

async fn execute_query(query: &str) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Execute the query with StreamRecord directly
    engine.execute_with_record(&parsed_query, &record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_set_header_basic() {
    let results = execute_query(
        "SELECT id, SET_HEADER('new_key', 'new_value') as set_result FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // The SET_HEADER function should return the value that was set
    assert_eq!(
        results[0].fields.get("set_result"),
        Some(&FieldValue::String("new_value".to_string()))
    );
}

#[tokio::test]
async fn test_set_header_with_field_value() {
    let results =
        execute_query("SELECT id, SET_HEADER('dynamic_key', name) as set_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // The SET_HEADER function should return the field value that was set
    assert_eq!(
        results[0].fields.get("set_result"),
        Some(&FieldValue::String("test".to_string()))
    );
}

#[tokio::test]
async fn test_set_header_with_integer() {
    let results =
        execute_query("SELECT id, SET_HEADER('number_key', id) as set_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // Integer should be converted to string
    assert_eq!(
        results[0].fields.get("set_result"),
        Some(&FieldValue::String("1".to_string()))
    );
}

#[tokio::test]
async fn test_set_header_with_float() {
    let results =
        execute_query("SELECT id, SET_HEADER('amount_key', amount) as set_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // Float should be converted to string
    assert_eq!(
        results[0].fields.get("set_result"),
        Some(&FieldValue::String("123.45".to_string()))
    );
}

#[tokio::test]
async fn test_set_header_with_boolean() {
    let results =
        execute_query("SELECT id, SET_HEADER('bool_key', active) as set_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // Boolean should be converted to string
    assert_eq!(
        results[0].fields.get("set_result"),
        Some(&FieldValue::String("true".to_string()))
    );
}

#[tokio::test]
async fn test_set_header_with_null() {
    let results =
        execute_query("SELECT id, SET_HEADER('null_key', NULL) as set_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // NULL should be converted to "null" string
    assert_eq!(
        results[0].fields.get("set_result"),
        Some(&FieldValue::String("null".to_string()))
    );
}

#[tokio::test]
async fn test_remove_header_existing() {
    let results =
        execute_query("SELECT id, REMOVE_HEADER('source') as remove_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // REMOVE_HEADER should return the removed value
    assert_eq!(
        results[0].fields.get("remove_result"),
        Some(&FieldValue::String("test-system".to_string()))
    );
}

#[tokio::test]
async fn test_remove_header_nonexistent() {
    let results =
        execute_query("SELECT id, REMOVE_HEADER('nonexistent') as remove_result FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // REMOVE_HEADER should return NULL for nonexistent headers
    assert_eq!(
        results[0].fields.get("remove_result"),
        Some(&FieldValue::Null)
    );
}

#[tokio::test]
async fn test_multiple_header_operations() {
    let results = execute_query(
        "SELECT 
            id,
            SET_HEADER('new_key1', 'value1') as set1_result,
            SET_HEADER('new_key2', name) as set2_result,
            REMOVE_HEADER('existing_header') as remove_result
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // Check return values
    assert_eq!(
        results[0].fields.get("set1_result"),
        Some(&FieldValue::String("value1".to_string()))
    );
    assert_eq!(
        results[0].fields.get("set2_result"),
        Some(&FieldValue::String("test".to_string()))
    );
    assert_eq!(
        results[0].fields.get("remove_result"),
        Some(&FieldValue::String("existing_value".to_string()))
    );
}

#[tokio::test]
async fn test_header_functions_with_sql_functions() {
    let results = execute_query(
        "SELECT 
            id,
            SET_HEADER('upper_name', UPPER(name)) as set_upper,
            SET_HEADER('rounded_amount', ROUND(amount, 1)) as set_rounded,
            SET_HEADER('length_info', LENGTH(name)) as set_length
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // Check return values
    assert_eq!(
        results[0].fields.get("set_upper"),
        Some(&FieldValue::String("TEST".to_string()))
    );
    assert_eq!(
        results[0].fields.get("set_rounded"),
        Some(&FieldValue::String("123.5".to_string()))
    );
    assert_eq!(
        results[0].fields.get("set_length"),
        Some(&FieldValue::String("4".to_string()))
    );
}

// Error handling tests
#[tokio::test]
async fn test_set_header_wrong_argument_count() {
    // Test with no arguments
    let result = execute_query("SELECT SET_HEADER() as result FROM test_stream").await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly two arguments"));

    // Test with one argument
    let result = execute_query("SELECT SET_HEADER('key') as result FROM test_stream").await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly two arguments"));

    // Test with three arguments
    let result =
        execute_query("SELECT SET_HEADER('key', 'value', 'extra') as result FROM test_stream")
            .await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly two arguments"));
}

#[tokio::test]
async fn test_remove_header_wrong_argument_count() {
    // Test with no arguments
    let result = execute_query("SELECT REMOVE_HEADER() as result FROM test_stream").await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));

    // Test with two arguments
    let result =
        execute_query("SELECT REMOVE_HEADER('key1', 'key2') as result FROM test_stream").await;

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("exactly one argument"));
}

#[tokio::test]
async fn test_set_header_with_non_string_key() {
    // Test that non-string keys are handled properly
    let results = execute_query("SELECT SET_HEADER(id, 'value') as result FROM test_stream")
        .await
        .unwrap();

    assert_eq!(results.len(), 1);

    // Integer key should be converted to string and function should work
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("value".to_string()))
    );
}

#[tokio::test]
async fn test_header_functions_in_complex_expression() {
    let results = execute_query(
        "SELECT
            id,
            CONCAT('Result: ', SET_HEADER('computed', CONCAT('id_', id))) as complex_result
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // The complex expression should work correctly
    assert_eq!(
        results[0].fields.get("complex_result"),
        Some(&FieldValue::String("Result: id_1".to_string()))
    );
}

// ===== Phase 2: Header Mutation Tests =====
// These tests verify that SET_HEADER and REMOVE_HEADER actually modify the output record's headers

#[tokio::test]
async fn test_set_header_mutation_applied_to_record() {
    let results =
        execute_query("SELECT id, SET_HEADER('trace-id', 'abc123') as trace FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // The mutation should actually modify the record's headers
    assert_eq!(
        results[0].headers.get("trace-id"),
        Some(&"abc123".to_string()),
        "SET_HEADER mutation should add header to output record"
    );
}

#[tokio::test]
async fn test_set_header_overwrite_existing() {
    let results = execute_query(
        "SELECT id, SET_HEADER('source', 'new-source') as new_source FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // The mutation should overwrite the existing header
    assert_eq!(
        results[0].headers.get("source"),
        Some(&"new-source".to_string()),
        "SET_HEADER should overwrite existing header"
    );
}

#[tokio::test]
async fn test_set_header_with_field_mutation() {
    let results =
        execute_query("SELECT id, SET_HEADER('name_header', name) as name_hdr FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // The field value should be set in the header
    assert_eq!(
        results[0].headers.get("name_header"),
        Some(&"test".to_string()),
        "SET_HEADER with field value should add field value to header"
    );
}

#[tokio::test]
async fn test_remove_header_mutation_applied() {
    let results =
        execute_query("SELECT id, REMOVE_HEADER('source') as old_source FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // The header should be removed from the record
    assert!(
        !results[0].headers.contains_key("source"),
        "REMOVE_HEADER mutation should remove header from output record"
    );

    // Function return value should still be correct
    assert_eq!(
        results[0].fields.get("old_source"),
        Some(&FieldValue::String("test-system".to_string())),
        "REMOVE_HEADER should return the removed value"
    );
}

#[tokio::test]
async fn test_multiple_mutations_to_same_header() {
    let results = execute_query(
        "SELECT
            id,
            SET_HEADER('request-id', 'first') as first_set,
            SET_HEADER('request-id', 'second') as second_set
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // Last SET_HEADER should win
    assert_eq!(
        results[0].headers.get("request-id"),
        Some(&"second".to_string()),
        "Last SET_HEADER mutation to same key should win"
    );
}

#[tokio::test]
async fn test_complex_mutation_sequence() {
    let results = execute_query(
        "SELECT
            id,
            SET_HEADER('trace-id', 'trace123') as trace,
            SET_HEADER('span-id', 'span456') as span,
            SET_HEADER('user-id', name) as user,
            REMOVE_HEADER('version') as old_version
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // Verify all mutations were applied
    assert_eq!(
        results[0].headers.get("trace-id"),
        Some(&"trace123".to_string()),
        "First SET_HEADER mutation should be applied"
    );
    assert_eq!(
        results[0].headers.get("span-id"),
        Some(&"span456".to_string()),
        "Second SET_HEADER mutation should be applied"
    );
    assert_eq!(
        results[0].headers.get("user-id"),
        Some(&"test".to_string()),
        "SET_HEADER with field value should be applied"
    );
    assert!(
        !results[0].headers.contains_key("version"),
        "REMOVE_HEADER mutation should be applied"
    );

    // Existing header should still be there
    assert_eq!(
        results[0].headers.get("existing_header"),
        Some(&"existing_value".to_string()),
        "Untouched headers should remain"
    );
}

#[tokio::test]
async fn test_set_header_with_numeric_value_mutation() {
    let results =
        execute_query("SELECT id, SET_HEADER('count', amount) as count_hdr FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // Numeric value should be converted to string in header
    assert_eq!(
        results[0].headers.get("count"),
        Some(&"123.45".to_string()),
        "Numeric values should be converted to strings in headers"
    );
}

#[tokio::test]
async fn test_mutation_preserves_original_headers() {
    let results =
        execute_query("SELECT id, SET_HEADER('new_trace', 'xyz') as new_tr FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);

    // Original headers should still be present
    assert_eq!(
        results[0].headers.get("source"),
        Some(&"test-system".to_string()),
        "Original headers should be preserved"
    );
    assert_eq!(
        results[0].headers.get("version"),
        Some(&"1.0.0".to_string()),
        "Original headers should be preserved"
    );

    // New header should be added
    assert_eq!(
        results[0].headers.get("new_trace"),
        Some(&"xyz".to_string()),
        "New header should be added"
    );
}
