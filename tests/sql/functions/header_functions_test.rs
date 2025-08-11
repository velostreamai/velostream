/*!
# Tests for Header Functions

Comprehensive test suite for header writing functions including SET_HEADER and REMOVE_HEADER.
Tests both functionality and error handling for Kafka message header manipulation.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

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
    }
}

fn convert_stream_record_to_internal(record: &StreamRecord) -> HashMap<String, InternalValue> {
    record
        .fields
        .iter()
        .map(|(k, v)| {
            let internal_val = match v {
                FieldValue::Integer(i) => InternalValue::Integer(*i),
                FieldValue::Float(f) => InternalValue::Number(*f),
                FieldValue::String(s) => InternalValue::String(s.clone()),
                FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                FieldValue::Null => InternalValue::Null,
                _ => InternalValue::String(format!("{:?}", v)),
            };
            (k.clone(), internal_val)
        })
        .collect()
}

async fn execute_query(
    query: &str,
) -> Result<Vec<HashMap<String, InternalValue>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Convert StreamRecord to HashMap<String, InternalValue>
    let internal_record = convert_stream_record_to_internal(&record);

    // Execute the query with internal record, including metadata
    engine
        .execute_with_metadata(
            &parsed_query,
            internal_record,
            record.headers,
            Some(record.timestamp),
            Some(record.offset),
            Some(record.partition),
        )
        .await?;

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
        results[0]["set_result"],
        InternalValue::String("new_value".to_string())
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
        results[0]["set_result"],
        InternalValue::String("test".to_string())
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
        results[0]["set_result"],
        InternalValue::String("1".to_string())
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
        results[0]["set_result"],
        InternalValue::String("123.45".to_string())
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
        results[0]["set_result"],
        InternalValue::String("true".to_string())
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
        results[0]["set_result"],
        InternalValue::String("null".to_string())
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
        results[0]["remove_result"],
        InternalValue::String("test-system".to_string())
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
    assert_eq!(results[0]["remove_result"], InternalValue::Null);
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
        results[0]["set1_result"],
        InternalValue::String("value1".to_string())
    );
    assert_eq!(
        results[0]["set2_result"],
        InternalValue::String("test".to_string())
    );
    assert_eq!(
        results[0]["remove_result"],
        InternalValue::String("existing_value".to_string())
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
        results[0]["set_upper"],
        InternalValue::String("TEST".to_string())
    );
    assert_eq!(
        results[0]["set_rounded"],
        InternalValue::String("123.5".to_string())
    );
    assert_eq!(
        results[0]["set_length"],
        InternalValue::String("4".to_string())
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
        results[0]["result"],
        InternalValue::String("value".to_string())
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
        results[0]["complex_result"],
        InternalValue::String("Result: id_1".to_string())
    );
}
