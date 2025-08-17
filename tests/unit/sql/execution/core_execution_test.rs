//! Unit tests for the core StreamExecutionEngine
//!
//! These tests verify the core orchestration functionality of the execution engine,
//! ensuring it properly delegates to the modular processor components.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{SelectField, StreamSource, StreamingQuery};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;

/// Helper function to create a test execution engine
fn create_test_engine() -> (
    StreamExecutionEngine,
    mpsc::UnboundedReceiver<HashMap<String, InternalValue>>,
) {
    let (output_tx, output_rx) = mpsc::unbounded_channel();
    let format = Arc::new(JsonFormat);
    let engine = StreamExecutionEngine::new(output_tx, format);
    (engine, output_rx)
}

#[tokio::test]
async fn test_simple_select() {
    let (mut engine, mut output_rx) = create_test_engine();

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    let mut record = HashMap::new();
    record.insert("id".to_string(), InternalValue::Number(123.0));

    engine.execute(&query, record).await.unwrap();

    let result = output_rx.recv().await.unwrap();
    assert!(result.contains_key("id"));
    // Handle both Number and Integer types due to type conversion
    match result.get("id") {
        Some(InternalValue::Number(val)) => assert_eq!(*val, 123.0),
        Some(InternalValue::Integer(val)) => assert_eq!(*val, 123),
        _ => panic!("Expected id to be Number or Integer"),
    }
}

#[tokio::test]
async fn test_processing_stats() {
    let (engine, _) = create_test_engine();

    let stats = engine.get_stats();
    assert_eq!(stats.total_records_processed, 0);
    assert_eq!(stats.active_queries, 0);
    assert!(stats.processors_stats.is_empty());
}

#[tokio::test]
async fn test_execute_with_headers() {
    let (mut engine, mut output_rx) = create_test_engine();

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("name".to_string())],
        from: StreamSource::Stream("users".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    let mut record = HashMap::new();
    record.insert(
        "name".to_string(),
        InternalValue::String("Alice".to_string()),
    );

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test-system".to_string());

    engine
        .execute_with_headers(&query, record, headers)
        .await
        .unwrap();

    let result = output_rx.recv().await.unwrap();
    assert!(result.contains_key("name"));
    assert_eq!(
        result.get("name"),
        Some(&InternalValue::String("Alice".to_string()))
    );
}

#[tokio::test]
async fn test_execute_with_metadata() {
    let (mut engine, mut output_rx) = create_test_engine();

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("value".to_string())],
        from: StreamSource::Stream("data".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    let mut record = HashMap::new();
    record.insert("value".to_string(), InternalValue::Integer(42));

    let headers = HashMap::new();
    let timestamp = Some(1234567890i64);
    let offset = Some(100i64);
    let partition = Some(5i32);

    engine
        .execute_with_metadata(&query, record, headers, timestamp, offset, partition)
        .await
        .unwrap();

    let result = output_rx.recv().await.unwrap();
    assert!(result.contains_key("value"));
    assert_eq!(result.get("value"), Some(&InternalValue::Integer(42)));
}

#[tokio::test]
async fn test_multiple_queries() {
    let (mut engine, mut output_rx) = create_test_engine();

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    // Execute first record
    let mut record1 = HashMap::new();
    record1.insert("id".to_string(), InternalValue::Number(1.0));
    engine.execute(&query, record1).await.unwrap();

    // Execute second record
    let mut record2 = HashMap::new();
    record2.insert("id".to_string(), InternalValue::Number(2.0));
    engine.execute(&query, record2).await.unwrap();

    // Check both results
    let result1 = output_rx.recv().await.unwrap();
    // Handle both Number and Integer types due to type conversion
    match result1.get("id") {
        Some(InternalValue::Number(val)) => assert_eq!(*val, 1.0),
        Some(InternalValue::Integer(val)) => assert_eq!(*val, 1),
        _ => panic!("Expected id to be Number or Integer"),
    }

    let result2 = output_rx.recv().await.unwrap();
    // Handle both Number and Integer types due to type conversion
    match result2.get("id") {
        Some(InternalValue::Number(val)) => assert_eq!(*val, 2.0),
        Some(InternalValue::Integer(val)) => assert_eq!(*val, 2),
        _ => panic!("Expected id to be Number or Integer"),
    }

    // Verify stats
    let stats = engine.get_stats();
    assert_eq!(stats.total_records_processed, 2);
}

#[tokio::test]
async fn test_engine_start() {
    let (mut engine, _) = create_test_engine();

    // The new streamlined engine should start successfully
    let result = engine.start().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_record_processing_workflow() {
    let (mut engine, mut output_rx) = create_test_engine();

    // Test that the engine properly processes different data types
    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("string_val".to_string()),
            SelectField::Column("int_val".to_string()),
            SelectField::Column("bool_val".to_string()),
        ],
        from: StreamSource::Stream("test".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    let mut record = HashMap::new();
    record.insert(
        "string_val".to_string(),
        InternalValue::String("test".to_string()),
    );
    record.insert("int_val".to_string(), InternalValue::Integer(42));
    record.insert("bool_val".to_string(), InternalValue::Boolean(true));

    engine.execute(&query, record).await.unwrap();

    let result = output_rx.recv().await.unwrap();
    assert_eq!(
        result.get("string_val"),
        Some(&InternalValue::String("test".to_string()))
    );
    assert_eq!(result.get("int_val"), Some(&InternalValue::Integer(42)));
    assert_eq!(result.get("bool_val"), Some(&InternalValue::Boolean(true)));
}

#[tokio::test]
async fn test_empty_group_by_flush() {
    let (mut engine, _) = create_test_engine();

    // Flushing GROUP BY results when no queries are active should work
    let result = engine.flush_group_by_results().await;
    assert!(result.is_ok());
}
