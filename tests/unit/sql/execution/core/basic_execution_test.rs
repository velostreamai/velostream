/*!
# Basic Execution Tests

Tests for fundamental execution engine functionality including engine creation,
simple SELECT queries, and basic field selection.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record(
    id: i64,
    customer_id: i64,
    amount: f64,
    status: Option<&str>,
) -> HashMap<String, InternalValue> {
    let mut record = HashMap::new();
    record.insert("id".to_string(), InternalValue::Integer(id));
    record.insert(
        "customer_id".to_string(),
        InternalValue::Integer(customer_id),
    );
    record.insert("amount".to_string(), InternalValue::Number(amount));
    if let Some(s) = status {
        record.insert("status".to_string(), InternalValue::String(s.to_string()));
    }
    record.insert(
        "timestamp".to_string(),
        InternalValue::Integer(chrono::Utc::now().timestamp()),
    );
    record
}

#[tokio::test]
async fn test_engine_creation() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let _engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    // Basic creation test - engine should start without errors
    assert!(true); // Engine created successfully
}

#[tokio::test]
async fn test_execute_simple_select() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    // Check that result was sent to channel
    let output = rx.try_recv();
    assert!(output.is_ok());
}

#[tokio::test]
async fn test_execute_specific_columns() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Expression {
                expr: Expr::Column("id".to_string()),
                alias: None,
            },
            SelectField::Expression {
                expr: Expr::Column("amount".to_string()),
                alias: Some("total".to_string()),
            },
        ],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    // Check output contains only selected fields
    let output = rx.try_recv().unwrap();
    assert!(output.contains_key("id"));
    assert!(output.contains_key("total")); // Should use alias
    assert!(!output.contains_key("customer_id")); // Should not be included
}

#[tokio::test]
async fn test_execute_with_literals() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Expression {
                expr: Expr::Literal(LiteralValue::Integer(42)),
                alias: Some("constant".to_string()),
            },
            SelectField::Expression {
                expr: Expr::Literal(LiteralValue::String("test".to_string())),
                alias: Some("message".to_string()),
            },
        ],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    assert_eq!(output.get("constant"), Some(&InternalValue::Integer(42)));
    assert_eq!(
        output.get("message"),
        Some(&InternalValue::String("test".to_string()))
    );
}

#[tokio::test]
async fn test_missing_column_returns_null() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Column("nonexistent_column".to_string()),
            alias: None,
        }],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    // Missing columns should return NULL
    assert_eq!(output.get("nonexistent_column"), Some(&InternalValue::Null));
}

#[tokio::test]
async fn test_multiple_records_processing() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        aggregation_mode: None,
    };

    // Process multiple records
    for i in 1..=5 {
        let record = create_test_record(i, 100 + i, 100.0 * i as f64, Some("pending"));
        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());
    }

    // Should have 5 outputs
    for _ in 1..=5 {
        let output = rx.try_recv();
        assert!(output.is_ok());
    }

    // No more outputs
    let no_output = rx.try_recv();
    assert!(no_output.is_err());
}

#[tokio::test]
async fn test_null_value_handling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Column("status".to_string()),
            alias: None,
        }],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        window: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, None); // No status (null)

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    // The status field should either be absent or null
    match output.get("status") {
        None => {}                      // Field is absent, which is acceptable
        Some(InternalValue::Null) => {} // Field is explicitly null, which is also acceptable
        _ => panic!("Expected null or absent status field"),
    }
}
