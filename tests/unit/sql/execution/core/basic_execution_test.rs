/*!
# Basic Execution Tests

Tests for fundamental execution engine functionality including engine creation,
simple SELECT queries, and basic field selection.
*/

use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::ast::{
    Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record(
    id: i64,
    customer_id: i64,
    amount: f64,
    status: Option<&str>,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    if let Some(s) = status {
        fields.insert("status".to_string(), FieldValue::String(s.to_string()));
    } else {
        fields.insert("status".to_string(), FieldValue::Null);
    }
    fields.insert(
        "timestamp".to_string(),
        FieldValue::Integer(chrono::Utc::now().timestamp()),
    );

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: id,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

#[tokio::test]
async fn test_engine_creation() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let _engine = StreamExecutionEngine::new(tx);

    // Basic creation test - engine should start without errors
    assert!(true); // Engine created successfully
}

#[tokio::test]
async fn test_execute_simple_select() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

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
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    // Check that result was sent to channel
    let output = rx.try_recv();
    assert!(output.is_ok());
}

#[tokio::test]
async fn test_execute_specific_columns() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

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
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    // Check output contains only selected fields
    let output = rx.try_recv().unwrap();
    assert!(output.fields.contains_key("id"));
    assert!(output.fields.contains_key("total")); // Should use alias
    assert!(!output.fields.contains_key("customer_id")); // Should not be included
}

#[tokio::test]
async fn test_execute_with_literals() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

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
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    assert_eq!(
        output.fields.get("constant"),
        Some(&FieldValue::Integer(42))
    );
    assert_eq!(
        output.fields.get("message"),
        Some(&FieldValue::String("test".to_string()))
    );
}

#[tokio::test]
async fn test_missing_column_returns_null() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

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
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    // Missing columns should return NULL
    assert_eq!(
        output.fields.get("nonexistent_column"),
        Some(&FieldValue::Null)
    );
}

#[tokio::test]
async fn test_multiple_records_processing() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

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
        emit_mode: None,
        properties: None,
    };

    // Process multiple records
    for i in 1..=5 {
        let record = create_test_record(i, 100 + i, 100.0 * i as f64, Some("pending"));
        let result = engine.execute_with_record(&query, record).await;
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
    let mut engine = StreamExecutionEngine::new(tx);

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
        emit_mode: None,
        properties: None,
    };

    let record = create_test_record(1, 100, 299.99, None); // No status (null)

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    // The status field should either be absent or null
    match output.fields.get("status") {
        None => {}                   // Field is absent, which is acceptable
        Some(FieldValue::Null) => {} // Field is explicitly null, which is also acceptable
        _ => panic!("Expected null or absent status field"),
    }
}
