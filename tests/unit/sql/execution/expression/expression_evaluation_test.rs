/*!
# Expression Evaluation Tests

Tests for SQL expression evaluation including arithmetic operations, boolean expressions,
and complex nested expressions.
*/

use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
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
async fn test_arithmetic_expressions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("amount".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Literal(LiteralValue::Float(1.1))),
            },
            alias: Some("amount_with_tax".to_string()),
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

    let record = create_test_record(1, 100, 100.0, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    if let Some(FieldValue::Float(result_float)) = output.fields.get("amount_with_tax") {
        assert!((result_float - 110.0).abs() < 0.001); // 100.0 * 1.1 = 110.0
    } else {
        panic!("Expected numeric result for arithmetic operation");
    }
}

#[tokio::test]
async fn test_boolean_expressions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("amount".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Float(200.0))),
            },
            alias: Some("is_large_order".to_string()),
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
    assert_eq!(
        output.fields.get("is_large_order"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_complex_expression_evaluation() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Complex expression: (amount * 1.1) + 10
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Column("amount".to_string())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Literal(LiteralValue::Float(1.1))),
                }),
                op: BinaryOperator::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(10))),
            },
            alias: Some("complex_calc".to_string()),
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

    let record = create_test_record(1, 100, 100.0, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    if let Some(FieldValue::Float(result_float)) = output.fields.get("complex_calc") {
        assert!((result_float - 120.0).abs() < 0.001); // (100.0 * 1.1) + 10 = 120.0
    } else {
        panic!("Expected numeric result for complex expression");
    }
}
