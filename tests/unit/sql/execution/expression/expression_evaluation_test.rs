/*!
# Expression Evaluation Tests

Tests for SQL expression evaluation including arithmetic operations, boolean expressions,
and complex nested expressions.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
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
async fn test_arithmetic_expressions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

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
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 100.0, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    if let Some(InternalValue::Number(result_float)) = output.get("amount_with_tax") {
        assert!((result_float - 110.0).abs() < 0.001); // 100.0 * 1.1 = 110.0
    } else {
        panic!("Expected numeric result for arithmetic operation");
    }
}

#[tokio::test]
async fn test_boolean_expressions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

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
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    assert_eq!(
        output.get("is_large_order"),
        Some(&InternalValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_complex_expression_evaluation() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

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
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 100.0, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    if let Some(InternalValue::Number(result_float)) = output.get("complex_calc") {
        assert!((result_float - 120.0).abs() < 0.001); // (100.0 * 1.1) + 10 = 120.0
    } else {
        panic!("Expected numeric result for complex expression");
    }
}
