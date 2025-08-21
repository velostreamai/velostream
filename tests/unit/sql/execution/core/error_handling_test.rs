/*!
# Error Handling Tests

Tests for proper error handling in SQL execution including type mismatches,
invalid operations, and edge cases.
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
async fn test_arithmetic_error_handling() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("status".to_string())), // String field
                op: BinaryOperator::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(10))),
            },
            alias: Some("invalid_operation".to_string()),
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
        aggregation_mode: None,
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute(&query, record).await;
    assert!(result.is_err()); // Should fail due to type mismatch
}
