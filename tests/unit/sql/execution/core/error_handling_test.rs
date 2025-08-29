/*!
# Error Handling Tests

Tests for proper error handling in SQL execution including type mismatches,
invalid operations, and edge cases.
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
        headers: HashMap::new(),
    }
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
    };

    let record = create_test_record(1, 100, 299.99, Some("pending"));

    let result = engine.execute_with_record(&query, record).await;
    assert!(result.is_err()); // Should fail due to type mismatch
}
