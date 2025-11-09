/*!
# Expression Evaluation Tests

Tests for SQL expression evaluation including arithmetic operations, boolean expressions,
and complex nested expressions.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};

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
        from_alias: None,
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

    let result = engine.execute_with_record(&query, &record).await;
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
        from_alias: None,
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

    let result = engine.execute_with_record(&query, &record).await;
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
        from_alias: None,
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

    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok());

    let output = rx.try_recv().unwrap();
    if let Some(FieldValue::Float(result_float)) = output.fields.get("complex_calc") {
        assert!((result_float - 120.0).abs() < 0.001); // (100.0 * 1.1) + 10 = 120.0
    } else {
        panic!("Expected numeric result for complex expression");
    }
}

#[tokio::test]
async fn test_in_operator_with_alias_in_case_when() {
    use velostream::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
    use velostream::velostream::sql::execution::expression::evaluator::SelectAliasContext;

    // Test IN operator with column alias in CASE WHEN
    // This tests the fix for: spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
    let record = {
        let mut fields = HashMap::new();
        fields.insert("price".to_string(), FieldValue::Float(100.0));
        fields.insert("volume".to_string(), FieldValue::Integer(1000));
        StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
    };

    // Create alias context with spike_classification = "EXTREME_SPIKE"
    let mut alias_context = SelectAliasContext::new();
    alias_context.add_alias(
        "spike_classification".to_string(),
        FieldValue::String("EXTREME_SPIKE".to_string()),
    );

    // Test IN operator: spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
    let in_expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("spike_classification".to_string())),
        op: BinaryOperator::In,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::String("EXTREME_SPIKE".to_string())),
            Expr::Literal(LiteralValue::String("STATISTICAL_ANOMALY".to_string())),
        ])),
    };

    let result = ExpressionEvaluator::evaluate_expression_value_with_alias_context(
        &in_expr,
        &record,
        &alias_context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(true));
}

#[tokio::test]
async fn test_in_operator_with_alias_false_case() {
    use velostream::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
    use velostream::velostream::sql::execution::expression::evaluator::SelectAliasContext;

    let record = {
        let mut fields = HashMap::new();
        fields.insert("price".to_string(), FieldValue::Float(100.0));
        StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
    };

    // Create alias context with spike_classification = "NORMAL"
    let mut alias_context = SelectAliasContext::new();
    alias_context.add_alias(
        "spike_classification".to_string(),
        FieldValue::String("NORMAL".to_string()),
    );

    // Test IN operator: spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
    let in_expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("spike_classification".to_string())),
        op: BinaryOperator::In,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::String("EXTREME_SPIKE".to_string())),
            Expr::Literal(LiteralValue::String("STATISTICAL_ANOMALY".to_string())),
        ])),
    };

    let result = ExpressionEvaluator::evaluate_expression_value_with_alias_context(
        &in_expr,
        &record,
        &alias_context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(false));
}

#[tokio::test]
async fn test_not_in_operator_with_alias() {
    use velostream::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
    use velostream::velostream::sql::execution::expression::evaluator::SelectAliasContext;

    let record = {
        let mut fields = HashMap::new();
        fields.insert("price".to_string(), FieldValue::Float(100.0));
        StreamRecord {
            fields,
            timestamp: chrono::Utc::now().timestamp_millis(),
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        }
    };

    // Create alias context
    let mut alias_context = SelectAliasContext::new();
    alias_context.add_alias(
        "spike_classification".to_string(),
        FieldValue::String("NORMAL".to_string()),
    );

    // Test NOT IN operator: spike_classification NOT IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY')
    let not_in_expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("spike_classification".to_string())),
        op: BinaryOperator::NotIn,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::String("EXTREME_SPIKE".to_string())),
            Expr::Literal(LiteralValue::String("STATISTICAL_ANOMALY".to_string())),
        ])),
    };

    let result = ExpressionEvaluator::evaluate_expression_value_with_alias_context(
        &not_in_expr,
        &record,
        &alias_context,
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), FieldValue::Boolean(true));
}
