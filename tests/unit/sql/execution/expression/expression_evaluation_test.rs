/*!
# Expression Evaluation Tests

Tests for SQL expression evaluation including arithmetic operations, boolean expressions,
and complex nested expressions.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use velostream::velostream::sql::execution::expression::evaluator::{
    ExpressionEvaluator, SelectAliasContext,
};
use velostream::velostream::sql::execution::types::system_columns;
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
        topic: None,
        key: None,
    }
}

#[tokio::test]
async fn test_arithmetic_expressions() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("amount".to_string())),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Literal(LiteralValue::Float(1.1))),
            },
            alias: Some("amount_with_tax".to_string()),
        }],
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
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
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("amount".to_string())),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expr::Literal(LiteralValue::Float(200.0))),
            },
            alias: Some("is_large_order".to_string()),
        }],
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
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
        distinct: false,
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
        key_fields: None,
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
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
            topic: None,
            key: None,
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
            topic: None,
            key: None,
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
            topic: None,
            key: None,
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

/// Test _EVENT_TIME system column fallback behavior
///
/// _EVENT_TIME should:
/// 1. Return event_time if explicitly set
/// 2. Fall back to _TIMESTAMP (processing time) if not set
///
/// This matches Flink/ksqlDB semantics where event time is always available.
#[test]
fn test_event_time_fallback_to_timestamp() {
    // Create a record WITHOUT explicit event_time (simulates default case)
    let record_without_event_time = {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        StreamRecord {
            fields,
            timestamp: 1640995200000, // Processing time: 2022-01-01 00:00:00 UTC
            offset: 0,
            partition: 0,
            event_time: None, // NOT SET - should fall back to timestamp
            headers: HashMap::new(),
            topic: None,
            key: None,
        }
    };

    // _event_time should return the processing timestamp when event_time is None
    let event_time_expr = Expr::Column(system_columns::EVENT_TIME.to_string());
    let result = ExpressionEvaluator::evaluate_expression_value(
        &event_time_expr,
        &record_without_event_time,
    );

    assert!(result.is_ok(), "Should evaluate without error");
    assert_eq!(
        result.unwrap(),
        FieldValue::Integer(1640995200000),
        "_EVENT_TIME should fall back to _TIMESTAMP when not explicitly set"
    );

    // Create a record WITH explicit event_time
    let record_with_event_time = {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(2));
        let explicit_event_time = chrono::DateTime::from_timestamp_millis(1640998800000).unwrap(); // 2022-01-01 01:00:00 UTC
        StreamRecord {
            fields,
            timestamp: 1640995200000, // Processing time: 2022-01-01 00:00:00 UTC
            offset: 1,
            partition: 0,
            event_time: Some(explicit_event_time), // EXPLICITLY SET
            headers: HashMap::new(),
            topic: None,
            key: None,
        }
    };

    // _EVENT_TIME should return the explicit event_time when set
    let result =
        ExpressionEvaluator::evaluate_expression_value(&event_time_expr, &record_with_event_time);

    assert!(result.is_ok(), "Should evaluate without error");
    assert_eq!(
        result.unwrap(),
        FieldValue::Integer(1640998800000),
        "_EVENT_TIME should return explicit event_time when set"
    );
}

/// Test that _TIMESTAMP and _EVENT_TIME are both always available
#[test]
fn test_system_timestamp_columns_always_available() {
    let record = {
        let mut fields = HashMap::new();
        fields.insert("data".to_string(), FieldValue::String("test".to_string()));
        StreamRecord {
            fields,
            timestamp: 1640995200000,
            offset: 42,
            partition: 3,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        }
    };

    // _timestamp should always be available
    let timestamp_expr = Expr::Column(system_columns::TIMESTAMP.to_string());
    let timestamp_result = ExpressionEvaluator::evaluate_expression_value(&timestamp_expr, &record);
    assert!(timestamp_result.is_ok());
    assert_eq!(
        timestamp_result.unwrap(),
        FieldValue::Integer(1640995200000)
    );

    // _event_time should always be available (falls back to _timestamp)
    let event_time_expr = Expr::Column(system_columns::EVENT_TIME.to_string());
    let event_time_result =
        ExpressionEvaluator::evaluate_expression_value(&event_time_expr, &record);
    assert!(event_time_result.is_ok());
    assert_eq!(
        event_time_result.unwrap(),
        FieldValue::Integer(1640995200000)
    );

    // _offset should always be available
    let offset_expr = Expr::Column(system_columns::OFFSET.to_string());
    let offset_result = ExpressionEvaluator::evaluate_expression_value(&offset_expr, &record);
    assert!(offset_result.is_ok());
    assert_eq!(offset_result.unwrap(), FieldValue::Integer(42));

    // _partition should always be available
    let partition_expr = Expr::Column(system_columns::PARTITION.to_string());
    let partition_result = ExpressionEvaluator::evaluate_expression_value(&partition_expr, &record);
    assert!(partition_result.is_ok());
    assert_eq!(partition_result.unwrap(), FieldValue::Integer(3));
}

/// Test _EVENT_TIME aliasing in SELECT - derived event time from user field
///
/// This tests the feature where users can set event time via SQL:
/// ```sql
/// SELECT trade_timestamp AS _EVENT_TIME, symbol, price FROM trades
/// ```
///
/// Use cases:
/// 1. Time simulation for testing
/// 2. Derived event time from nested/computed fields
/// 3. Event time extracted via UDF or expression
#[tokio::test]
async fn test_event_time_aliasing_in_select() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Query that aliases trade_timestamp as _EVENT_TIME
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![
            SelectField::AliasedColumn {
                column: "trade_timestamp".to_string(),
                alias: system_columns::EVENT_TIME.to_string(),
            },
            SelectField::Column("symbol".to_string()),
            SelectField::Column("price".to_string()),
        ],
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    // Create a record with a specific trade_timestamp
    let derived_event_time_ms: i64 = 1640998800000; // 2022-01-01 01:00:00 UTC
    let processing_time_ms: i64 = 1640995200000; // 2022-01-01 00:00:00 UTC (earlier)

    let mut fields = HashMap::new();
    fields.insert(
        "trade_timestamp".to_string(),
        FieldValue::Integer(derived_event_time_ms),
    );
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(150.0));

    let record = StreamRecord {
        fields,
        timestamp: processing_time_ms,
        offset: 1,
        partition: 0,
        event_time: None, // Not set at source level
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok(), "Query should execute: {:?}", result.err());

    let output = rx.try_recv().unwrap();

    // _event_time is a system column — it should NOT be in output fields.
    // It is stripped from the HashMap and only stored as record.event_time metadata.
    assert!(
        !output.fields.contains_key(system_columns::EVENT_TIME),
        "_event_time is a system column and should be stripped from output fields"
    );

    // Verify the event_time struct field was set from the alias
    assert!(
        output.event_time.is_some(),
        "event_time struct field should be set from _EVENT_TIME alias"
    );
    assert_eq!(
        output.event_time.unwrap().timestamp_millis(),
        derived_event_time_ms,
        "event_time should match the aliased trade_timestamp value"
    );
}

/// Test _EVENT_TIME aliasing preserves input event_time when not explicitly set
#[tokio::test]
async fn test_event_time_preserved_when_not_aliased() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Query that does NOT alias anything as _EVENT_TIME
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![
            SelectField::Column("symbol".to_string()),
            SelectField::Column("price".to_string()),
        ],
        key_fields: None,
        from: StreamSource::Stream("trades".to_string()),
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
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    // Create a record WITH explicit event_time set at source level
    let source_event_time = chrono::DateTime::from_timestamp_millis(1640998800000).unwrap();

    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(150.0));

    let record = StreamRecord {
        fields,
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
        event_time: Some(source_event_time), // Set at source level
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok(), "Query should execute: {:?}", result.err());

    let output = rx.try_recv().unwrap();

    // Verify event_time is preserved from input record
    assert!(
        output.event_time.is_some(),
        "event_time should be preserved from input"
    );
    assert_eq!(
        output.event_time.unwrap().timestamp_millis(),
        source_event_time.timestamp_millis(),
        "event_time should match input record's event_time"
    );
}
