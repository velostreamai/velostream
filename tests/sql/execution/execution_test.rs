use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    BinaryOperator, DataType, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
    WindowSpec,
};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::schema::{FieldDefinition, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            FieldDefinition::required("id".to_string(), DataType::Integer),
            FieldDefinition::required("customer_id".to_string(), DataType::Integer),
            FieldDefinition::required("amount".to_string(), DataType::Float),
            FieldDefinition::optional("status".to_string(), DataType::String),
            FieldDefinition::required("timestamp".to_string(), DataType::Timestamp),
        ])
    }

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
        };

        let record = create_test_record(1, 100, 299.99, Some("pending"));

        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        // Missing columns should return NULL
        assert_eq!(output.get("nonexistent_column"), Some(&InternalValue::Null));
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
        };

        let record = create_test_record(1, 100, 299.99, Some("pending"));

        let result = engine.execute(&query, record).await;
        assert!(result.is_err()); // Should fail due to type mismatch
    }

    #[tokio::test]
    async fn test_windowed_execution_tumbling() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
                alias: Some("total_amount".to_string()),
            }],
            from: StreamSource::Stream("orders".to_string()),
            joins: None,
            where_clause: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(300), // 5 minutes
                time_column: Some("timestamp".to_string()),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record(1, 100, 299.99, Some("pending"));

        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv();
        assert!(output.is_ok());
    }

    #[tokio::test]
    async fn test_sliding_window_execution() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
                alias: Some("avg_amount".to_string()),
            }],
            from: StreamSource::Stream("orders".to_string()),
            joins: None,
            where_clause: None,
            window: Some(WindowSpec::Sliding {
                size: Duration::from_secs(600),    // 10 minutes
                advance: Duration::from_secs(300), // 5 minutes
                time_column: Some("timestamp".to_string()),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record(1, 100, 299.99, Some("pending"));

        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv();
        assert!(output.is_ok());
    }

    #[tokio::test]
    async fn test_session_window_execution() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::Column("id".to_string())],
                },
                alias: Some("session_count".to_string()),
            }],
            from: StreamSource::Stream("orders".to_string()),
            joins: None,
            where_clause: None,
            window: Some(WindowSpec::Session {
                gap: Duration::from_secs(30), // 30 seconds
                partition_by: vec!["customer_id".to_string()],
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record(1, 100, 299.99, Some("pending"));

        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv();
        assert!(output.is_ok());
    }

    #[tokio::test]
    async fn test_aggregation_functions() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));

        let query = StreamingQuery::Select {
            fields: vec![
                SelectField::Expression {
                    expr: Expr::Function {
                        name: "COUNT".to_string(),
                        args: vec![Expr::Literal(LiteralValue::Integer(1))],
                    },
                    alias: Some("count".to_string()),
                },
                SelectField::Expression {
                    expr: Expr::Function {
                        name: "MAX".to_string(),
                        args: vec![Expr::Column("amount".to_string())],
                    },
                    alias: Some("max_amount".to_string()),
                },
                SelectField::Expression {
                    expr: Expr::Function {
                        name: "MIN".to_string(),
                        args: vec![Expr::Column("amount".to_string())],
                    },
                    alias: Some("min_amount".to_string()),
                },
            ],
            from: StreamSource::Stream("orders".to_string()),
            joins: None,
            where_clause: None,
            window: Some(WindowSpec::Tumbling {
                size: Duration::from_secs(60),
                time_column: Some("timestamp".to_string()),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
        };

        let record = create_test_record(1, 100, 299.99, Some("pending"));

        let result = engine.execute(&query, record).await;
        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();
        assert!(output.contains_key("count"));
        assert!(output.contains_key("max_amount"));
        assert!(output.contains_key("min_amount"));
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
}
