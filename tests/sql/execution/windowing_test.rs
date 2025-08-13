/*!
# Windowing Tests

Tests for windowed query execution including tumbling, sliding, and session windows.
Tests window-based aggregations and time-based processing.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    Expr, SelectField, StreamSource, StreamingQuery, WindowSpec,
};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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
                    args: vec![Expr::Literal(
                        ferrisstreams::ferris::sql::ast::LiteralValue::Integer(1),
                    )],
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
