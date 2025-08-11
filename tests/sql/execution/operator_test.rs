/*!
# Operator Tests

Tests for SQL operators including LIKE, NOT LIKE, and other specialized operators.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_like_operator() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx, serialization_format);

    // Create test record
    let mut record = HashMap::new();
    record.insert(
        "text_field".to_string(),
        InternalValue::String("Hello World".to_string()),
    );

    // Test cases for LIKE operator
    let test_cases = vec![
        ("Hello%", true),      // Matches prefix
        ("%World", true),      // Matches suffix
        ("%llo%", true),       // Matches substring
        ("Hello World", true), // Exact match
        ("hello%", false),     // Case sensitive no match
        ("Goodbye%", false),   // No match
    ];

    for (pattern, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("text_field".to_string())),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::Literal(LiteralValue::String(pattern.to_string()))),
                },
                alias: Some("like_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            where_clause: None,
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
        };

        let result = engine.execute(&query, record.clone()).await;
        assert!(
            result.is_ok(),
            "LIKE operator evaluation failed for pattern: {}",
            pattern
        );

        // Check if we got output (indicates match)
        let got_output = rx.try_recv().is_ok();
        assert_eq!(got_output, expected, "Pattern '{}' failed", pattern);
    }
}

#[tokio::test]
async fn test_not_like_operator() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx, serialization_format);

    // Create test record
    let mut record = HashMap::new();
    record.insert(
        "text_field".to_string(),
        InternalValue::String("Hello World".to_string()),
    );

    // Test cases for NOT LIKE operator
    let test_cases = vec![
        ("Hello%", false),      // Matches prefix (so NOT LIKE is false)
        ("%World", false),      // Matches suffix (so NOT LIKE is false)
        ("%llo%", false),       // Matches substring (so NOT LIKE is false)
        ("Hello World", false), // Matches exact (so NOT LIKE is false)
        ("hello%", true),       // Case sensitive no match (so NOT LIKE is true)
        ("Goodbye%", true),     // No match (so NOT LIKE is true)
    ];

    for (pattern, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("text_field".to_string())),
                    op: BinaryOperator::NotLike,
                    right: Box::new(Expr::Literal(LiteralValue::String(pattern.to_string()))),
                },
                alias: Some("not_like_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            where_clause: None,
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
        };

        let result = engine.execute(&query, record.clone()).await;
        assert!(
            result.is_ok(),
            "NOT LIKE operator evaluation failed for pattern: {}",
            pattern
        );

        // Check if we got output (indicates match)
        let got_output = rx.try_recv().is_ok();
        assert_eq!(
            got_output, expected,
            "NOT LIKE pattern '{}' failed",
            pattern
        );
    }
}

#[tokio::test]
async fn test_like_operator_edge_cases() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx, serialization_format);

    // Create test record with various types
    let mut record = HashMap::new();
    record.insert(
        "text_field".to_string(),
        InternalValue::String("Hello World".to_string()),
    );
    record.insert("null_field".to_string(), InternalValue::Null);
    record.insert(
        "number_field".to_string(),
        InternalValue::String("123".to_string()),
    );

    // Edge cases for text_field
    let test_cases = vec![
        // Empty pattern
        ("", false), // Empty pattern usually doesn't match
        // Just wildcards
        ("%", true),
        ("%%", true),
        // Complex patterns
        ("H%W%d", true),
        ("%H%W%d%", true),
    ];

    for (pattern, expected) in test_cases {
        let query = StreamingQuery::Select {
            fields: vec![SelectField::Expression {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("text_field".to_string())),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::Literal(LiteralValue::String(pattern.to_string()))),
                },
                alias: Some("like_result".to_string()),
            }],
            from: StreamSource::Stream("test".to_string()),
            where_clause: None,
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
        };

        let result = engine.execute(&query, record.clone()).await;
        assert!(
            result.is_ok(),
            "LIKE operator evaluation failed for pattern: {}",
            pattern
        );

        // Check if we got output (indicates match)
        let got_output = rx.try_recv().is_ok();
        assert_eq!(got_output, expected, "Pattern '{}' failed", pattern);
    }

    // Test NULL field - should not match anything
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("null_field".to_string())),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Literal(LiteralValue::String("%".to_string()))),
            },
            alias: Some("null_like_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        where_clause: None,
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    let result = engine.execute(&query, record.clone()).await;
    assert!(result.is_ok());
    // NULL LIKE anything should not produce output or should produce false
    let got_output = rx.try_recv().is_ok();
    assert_eq!(got_output, false);

    // Test numeric field with LIKE
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("number_field".to_string())),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Literal(LiteralValue::String("%3".to_string()))),
            },
            alias: Some("number_like_result".to_string()),
        }],
        from: StreamSource::Stream("test".to_string()),
        where_clause: None,
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
    };

    let result = engine.execute(&query, record.clone()).await;
    assert!(result.is_ok());
    // "123" LIKE "%3" should match
    let got_output = rx.try_recv().is_ok();
    assert_eq!(got_output, true);
}
