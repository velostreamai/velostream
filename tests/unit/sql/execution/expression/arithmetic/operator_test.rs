/*!
# Operator Tests

Tests for SQL operators including LIKE, NOT LIKE, and other specialized operators.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};

fn create_test_record(text_field: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "text_field".to_string(),
        FieldValue::String(text_field.to_string()),
    );

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

fn create_edge_case_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "text_field".to_string(),
        FieldValue::String("Hello World".to_string()),
    );
    fields.insert("null_field".to_string(), FieldValue::Null);
    fields.insert(
        "number_field".to_string(),
        FieldValue::String("123".to_string()),
    );

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

#[tokio::test]
async fn test_like_operator() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record
    let record = create_test_record("Hello World");

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
            distinct: false,
            fields: vec![SelectField::Expression {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("text_field".to_string())),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::Literal(LiteralValue::String(pattern.to_string()))),
                },
                alias: Some("like_result".to_string()),
            }],
            key_fields: None,
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,
            where_clause: None,
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(
            result.is_ok(),
            "LIKE operator evaluation failed for pattern: {}",
            pattern
        );

        // Check the boolean result value
        let output = rx.try_recv().unwrap();
        let like_result = output.fields.get("like_result").unwrap();
        match like_result {
            FieldValue::Boolean(result) => {
                assert_eq!(*result, expected, "Pattern '{}' failed", pattern);
            }
            _ => panic!("Expected boolean result for LIKE operation"),
        }
    }
}

#[tokio::test]
async fn test_not_like_operator() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record
    let record = create_test_record("Hello World");

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
            distinct: false,
            fields: vec![SelectField::Expression {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("text_field".to_string())),
                    op: BinaryOperator::NotLike,
                    right: Box::new(Expr::Literal(LiteralValue::String(pattern.to_string()))),
                },
                alias: Some("not_like_result".to_string()),
            }],
            key_fields: None,
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,
            where_clause: None,
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(
            result.is_ok(),
            "NOT LIKE operator evaluation failed for pattern: {}",
            pattern
        );

        // Check the boolean result value
        let output = rx.try_recv().unwrap();
        let not_like_result = output.fields.get("not_like_result").unwrap();
        match not_like_result {
            FieldValue::Boolean(result) => {
                assert_eq!(*result, expected, "NOT LIKE pattern '{}' failed", pattern);
            }
            _ => panic!("Expected boolean result for NOT LIKE operation"),
        }
    }
}

#[tokio::test]
async fn test_like_operator_edge_cases() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record with various types
    let record = create_edge_case_record();

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
            distinct: false,
            fields: vec![SelectField::Expression {
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column("text_field".to_string())),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::Literal(LiteralValue::String(pattern.to_string()))),
                },
                alias: Some("like_result".to_string()),
            }],
            key_fields: None,
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,
            where_clause: None,
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(
            result.is_ok(),
            "LIKE operator evaluation failed for pattern: {}",
            pattern
        );

        // Check the boolean result value
        let output = rx.try_recv().unwrap();
        let like_result = output.fields.get("like_result").unwrap();
        match like_result {
            FieldValue::Boolean(result) => {
                assert_eq!(*result, expected, "Pattern '{}' failed", pattern);
            }
            _ => panic!("Expected boolean result for LIKE operation"),
        }
    }

    // Test NULL field - should not match anything
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("null_field".to_string())),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Literal(LiteralValue::String("%".to_string()))),
            },
            alias: Some("null_like_result".to_string()),
        }],
        key_fields: None,
        from: StreamSource::Stream("test".to_string()),
        from_alias: None,
        where_clause: None,
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok());
    // NULL LIKE anything should return false
    let output = rx.try_recv().unwrap();
    let null_like_result = output.fields.get("null_like_result").unwrap();
    match null_like_result {
        FieldValue::Boolean(result) => {
            assert!(!(*result), "NULL LIKE anything should return false");
        }
        _ => panic!("Expected boolean result for NULL LIKE operation"),
    }

    // Test numeric field with LIKE
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::BinaryOp {
                left: Box::new(Expr::Column("number_field".to_string())),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Literal(LiteralValue::String("%3".to_string()))),
            },
            alias: Some("number_like_result".to_string()),
        }],
        key_fields: None,
        from: StreamSource::Stream("test".to_string()),
        from_alias: None,
        where_clause: None,
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok());
    // "123" LIKE "%3" should match
    let output = rx.try_recv().unwrap();
    let number_like_result = output.fields.get("number_like_result").unwrap();
    match number_like_result {
        FieldValue::Boolean(result) => {
            assert!(*result, "\"123\" LIKE \"%3\" should match");
        }
        _ => panic!("Expected boolean result for number LIKE operation"),
    }
}

// =============================================================================
// IN/NOT IN OPERATOR TESTS
// =============================================================================

#[tokio::test]
async fn test_in_operator_basic() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(2));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));
    fields.insert("amount".to_string(), FieldValue::Float(150.0));

    let record = StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Test cases for IN operator
    let test_cases = vec![
        // Integer IN tests
        ("id IN (1, 2, 3)", true),  // Matches
        ("id IN (4, 5, 6)", false), // No match
        ("id IN (2)", true),        // Single value match
        ("id IN (1)", false),       // Single value no match
        // String IN tests
        ("name IN ('test', 'foo', 'bar')", true), // Matches
        ("name IN ('hello', 'world')", false),    // No match
        ("name IN ('test')", true),               // Single string match
        // Float IN tests
        ("amount IN (100.0, 150.0, 200.0)", true), // Matches
        ("amount IN (99.9, 199.9)", false),        // No match
        ("amount IN (150.0)", true),               // Single float match
        // Mixed type IN tests (should work with type conversion)
        ("id IN (1, 2.0, 3)", true), // Int in mixed list
    ];

    for (query_str, expected) in test_cases {
        let query = StreamingQuery::Select {
            distinct: false,
            fields: vec![SelectField::Expression {
                expr: Expr::Column("id".to_string()), // Just select id to have some output
                alias: None,
            }],
            key_fields: None,
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(match query_str.split(" IN ").next().unwrap() {
                    "id" => Expr::Column("id".to_string()),
                    "name" => Expr::Column("name".to_string()),
                    "amount" => Expr::Column("amount".to_string()),
                    _ => Expr::Column("id".to_string()),
                }),
                op: BinaryOperator::In,
                right: Box::new(parse_in_list(query_str.split(" IN ").nth(1).unwrap())),
            }),
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(
            result.is_ok(),
            "IN operator evaluation failed for query: {}",
            query_str
        );

        // Check if we got output (indicates match)
        let got_output = rx.try_recv().is_ok();
        assert_eq!(got_output, expected, "Query '{}' failed", query_str);
    }
}

#[tokio::test]
async fn test_not_in_operator_basic() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(2));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));

    let record = StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Test NOT IN operator - opposite of IN results
    let test_cases = vec![
        ("id NOT IN (1, 2, 3)", false), // Does not match (value IS in list)
        ("id NOT IN (4, 5, 6)", true),  // Matches (value is NOT in list)
        ("id NOT IN (2)", false),       // Does not match (value IS in list)
        ("id NOT IN (1)", true),        // Matches (value is NOT in list)
        ("name NOT IN ('test', 'foo')", false), // Does not match
        ("name NOT IN ('hello', 'world')", true), // Matches
    ];

    for (query_desc, expected) in test_cases {
        let parts: Vec<&str> = query_desc.split(" NOT IN ").collect();
        let column = parts[0];
        let list_str = parts[1];

        let query = StreamingQuery::Select {
            distinct: false,
            fields: vec![SelectField::Expression {
                expr: Expr::Column("id".to_string()),
                alias: None,
            }],
            key_fields: None,
            from: StreamSource::Stream("test".to_string()),
            from_alias: None,
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(column.to_string())),
                op: BinaryOperator::NotIn,
                right: Box::new(parse_in_list(list_str)),
            }),
            joins: None,
            group_by: None,
            having: None,
            window: None,
            order_by: None,
            limit: None,
            emit_mode: None,
            properties: None,
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
        };

        let result = engine.execute_with_record(&query, &record).await;
        assert!(
            result.is_ok(),
            "NOT IN operator evaluation failed for query: {}",
            query_desc
        );

        let got_output = rx.try_recv().is_ok();
        assert_eq!(got_output, expected, "Query '{}' failed", query_desc);
    }
}

#[tokio::test]
async fn test_in_operator_with_null_values() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record with NULL value
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("nullable_field".to_string(), FieldValue::Null);

    let record = StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Test NULL IN list (should never match)
    let query = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::Column("id".to_string()),
            alias: None,
        }],
        key_fields: None,
        from: StreamSource::Stream("test".to_string()),
        from_alias: None,
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("nullable_field".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::String("test".to_string())),
                Expr::Literal(LiteralValue::Null),
            ])),
        }),
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok());

    // NULL IN anything should not match
    let got_output = rx.try_recv().is_ok();
    assert!(!got_output, "NULL IN list should not match");

    // Test NOT IN with NULL - should also not match
    let query_not_in = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::Column("id".to_string()),
            alias: None,
        }],
        key_fields: None,
        from: StreamSource::Stream("test".to_string()),
        from_alias: None,
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("nullable_field".to_string())),
            op: BinaryOperator::NotIn,
            right: Box::new(Expr::List(vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::String("test".to_string())),
            ])),
        }),
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let result_not_in = engine.execute_with_record(&query_not_in, &record).await;
    assert!(result_not_in.is_ok());

    // NULL NOT IN anything should also not match
    let got_output_not_in = rx.try_recv().is_ok();
    assert!(!got_output_not_in, "NULL NOT IN list should not match");
}

#[tokio::test]
async fn test_in_operator_edge_cases() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test record
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(5));
    fields.insert("name".to_string(), FieldValue::String("hello".to_string()));

    let record = StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Test empty list (should never match)
    // Note: This might not be parseable, but if it is, should never match

    // Test large list
    let large_list = (1..=100)
        .map(|i| Expr::Literal(LiteralValue::Integer(i)))
        .collect();
    let query_large = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::Column("id".to_string()),
            alias: None,
        }],
        key_fields: None,
        from: StreamSource::Stream("test".to_string()),
        from_alias: None,
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(large_list)),
        }),
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let result_large = engine.execute_with_record(&query_large, &record).await;
    assert!(result_large.is_ok());

    // Should match since 5 is in 1..=100
    let got_output_large = rx.try_recv().is_ok();
    assert!(got_output_large, "Large list should match");

    // Test with duplicate values in list (should still work)
    let query_duplicates = StreamingQuery::Select {
        distinct: false,
        fields: vec![SelectField::Expression {
            expr: Expr::Column("id".to_string()),
            alias: None,
        }],
        key_fields: None,
        from: StreamSource::Stream("test".to_string()),
        from_alias: None,
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::In,
            right: Box::new(Expr::List(vec![
                Expr::Literal(LiteralValue::Integer(5)),
                Expr::Literal(LiteralValue::Integer(5)),
                Expr::Literal(LiteralValue::Integer(5)),
            ])),
        }),
        joins: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
        job_mode: None,
        batch_size: None,
        num_partitions: None,
        partitioning_strategy: None,
    };

    let result_duplicates = engine.execute_with_record(&query_duplicates, &record).await;
    assert!(result_duplicates.is_ok());

    // Should match
    let got_output_duplicates = rx.try_recv().is_ok();
    assert!(
        got_output_duplicates,
        "Duplicate values in list should still match"
    );
}

// Helper function to parse simple IN lists for testing
// This is a simplified parser just for testing - the real parser handles this
fn parse_in_list(list_str: &str) -> Expr {
    // Remove parentheses and split by comma
    let inner = list_str
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')');
    let items: Vec<Expr> = inner
        .split(',')
        .map(|item| {
            let trimmed = item.trim();
            if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
                // String literal
                let content = &trimmed[1..trimmed.len() - 1];
                Expr::Literal(LiteralValue::String(content.to_string()))
            } else if trimmed.contains('.') {
                // Float literal
                let value: f64 = trimmed.parse().expect("Invalid float in test");
                Expr::Literal(LiteralValue::Float(value))
            } else {
                // Integer literal
                let value: i64 = trimmed.parse().expect("Invalid integer in test");
                Expr::Literal(LiteralValue::Integer(value))
            }
        })
        .collect();

    Expr::List(items)
}
