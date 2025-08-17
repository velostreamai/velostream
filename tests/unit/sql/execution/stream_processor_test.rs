//! Unit tests for the StreamProcessor component
//!
//! These tests verify the streaming query processing functionality, ensuring proper
//! orchestration of modular processor components for query execution.

use std::collections::HashMap;

use ferrisstreams::ferris::sql::ast::{
    BinaryOperator, Expr, LiteralValue, SelectField, StreamSource, StreamingQuery,
};
use ferrisstreams::ferris::sql::execution::stream_processor::{ProcessingResult, StreamProcessor};
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};

/// Helper function to create a test stream record
fn create_test_record(id: i64, name: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));

    StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
    }
}

#[test]
fn test_simple_select() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("users".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();
    let record = create_test_record(1, "Alice");

    let result = processor.process_record(record).unwrap();
    match result {
        ProcessingResult::Record(output) => {
            assert!(output.fields.contains_key("id"));
            assert_eq!(output.fields.get("id"), Some(&FieldValue::Integer(1)));
        }
        _ => panic!("Expected record output"),
    }
}

#[test]
fn test_filter() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("users".to_string()),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("id".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(5))),
        }),
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();

    // Record that should pass filter
    let record1 = create_test_record(10, "Alice");
    let result1 = processor.process_record(record1).unwrap();
    assert!(matches!(result1, ProcessingResult::Record(_)));

    // Record that should be filtered out
    let record2 = create_test_record(3, "Bob");
    let result2 = processor.process_record(record2).unwrap();
    assert!(matches!(result2, ProcessingResult::NoOutput));
}

#[test]
fn test_limit() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("users".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: Some(2),
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();

    // First record
    let record1 = create_test_record(1, "Alice");
    let result1 = processor.process_record(record1).unwrap();
    assert!(matches!(result1, ProcessingResult::Record(_)));
    assert!(!processor.is_completed());

    // Second record
    let record2 = create_test_record(2, "Bob");
    let result2 = processor.process_record(record2).unwrap();
    assert!(matches!(result2, ProcessingResult::Record(_)));
    assert!(processor.is_completed());

    // Third record should return Completed
    let record3 = create_test_record(3, "Charlie");
    let result3 = processor.process_record(record3).unwrap();
    assert!(matches!(result3, ProcessingResult::Completed));
}

#[test]
fn test_projection_with_expressions() {
    let query = StreamingQuery::Select {
        fields: vec![
            SelectField::Column("name".to_string()),
            SelectField::AliasedColumn {
                column: "id".to_string(),
                alias: "user_id".to_string(),
            },
            SelectField::Expression {
                expr: Expr::Literal(LiteralValue::String("active".to_string())),
                alias: Some("status".to_string()),
            },
        ],
        from: StreamSource::Stream("users".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();
    let record = create_test_record(42, "Alice");

    let result = processor.process_record(record).unwrap();
    match result {
        ProcessingResult::Record(output) => {
            assert_eq!(
                output.fields.get("name"),
                Some(&FieldValue::String("Alice".to_string()))
            );
            assert_eq!(output.fields.get("user_id"), Some(&FieldValue::Integer(42)));
            assert_eq!(
                output.fields.get("status"),
                Some(&FieldValue::String("active".to_string()))
            );
            assert!(!output.fields.contains_key("id")); // Original "id" should not be present
        }
        _ => panic!("Expected record output"),
    }
}

#[test]
fn test_wildcard_projection() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("users".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();
    let record = create_test_record(99, "Bob");

    let result = processor.process_record(record).unwrap();
    match result {
        ProcessingResult::Record(output) => {
            // Wildcard should include all fields
            assert_eq!(output.fields.get("id"), Some(&FieldValue::Integer(99)));
            assert_eq!(
                output.fields.get("name"),
                Some(&FieldValue::String("Bob".to_string()))
            );
            assert_eq!(output.fields.len(), 2);
        }
        _ => panic!("Expected record output"),
    }
}

#[test]
fn test_complex_filter_expressions() {
    // Test with a more complex WHERE clause
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("users".to_string()),
        where_clause: Some(Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expr::Literal(LiteralValue::Integer(5))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("id".to_string())),
                op: BinaryOperator::LessThan,
                right: Box::new(Expr::Literal(LiteralValue::Integer(15))),
            }),
        }),
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();

    // Test records with different id values
    let test_cases = vec![
        (3, false),  // Should be filtered out (< 5)
        (5, true),   // Should pass (>= 5 and < 15)
        (10, true),  // Should pass (>= 5 and < 15)
        (15, false), // Should be filtered out (>= 15)
        (20, false), // Should be filtered out (>= 15)
    ];

    for (id, should_pass) in test_cases {
        let record = create_test_record(id, &format!("User{}", id));
        let result = processor.process_record(record).unwrap();

        if should_pass {
            assert!(
                matches!(result, ProcessingResult::Record(_)),
                "Record with id {} should pass filter",
                id
            );
        } else {
            assert!(
                matches!(result, ProcessingResult::NoOutput),
                "Record with id {} should be filtered out",
                id
            );
        }
    }
}

#[test]
fn test_processor_statistics() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("data".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();

    // Check initial stats
    let initial_stats = processor.get_stats();
    assert_eq!(initial_stats.records_processed, 0);
    assert!(!initial_stats.is_windowed);
    assert!(!initial_stats.has_aggregation);
    assert!(!initial_stats.has_joins);

    // Process a record
    let record = create_test_record(1, "Test");
    processor.process_record(record).unwrap();

    // Check updated stats
    let updated_stats = processor.get_stats();
    assert_eq!(updated_stats.records_processed, 1);
}

#[test]
fn test_empty_group_by_results() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("data".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };

    let mut processor = StreamProcessor::new(query).unwrap();

    // Emitting GROUP BY results when there's no GROUP BY should return empty vec
    let results = processor.emit_group_by_results().unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_processor_creation_with_different_query_types() {
    // Test that processor can be created with various query configurations

    // Simple SELECT
    let simple_query = StreamingQuery::Select {
        fields: vec![SelectField::Column("id".to_string())],
        from: StreamSource::Stream("data".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };
    assert!(StreamProcessor::new(simple_query).is_ok());

    // SELECT with aggregation
    let agg_query = StreamingQuery::Select {
        fields: vec![SelectField::Expression {
            expr: Expr::Function {
                name: "COUNT".to_string(),
                args: vec![],
            },
            alias: Some("total".to_string()),
        }],
        from: StreamSource::Stream("events".to_string()),
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        joins: None,
        limit: None,
        order_by: None,
    };
    assert!(StreamProcessor::new(agg_query).is_ok());
}
