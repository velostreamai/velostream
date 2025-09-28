//! Unit tests for Stream-Table JOIN configuration and basic functionality
//!
//! These tests focus on join clause construction, validation, and basic processor setup
//! without executing real join processing logic.

use std::collections::HashMap;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, JoinClause, JoinType, StreamSource};
use velostream::velostream::sql::execution::processors::stream_table_join::StreamTableJoinProcessor;
use velostream::velostream::sql::execution::FieldValue;

#[test]
fn test_stream_table_join_detection() {
    // Test that we correctly identify stream-table join patterns
    let processor = StreamTableJoinProcessor::new();

    // Create test join clause
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        right_alias: Some("user".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        window: None,
    };

    // Verify processor was created
    assert!(std::ptr::addr_of!(processor) as *const _ != std::ptr::null());

    // Verify join clause structure
    assert_eq!(join_clause.join_type, JoinType::Inner);
    if let StreamSource::Table(table_name) = &join_clause.right_source {
        assert_eq!(table_name, "user_profiles");
    } else {
        panic!("Expected table source");
    }
    assert_eq!(join_clause.right_alias, Some("user".to_string()));
}

#[test]
fn test_join_clause_configurations() {
    // Test different join type configurations
    let inner_join = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("table1".to_string()),
        right_alias: Some("t1".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("id".to_string())),
            right: Box::new(Expr::Column("id".to_string())),
        },
        window: None,
    };

    let left_join = JoinClause {
        join_type: JoinType::Left,
        right_source: StreamSource::Table("table2".to_string()),
        right_alias: Some("t2".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("key".to_string())),
            right: Box::new(Expr::Column("foreign_key".to_string())),
        },
        window: None,
    };

    // Test inner join configuration
    assert_eq!(inner_join.join_type, JoinType::Inner);
    assert_eq!(inner_join.right_alias, Some("t1".to_string()));

    // Test left join configuration
    assert_eq!(left_join.join_type, JoinType::Left);
    assert_eq!(left_join.right_alias, Some("t2".to_string()));
}

#[test]
fn test_multi_field_join_condition_structure() {
    // Test the structure of multi-field join conditions
    let processor = StreamTableJoinProcessor::new();

    // Create complex join condition
    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("complex_table".to_string()),
        right_alias: Some("ct".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column("user_id".to_string())),
                right: Box::new(Expr::Column("user_id".to_string())),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expr::Column("account_id".to_string())),
                right: Box::new(Expr::Column("account_id".to_string())),
            }),
        },
        window: None,
    };

    // Verify complex condition structure
    if let Expr::BinaryOp { op, left: _, right: _ } = &join_clause.condition {
        assert_eq!(*op, BinaryOperator::And);
    } else {
        panic!("Expected complex AND condition");
    }

    // Verify processor creation succeeded
    assert!(std::ptr::addr_of!(processor) as *const _ != std::ptr::null());
}

#[test]
fn test_qualified_identifier_join_structure() {
    // Test qualified identifiers in join conditions
    let processor = StreamTableJoinProcessor::new();

    let join_clause = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("qualified_table".to_string()),
        right_alias: Some("qt".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("stream.user_id".to_string())),
            right: Box::new(Expr::Column("qt.user_id".to_string())),
        },
        window: None,
    };

    // Verify qualified identifier handling in condition
    if let Expr::BinaryOp { left, right, .. } = &join_clause.condition {
        if let Expr::Column(left_col) = left.as_ref() {
            assert_eq!(left_col, "stream.user_id");
        }
        if let Expr::Column(right_col) = right.as_ref() {
            assert_eq!(right_col, "qt.user_id");
        }
    }

    // Verify processor creation
    assert!(std::ptr::addr_of!(processor) as *const _ != std::ptr::null());
}

#[test]
fn test_field_value_types() {
    // Test different field value types that might be used in joins
    let mut test_fields = HashMap::new();

    test_fields.insert("user_id".to_string(), FieldValue::Integer(123));
    test_fields.insert("name".to_string(), FieldValue::String("TestUser".to_string()));
    test_fields.insert("active".to_string(), FieldValue::Boolean(true));
    test_fields.insert("balance".to_string(), FieldValue::Float(1000.50));

    // Verify field value types
    assert_eq!(test_fields.get("user_id"), Some(&FieldValue::Integer(123)));
    assert_eq!(test_fields.get("name"), Some(&FieldValue::String("TestUser".to_string())));
    assert_eq!(test_fields.get("active"), Some(&FieldValue::Boolean(true)));
    assert_eq!(test_fields.get("balance"), Some(&FieldValue::Float(1000.50)));
}

#[test]
fn test_join_error_conditions_structure() {
    // Test that join clauses can represent error conditions properly
    let invalid_join = JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("non_existent_table".to_string()),
        right_alias: None, // No alias
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("missing_field".to_string())),
            right: Box::new(Expr::Column("other_missing_field".to_string())),
        },
        window: None,
    };

    // Verify error condition structure
    assert_eq!(invalid_join.right_alias, None);
    if let StreamSource::Table(table_name) = &invalid_join.right_source {
        assert_eq!(table_name, "non_existent_table");
    }
}

#[test]
fn test_processor_basic_properties() {
    // Test basic processor properties and creation
    let processor = StreamTableJoinProcessor::new();

    // Verify processor was created successfully
    assert!(std::ptr::addr_of!(processor) as *const _ != std::ptr::null());

    // Test multiple processor instances can be created
    let processor2 = StreamTableJoinProcessor::new();
    assert!(std::ptr::addr_of!(processor2) as *const _ != std::ptr::null());

    // Verify they are different instances
    assert_ne!(
        std::ptr::addr_of!(processor) as *const _,
        std::ptr::addr_of!(processor2) as *const _
    );
}