//! Unit tests for field validator runtime validation
//!
//! Tests for FieldValidator, FieldValidationError, and ValidationContext.
//! These tests verify that field existence and type compatibility are properly validated at runtime.

use std::collections::HashMap;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue, UnaryOperator};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::validation::{
    FieldValidationError, FieldValidator, ValidationContext,
};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(99.99));
    fields.insert("active".to_string(), FieldValue::Boolean(true));
    StreamRecord::new(fields)
}

#[test]
fn test_validate_field_exists_success() {
    let record = create_test_record();
    let result =
        FieldValidator::validate_field_exists(&record, "id", ValidationContext::SelectClause);
    assert!(result.is_ok());
}

#[test]
fn test_validate_field_exists_missing() {
    let record = create_test_record();
    let result =
        FieldValidator::validate_field_exists(&record, "missing", ValidationContext::SelectClause);
    assert!(matches!(
        result,
        Err(FieldValidationError::FieldNotFound { field_name, .. }) if field_name == "missing"
    ));
}

#[test]
fn test_validate_multiple_fields_exist_all_found() {
    let record = create_test_record();
    let result = FieldValidator::validate_fields_exist(
        &record,
        &["id", "name", "price"],
        ValidationContext::GroupBy,
    );
    assert!(result.is_ok());
}

#[test]
fn test_validate_multiple_fields_exist_some_missing() {
    let record = create_test_record();
    let result = FieldValidator::validate_fields_exist(
        &record,
        &["id", "missing1", "missing2"],
        ValidationContext::GroupBy,
    );
    assert!(matches!(
        result,
        Err(FieldValidationError::MultipleFieldsMissing { field_names, .. }) if field_names.len() == 2
    ));
}

#[test]
fn test_validate_field_type_numeric_success() {
    let value = FieldValue::Float(99.99);
    let result = FieldValidator::validate_field_type(
        "price",
        &value,
        "numeric",
        ValidationContext::Aggregation,
        |v| v.is_numeric(),
    );
    assert!(result.is_ok());
}

#[test]
fn test_validate_field_type_mismatch() {
    let value = FieldValue::String("not_numeric".to_string());
    let result = FieldValidator::validate_field_type(
        "price",
        &value,
        "numeric",
        ValidationContext::Aggregation,
        |v| v.is_numeric(),
    );
    match result {
        Err(FieldValidationError::TypeMismatch {
            expected_type,
            actual_type,
            ..
        }) => {
            assert_eq!(expected_type, "numeric");
            assert_eq!(actual_type, "STRING");
        }
        _ => panic!("Expected TypeMismatch error"),
    }
}

#[test]
fn test_extract_field_names_from_column() {
    let expr = Expr::Column("name".to_string());
    let fields = FieldValidator::extract_field_names(&expr);
    assert!(fields.contains("name"));
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_extract_field_names_from_binary_op() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("price".to_string())),
        op: BinaryOperator::Add,
        right: Box::new(Expr::Column("tax".to_string())),
    };
    let fields = FieldValidator::extract_field_names(&expr);
    assert!(fields.contains("price"));
    assert!(fields.contains("tax"));
    assert_eq!(fields.len(), 2);
}

#[test]
fn test_validate_expressions_all_found() {
    let record = create_test_record();
    let expressions = vec![
        Expr::Column("id".to_string()),
        Expr::Column("name".to_string()),
    ];
    let result = FieldValidator::validate_expressions(
        &record,
        &expressions,
        ValidationContext::SelectClause,
    );
    assert!(result.is_ok());
}

#[test]
fn test_validate_expressions_missing_field() {
    let record = create_test_record();
    let expressions = vec![
        Expr::Column("id".to_string()),
        Expr::Column("missing".to_string()),
    ];
    let result = FieldValidator::validate_expressions(
        &record,
        &expressions,
        ValidationContext::SelectClause,
    );
    assert!(matches!(
        result,
        Err(FieldValidationError::FieldNotFound { field_name, .. }) if field_name == "missing"
    ));
}

#[test]
fn test_extract_field_names_from_unary_op() {
    let expr = Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expr::Column("active".to_string())),
    };
    let fields = FieldValidator::extract_field_names(&expr);
    assert!(fields.contains("active"));
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_extract_field_names_from_between() {
    let expr = Expr::Between {
        expr: Box::new(Expr::Column("price".to_string())),
        low: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        high: Box::new(Expr::Literal(LiteralValue::Integer(100))),
        negated: false,
    };
    let fields = FieldValidator::extract_field_names(&expr);
    assert!(fields.contains("price"));
    assert_eq!(fields.len(), 1);
}

#[test]
fn test_extract_field_names_from_case_statement() {
    let expr = Expr::Case {
        when_clauses: vec![(
            Expr::BinaryOp {
                left: Box::new(Expr::Column("status".to_string())),
                op: BinaryOperator::Equal,
                right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
            },
            Expr::Column("price".to_string()),
        )],
        else_clause: Some(Box::new(Expr::Column("id".to_string()))),
    };
    let fields = FieldValidator::extract_field_names(&expr);
    assert!(fields.contains("status"));
    assert!(fields.contains("price"));
    assert!(fields.contains("id"));
    assert_eq!(fields.len(), 3);
}

#[test]
fn test_validate_empty_record() {
    let fields = HashMap::new();
    let record = StreamRecord::new(fields);
    let result = FieldValidator::validate_field_exists(
        &record,
        "any_field",
        ValidationContext::SelectClause,
    );
    assert!(matches!(
        result,
        Err(FieldValidationError::FieldNotFound { field_name, .. }) if field_name == "any_field"
    ));
}

#[test]
fn test_validate_empty_field_list() {
    let record = create_test_record();
    let result = FieldValidator::validate_fields_exist(&record, &[], ValidationContext::GroupBy);
    // Empty field list should succeed (no fields to validate)
    assert!(result.is_ok());
}

#[test]
fn test_validation_contexts_provide_context_in_errors() {
    let record = create_test_record();
    let contexts = vec![
        ValidationContext::GroupBy,
        ValidationContext::PartitionBy,
        ValidationContext::SelectClause,
        ValidationContext::WhereClause,
        ValidationContext::JoinCondition,
        ValidationContext::Aggregation,
        ValidationContext::HavingClause,
        ValidationContext::WindowFrame,
    ];

    for context in contexts {
        let result = FieldValidator::validate_field_exists(&record, "nonexistent", context);
        match result {
            Err(FieldValidationError::FieldNotFound {
                field_name,
                context: error_context,
            }) => {
                assert_eq!(field_name, "nonexistent");
                // Verify context is included in error message
                assert!(!error_context.is_empty());
            }
            _ => panic!("Expected FieldNotFound error for context: {:?}", context),
        }
    }
}
