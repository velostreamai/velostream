//! Unit tests for SELECT processor field validation
//!
//! Tests for Phase 3 validation gates added to SelectProcessor:
//! - WHERE clause field validation
//! - GROUP BY entry-point field validation
//! - SELECT clause expression field validation
//! - HAVING clause field validation

use std::collections::HashMap;
use velostream::velostream::sql::{
    ast::{BinaryOperator, Expr, LiteralValue, SelectField, StreamSource},
    execution::{
        processors::{ProcessorContext, SelectProcessor},
        FieldValue, StreamRecord,
    },
    parser::StreamingSqlParser,
    SqlError, StreamingQuery,
};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));
    fields.insert("price".to_string(), FieldValue::Float(99.99));
    fields.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );
    fields.insert("quantity".to_string(), FieldValue::Integer(10));

    StreamRecord {
        fields,
        timestamp: 0,
        offset: 1,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
    }
}

#[test]
fn test_where_clause_field_validation_success() {
    // Test: WHERE clause with valid field name should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream WHERE status = 'active'";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - status field exists
    assert!(result.is_ok());
}

#[test]
fn test_where_clause_field_validation_missing_field() {
    // Test: WHERE clause with missing field should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream WHERE nonexistent = 'value'";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should fail - nonexistent field doesn't exist
    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("nonexistent"));
        }
        _ => panic!("Expected ExecutionError"),
    }
}

#[test]
fn test_group_by_field_validation_success() {
    // Test: GROUP BY with valid field name should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT status, COUNT(id) as count FROM test_stream GROUP BY status";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - status field exists
    assert!(result.is_ok());
}

#[test]
fn test_group_by_field_validation_missing_field() {
    // Test: GROUP BY with missing field should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT status, COUNT(id) as count FROM test_stream GROUP BY missing_field";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should fail - missing_field doesn't exist
    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("missing_field"));
        }
        _ => panic!("Expected ExecutionError"),
    }
}

#[test]
fn test_select_field_expression_validation_success() {
    // Test: SELECT expression with valid fields should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT price * quantity as total_value FROM test_stream";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - price and quantity fields exist
    assert!(result.is_ok());
}

#[test]
fn test_select_field_expression_validation_missing_field() {
    // Test: SELECT expression with missing field should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT price * invalid_quantity as total_value FROM test_stream";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should fail - invalid_quantity field doesn't exist
    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("invalid_quantity"));
        }
        _ => panic!("Expected ExecutionError"),
    }
}

#[test]
fn test_having_clause_field_validation_with_original_field() {
    // Test: HAVING clause validation with field from original record
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT status, SUM(quantity) as total_qty FROM test_stream GROUP BY status \
         HAVING status != 'inactive'";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - status field exists in original record
    assert!(result.is_ok());
}

#[test]
fn test_having_clause_field_validation_with_computed_field() {
    // Test: HAVING clause validation with field from computed results
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT status, SUM(quantity) as total_qty FROM test_stream GROUP BY status \
         HAVING total_qty > 5";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - total_qty is a computed field in results
    assert!(result.is_ok());
}

#[test]
fn test_having_clause_field_validation_missing_field() {
    // Test: HAVING clause with missing field should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT status, SUM(quantity) as total_qty FROM test_stream GROUP BY status \
         HAVING nonexistent_field > 5";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should fail - nonexistent_field doesn't exist
    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("nonexistent_field"));
        }
        _ => panic!("Expected ExecutionError"),
    }
}

#[test]
fn test_all_validations_together() {
    // Test: Complex query with all validation gates
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str =
        "SELECT status, COUNT(id) as count, SUM(quantity) as total_qty FROM test_stream \
         WHERE price > 50.0 \
         GROUP BY status \
         HAVING total_qty > 5";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - all fields exist
    assert!(result.is_ok());
}
