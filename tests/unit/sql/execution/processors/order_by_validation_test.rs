//! Unit tests for ORDER BY processor field validation
//!
//! Tests for Phase 4 validation gates added to SelectProcessor:
//! - ORDER BY clause field validation
//! - Single and multi-column ORDER BY support
//! - Complex expression validation in ORDER BY
//! - Missing field error detection

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
fn test_order_by_single_column_field_validation_success() {
    // Test: ORDER BY with valid field name should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY price";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - price field exists
    assert!(result.is_ok());
}

#[test]
fn test_order_by_single_column_missing_field() {
    // Test: ORDER BY with missing field should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY nonexistent";

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
fn test_order_by_multiple_columns_success() {
    // Test: ORDER BY with multiple valid fields should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY status, price";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - both status and price fields exist
    assert!(result.is_ok());
}

#[test]
fn test_order_by_multiple_columns_one_missing() {
    // Test: ORDER BY with one missing field in multi-column should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY status, invalid_field";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should fail - invalid_field doesn't exist
    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("invalid_field"));
        }
        _ => panic!("Expected ExecutionError"),
    }
}

#[test]
fn test_order_by_with_direction_asc_success() {
    // Test: ORDER BY with ASC direction on valid field should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY price ASC";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - price field exists with ASC
    assert!(result.is_ok());
}

#[test]
fn test_order_by_with_direction_desc_success() {
    // Test: ORDER BY with DESC direction on valid field should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY price DESC";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - price field exists with DESC
    assert!(result.is_ok());
}

#[test]
fn test_order_by_mixed_directions_success() {
    // Test: ORDER BY with mixed ASC/DESC directions should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY status ASC, price DESC";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - both fields exist with different directions
    assert!(result.is_ok());
}

#[test]
fn test_order_by_expression_field_validation_success() {
    // Test: ORDER BY with valid expression should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY price * quantity";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - price and quantity fields exist
    assert!(result.is_ok());
}

#[test]
fn test_order_by_expression_missing_field() {
    // Test: ORDER BY expression with missing field should fail
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream ORDER BY price * invalid_qty";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should fail - invalid_qty field doesn't exist
    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("invalid_qty"));
        }
        _ => panic!("Expected ExecutionError"),
    }
}

#[test]
fn test_order_by_with_alias_in_select() {
    // Test: ORDER BY with aliased field from SELECT should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT price AS item_price FROM test_stream ORDER BY price";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - ORDER BY validates against result fields
    assert!(result.is_ok());
}

#[test]
fn test_order_by_with_where_clause_success() {
    // Test: ORDER BY combined with WHERE clause should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream WHERE status = 'active' ORDER BY price";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - both WHERE and ORDER BY fields exist
    assert!(result.is_ok());
}

#[test]
fn test_order_by_with_complex_select_success() {
    // Test: ORDER BY with complex SELECT and expressions should pass
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str =
        "SELECT id, name, price * quantity as total FROM test_stream ORDER BY total";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - total alias and price/quantity fields exist
    assert!(result.is_ok());
}

#[test]
fn test_order_by_empty_clause_success() {
    // Test: Query without ORDER BY should pass (no validation needed)
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT * FROM test_stream";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - no ORDER BY clause to validate
    assert!(result.is_ok());
}

#[test]
fn test_order_by_all_validations_together() {
    // Test: Complex query with all validation gates (WHERE, ORDER BY)
    let record = create_test_record();
    let parser = StreamingSqlParser::new();
    let query_str =
        "SELECT id, name, price as item_price FROM test_stream WHERE quantity > 5 ORDER BY price DESC";

    let query = parser.parse(query_str).expect("Should parse");
    let mut context = ProcessorContext::new("test");

    let result = SelectProcessor::process(&query, &record, &mut context);

    // Should succeed - all fields exist in all clauses
    assert!(result.is_ok());
}
