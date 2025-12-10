/*!
# DELETE Processor Tests

Comprehensive test suite for DELETE operations.
*/

use std::collections::HashMap;
use velostream::velostream::sql::ast::{Expr, LiteralValue};
use velostream::velostream::sql::execution::processors::DeleteProcessor;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(100));
    fields.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    fields.insert("age".to_string(), FieldValue::Integer(25));
    fields.insert("active".to_string(), FieldValue::Boolean(true));
    fields.insert(
        "status".to_string(),
        FieldValue::String("pending".to_string()),
    );

    StreamRecord {
        fields,
        headers: {
            let mut headers = HashMap::new();
            headers.insert("source".to_string(), "test".to_string());
            headers
        },
        timestamp: 1234567890000,
        offset: 20,
        partition: 1,
        event_time: None,
        topic: None,
        key: None,
    }
}

// Test 1: DELETE with WHERE clause match
#[tokio::test]
async fn test_delete_with_where_match() {
    let table_name = "users";
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: velostream::velostream::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(100))),
    });
    let input_record = create_test_record();

    let result = DeleteProcessor::process_delete(table_name, &where_clause, &input_record);

    assert!(result.is_ok());
    let tombstone = result.unwrap();
    assert!(tombstone.is_some());

    let record = tombstone.unwrap();

    // Check tombstone markers
    assert_eq!(
        record.fields.get("__deleted"),
        Some(&FieldValue::Boolean(true))
    );
    assert!(record.fields.get("__deleted_at").is_some());

    // Check preserved key fields
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(100))); // Key preserved

    // Check metadata headers
    assert_eq!(record.headers.get("operation"), Some(&"DELETE".to_string()));
    assert_eq!(
        record.headers.get("table_name"),
        Some(&table_name.to_string())
    );
    assert_eq!(record.headers.get("tombstone"), Some(&"true".to_string()));
    assert!(record.headers.get("deleted_at").is_some());
    assert_eq!(
        record.headers.get("original_timestamp"),
        Some(&input_record.timestamp.to_string())
    );
    assert_eq!(
        record.headers.get("original_offset"),
        Some(&input_record.offset.to_string())
    );

    // Check new timestamp and offset
    assert!(record.timestamp > input_record.timestamp);
    assert_eq!(record.offset, input_record.offset + 1);
    assert_eq!(record.partition, input_record.partition);
}

// Test 2: DELETE with WHERE clause no match
#[tokio::test]
async fn test_delete_with_where_no_match() {
    let table_name = "users";
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: velostream::velostream::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(999))), // No match
    });
    let input_record = create_test_record();

    let result = DeleteProcessor::process_delete(table_name, &where_clause, &input_record);

    assert!(result.is_ok());
    let tombstone = result.unwrap();
    assert!(tombstone.is_none()); // No deletion should occur
}

// Test 3: DELETE without WHERE clause (delete all matching)
#[tokio::test]
async fn test_delete_without_where() {
    let table_name = "temp_data";
    let where_clause = None;
    let input_record = create_test_record();

    let result = DeleteProcessor::process_delete(table_name, &where_clause, &input_record);

    assert!(result.is_ok());
    let tombstone = result.unwrap();
    assert!(tombstone.is_some()); // Should delete all records

    let record = tombstone.unwrap();
    assert_eq!(
        record.fields.get("__deleted"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(record.headers.get("operation"), Some(&"DELETE".to_string()));
}

// Test 4: DELETE with complex WHERE condition
#[tokio::test]
async fn test_delete_with_complex_where() {
    let table_name = "users";
    // WHERE age > 18 AND status = 'pending'
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: velostream::velostream::sql::ast::BinaryOperator::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(18))),
        }),
        op: velostream::velostream::sql::ast::BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("status".to_string())),
            op: velostream::velostream::sql::ast::BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::String("pending".to_string()))),
        }),
    });
    let input_record = create_test_record(); // age=25, status='pending'

    let result = DeleteProcessor::process_delete(table_name, &where_clause, &input_record);

    assert!(result.is_ok());
    let tombstone = result.unwrap();
    assert!(tombstone.is_some()); // Should match: 25 > 18 AND 'pending' = 'pending'

    let record = tombstone.unwrap();
    assert_eq!(
        record.fields.get("__deleted"),
        Some(&FieldValue::Boolean(true))
    );
}

// Test 5: CREATE tombstone record directly
#[tokio::test]
async fn test_create_tombstone_record() {
    let table_name = "test_table";
    let input_record = create_test_record();

    let result = DeleteProcessor::create_tombstone_record(&input_record, table_name);

    assert!(result.is_ok());
    let tombstone = result.unwrap();

    // Check tombstone structure
    assert_eq!(
        tombstone.fields.get("__deleted"),
        Some(&FieldValue::Boolean(true))
    );
    assert!(tombstone.fields.get("__deleted_at").is_some());
    assert_eq!(tombstone.fields.get("id"), Some(&FieldValue::Integer(100))); // Key preserved

    // Check headers
    assert_eq!(
        tombstone.headers.get("operation"),
        Some(&"DELETE".to_string())
    );
    assert_eq!(
        tombstone.headers.get("table_name"),
        Some(&table_name.to_string())
    );
    assert_eq!(
        tombstone.headers.get("tombstone"),
        Some(&"true".to_string())
    );

    // Check timestamp and offset changes
    assert!(tombstone.timestamp > input_record.timestamp);
    assert_eq!(tombstone.offset, input_record.offset + 1);
    assert_eq!(tombstone.partition, input_record.partition);
}

// Test 6: CREATE soft delete record
#[tokio::test]
async fn test_create_soft_delete_record() {
    let table_name = "users";
    let input_record = create_test_record();

    let result = DeleteProcessor::create_soft_delete_record(&input_record, table_name);

    assert!(result.is_ok());
    let soft_deleted = result.unwrap();

    // Check soft delete markers
    assert_eq!(
        soft_deleted.fields.get("deleted"),
        Some(&FieldValue::Boolean(true))
    );
    assert!(soft_deleted.fields.get("deleted_at").is_some());

    // Check original fields are preserved
    assert_eq!(
        soft_deleted.fields.get("id"),
        Some(&FieldValue::Integer(100))
    );
    assert_eq!(
        soft_deleted.fields.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        soft_deleted.fields.get("age"),
        Some(&FieldValue::Integer(25))
    );
    assert_eq!(
        soft_deleted.fields.get("active"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(
        soft_deleted.fields.get("status"),
        Some(&FieldValue::String("pending".to_string()))
    );

    // Check headers
    assert_eq!(
        soft_deleted.headers.get("operation"),
        Some(&"SOFT_DELETE".to_string())
    );
    assert_eq!(
        soft_deleted.headers.get("table_name"),
        Some(&table_name.to_string())
    );
}

// Test 7: Validation - Empty table name
#[tokio::test]
async fn test_delete_validation_empty_table() {
    let table_name = "";
    let where_clause = None;

    let result = DeleteProcessor::validate_delete(table_name, &where_clause);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("Table name cannot be empty"));
}

// Test 8: Validation succeeds for valid DELETE
#[tokio::test]
async fn test_delete_validation_valid() {
    let table_name = "valid_table";
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: velostream::velostream::sql::ast::BinaryOperator::GreaterThan,
        right: Box::new(Expr::Literal(LiteralValue::Integer(0))),
    });

    let result = DeleteProcessor::validate_delete(table_name, &where_clause);

    assert!(result.is_ok());
}

// Test 9: WHERE clause matching helper
#[tokio::test]
async fn test_matches_where_clause() {
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("active".to_string())),
        op: velostream::velostream::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
    });
    let input_record = create_test_record(); // active = true

    let result = DeleteProcessor::matches_where_clause(&where_clause, &input_record);

    assert!(result.is_ok());
    assert!(result.unwrap()); // Should match

    // Test no match
    let no_match_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("active".to_string())),
        op: velostream::velostream::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
    });

    let result2 = DeleteProcessor::matches_where_clause(&no_match_clause, &input_record);

    assert!(result2.is_ok());
    assert!(!result2.unwrap()); // Should not match
}

// Test 10: Get referenced columns
#[tokio::test]
async fn test_get_referenced_columns() {
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: velostream::velostream::sql::ast::BinaryOperator::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(18))),
        }),
        op: velostream::velostream::sql::ast::BinaryOperator::And,
        right: Box::new(Expr::Column("status".to_string())),
    });

    let columns = DeleteProcessor::get_referenced_columns(&where_clause);

    assert!(columns.contains(&"age".to_string()));
    assert!(columns.contains(&"status".to_string()));

    // Test with no WHERE clause
    let columns_empty = DeleteProcessor::get_referenced_columns(&None);
    assert!(columns_empty.is_empty());
}

// Test 11: Is tombstone record detection
#[tokio::test]
async fn test_is_tombstone_record() {
    let table_name = "test_table";
    let input_record = create_test_record();

    // Create tombstone and test detection
    let tombstone = DeleteProcessor::create_tombstone_record(&input_record, table_name).unwrap();
    assert!(DeleteProcessor::is_tombstone_record(&tombstone));

    // Test normal record is not tombstone
    assert!(!DeleteProcessor::is_tombstone_record(&input_record));

    // Test tombstone by field marker
    let mut tombstone_by_field = input_record.clone();
    tombstone_by_field
        .fields
        .insert("__deleted".to_string(), FieldValue::Boolean(true));
    assert!(DeleteProcessor::is_tombstone_record(&tombstone_by_field));

    // Test tombstone by header marker
    let mut tombstone_by_header = input_record.clone();
    tombstone_by_header
        .headers
        .insert("tombstone".to_string(), "true".to_string());
    assert!(DeleteProcessor::is_tombstone_record(&tombstone_by_header));
}

// Test 12: Extract tombstone metadata
#[tokio::test]
async fn test_extract_tombstone_metadata() {
    let table_name = "users";
    let input_record = create_test_record();
    let tombstone = DeleteProcessor::create_tombstone_record(&input_record, table_name).unwrap();

    let metadata = DeleteProcessor::extract_tombstone_metadata(&tombstone);

    assert_eq!(metadata.get("table_name"), Some(&table_name.to_string()));
    assert!(metadata.get("deleted_at").is_some());
    assert_eq!(
        metadata.get("original_timestamp"),
        Some(&input_record.timestamp.to_string())
    );
    assert_eq!(
        metadata.get("original_offset"),
        Some(&input_record.offset.to_string())
    );
    assert_eq!(metadata.get("key_id"), Some(&"100".to_string())); // Key field preserved
}

// Test 13: DELETE with IN condition
#[tokio::test]
async fn test_delete_with_in_condition() {
    let table_name = "users";
    // WHERE id IN (100, 200, 300)
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: velostream::velostream::sql::ast::BinaryOperator::In,
        right: Box::new(Expr::List(vec![
            Expr::Literal(LiteralValue::Integer(100)),
            Expr::Literal(LiteralValue::Integer(200)),
            Expr::Literal(LiteralValue::Integer(300)),
        ])),
    });
    let input_record = create_test_record(); // id = 100

    let result = DeleteProcessor::process_delete(table_name, &where_clause, &input_record);

    assert!(result.is_ok());
    let tombstone = result.unwrap();
    assert!(tombstone.is_some()); // Should match (100 is in the list)

    let record = tombstone.unwrap();
    assert_eq!(
        record.fields.get("__deleted"),
        Some(&FieldValue::Boolean(true))
    );
}

// Test 14: DELETE with NOT condition
#[tokio::test]
async fn test_delete_with_not_condition() {
    let table_name = "users";
    // WHERE NOT active
    let where_clause = Some(Expr::UnaryOp {
        op: velostream::velostream::sql::ast::UnaryOperator::Not,
        expr: Box::new(Expr::Column("active".to_string())),
    });
    let input_record = create_test_record(); // active = true

    let result = DeleteProcessor::process_delete(table_name, &where_clause, &input_record);

    assert!(result.is_ok());
    let tombstone = result.unwrap();
    assert!(tombstone.is_none()); // Should not match (NOT true = false)
}

// Test 15: Multiple field preservation in tombstone
#[tokio::test]
async fn test_tombstone_multiple_key_preservation() {
    let table_name = "compound_key_table";
    let mut input_record = create_test_record();
    // Add a key field to test multiple key preservation
    input_record.fields.insert(
        "key".to_string(),
        FieldValue::String("user_key_123".to_string()),
    );

    let tombstone = DeleteProcessor::create_tombstone_record(&input_record, table_name).unwrap();

    // Both key fields should be preserved
    assert_eq!(tombstone.fields.get("id"), Some(&FieldValue::Integer(100)));
    assert_eq!(
        tombstone.fields.get("key"),
        Some(&FieldValue::String("user_key_123".to_string()))
    );

    // Tombstone markers should be present
    assert_eq!(
        tombstone.fields.get("__deleted"),
        Some(&FieldValue::Boolean(true))
    );
    assert!(tombstone.fields.get("__deleted_at").is_some());

    // Other fields should not be preserved (tombstone pattern)
    assert!(tombstone.fields.get("name").is_none());
    assert!(tombstone.fields.get("age").is_none());
    assert!(tombstone.fields.get("active").is_none());
    assert!(tombstone.fields.get("status").is_none());
}
