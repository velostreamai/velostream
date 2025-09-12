/*!
# UPDATE Processor Tests

Comprehensive test suite for UPDATE operations.
*/

use ferrisstreams::ferris::sql::ast::{Expr, LiteralValue};
use ferrisstreams::ferris::sql::execution::processors::UpdateProcessor;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamRecord};
use std::collections::HashMap;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(100));
    fields.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    fields.insert("age".to_string(), FieldValue::Integer(25));
    fields.insert("active".to_string(), FieldValue::Boolean(true));
    fields.insert("balance".to_string(), FieldValue::Float(1000.0));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp: 1234567890000,
        offset: 10,
        partition: 0,
    }
}

// Test 1: Simple UPDATE with WHERE clause match
#[tokio::test]
async fn test_update_with_where_match() {
    let table_name = "users";
    let assignments = vec![
        (
            "name".to_string(),
            Expr::Literal(LiteralValue::String("Bob".to_string())),
        ),
        ("age".to_string(), Expr::Literal(LiteralValue::Integer(30))),
    ];
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: ferrisstreams::ferris::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(100))),
    });
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some());

    let record = updated_record.unwrap();
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(100))); // Unchanged
    assert_eq!(
        record.fields.get("name"),
        Some(&FieldValue::String("Bob".to_string()))
    ); // Updated
    assert_eq!(record.fields.get("age"), Some(&FieldValue::Integer(30))); // Updated
    assert_eq!(
        record.fields.get("active"),
        Some(&FieldValue::Boolean(true))
    ); // Unchanged
    assert_eq!(
        record.fields.get("balance"),
        Some(&FieldValue::Float(1000.0))
    ); // Unchanged

    // Check metadata
    assert!(record.timestamp > input_record.timestamp); // New timestamp
    assert_eq!(record.offset, input_record.offset + 1); // Incremented offset
    assert_eq!(record.headers.get("operation"), Some(&"UPDATE".to_string()));
}

// Test 2: UPDATE with WHERE clause no match
#[tokio::test]
async fn test_update_with_where_no_match() {
    let table_name = "users";
    let assignments = vec![(
        "name".to_string(),
        Expr::Literal(LiteralValue::String("Bob".to_string())),
    )];
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: ferrisstreams::ferris::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(999))), // No match
    });
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_none()); // No update should occur
}

// Test 3: UPDATE without WHERE clause (update all)
#[tokio::test]
async fn test_update_without_where() {
    let table_name = "products";
    let assignments = vec![(
        "active".to_string(),
        Expr::Literal(LiteralValue::Boolean(false)),
    )];
    let where_clause = None;
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some());

    let record = updated_record.unwrap();
    assert_eq!(
        record.fields.get("active"),
        Some(&FieldValue::Boolean(false))
    ); // Updated
    assert!(record.headers.get("operation").is_some());
}

// Test 4: UPDATE with expression-based assignment
#[tokio::test]
async fn test_update_with_expression_assignment() {
    let table_name = "accounts";
    let assignments = vec![
        // Increment age by 1
        (
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: ferrisstreams::ferris::sql::ast::BinaryOperator::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            },
        ),
        // Double the balance
        (
            "balance".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("balance".to_string())),
                op: ferrisstreams::ferris::sql::ast::BinaryOperator::Multiply,
                right: Box::new(Expr::Literal(LiteralValue::Float(2.0))),
            },
        ),
    ];
    let where_clause = None;
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some());

    let record = updated_record.unwrap();
    assert_eq!(record.fields.get("age"), Some(&FieldValue::Integer(26))); // 25 + 1
    assert_eq!(
        record.fields.get("balance"),
        Some(&FieldValue::Float(2000.0))
    ); // 1000.0 * 2.0
}

// Test 5: UPDATE with multiple complex conditions
#[tokio::test]
async fn test_update_with_complex_where() {
    let table_name = "users";
    let assignments = vec![(
        "name".to_string(),
        Expr::Literal(LiteralValue::String("Updated".to_string())),
    )];
    // WHERE age > 18 AND active = true
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("age".to_string())),
            op: ferrisstreams::ferris::sql::ast::BinaryOperator::GreaterThan,
            right: Box::new(Expr::Literal(LiteralValue::Integer(18))),
        }),
        op: ferrisstreams::ferris::sql::ast::BinaryOperator::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Column("active".to_string())),
            op: ferrisstreams::ferris::sql::ast::BinaryOperator::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Boolean(true))),
        }),
    });
    let input_record = create_test_record(); // age=25, active=true

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some()); // Should match: 25 > 18 AND true = true

    let record = updated_record.unwrap();
    assert_eq!(
        record.fields.get("name"),
        Some(&FieldValue::String("Updated".to_string()))
    );
}

// Test 6: UPDATE with new field creation
#[tokio::test]
async fn test_update_create_new_field() {
    let table_name = "users";
    let assignments = vec![
        (
            "new_field".to_string(),
            Expr::Literal(LiteralValue::String("new_value".to_string())),
        ),
        (
            "timestamp".to_string(),
            Expr::Literal(LiteralValue::Integer(1699999999)),
        ),
    ];
    let where_clause = None;
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some());

    let record = updated_record.unwrap();
    // Original fields preserved
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(100)));
    assert_eq!(
        record.fields.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    // New fields added
    assert_eq!(
        record.fields.get("new_field"),
        Some(&FieldValue::String("new_value".to_string()))
    );
    assert_eq!(
        record.fields.get("timestamp"),
        Some(&FieldValue::Integer(1699999999))
    );
}

// Test 7: Validation - Empty table name
#[tokio::test]
async fn test_update_validation_empty_table() {
    let table_name = "";
    let assignments = vec![(
        "name".to_string(),
        Expr::Literal(LiteralValue::String("Test".to_string())),
    )];
    let where_clause = None;

    let result = UpdateProcessor::validate_update(table_name, &assignments, &where_clause);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("Table name cannot be empty"));
}

// Test 8: Validation - Empty assignments
#[tokio::test]
async fn test_update_validation_empty_assignments() {
    let table_name = "test_table";
    let assignments = vec![]; // No assignments
    let where_clause = None;

    let result = UpdateProcessor::validate_update(table_name, &assignments, &where_clause);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error
        .to_string()
        .contains("UPDATE must have at least one SET assignment"));
}

// Test 9: Validation - Empty column name in assignment
#[tokio::test]
async fn test_update_validation_empty_column() {
    let table_name = "test_table";
    let assignments = vec![
        (
            "".to_string(),
            Expr::Literal(LiteralValue::String("value".to_string())),
        ), // Empty column name
    ];
    let where_clause = None;

    let result = UpdateProcessor::validate_update(table_name, &assignments, &where_clause);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error
        .to_string()
        .contains("Column name in SET clause cannot be empty"));
}

// Test 10: Get affected columns
#[tokio::test]
async fn test_get_affected_columns() {
    let assignments = vec![
        ("name".to_string(), Expr::Column("old_name".to_string())),
        (
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("current_age".to_string())),
                op: ferrisstreams::ferris::sql::ast::BinaryOperator::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            },
        ),
    ];
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("status".to_string())),
        op: ferrisstreams::ferris::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::String("active".to_string()))),
    });

    let columns = UpdateProcessor::get_affected_columns(&assignments, &where_clause);

    // Should include: assigned columns + columns referenced in expressions + WHERE columns
    let mut sorted_columns = columns.clone();
    sorted_columns.sort();

    assert!(sorted_columns.contains(&"name".to_string()));
    assert!(sorted_columns.contains(&"age".to_string()));
    assert!(sorted_columns.contains(&"old_name".to_string()));
    assert!(sorted_columns.contains(&"current_age".to_string()));
    assert!(sorted_columns.contains(&"status".to_string()));
}

// Test 11: WHERE clause matching helper
#[tokio::test]
async fn test_matches_where_clause() {
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: ferrisstreams::ferris::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(100))),
    });
    let input_record = create_test_record(); // id = 100

    let result = UpdateProcessor::matches_where_clause(&where_clause, &input_record);

    assert!(result.is_ok());
    assert!(result.unwrap()); // Should match

    // Test no match
    let no_match_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("id".to_string())),
        op: ferrisstreams::ferris::sql::ast::BinaryOperator::Equal,
        right: Box::new(Expr::Literal(LiteralValue::Integer(999))),
    });

    let result2 = UpdateProcessor::matches_where_clause(&no_match_clause, &input_record);

    assert!(result2.is_ok());
    assert!(!result2.unwrap()); // Should not match
}

// Test 12: Apply assignments helper
#[tokio::test]
async fn test_apply_assignments() {
    let assignments = vec![
        (
            "name".to_string(),
            Expr::Literal(LiteralValue::String("NewName".to_string())),
        ),
        (
            "age".to_string(),
            Expr::BinaryOp {
                left: Box::new(Expr::Column("age".to_string())),
                op: ferrisstreams::ferris::sql::ast::BinaryOperator::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(5))),
            },
        ),
    ];
    let input_record = create_test_record();

    let result = UpdateProcessor::apply_assignments(&assignments, &input_record);

    assert!(result.is_ok());
    let updated_fields = result.unwrap();

    assert_eq!(
        updated_fields.get("name"),
        Some(&FieldValue::String("NewName".to_string()))
    );
    assert_eq!(updated_fields.get("age"), Some(&FieldValue::Integer(30))); // 25 + 5
    assert_eq!(updated_fields.get("id"), Some(&FieldValue::Integer(100))); // Preserved
    assert_eq!(
        updated_fields.get("balance"),
        Some(&FieldValue::Float(1000.0))
    ); // Preserved
}

// Test 13: Create update metadata
#[tokio::test]
async fn test_create_update_metadata() {
    let assignments = vec![
        (
            "name".to_string(),
            Expr::Literal(LiteralValue::String("Updated".to_string())),
        ),
        ("age".to_string(), Expr::Literal(LiteralValue::Integer(30))),
    ];
    let input_record = create_test_record();

    let metadata = UpdateProcessor::create_update_metadata(&input_record, &assignments);

    assert_eq!(metadata.get("operation"), Some(&"UPDATE".to_string()));
    assert!(metadata.get("updated_at").is_some());
    assert_eq!(
        metadata.get("original_timestamp"),
        Some(&input_record.timestamp.to_string())
    );
    assert_eq!(
        metadata.get("original_offset"),
        Some(&input_record.offset.to_string())
    );
    assert_eq!(
        metadata.get("updated_columns"),
        Some(&"name,age".to_string())
    );
}

// Test 14: UPDATE with NULL assignment
#[tokio::test]
async fn test_update_with_null_assignment() {
    let table_name = "users";
    let assignments = vec![
        ("name".to_string(), Expr::Literal(LiteralValue::Null)),
        ("balance".to_string(), Expr::Literal(LiteralValue::Null)),
    ];
    let where_clause = None;
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some());

    let record = updated_record.unwrap();
    assert_eq!(record.fields.get("name"), Some(&FieldValue::Null));
    assert_eq!(record.fields.get("balance"), Some(&FieldValue::Null));
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(100))); // Preserved
}

// Test 15: UPDATE performance with many assignments
#[tokio::test]
async fn test_update_performance_many_assignments() {
    let table_name = "wide_table";
    let mut assignments = Vec::new();

    // Create 50 field assignments
    for i in 0..50 {
        assignments.push((
            format!("field_{}", i),
            Expr::Literal(LiteralValue::Integer(i)),
        ));
    }

    let where_clause = None;
    let input_record = create_test_record();

    let result =
        UpdateProcessor::process_update(table_name, &assignments, &where_clause, &input_record);

    assert!(result.is_ok());
    let updated_record = result.unwrap();
    assert!(updated_record.is_some());

    let record = updated_record.unwrap();

    // Verify all assignments were applied
    for i in 0..50 {
        let field_name = format!("field_{}", i);
        assert_eq!(
            record.fields.get(&field_name),
            Some(&FieldValue::Integer(i))
        );
    }

    // Original fields should be preserved
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(100)));
}
