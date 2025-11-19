/*!
# INSERT Processor Tests

Comprehensive test suite for INSERT INTO operations.
*/

use std::collections::HashMap;
use velostream::velostream::sql::ast::{Expr, InsertSource, LiteralValue, StreamingQuery};
use velostream::velostream::sql::execution::processors::InsertProcessor;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("context_id".to_string(), FieldValue::Integer(42));
    fields.insert(
        "context_name".to_string(),
        FieldValue::String("test_context".to_string()),
    );

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp: 1234567890000,
        offset: 1,
        partition: 0,
    }
}

// Test 1: INSERT VALUES with single row
#[tokio::test]
async fn test_insert_values_single_row() {
    let table_name = "test_table";
    let columns = Some(vec!["id".to_string(), "name".to_string()]);
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Literal(LiteralValue::Integer(100)),
            Expr::Literal(LiteralValue::String("Alice".to_string())),
        ]],
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(100)));
    assert_eq!(
        record.fields.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(record.timestamp, input_record.timestamp);
    assert_eq!(record.partition, input_record.partition);
}

// Test 2: INSERT VALUES with multiple rows
#[tokio::test]
async fn test_insert_values_multiple_rows() {
    let table_name = "users";
    let columns = Some(vec![
        "id".to_string(),
        "name".to_string(),
        "active".to_string(),
    ]);
    let values_source = InsertSource::Values {
        rows: vec![
            vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::String("Alice".to_string())),
                Expr::Literal(LiteralValue::Boolean(true)),
            ],
            vec![
                Expr::Literal(LiteralValue::Integer(2)),
                Expr::Literal(LiteralValue::String("Bob".to_string())),
                Expr::Literal(LiteralValue::Boolean(false)),
            ],
        ],
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 2);

    // Check first record
    let record1 = &records[0];
    assert_eq!(record1.fields.get("id"), Some(&FieldValue::Integer(1)));
    assert_eq!(
        record1.fields.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        record1.fields.get("active"),
        Some(&FieldValue::Boolean(true))
    );
    assert_eq!(record1.offset, input_record.offset);

    // Check second record
    let record2 = &records[1];
    assert_eq!(record2.fields.get("id"), Some(&FieldValue::Integer(2)));
    assert_eq!(
        record2.fields.get("name"),
        Some(&FieldValue::String("Bob".to_string()))
    );
    assert_eq!(
        record2.fields.get("active"),
        Some(&FieldValue::Boolean(false))
    );
    assert_eq!(record2.offset, input_record.offset + 1);
}

// Test 3: INSERT VALUES without explicit columns
#[tokio::test]
async fn test_insert_values_no_explicit_columns() {
    let table_name = "products";
    let columns = None;
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Literal(LiteralValue::Integer(101)),
            Expr::Literal(LiteralValue::String("Widget".to_string())),
            Expr::Literal(LiteralValue::Float(29.99)),
        ]],
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    // Should use default column names
    assert_eq!(record.fields.get("col_0"), Some(&FieldValue::Integer(101)));
    assert_eq!(
        record.fields.get("col_1"),
        Some(&FieldValue::String("Widget".to_string()))
    );
    assert_eq!(record.fields.get("col_2"), Some(&FieldValue::Float(29.99)));
}

// Test 4: INSERT ... SELECT
#[tokio::test]
async fn test_insert_select() {
    let table_name = "target_table";
    let columns = Some(vec!["id".to_string(), "name".to_string()]);

    // Create a mock SELECT query that selects the fields we need
    let select_query = StreamingQuery::Select {
        fields: vec![
            velostream::velostream::sql::ast::SelectField::Expression {
                expr: velostream::velostream::sql::ast::Expr::Literal(
                    velostream::velostream::sql::ast::LiteralValue::Integer(42),
                ),
                alias: Some("id".to_string()),
            },
            velostream::velostream::sql::ast::SelectField::Expression {
                expr: velostream::velostream::sql::ast::Expr::Literal(
                    velostream::velostream::sql::ast::LiteralValue::String("test_name".to_string()),
                ),
                alias: Some("name".to_string()),
            },
        ],
        from_alias: None,
        from: velostream::velostream::sql::ast::StreamSource::Table("source_table".to_string()),
        joins: None,
        where_clause: None,
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

    let select_source = InsertSource::Select {
        query: Box::new(select_query),
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &select_source, &input_record);

    // Currently returns mock data - in full implementation would execute SELECT
    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert!(record.fields.contains_key("id"));
    assert!(record.fields.contains_key("name"));
}

// Test 5: Error - Column count mismatch
#[tokio::test]
async fn test_insert_column_count_mismatch() {
    let table_name = "test_table";
    let columns = Some(vec!["id".to_string(), "name".to_string()]); // 2 columns
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Literal(LiteralValue::Integer(100)),
            // Missing second value - should cause error
        ]],
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("Column count mismatch"));
}

// Test 6: Validation - Empty table name
#[tokio::test]
async fn test_insert_validation_empty_table() {
    let table_name = "";
    let columns = Some(vec!["id".to_string()]);
    let values_source = InsertSource::Values {
        rows: vec![vec![Expr::Literal(LiteralValue::Integer(100))]],
    };

    let result = InsertProcessor::validate_insert(table_name, &columns, &values_source);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("Table name cannot be empty"));
}

// Test 7: Validation - Empty VALUES list
#[tokio::test]
async fn test_insert_validation_empty_values() {
    let table_name = "test_table";
    let columns = Some(vec!["id".to_string()]);
    let values_source = InsertSource::Values {
        rows: vec![], // Empty rows
    };

    let result = InsertProcessor::validate_insert(table_name, &columns, &values_source);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("VALUES list cannot be empty"));
}

// Test 8: Validation - Inconsistent row lengths
#[tokio::test]
async fn test_insert_validation_inconsistent_rows() {
    let table_name = "test_table";
    let columns = None;
    let values_source = InsertSource::Values {
        rows: vec![
            vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::String("Alice".to_string())),
            ],
            vec![
                Expr::Literal(LiteralValue::Integer(2)),
                // Missing second value - inconsistent with first row
            ],
        ],
    };

    let result = InsertProcessor::validate_insert(table_name, &columns, &values_source);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("has") && error.to_string().contains("values but expected"));
}

// Test 9: Get insert columns with explicit columns
#[tokio::test]
async fn test_get_insert_columns_explicit() {
    let columns = Some(vec![
        "id".to_string(),
        "name".to_string(),
        "email".to_string(),
    ]);
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Literal(LiteralValue::Integer(1)),
            Expr::Literal(LiteralValue::String("Test".to_string())),
            Expr::Literal(LiteralValue::String("test@example.com".to_string())),
        ]],
    };

    let result = InsertProcessor::get_insert_columns(&columns, &values_source);

    assert!(result.is_ok());
    let column_names = result.unwrap();
    assert_eq!(column_names, vec!["id", "name", "email"]);
}

// Test 10: Get insert columns with inferred columns
#[tokio::test]
async fn test_get_insert_columns_inferred() {
    let columns = None;
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Literal(LiteralValue::Integer(1)),
            Expr::Literal(LiteralValue::String("Test".to_string())),
            Expr::Literal(LiteralValue::Boolean(true)),
        ]],
    };

    let result = InsertProcessor::get_insert_columns(&columns, &values_source);

    assert!(result.is_ok());
    let column_names = result.unwrap();
    assert_eq!(column_names, vec!["col_0", "col_1", "col_2"]);
}

// Test 11: INSERT with expression evaluation
#[tokio::test]
async fn test_insert_with_expressions() {
    let table_name = "calculated_table";
    let columns = Some(vec!["context_id".to_string(), "calculated".to_string()]);
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Column("context_id".to_string()), // Reference to input record field
            Expr::Literal(LiteralValue::String("computed_value".to_string())),
        ]],
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    // Should evaluate expression to get value from input record
    assert_eq!(
        record.fields.get("context_id"),
        Some(&FieldValue::Integer(42))
    );
    assert_eq!(
        record.fields.get("calculated"),
        Some(&FieldValue::String("computed_value".to_string()))
    );
}

// Test 12: INSERT with null values
#[tokio::test]
async fn test_insert_with_null_values() {
    let table_name = "nullable_table";
    let columns = Some(vec!["id".to_string(), "optional_field".to_string()]);
    let values_source = InsertSource::Values {
        rows: vec![vec![
            Expr::Literal(LiteralValue::Integer(1)),
            Expr::Literal(LiteralValue::Null),
        ]],
    };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(1)));
    assert_eq!(record.fields.get("optional_field"), Some(&FieldValue::Null));
}

// Test 13: INSERT performance with many rows
#[tokio::test]
async fn test_insert_performance_many_rows() {
    let table_name = "bulk_table";
    let columns = Some(vec!["id".to_string(), "batch_id".to_string()]);

    // Create 100 rows for bulk insert
    let mut rows = Vec::new();
    for i in 0..100 {
        rows.push(vec![
            Expr::Literal(LiteralValue::Integer(i)),
            Expr::Literal(LiteralValue::Integer(42)), // batch_id
        ]);
    }

    let values_source = InsertSource::Values { rows };
    let input_record = create_test_record();

    let result =
        InsertProcessor::process_insert(table_name, &columns, &values_source, &input_record);

    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 100);

    // Verify offsets are incremented correctly
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, input_record.offset + (index as i64));
        assert_eq!(
            record.fields.get("id"),
            Some(&FieldValue::Integer(index as i64))
        );
        assert_eq!(
            record.fields.get("batch_id"),
            Some(&FieldValue::Integer(42))
        );
    }
}
