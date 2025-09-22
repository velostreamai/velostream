/*!
# KTable SQL Query Interface Unit Tests

Comprehensive unit tests for the KTable SQL query interface implementation.
Tests cover:
- ExpressionEvaluator functionality with SQL AST integration
- SqlQueryable trait methods (sql_filter, sql_exists, sql_column_values, sql_scalar)
- Type coercion and conversion
- Error handling and edge cases
- Financial services specific scenarios
- JSON to FieldValue conversion
*/

use serde_json::json;
use std::collections::HashMap;
use velostream::velostream::kafka::ktable_sql::{ExpressionEvaluator, SqlDataSource, SqlQueryable};
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::types::FieldValue;

#[test]
fn test_expression_evaluator_comparison_operators() {
    let evaluator = ExpressionEvaluator::new();

    // Test all comparison operators
    let operators = vec![
        ("age = 25", "age", 25, true),
        ("age != 25", "age", 30, true),
        ("age < 30", "age", 25, true),
        ("age <= 25", "age", 25, true),
        ("age > 20", "age", 25, true),
        ("age >= 25", "age", 25, true),
    ];

    for (clause, field, value, expected) in operators {
        let predicate = evaluator.parse_where_clause(clause).expect("Should parse");

        let mut fields = HashMap::new();
        fields.insert(field.to_string(), FieldValue::Integer(value));
        let record = FieldValue::Struct(fields);

        assert_eq!(
            predicate(&"key1".to_string(), &record),
            expected,
            "Failed for clause: {}",
            clause
        );
    }
}

#[test]
fn test_expression_evaluator_logical_operators() {
    let evaluator = ExpressionEvaluator::new();

    // Test AND operator
    let predicate = evaluator
        .parse_where_clause("age > 20 AND status = 'active'")
        .expect("Should parse");

    let mut fields = HashMap::new();
    fields.insert("age".to_string(), FieldValue::Integer(25));
    fields.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );
    let record = FieldValue::Struct(fields);

    assert!(predicate(&"key1".to_string(), &record));

    // Test OR operator
    let predicate = evaluator
        .parse_where_clause("age < 18 OR status = 'premium'")
        .expect("Should parse");

    let mut fields = HashMap::new();
    fields.insert("age".to_string(), FieldValue::Integer(25));
    fields.insert(
        "status".to_string(),
        FieldValue::String("premium".to_string()),
    );
    let record = FieldValue::Struct(fields);

    assert!(predicate(&"key1".to_string(), &record));
}

#[test]
fn test_expression_evaluator_type_coercion() {
    let evaluator = ExpressionEvaluator::new();

    // Test integer/float comparison
    let predicate = evaluator
        .parse_where_clause("score > 85")
        .expect("Should parse");

    let mut fields = HashMap::new();
    fields.insert("score".to_string(), FieldValue::Float(90.5));
    let record = FieldValue::Struct(fields);

    assert!(predicate(&"key1".to_string(), &record));
}

#[test]
fn test_sql_queryable_filter() {
    let datasource = create_test_datasource();

    // Test basic filtering
    let result = datasource.sql_filter("tier = 'premium'").unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains_key("user1"));
    assert!(result.contains_key("user3"));

    // Test complex filtering
    let result = datasource
        .sql_filter("tier = 'premium' AND score >= 85.5")
        .unwrap();
    assert_eq!(result.len(), 2);

    // Test no matches
    let result = datasource.sql_filter("tier = 'nonexistent'").unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_sql_queryable_exists() {
    let datasource = create_test_datasource();

    // Test EXISTS with match
    assert!(datasource.sql_exists("tier = 'premium'").unwrap());
    assert!(datasource.sql_exists("active = true").unwrap());

    // Test EXISTS with no match
    assert!(!datasource.sql_exists("tier = 'platinum'").unwrap());
    assert!(!datasource.sql_exists("age = 100").unwrap());

    // Test complex EXISTS
    assert!(datasource
        .sql_exists("tier = 'premium' AND score > 80")
        .unwrap());
    assert!(!datasource
        .sql_exists("tier = 'premium' AND age > 100")
        .unwrap());
}

#[test]
fn test_sql_queryable_column_values() {
    let datasource = create_test_datasource();

    // Test column extraction
    let values = datasource
        .sql_column_values("id", "tier = 'premium'")
        .unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&FieldValue::String("user1".to_string())));
    assert!(values.contains(&FieldValue::String("user3".to_string())));

    // Test with complex condition
    let values = datasource
        .sql_column_values("score", "active = true")
        .unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&FieldValue::Float(85.5)));
    assert!(values.contains(&FieldValue::Float(90.0)));

    // Test no matches
    let values = datasource
        .sql_column_values("id", "tier = 'nonexistent'")
        .unwrap();
    assert!(values.is_empty());
}

#[test]
fn test_sql_queryable_scalar() {
    let datasource = create_test_datasource();

    // Test scalar query with single result
    let value = datasource.sql_scalar("name", "id = 'user1'").unwrap();
    assert_eq!(value, FieldValue::String("Alice".to_string()));

    // Test scalar query with no results
    let value = datasource.sql_scalar("name", "id = 'nonexistent'").unwrap();
    assert_eq!(value, FieldValue::Null);

    // Test scalar query with multiple results (should error)
    let result = datasource.sql_scalar("tier", "tier = 'premium'");
    assert!(result.is_err());
    if let Err(SqlError::ExecutionError { message, .. }) = result {
        assert!(message.contains("more than one row"));
    }
}

#[test]
fn test_null_handling() {
    let mut records = HashMap::new();

    // Create record with NULL field
    records.insert(
        "test1".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::String("test1".to_string()));
            fields.insert("optional_field".to_string(), FieldValue::Null);
            fields.insert("active".to_string(), FieldValue::Boolean(true));
            fields
        }),
    );

    let datasource = MockDataSource { records };

    // Test NULL comparison
    let result = datasource.sql_filter("optional_field = null").unwrap();
    assert_eq!(result.len(), 1);

    // Test IS NULL (when implemented)
    // Currently our parser doesn't support IS NULL syntax yet
    // This is a placeholder for future enhancement
}

#[test]
fn test_financial_services_scenarios() {
    let datasource = create_financial_test_datasource();

    // Test risk validation
    let high_risk = datasource
        .sql_exists("risk_score > 80 AND tier = 'retail'")
        .unwrap();
    assert!(!high_risk); // Should be false for our test data

    // Test institutional users
    let institutional_ids = datasource
        .sql_column_values("id", "tier = 'institutional'")
        .unwrap();
    assert_eq!(institutional_ids.len(), 1);

    // Test balance filtering
    let high_value = datasource.sql_filter("balance >= 1000000").unwrap();
    assert!(!high_value.is_empty());

    // Test complex financial conditions
    let qualified = datasource
        .sql_filter("balance > 500000 AND tier = 'premium' AND risk_score < 70")
        .unwrap();
    assert!(!qualified.is_empty());
}

#[test]
fn test_error_handling() {
    let datasource = create_test_datasource();

    // Test invalid syntax
    let result = datasource.sql_filter("invalid syntax here");
    assert!(result.is_err());

    // Test unsupported operators (when not implemented)
    let result = datasource.sql_filter("age BETWEEN 20 AND 30");
    assert!(result.is_err());

    // Test non-existent column in scalar query
    let result = datasource.sql_scalar("nonexistent_column", "id = 'user1'");
    // Should return Null for missing columns
    assert_eq!(result.unwrap(), FieldValue::Null);
}

#[test]
fn test_complex_expressions() {
    let datasource = create_test_datasource();

    // Test simple logical operators (parentheses not yet supported)
    let result = datasource
        .sql_filter("tier = 'premium' AND active = true")
        .unwrap();
    assert_eq!(result.len(), 2); // user1 and user3

    // Test mixed data types
    let result = datasource
        .sql_filter("age > 20 AND score >= 85.0 AND tier != 'basic'")
        .unwrap();
    assert_eq!(result.len(), 2);

    // Test string comparisons
    let result = datasource.sql_filter("name != 'Bob'").unwrap();
    assert_eq!(result.len(), 2); // Alice and Charlie

    // Test OR operations
    let result = datasource
        .sql_filter("tier = 'premium' OR tier = 'basic'")
        .unwrap();
    assert_eq!(result.len(), 3); // All users
}

#[test]
fn test_json_to_field_value_conversion() {
    // Test the JSON conversion functionality
    let json_val = json!({
        "id": "test123",
        "balance": 50000.75,
        "active": true,
        "metadata": {
            "created": "2024-01-01",
            "score": 95
        },
        "tags": ["premium", "verified"]
    });

    let field_val = MockDataSource::json_to_field_value(&json_val);

    if let FieldValue::Struct(fields) = field_val {
        assert_eq!(
            fields.get("id"),
            Some(&FieldValue::String("test123".to_string()))
        );
        assert_eq!(fields.get("balance"), Some(&FieldValue::Float(50000.75)));
        assert_eq!(fields.get("active"), Some(&FieldValue::Boolean(true)));

        // Test nested object
        if let Some(FieldValue::Struct(metadata)) = fields.get("metadata") {
            assert_eq!(metadata.get("score"), Some(&FieldValue::Integer(95)));
        }

        // Test array
        if let Some(FieldValue::Array(tags)) = fields.get("tags") {
            assert_eq!(tags.len(), 2);
        }
    } else {
        panic!("Expected Struct FieldValue");
    }
}

// Helper function to create test data
fn create_test_datasource() -> MockDataSource {
    let mut records = HashMap::new();

    records.insert(
        "user1".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::String("user1".to_string()));
            fields.insert("name".to_string(), FieldValue::String("Alice".to_string()));
            fields.insert("age".to_string(), FieldValue::Integer(30));
            fields.insert("active".to_string(), FieldValue::Boolean(true));
            fields.insert(
                "tier".to_string(),
                FieldValue::String("premium".to_string()),
            );
            fields.insert("score".to_string(), FieldValue::Float(85.5));
            fields
        }),
    );

    records.insert(
        "user2".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::String("user2".to_string()));
            fields.insert("name".to_string(), FieldValue::String("Bob".to_string()));
            fields.insert("age".to_string(), FieldValue::Integer(25));
            fields.insert("active".to_string(), FieldValue::Boolean(false));
            fields.insert("tier".to_string(), FieldValue::String("basic".to_string()));
            fields.insert("score".to_string(), FieldValue::Float(72.3));
            fields
        }),
    );

    records.insert(
        "user3".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::String("user3".to_string()));
            fields.insert(
                "name".to_string(),
                FieldValue::String("Charlie".to_string()),
            );
            fields.insert("age".to_string(), FieldValue::Integer(35));
            fields.insert("active".to_string(), FieldValue::Boolean(true));
            fields.insert(
                "tier".to_string(),
                FieldValue::String("premium".to_string()),
            );
            fields.insert("score".to_string(), FieldValue::Float(90.0));
            fields
        }),
    );

    MockDataSource { records }
}

// Helper function for financial services test data
fn create_financial_test_datasource() -> MockDataSource {
    let mut records = HashMap::new();

    records.insert(
        "trader1".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::String("trader1".to_string()));
            fields.insert(
                "tier".to_string(),
                FieldValue::String("premium".to_string()),
            );
            fields.insert("balance".to_string(), FieldValue::Float(750000.0));
            fields.insert("risk_score".to_string(), FieldValue::Integer(65));
            fields.insert("verified".to_string(), FieldValue::Boolean(true));
            fields
        }),
    );

    records.insert(
        "institution1".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert(
                "id".to_string(),
                FieldValue::String("institution1".to_string()),
            );
            fields.insert(
                "tier".to_string(),
                FieldValue::String("institutional".to_string()),
            );
            fields.insert("balance".to_string(), FieldValue::Float(5000000.0));
            fields.insert("risk_score".to_string(), FieldValue::Integer(45));
            fields.insert("verified".to_string(), FieldValue::Boolean(true));
            fields
        }),
    );

    records.insert(
        "retail1".to_string(),
        FieldValue::Struct({
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::String("retail1".to_string()));
            fields.insert("tier".to_string(), FieldValue::String("retail".to_string()));
            fields.insert("balance".to_string(), FieldValue::Float(25000.0));
            fields.insert("risk_score".to_string(), FieldValue::Integer(75));
            fields.insert("verified".to_string(), FieldValue::Boolean(false));
            fields
        }),
    );

    MockDataSource { records }
}

// Mock implementation for testing
struct MockDataSource {
    records: HashMap<String, FieldValue>,
}

impl SqlDataSource for MockDataSource {
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError> {
        Ok(self.records.clone())
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError> {
        Ok(self.records.get(key).cloned())
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }
}

impl MockDataSource {
    // Helper method for JSON conversion testing
    fn json_to_field_value(json_val: &serde_json::Value) -> FieldValue {
        match json_val {
            serde_json::Value::Null => FieldValue::Null,
            serde_json::Value::Bool(b) => FieldValue::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    FieldValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    FieldValue::Float(f)
                } else {
                    FieldValue::String(n.to_string())
                }
            }
            serde_json::Value::String(s) => FieldValue::String(s.clone()),
            serde_json::Value::Array(arr) => {
                let field_array: Vec<FieldValue> =
                    arr.iter().map(Self::json_to_field_value).collect();
                FieldValue::Array(field_array)
            }
            serde_json::Value::Object(obj) => {
                let mut field_map = HashMap::new();
                for (key, value) in obj {
                    field_map.insert(key.clone(), Self::json_to_field_value(value));
                }
                FieldValue::Struct(field_map)
            }
        }
    }
}
