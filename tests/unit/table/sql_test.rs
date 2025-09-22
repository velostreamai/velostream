/*!
# Table SQL Query Interface Unit Tests

Comprehensive unit tests for the Table SQL query interface implementation.
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
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::sql::{ExpressionEvaluator, SqlDataSource, SqlQueryable};

// Mock data source for wildcard testing
struct MockWildcardDataSource {
    records: HashMap<String, FieldValue>,
}

impl MockWildcardDataSource {
    fn new() -> Self {
        let mut records = HashMap::new();

        // Create portfolio record with nested positions
        let mut positions = HashMap::new();

        // AAPL position
        let mut aapl_position = HashMap::new();
        aapl_position.insert("shares".to_string(), FieldValue::Integer(150));
        aapl_position.insert("price".to_string(), FieldValue::ScaledInteger(15025, 2)); // $150.25
        positions.insert("AAPL".to_string(), FieldValue::Struct(aapl_position));

        // MSFT position
        let mut msft_position = HashMap::new();
        msft_position.insert("shares".to_string(), FieldValue::Integer(75));
        msft_position.insert("price".to_string(), FieldValue::ScaledInteger(33025, 2)); // $330.25
        positions.insert("MSFT".to_string(), FieldValue::Struct(msft_position));

        // TSLA position (small holding)
        let mut tsla_position = HashMap::new();
        tsla_position.insert("shares".to_string(), FieldValue::Integer(25));
        tsla_position.insert("price".to_string(), FieldValue::ScaledInteger(45050, 2)); // $450.50
        positions.insert("TSLA".to_string(), FieldValue::Struct(tsla_position));

        let mut portfolio = HashMap::new();
        portfolio.insert(
            "user_id".to_string(),
            FieldValue::String("trader-123".to_string()),
        );
        portfolio.insert("positions".to_string(), FieldValue::Struct(positions));

        records.insert("portfolio-001".to_string(), FieldValue::Struct(portfolio));

        Self { records }
    }
}

impl SqlDataSource for MockWildcardDataSource {
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

#[test]
fn test_wildcard_queries_basic() {
    let data_source = MockWildcardDataSource::new();

    // Test wildcard query to find all positions with shares > 100
    let large_positions = data_source.sql_wildcard_values("positions.*.shares > 100");
    assert!(large_positions.is_ok());
    let positions = large_positions.unwrap();

    // Should find AAPL (150 shares) but not MSFT (75 shares) or TSLA (25 shares)
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0], FieldValue::Integer(150));
}

#[test]
fn test_wildcard_queries_less_than() {
    let data_source = MockWildcardDataSource::new();

    // Test wildcard query to find all positions with shares < 100
    let small_positions = data_source.sql_wildcard_values("positions.*.shares < 100");
    assert!(small_positions.is_ok());
    let positions = small_positions.unwrap();

    // Should find MSFT (75 shares) and TSLA (25 shares) but not AAPL (150 shares)
    assert_eq!(positions.len(), 2);
    assert!(positions.contains(&FieldValue::Integer(75))); // MSFT
    assert!(positions.contains(&FieldValue::Integer(25))); // TSLA
}

#[test]
fn test_wildcard_queries_no_comparison() {
    let data_source = MockWildcardDataSource::new();

    // Test wildcard query without comparison - should return all shares values
    let all_shares = data_source.sql_wildcard_values("positions.*.shares");
    assert!(all_shares.is_ok());
    let shares = all_shares.unwrap();

    // Should find all three positions
    assert_eq!(shares.len(), 3);
    assert!(shares.contains(&FieldValue::Integer(150))); // AAPL
    assert!(shares.contains(&FieldValue::Integer(75))); // MSFT
    assert!(shares.contains(&FieldValue::Integer(25))); // TSLA
}

#[test]
fn test_wildcard_queries_with_scaled_integer() {
    let data_source = MockWildcardDataSource::new();

    // Test wildcard query with ScaledInteger (prices)
    let expensive_positions = data_source.sql_wildcard_values("positions.*.price > 300");
    assert!(expensive_positions.is_ok());
    let positions = expensive_positions.unwrap();

    // Should find MSFT ($330.25) and TSLA ($450.50) but not AAPL ($150.25)
    assert_eq!(positions.len(), 2);
    assert!(positions.contains(&FieldValue::ScaledInteger(33025, 2))); // MSFT
    assert!(positions.contains(&FieldValue::ScaledInteger(45050, 2))); // TSLA
}

#[test]
fn test_wildcard_queries_invalid_path() {
    let data_source = MockWildcardDataSource::new();

    // Test wildcard query with invalid path
    let result = data_source.sql_wildcard_values("invalid.*.field > 100");
    assert!(result.is_ok());
    let values = result.unwrap();
    assert!(values.is_empty()); // Should return empty vector for invalid paths
}

#[test]
fn test_wildcard_queries_invalid_comparison() {
    let data_source = MockWildcardDataSource::new();

    // Test wildcard query with invalid comparison value
    let result = data_source.sql_wildcard_values("positions.*.shares > invalid");
    assert!(result.is_err()); // Should return error for invalid numeric threshold
}

#[test]
fn test_financial_portfolio_wildcard_scenarios() {
    let data_source = MockWildcardDataSource::new();

    // Test realistic financial scenarios

    // 1. Risk management: Find all positions with high concentration (>100 shares)
    let high_concentration = data_source.sql_wildcard_values("positions.*.shares > 100");
    assert!(high_concentration.is_ok());
    assert_eq!(high_concentration.unwrap().len(), 1); // Only AAPL

    // 2. Liquidity check: Find all positions with moderate holdings (50-100 shares)
    let moderate_positions = data_source.sql_wildcard_values("positions.*.shares > 50");
    assert!(moderate_positions.is_ok());
    assert_eq!(moderate_positions.unwrap().len(), 2); // AAPL and MSFT

    // 3. Price analysis: Find all positions with high-value stocks (>$200)
    let high_value_stocks = data_source.sql_wildcard_values("positions.*.price > 200");
    assert!(high_value_stocks.is_ok());
    assert_eq!(high_value_stocks.unwrap().len(), 2); // MSFT and TSLA
}

#[test]
fn test_wildcard_edge_cases() {
    let data_source = MockWildcardDataSource::new();

    // Test edge cases

    // 1. Empty wildcard path
    let empty_result = data_source.sql_wildcard_values("");
    assert!(empty_result.is_err());

    // 2. Just wildcard
    let just_wildcard = data_source.sql_wildcard_values("*");
    assert!(just_wildcard.is_ok());
    // Should return the whole portfolio record

    // 3. Multiple wildcards (not currently supported, should handle gracefully)
    let multiple_wildcards = data_source.sql_wildcard_values("*.*.shares");
    assert!(multiple_wildcards.is_ok());
    // Behavior depends on implementation - should handle gracefully

    // 4. Wildcard at end
    let wildcard_end = data_source.sql_wildcard_values("positions.*");
    assert!(wildcard_end.is_ok());
    // Should return all position objects
}
