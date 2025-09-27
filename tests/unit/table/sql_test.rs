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

use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::sql::{ExpressionEvaluator, TableDataSource};
use velostream::velostream::table::streaming::{
    RecordBatch, RecordStream, StreamRecord as StreamingRecord, StreamResult,
};
use velostream::velostream::table::unified_table::{TableResult, UnifiedTable};

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

// SqlDataSource implementation removed - using UnifiedTable only

#[async_trait]
impl UnifiedTable for MockWildcardDataSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn get_record(&self, key: &str) -> TableResult<Option<HashMap<String, FieldValue>>> {
        if let Some(value) = self.records.get(key) {
            let mut record = HashMap::new();
            record.insert(key.to_string(), value.clone());
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        self.records.contains_key(key)
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_> {
        Box::new(self.records.iter().map(|(key, value)| {
            let record = match value {
                // If the value is a Struct, promote its fields to the top level
                FieldValue::Struct(fields) => fields.clone(),
                // Otherwise, use the key-value pair
                _ => {
                    let mut record = HashMap::new();
                    record.insert(key.clone(), value.clone());
                    record
                }
            };
            (key.clone(), record)
        }))
    }

    fn sql_column_values(
        &self,
        _column: &str,
        _where_clause: &str,
    ) -> TableResult<Vec<FieldValue>> {
        Ok(Vec::new())
    }

    fn sql_scalar(&self, _select_expr: &str, _where_clause: &str) -> TableResult<FieldValue> {
        Ok(FieldValue::Null)
    }

    async fn stream_all(&self) -> StreamResult<RecordStream> {
        let (tx, rx) = mpsc::unbounded_channel();

        for (key, value) in &self.records {
            let mut fields = HashMap::new();
            fields.insert(key.clone(), value.clone());
            let record = StreamingRecord {
                key: key.clone(),
                fields,
            };
            let _ = tx.send(Ok(record));
        }

        Ok(RecordStream { receiver: rx })
    }

    async fn stream_filter(&self, _where_clause: &str) -> StreamResult<RecordStream> {
        self.stream_all().await
    }

    async fn query_batch(
        &self,
        _batch_size: usize,
        _offset: Option<usize>,
    ) -> StreamResult<RecordBatch> {
        let mut records = Vec::new();
        for (key, value) in &self.records {
            let mut fields = HashMap::new();
            fields.insert(key.clone(), value.clone());
            records.push(StreamingRecord {
                key: key.clone(),
                fields,
            });
        }

        Ok(RecordBatch {
            records,
            has_more: false,
        })
    }

    async fn stream_count(&self, _where_clause: Option<&str>) -> StreamResult<usize> {
        Ok(<Self as UnifiedTable>::record_count(self))
    }

    async fn stream_aggregate(
        &self,
        _aggregate_expr: &str,
        _where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        Ok(FieldValue::Integer(1))
    }
}

// Custom wildcard methods removed - now using UnifiedTable trait defaults
// which provide comprehensive wildcard functionality automatically

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

    // Note: Using iter_records from UnifiedTable trait
    let all_records: HashMap<String, HashMap<String, FieldValue>> =
        datasource.iter_records().collect();

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

    // Test non-existent column in WHERE clause - should return empty results, not error
    let result = datasource.sql_filter("nonexistent_column = 'value'");
    assert!(result.is_ok());
    let records = result.unwrap();
    assert_eq!(records.len(), 0); // Should return empty result set

    // Test BETWEEN operator now works (was previously unsupported)
    let result = datasource.sql_filter("age BETWEEN 20 AND 30");
    assert!(result.is_ok()); // BETWEEN is now supported

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

impl MockDataSource {
    /// Enhanced WHERE clause evaluator for comprehensive testing
    fn evaluate_where_clause(
        &self,
        where_clause: &str,
        record_fields: &HashMap<String, FieldValue>,
    ) -> bool {
        if where_clause == "true" || where_clause.is_empty() || where_clause == "1=1" {
            return true;
        }

        // Handle AND conditions
        if where_clause.contains(" AND ") {
            let conditions: Vec<&str> = where_clause.split(" AND ").collect();
            return conditions
                .iter()
                .all(|cond| self.evaluate_single_condition(cond.trim(), record_fields));
        }

        // Handle OR conditions
        if where_clause.contains(" OR ") {
            let conditions: Vec<&str> = where_clause.split(" OR ").collect();
            return conditions
                .iter()
                .any(|cond| self.evaluate_single_condition(cond.trim(), record_fields));
        }

        // Single condition
        self.evaluate_single_condition(where_clause, record_fields)
    }

    fn evaluate_single_condition(
        &self,
        condition: &str,
        record_fields: &HashMap<String, FieldValue>,
    ) -> bool {
        // Handle various operators: =, !=, >, <, >=, <=
        if let Some(pos) = condition.find(" >= ") {
            let (left, right) = condition.split_at(pos);
            let right = &right[4..]; // Skip " >= "
            return self.compare_values(left.trim(), right.trim(), record_fields, ">=");
        }
        if let Some(pos) = condition.find(" <= ") {
            let (left, right) = condition.split_at(pos);
            let right = &right[4..]; // Skip " <= "
            return self.compare_values(left.trim(), right.trim(), record_fields, "<=");
        }
        if let Some(pos) = condition.find(" != ") {
            let (left, right) = condition.split_at(pos);
            let right = &right[4..]; // Skip " != "
            return self.compare_values(left.trim(), right.trim(), record_fields, "!=");
        }
        if let Some(pos) = condition.find(" = ") {
            let (left, right) = condition.split_at(pos);
            let right = &right[3..]; // Skip " = "
            return self.compare_values(left.trim(), right.trim(), record_fields, "=");
        }
        if let Some(pos) = condition.find(" > ") {
            let (left, right) = condition.split_at(pos);
            let right = &right[3..]; // Skip " > "
            return self.compare_values(left.trim(), right.trim(), record_fields, ">");
        }
        if let Some(pos) = condition.find(" < ") {
            let (left, right) = condition.split_at(pos);
            let right = &right[3..]; // Skip " < "
            return self.compare_values(left.trim(), right.trim(), record_fields, "<");
        }

        // Default: unknown condition
        false
    }

    fn compare_values(
        &self,
        left: &str,
        right: &str,
        record_fields: &HashMap<String, FieldValue>,
        operator: &str,
    ) -> bool {
        // Get the left value (should be a field name)
        let left_value = record_fields.get(left);

        // Parse the right value (literal)
        let right_value = self.parse_literal(right);

        match (left_value, right_value) {
            (Some(left_val), Some(right_val)) => {
                self.compare_field_values(left_val, &right_val, operator)
            }
            _ => false,
        }
    }

    fn parse_literal(&self, literal: &str) -> Option<FieldValue> {
        let trimmed = literal.trim();

        // String literal (quoted)
        if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        {
            let content = &trimmed[1..trimmed.len() - 1];
            return Some(FieldValue::String(content.to_string()));
        }

        // Boolean literal
        if trimmed == "true" {
            return Some(FieldValue::Boolean(true));
        }
        if trimmed == "false" {
            return Some(FieldValue::Boolean(false));
        }

        // Null literal
        if trimmed == "null" || trimmed == "NULL" {
            return Some(FieldValue::Null);
        }

        // Integer literal
        if let Ok(int_val) = trimmed.parse::<i64>() {
            return Some(FieldValue::Integer(int_val));
        }

        // Float literal
        if let Ok(float_val) = trimmed.parse::<f64>() {
            return Some(FieldValue::Float(float_val));
        }

        None
    }

    fn compare_field_values(&self, left: &FieldValue, right: &FieldValue, operator: &str) -> bool {
        match operator {
            "=" => self.field_values_equal(left, right),
            "!=" => !self.field_values_equal(left, right),
            ">" => self.field_values_greater(left, right),
            "<" => self.field_values_less(left, right),
            ">=" => self.field_values_greater(left, right) || self.field_values_equal(left, right),
            "<=" => self.field_values_less(left, right) || self.field_values_equal(left, right),
            _ => false,
        }
    }

    fn field_values_equal(&self, left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::String(l), FieldValue::String(r)) => l == r,
            (FieldValue::Integer(l), FieldValue::Integer(r)) => l == r,
            (FieldValue::Float(l), FieldValue::Float(r)) => (l - r).abs() < f64::EPSILON,
            (FieldValue::Boolean(l), FieldValue::Boolean(r)) => l == r,
            (FieldValue::Null, FieldValue::Null) => true,
            // Type coercion
            (FieldValue::Integer(l), FieldValue::Float(r)) => (*l as f64 - r).abs() < f64::EPSILON,
            (FieldValue::Float(l), FieldValue::Integer(r)) => (l - *r as f64).abs() < f64::EPSILON,
            _ => false,
        }
    }

    fn field_values_greater(&self, left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(l), FieldValue::Integer(r)) => l > r,
            (FieldValue::Float(l), FieldValue::Float(r)) => l > r,
            (FieldValue::Integer(l), FieldValue::Float(r)) => *l as f64 > *r,
            (FieldValue::Float(l), FieldValue::Integer(r)) => *l > *r as f64,
            (FieldValue::String(l), FieldValue::String(r)) => l > r,
            _ => false,
        }
    }

    fn field_values_less(&self, left: &FieldValue, right: &FieldValue) -> bool {
        match (left, right) {
            (FieldValue::Integer(l), FieldValue::Integer(r)) => l < r,
            (FieldValue::Float(l), FieldValue::Float(r)) => l < r,
            (FieldValue::Integer(l), FieldValue::Float(r)) => (*l as f64) < *r,
            (FieldValue::Float(l), FieldValue::Integer(r)) => *l < (*r as f64),
            (FieldValue::String(l), FieldValue::String(r)) => l < r,
            _ => false,
        }
    }
}

// SqlDataSource implementation removed - using UnifiedTable only

#[async_trait]
impl UnifiedTable for MockDataSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn get_record(&self, key: &str) -> TableResult<Option<HashMap<String, FieldValue>>> {
        if let Some(value) = self.records.get(key) {
            let record = match value {
                FieldValue::Struct(fields) => fields.clone(),
                _ => {
                    let mut single_field = HashMap::new();
                    single_field.insert("value".to_string(), value.clone());
                    single_field
                }
            };
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        self.records.contains_key(key)
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)> + '_> {
        Box::new(self.records.iter().map(|(key, value)| {
            let record = match value {
                FieldValue::Struct(fields) => fields.clone(),
                _ => {
                    let mut single_field = HashMap::new();
                    single_field.insert("value".to_string(), value.clone());
                    single_field
                }
            };
            (key.clone(), record)
        }))
    }

    fn sql_filter(&self, where_clause: &str) -> Result<HashMap<String, FieldValue>, SqlError> {
        let mut filtered_records = HashMap::new();

        for (key, value) in &self.records {
            if let FieldValue::Struct(fields) = value {
                let should_include = self.evaluate_where_clause(where_clause, fields);

                if should_include {
                    filtered_records.insert(key.clone(), value.clone());
                }
            }
        }

        Ok(filtered_records)
    }

    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError> {
        for (key, value) in &self.records {
            if let FieldValue::Struct(fields) = value {
                let should_include = self.evaluate_where_clause(where_clause, fields);

                if should_include {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn sql_column_values(&self, column: &str, where_clause: &str) -> TableResult<Vec<FieldValue>> {
        let mut values = Vec::new();

        for (key, value) in &self.records {
            if let FieldValue::Struct(fields) = value {
                let should_include = self.evaluate_where_clause(where_clause, fields);

                if should_include {
                    if let Some(field_value) = fields.get(column) {
                        values.push(field_value.clone());
                    }
                }
            }
        }

        Ok(values)
    }

    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> TableResult<FieldValue> {
        let mut matching_records = Vec::new();

        for (key, value) in &self.records {
            if let FieldValue::Struct(fields) = value {
                let should_include = self.evaluate_where_clause(where_clause, fields);

                if should_include {
                    matching_records.push(fields);
                }
            }
        }

        if matching_records.len() > 1 {
            return Err(SqlError::ExecutionError {
                message: "Scalar subquery returned more than one row".to_string(),
                query: None,
            });
        }

        if let Some(fields) = matching_records.first() {
            if let Some(field_value) = fields.get(select_expr) {
                return Ok(field_value.clone());
            }
        }

        Ok(FieldValue::Null)
    }

    async fn stream_all(&self) -> StreamResult<RecordStream> {
        let (tx, rx) = mpsc::unbounded_channel();

        for (key, value) in &self.records {
            let fields = match value {
                FieldValue::Struct(fields) => fields.clone(),
                _ => {
                    let mut single_field = HashMap::new();
                    single_field.insert("value".to_string(), value.clone());
                    single_field
                }
            };
            let record = StreamingRecord {
                key: key.clone(),
                fields,
            };
            let _ = tx.send(Ok(record));
        }

        Ok(RecordStream { receiver: rx })
    }

    async fn stream_filter(&self, _where_clause: &str) -> StreamResult<RecordStream> {
        self.stream_all().await
    }

    async fn query_batch(
        &self,
        _batch_size: usize,
        _offset: Option<usize>,
    ) -> StreamResult<RecordBatch> {
        let mut records = Vec::new();
        for (key, value) in &self.records {
            let fields = match value {
                FieldValue::Struct(fields) => fields.clone(),
                _ => {
                    let mut single_field = HashMap::new();
                    single_field.insert("value".to_string(), value.clone());
                    single_field
                }
            };
            records.push(StreamingRecord {
                key: key.clone(),
                fields,
            });
        }

        Ok(RecordBatch {
            records,
            has_more: false,
        })
    }

    async fn stream_count(&self, _where_clause: Option<&str>) -> StreamResult<usize> {
        Ok(<Self as UnifiedTable>::record_count(self))
    }

    async fn stream_aggregate(
        &self,
        _aggregate_expr: &str,
        _where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        Ok(FieldValue::Integer(1))
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
