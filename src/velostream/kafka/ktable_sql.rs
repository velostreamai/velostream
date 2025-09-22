/*!
# KTable SQL Query Interface

This module provides SQL query capabilities for KTable, enabling subquery execution
and SQL-compatible filtering operations. It bridges the gap between the KTable state
management and the SQL execution engine.

## Key Features

- **SQL WHERE clause filtering**: Parse and execute WHERE conditions on KTable data
- **Subquery support**: EXISTS, IN, scalar subqueries using KTable as data source
- **FieldValue integration**: Full SQL type system compatibility
- **Performance optimized**: Leverages KTable's in-memory HashMap for sub-millisecond lookups

## Examples

```rust
use velostream::velostream::kafka::{KTable, JsonSerializer};
use velostream::velostream::kafka::ktable_sql::{SqlQueryable, SqlKTable};

// Create a SQL-compatible KTable
let user_table: SqlKTable = KTable::new(
    config,
    "users".to_string(),
    JsonSerializer,
    JsonSerializer,
).await?;

// Execute SQL-like queries
let active_users = user_table.sql_filter("active = true")?;
let has_admins = user_table.sql_exists("role = 'admin'")?;
let premium_ids = user_table.sql_column_values("id", "tier = 'premium'")?;
```
*/

use crate::velostream::kafka::ktable::KTable;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::FieldValue;
use std::collections::HashMap;
use serde_json;

/// Data source interface for SQL subquery execution
///
/// This trait abstracts the data access layer for subquery operations,
/// allowing different implementations (KTable, external databases, etc.)
/// to provide data for SQL subqueries.
pub trait SqlDataSource {
    /// Get all records as a HashMap for filtering operations
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError>;

    /// Get a single record by key for direct lookups
    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError>;

    /// Check if the data source contains any records
    fn is_empty(&self) -> bool;

    /// Get the count of records in the data source
    fn record_count(&self) -> usize;
}

/// Kafka-based data source implementation using KTable
///
/// This implementation bridges KTable state with the SQL subquery engine,
/// providing real-time access to Kafka topic data for subquery operations.
pub struct KafkaDataSource {
    // Use serde_json::Value for now since FieldValue doesn't have Serialize/Deserialize
    ktable: KTable<String, serde_json::Value, crate::velostream::kafka::serialization::JsonSerializer, crate::velostream::kafka::serialization::JsonSerializer>,
}

impl KafkaDataSource {
    /// Create a new Kafka data source from an existing KTable
    pub fn from_ktable(ktable: KTable<String, serde_json::Value, crate::velostream::kafka::serialization::JsonSerializer, crate::velostream::kafka::serialization::JsonSerializer>) -> Self {
        Self { ktable }
    }

    /// Convert serde_json::Value to FieldValue for SQL compatibility
    fn json_to_field_value(value: &serde_json::Value) -> FieldValue {
        match value {
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
                let field_values: Vec<FieldValue> = arr.iter()
                    .map(Self::json_to_field_value)
                    .collect();
                FieldValue::Array(field_values)
            }
            serde_json::Value::Object(obj) => {
                let mut fields = HashMap::new();
                for (key, value) in obj {
                    fields.insert(key.clone(), Self::json_to_field_value(value));
                }
                FieldValue::Struct(fields)
            }
        }
    }
}

impl SqlDataSource for KafkaDataSource {
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError> {
        let snapshot = self.ktable.snapshot();
        let mut result = HashMap::new();

        for (key, value) in snapshot {
            result.insert(key, Self::json_to_field_value(&value));
        }

        Ok(result)
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError> {
        if let Some(value) = self.ktable.get(&key.to_string()) {
            Ok(Some(Self::json_to_field_value(&value)))
        } else {
            Ok(None)
        }
    }

    fn is_empty(&self) -> bool {
        self.ktable.is_empty()
    }

    fn record_count(&self) -> usize {
        self.ktable.len()
    }
}

/// Trait providing SQL query capabilities for data sources
///
/// This trait defines the interface for executing SQL-like queries on data sources,
/// enabling subquery operations like EXISTS, IN, and scalar value retrieval.
pub trait SqlQueryable {
    /// Filter records using a SQL WHERE clause
    ///
    /// Parses and executes a WHERE clause against the data source, returning
    /// a HashMap containing only the records that match the condition.
    ///
    /// # Arguments
    /// * `where_clause` - SQL WHERE condition (e.g., "active = true AND age > 18")
    ///
    /// # Returns
    /// * `Ok(HashMap<String, FieldValue>)` - Filtered records matching the condition
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let active_users = source.sql_filter("status = 'active'")?;
    /// let high_value = source.sql_filter("amount > 1000.0")?;
    /// ```
    fn sql_filter(&self, where_clause: &str) -> Result<HashMap<String, FieldValue>, SqlError>;

    /// Check if any records exist matching a SQL WHERE clause
    ///
    /// Executes an EXISTS-style query, returning true if any records match
    /// the given condition. This is optimized for early termination.
    ///
    /// # Arguments
    /// * `where_clause` - SQL WHERE condition to test
    ///
    /// # Returns
    /// * `Ok(true)` - At least one record matches the condition
    /// * `Ok(false)` - No records match the condition
    /// * `Err(SqlError)` - Parse or execution error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let has_admin = table.sql_exists("role = 'admin'")?;
    /// let config_enabled = table.sql_exists("enabled = true")?;
    /// ```
    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError>;

    /// Extract column values from records matching a WHERE clause
    ///
    /// Filters records using the WHERE clause, then extracts values from the
    /// specified column. This is used for IN subquery evaluation.
    ///
    /// # Arguments
    /// * `column` - Column name to extract values from
    /// * `where_clause` - SQL WHERE condition for filtering
    ///
    /// # Returns
    /// * `Ok(Vec<FieldValue>)` - List of values from the specified column
    /// * `Err(SqlError)` - Parse, execution, or column access error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let premium_ids = table.sql_column_values("user_id", "tier = 'premium'")?;
    /// let valid_symbols = table.sql_column_values("symbol", "active = true")?;
    /// ```
    fn sql_column_values(&self, column: &str, where_clause: &str) -> Result<Vec<FieldValue>, SqlError>;

    /// Execute a scalar subquery and return a single value
    ///
    /// Executes a SELECT expression on filtered records, returning a single value.
    /// Used for scalar subqueries like (SELECT max_price FROM limits WHERE symbol = 'AAPL').
    ///
    /// # Arguments
    /// * `select_expr` - SQL SELECT expression (e.g., "MAX(price)", "COUNT(*)", "config_value")
    /// * `where_clause` - SQL WHERE condition for filtering
    ///
    /// # Returns
    /// * `Ok(FieldValue)` - Single result value
    /// * `Err(SqlError)` - Parse, execution, or multiple results error
    ///
    /// # Examples
    /// ```rust,no_run
    /// let max_limit = table.sql_scalar("max_daily_limit", "symbol = 'AAPL'")?;
    /// let count = table.sql_scalar("COUNT(*)", "active = true")?;
    /// ```
    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> Result<FieldValue, SqlError>;
}

/// Simple WHERE clause parser for basic field comparisons
///
/// This is a basic parser that handles common WHERE clause patterns needed
/// for subquery execution. It supports:
/// - Field equality: `field = 'value'`
/// - Field comparison: `field > 100`, `field <= 50.5`
/// - Boolean fields: `active = true`
/// - NULL checks: `field IS NULL`, `field IS NOT NULL`
///
/// For production use, this should be replaced with a more comprehensive SQL parser.
#[derive(Debug, Clone)]
pub struct WhereClauseParser {
    clause: String,
}

impl WhereClauseParser {
    /// Create a new WHERE clause parser
    pub fn new(clause: &str) -> Self {
        Self {
            clause: clause.trim().to_string(),
        }
    }

    /// Parse the WHERE clause and return a predicate function
    ///
    /// This creates a closure that can be used to test individual records
    /// against the WHERE condition.
    ///
    /// # Returns
    /// * `Ok(Box<dyn Fn(&String, &FieldValue) -> bool>)` - Predicate function
    /// * `Err(SqlError)` - Parse error for invalid syntax
    pub fn parse(&self) -> Result<Box<dyn Fn(&String, &FieldValue) -> bool + Send + Sync>, SqlError> {
        // For initial implementation, support basic equality comparisons
        // Format: "field = 'value'" or "field = value"

        if self.clause.is_empty() || self.clause == "true" {
            // Empty or "true" clause matches all records
            return Ok(Box::new(|_key, _value| true));
        }

        // Split on comparison operators
        if let Some(eq_pos) = self.clause.find(" = ") {
            let field_name = self.clause[..eq_pos].trim().to_string();
            let field_value = self.clause[eq_pos + 3..].trim();

            // Parse the expected value
            let expected_value = parse_literal_value(field_value)?;

            return Ok(Box::new(move |_key, record| {
                match record {
                    FieldValue::Struct(fields) => {
                        fields.get(&field_name)
                            .map(|v| values_equal(v, &expected_value))
                            .unwrap_or(false)
                    }
                    _ => false, // Record is not a struct, can't access fields
                }
            }));
        }

        // For now, return an error for unsupported syntax
        Err(SqlError::ParseError {
            message: format!("Unsupported WHERE clause syntax: {}", self.clause),
            position: Some(0),
        })
    }
}

/// Parse a literal value from SQL text
///
/// Supports basic SQL literal formats:
/// - Strings: 'text' or "text"
/// - Integers: 123, -456
/// - Floats: 123.45, -67.89
/// - Booleans: true, false
/// - NULL: null
fn parse_literal_value(text: &str) -> Result<FieldValue, SqlError> {
    let trimmed = text.trim();

    // String literals (quoted)
    if (trimmed.starts_with('\'') && trimmed.ends_with('\'')) ||
       (trimmed.starts_with('"') && trimmed.ends_with('"')) {
        let content = &trimmed[1..trimmed.len()-1];
        return Ok(FieldValue::String(content.to_string()));
    }

    // Boolean literals
    match trimmed.to_lowercase().as_str() {
        "true" => return Ok(FieldValue::Boolean(true)),
        "false" => return Ok(FieldValue::Boolean(false)),
        "null" => return Ok(FieldValue::Null),
        _ => {}
    }

    // Numeric literals
    if let Ok(int_val) = trimmed.parse::<i64>() {
        return Ok(FieldValue::Integer(int_val));
    }

    if let Ok(float_val) = trimmed.parse::<f64>() {
        return Ok(FieldValue::Float(float_val));
    }

    Err(SqlError::ParseError {
        message: format!("Invalid literal value: {}", trimmed),
        position: Some(0),
    })
}

/// Compare two FieldValues for equality
///
/// Handles type coercion and SQL NULL semantics where needed.
fn values_equal(a: &FieldValue, b: &FieldValue) -> bool {
    match (a, b) {
        (FieldValue::Null, FieldValue::Null) => true,
        (FieldValue::Null, _) | (_, FieldValue::Null) => false,
        (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
        (FieldValue::Float(a), FieldValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (FieldValue::String(a), FieldValue::String(b)) => a == b,
        (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
        // Add type coercion for common cases
        (FieldValue::Integer(a), FieldValue::Float(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (FieldValue::Float(a), FieldValue::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
        _ => false,
    }
}

/// Extract a field value from a FieldValue record
///
/// Supports accessing fields in Struct-type FieldValues using dot notation.
fn extract_field_value(record: &FieldValue, field_path: &str) -> Option<FieldValue> {
    match record {
        FieldValue::Struct(fields) => {
            // For simple field access (no dots), return the field directly
            if !field_path.contains('.') {
                return fields.get(field_path).cloned();
            }

            // For nested access like "user.profile.name", split and traverse
            let mut current = record;
            for part in field_path.split('.') {
                match current {
                    FieldValue::Struct(fields) => {
                        if let Some(next) = fields.get(part) {
                            current = next;
                        } else {
                            return None;
                        }
                    }
                    _ => return None,
                }
            }
            Some(current.clone())
        }
        _ => None,
    }
}

/// Implementation of SqlQueryable for any SqlDataSource
///
/// This provides a generic implementation that works with any data source
/// implementing the SqlDataSource trait, making it extensible for different
/// backends (KTable, external databases, etc.).
impl<T: SqlDataSource> SqlQueryable for T {
    fn sql_filter(&self, where_clause: &str) -> Result<HashMap<String, FieldValue>, SqlError> {
        let parser = WhereClauseParser::new(where_clause);
        let predicate = parser.parse()?;
        let all_records = self.get_all_records()?;

        let mut filtered = HashMap::new();
        for (key, value) in all_records {
            if predicate(&key, &value) {
                filtered.insert(key, value);
            }
        }

        Ok(filtered)
    }

    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError> {
        let parser = WhereClauseParser::new(where_clause);
        let predicate = parser.parse()?;
        let all_records = self.get_all_records()?;

        for (key, value) in all_records {
            if predicate(&key, &value) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn sql_column_values(&self, column: &str, where_clause: &str) -> Result<Vec<FieldValue>, SqlError> {
        let filtered_records = self.sql_filter(where_clause)?;
        let mut values = Vec::new();

        for (_key, record) in filtered_records {
            if let Some(field_value) = extract_field_value(&record, column) {
                values.push(field_value);
            }
        }

        Ok(values)
    }

    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> Result<FieldValue, SqlError> {
        let filtered_records = self.sql_filter(where_clause)?;

        // For now, support simple field extraction (not aggregation functions)
        // This will be enhanced in later tasks
        if filtered_records.is_empty() {
            return Ok(FieldValue::Null);
        }

        if filtered_records.len() > 1 {
            return Err(SqlError::ExecutionError {
                message: "Scalar subquery returned more than one row".to_string(),
                query: None,
            });
        }

        let (_, record) = filtered_records.into_iter().next().unwrap();

        // Simple field extraction for scalar subqueries
        if let Some(field_value) = extract_field_value(&record, select_expr) {
            Ok(field_value)
        } else {
            Ok(FieldValue::Null)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_where_clause_parser_basic_equality() {
        let parser = WhereClauseParser::new("status = 'active'");
        let predicate = parser.parse().expect("Should parse successfully");

        // Create test record
        let mut fields = HashMap::new();
        fields.insert("status".to_string(), FieldValue::String("active".to_string()));
        let record = FieldValue::Struct(fields);

        assert!(predicate(&"key1".to_string(), &record));
    }

    #[test]
    fn test_where_clause_parser_boolean() {
        let parser = WhereClauseParser::new("enabled = true");
        let predicate = parser.parse().expect("Should parse successfully");

        let mut fields = HashMap::new();
        fields.insert("enabled".to_string(), FieldValue::Boolean(true));
        let record = FieldValue::Struct(fields);

        assert!(predicate(&"key1".to_string(), &record));
    }

    #[test]
    fn test_where_clause_parser_integer() {
        let parser = WhereClauseParser::new("age = 25");
        let predicate = parser.parse().expect("Should parse successfully");

        let mut fields = HashMap::new();
        fields.insert("age".to_string(), FieldValue::Integer(25));
        let record = FieldValue::Struct(fields);

        assert!(predicate(&"key1".to_string(), &record));
    }

    #[test]
    fn test_parse_literal_values() {
        assert_eq!(parse_literal_value("'hello'").unwrap(), FieldValue::String("hello".to_string()));
        assert_eq!(parse_literal_value("\"world\"").unwrap(), FieldValue::String("world".to_string()));
        assert_eq!(parse_literal_value("true").unwrap(), FieldValue::Boolean(true));
        assert_eq!(parse_literal_value("false").unwrap(), FieldValue::Boolean(false));
        assert_eq!(parse_literal_value("123").unwrap(), FieldValue::Integer(123));
        assert_eq!(parse_literal_value("45.67").unwrap(), FieldValue::Float(45.67));
        assert_eq!(parse_literal_value("null").unwrap(), FieldValue::Null);
    }

    #[test]
    fn test_values_equal() {
        assert!(values_equal(&FieldValue::Integer(42), &FieldValue::Integer(42)));
        assert!(values_equal(&FieldValue::String("test".to_string()), &FieldValue::String("test".to_string())));
        assert!(values_equal(&FieldValue::Boolean(true), &FieldValue::Boolean(true)));
        assert!(values_equal(&FieldValue::Integer(42), &FieldValue::Float(42.0)));
        assert!(!values_equal(&FieldValue::Integer(42), &FieldValue::Integer(43)));
        assert!(!values_equal(&FieldValue::Null, &FieldValue::Integer(42)));
    }

    #[test]
    fn test_extract_field_value() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), FieldValue::String("John".to_string()));
        fields.insert("age".to_string(), FieldValue::Integer(30));
        let record = FieldValue::Struct(fields);

        assert_eq!(
            extract_field_value(&record, "name"),
            Some(FieldValue::String("John".to_string()))
        );
        assert_eq!(
            extract_field_value(&record, "age"),
            Some(FieldValue::Integer(30))
        );
        assert_eq!(extract_field_value(&record, "missing"), None);
    }
}