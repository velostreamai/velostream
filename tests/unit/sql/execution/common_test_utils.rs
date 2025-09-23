/*!
# Common Test Utilities for SQL Execution Tests

This module provides shared utilities for all SQL execution tests, including:
- Mock table implementations for SqlQueryable trait
- Common test data sources and builders
- Test record creation utilities
- SQL execution helpers with context injection
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::table::sql::SqlQueryable;

// ============================================================================
// MOCK TABLE IMPLEMENTATION
// ============================================================================

/// Mock table implementation for testing
///
/// Provides a simple in-memory table that implements SqlQueryable
/// for testing subquery execution without needing real data sources.
pub struct MockTable {
    pub name: String,
    pub records: Vec<StreamRecord>,
}

impl MockTable {
    /// Create a new mock table with the given name and records
    pub fn new(name: String, records: Vec<StreamRecord>) -> Self {
        Self { name, records }
    }

    /// Helper to evaluate a simple WHERE clause against a record
    /// This is a simplified implementation for testing
    fn evaluate_where_clause(&self, record: &StreamRecord, where_clause: &str) -> bool {
        // For testing, we'll implement basic equality checks
        // Format: "column = value" or "column = 'string'"

        if where_clause.is_empty() || where_clause == "true" {
            return true;
        }

        if where_clause == "false" {
            return false;
        }

        // Simple parsing for basic conditions
        let parts: Vec<&str> = where_clause.split_whitespace().collect();
        if parts.len() >= 3 {
            let column = parts[0];
            let operator = parts[1];
            let value_str = parts[2..].join(" ");

            if let Some(field_value) = record.fields.get(column) {
                match operator {
                    "=" | "==" => {
                        return self.compare_values(field_value, &value_str, "=");
                    }
                    "!=" | "<>" => {
                        return self.compare_values(field_value, &value_str, "!=");
                    }
                    ">" => {
                        return self.compare_values(field_value, &value_str, ">");
                    }
                    "<" => {
                        return self.compare_values(field_value, &value_str, "<");
                    }
                    ">=" => {
                        return self.compare_values(field_value, &value_str, ">=");
                    }
                    "<=" => {
                        return self.compare_values(field_value, &value_str, "<=");
                    }
                    "IN" => {
                        // For IN clauses, we'll skip for now as they're more complex
                        return true;
                    }
                    _ => {}
                }
            }
        }

        // Default to true for unsupported clauses in test mode
        true
    }

    fn compare_values(&self, field_value: &FieldValue, value_str: &str, operator: &str) -> bool {
        // Remove quotes if present
        let clean_value = value_str.trim_matches('\'').trim_matches('"');

        match field_value {
            FieldValue::Integer(i) => {
                if let Ok(v) = clean_value.parse::<i64>() {
                    match operator {
                        "=" => *i == v,
                        "!=" => *i != v,
                        ">" => *i > v,
                        "<" => *i < v,
                        ">=" => *i >= v,
                        "<=" => *i <= v,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            FieldValue::Float(f) => {
                if let Ok(v) = clean_value.parse::<f64>() {
                    match operator {
                        "=" => (*f - v).abs() < 0.0001,
                        "!=" => (*f - v).abs() >= 0.0001,
                        ">" => *f > v,
                        "<" => *f < v,
                        ">=" => *f >= v,
                        "<=" => *f <= v,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            FieldValue::String(s) => match operator {
                "=" => s == clean_value,
                "!=" => s != clean_value,
                _ => false,
            },
            FieldValue::Boolean(b) => {
                if let Ok(v) = clean_value.parse::<bool>() {
                    match operator {
                        "=" => *b == v,
                        "!=" => *b != v,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl SqlQueryable for MockTable {
    fn sql_filter(&self, where_clause: &str) -> Result<HashMap<String, FieldValue>, SqlError> {
        // For testing, return the first matching record's fields
        for record in &self.records {
            if self.evaluate_where_clause(record, where_clause) {
                return Ok(record.fields.clone());
            }
        }

        // Return empty if no records match
        Ok(HashMap::new())
    }

    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError> {
        // Check if any record matches the WHERE clause
        for record in &self.records {
            if self.evaluate_where_clause(record, where_clause) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn sql_column_values(
        &self,
        column: &str,
        where_clause: &str,
    ) -> Result<Vec<FieldValue>, SqlError> {
        let mut values = Vec::new();

        for record in &self.records {
            if self.evaluate_where_clause(record, where_clause) {
                if let Some(value) = record.fields.get(column) {
                    values.push(value.clone());
                }
            }
        }

        Ok(values)
    }

    fn sql_scalar(&self, select_expr: &str, where_clause: &str) -> Result<FieldValue, SqlError> {
        // Simple implementation for COUNT(*) and other basic expressions
        if select_expr == "COUNT(*)" {
            let mut count = 0;
            for record in &self.records {
                if self.evaluate_where_clause(record, where_clause) {
                    count += 1;
                }
            }
            return Ok(FieldValue::Integer(count));
        }

        // For column selection, return the first matching value
        for record in &self.records {
            if self.evaluate_where_clause(record, where_clause) {
                if let Some(value) = record.fields.get(select_expr) {
                    return Ok(value.clone());
                }
            }
        }

        // Return NULL if no match
        Ok(FieldValue::Null)
    }

    fn sql_wildcard_values(&self, wildcard_expr: &str) -> Result<Vec<FieldValue>, SqlError> {
        // For testing, we'll return empty vec as wildcard support is not needed for basic tests
        Ok(Vec::new())
    }

    fn sql_wildcard_aggregate(&self, aggregate_expr: &str) -> Result<FieldValue, SqlError> {
        // For testing, we'll return NULL as wildcard aggregates are not needed for basic tests
        Ok(FieldValue::Null)
    }
}

// ============================================================================
// TEST DATA BUILDERS
// ============================================================================

/// Common test data builder for creating realistic test records
pub struct TestDataBuilder;

impl TestDataBuilder {
    /// Create a basic user record
    pub fn user_record(id: i64, name: &str, email: &str, status: &str) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("user_id".to_string(), FieldValue::Integer(id)); // For compatibility
        fields.insert("name".to_string(), FieldValue::String(name.to_string()));
        fields.insert("email".to_string(), FieldValue::String(email.to_string()));
        fields.insert("status".to_string(), FieldValue::String(status.to_string()));

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1640995200000,
            offset: 1,
            partition: 0,
        }
    }

    /// Create a basic order record
    pub fn order_record(
        id: i64,
        user_id: i64,
        amount: f64,
        status: &str,
        product_id: Option<i64>,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
        fields.insert("amount".to_string(), FieldValue::Float(amount));
        fields.insert("status".to_string(), FieldValue::String(status.to_string()));
        if let Some(pid) = product_id {
            fields.insert("product_id".to_string(), FieldValue::Integer(pid));
        }

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1640995200000,
            offset: 1,
            partition: 0,
        }
    }

    /// Create a product record
    pub fn product_record(
        id: i64,
        name: &str,
        price: f64,
        category: &str,
        owner_id: Option<i64>,
        category_id: Option<i64>,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("name".to_string(), FieldValue::String(name.to_string()));
        fields.insert("product_name".to_string(), FieldValue::String(name.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("category".to_string(), FieldValue::String(category.to_string()));
        if let Some(oid) = owner_id {
            fields.insert("owner_id".to_string(), FieldValue::Integer(oid));
        }
        if let Some(cid) = category_id {
            fields.insert("category_id".to_string(), FieldValue::Integer(cid));
        }

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1640995200000,
            offset: 1,
            partition: 0,
        }
    }

    /// Create a category record
    pub fn category_record(id: i64, name: &str, active: bool, featured: bool) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("name".to_string(), FieldValue::String(name.to_string()));
        fields.insert("active".to_string(), FieldValue::Boolean(active));
        fields.insert("featured".to_string(), FieldValue::Boolean(featured));

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1640995200000,
            offset: 1,
            partition: 0,
        }
    }

    /// Create a config record
    pub fn config_record(
        id: i64,
        config_name: &str,
        active: bool,
        enabled: bool,
        max_value: Option<i64>,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(id));
        fields.insert("config_name".to_string(), FieldValue::String(config_name.to_string()));
        fields.insert("active".to_string(), FieldValue::Boolean(active));
        fields.insert("enabled".to_string(), FieldValue::Boolean(enabled));
        if let Some(max_val) = max_value {
            fields.insert("max_value".to_string(), FieldValue::Integer(max_val));
        }

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1640995200000,
            offset: 1,
            partition: 0,
        }
    }

    /// Create a basic ticker record for financial tests
    pub fn ticker_record(
        symbol: &str,
        price: f64,
        volume: i64,
        timestamp_seconds: i64,
    ) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("volume".to_string(), FieldValue::Integer(volume));
        fields.insert("bid".to_string(), FieldValue::Float(price - 0.01));
        fields.insert("ask".to_string(), FieldValue::Float(price + 0.01));
        fields.insert("spread".to_string(), FieldValue::Float(0.02));
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer(timestamp_seconds * 1000),
        );

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: timestamp_seconds * 1000,
            offset: 1,
            partition: 0,
        }
    }
}

// ============================================================================
// STANDARD TEST DATA SETS
// ============================================================================

/// Standard test data sets that can be reused across multiple tests
pub struct StandardTestData;

impl StandardTestData {
    /// Create standard user table data
    pub fn users() -> Vec<StreamRecord> {
        vec![
            TestDataBuilder::user_record(100, "Test User", "user@example.com", "active"),
            TestDataBuilder::user_record(101, "Another User", "user2@example.com", "active"),
            TestDataBuilder::user_record(102, "Inactive User", "user3@example.com", "inactive"),
        ]
    }

    /// Create standard orders table data
    pub fn orders() -> Vec<StreamRecord> {
        vec![
            TestDataBuilder::order_record(1001, 100, 250.50, "completed", Some(50)),
            TestDataBuilder::order_record(1002, 100, 175.25, "pending", Some(51)),
            TestDataBuilder::order_record(1003, 101, 500.00, "completed", Some(52)),
            TestDataBuilder::order_record(1004, 102, 99.99, "cancelled", Some(53)),
        ]
    }

    /// Create standard products table data
    pub fn products() -> Vec<StreamRecord> {
        vec![
            TestDataBuilder::product_record(1, "laptop", 999.99, "electronics", Some(100), Some(10)),
            TestDataBuilder::product_record(2, "phone", 599.99, "electronics", Some(101), Some(10)),
            TestDataBuilder::product_record(3, "book", 29.99, "books", Some(100), Some(11)),
            TestDataBuilder::product_record(4, "tablet", 349.99, "electronics", Some(102), Some(10)),
        ]
    }

    /// Create standard categories table data
    pub fn categories() -> Vec<StreamRecord> {
        vec![
            TestDataBuilder::category_record(10, "Electronics", true, true),
            TestDataBuilder::category_record(11, "Books", false, false),
            TestDataBuilder::category_record(12, "Clothing", true, false),
        ]
    }

    /// Create standard config table data
    pub fn config() -> Vec<StreamRecord> {
        vec![
            TestDataBuilder::config_record(1, "production", true, true, Some(100)),
            TestDataBuilder::config_record(2, "staging", false, false, Some(200)),
            TestDataBuilder::config_record(3, "development", true, true, Some(50)),
        ]
    }

    /// Create empty blocks table (for NOT EXISTS tests)
    pub fn blocks() -> Vec<StreamRecord> {
        Vec::new()
    }

    /// Create comprehensive test data sources for subquery testing
    pub fn comprehensive_test_data() -> HashMap<String, Vec<StreamRecord>> {
        let mut data_sources = HashMap::new();

        data_sources.insert("users".to_string(), Self::users());
        data_sources.insert("orders".to_string(), Self::orders());
        data_sources.insert("products".to_string(), Self::products());
        data_sources.insert("categories".to_string(), Self::categories());
        data_sources.insert("config".to_string(), Self::config());
        data_sources.insert("blocks".to_string(), Self::blocks());

        // Legacy aliases for backward compatibility
        data_sources.insert("active_configs".to_string(), vec![
            TestDataBuilder::config_record(1, "production", true, true, Some(100)),
        ]);

        data_sources
    }
}

// ============================================================================
// TEST EXECUTION HELPERS
// ============================================================================

/// Test execution helper that sets up proper context injection
pub struct TestExecutor;

impl TestExecutor {
    /// Create a context customizer that injects standard test tables
    pub fn create_standard_context_customizer() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
        Arc::new(move |context: &mut ProcessorContext| {
            let test_data = StandardTestData::comprehensive_test_data();

            for (name, records) in test_data {
                let mock_table = MockTable::new(name.clone(), records);
                context.load_reference_table(
                    &name,
                    Arc::new(mock_table) as Arc<dyn SqlQueryable + Send + Sync>
                );
            }

            println!("DEBUG: Injected {} test tables into context", context.state_tables.len());
        })
    }

    /// Create a context customizer with custom data
    pub fn create_custom_context_customizer(
        custom_data: HashMap<String, Vec<StreamRecord>>
    ) -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
        Arc::new(move |context: &mut ProcessorContext| {
            for (name, records) in &custom_data {
                let mock_table = MockTable::new(name.clone(), records.clone());
                context.load_reference_table(
                    name,
                    Arc::new(mock_table) as Arc<dyn SqlQueryable + Send + Sync>
                );
            }

            println!("DEBUG: Injected {} custom test tables into context", context.state_tables.len());
        })
    }

    /// Execute a SQL query with standard test data
    pub async fn execute_with_standard_data(
        query: &str,
        input_record: Option<StreamRecord>,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Set up the context customizer to inject test tables
        engine.context_customizer = Some(Self::create_standard_context_customizer());

        let parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(query)?;

        let record = input_record.unwrap_or_else(|| {
            TestDataBuilder::user_record(1, "Test User", "test@example.com", "active")
        });

        engine.execute_with_record(&parsed_query, record).await?;

        let mut results = Vec::new();
        while let Ok(result) = rx.try_recv() {
            results.push(result);
        }
        Ok(results)
    }

    /// Execute a SQL query with custom test data
    pub async fn execute_with_custom_data(
        query: &str,
        test_data: HashMap<String, Vec<StreamRecord>>,
        input_record: Option<StreamRecord>,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Set up the context customizer to inject custom test tables
        engine.context_customizer = Some(Self::create_custom_context_customizer(test_data));

        let parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(query)?;

        let record = input_record.unwrap_or_else(|| {
            TestDataBuilder::user_record(1, "Test User", "test@example.com", "active")
        });

        engine.execute_with_record(&parsed_query, record).await?;

        let mut results = Vec::new();
        while let Ok(result) = rx.try_recv() {
            results.push(result);
        }
        Ok(results)
    }
}

// ============================================================================
// COMMON TEST RECORDS
// ============================================================================

/// Common test record creators for standard use cases
pub struct CommonTestRecords;

impl CommonTestRecords {
    /// Create a test record suitable for subquery join tests
    pub fn subquery_join_record() -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        fields.insert("user_id".to_string(), FieldValue::Integer(100));
        fields.insert("order_id".to_string(), FieldValue::Integer(500));
        fields.insert("name".to_string(), FieldValue::String("Test User".to_string()));
        fields.insert("amount".to_string(), FieldValue::Float(250.0));
        fields.insert("status".to_string(), FieldValue::String("active".to_string()));

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1234567890000,
            offset: 1,
            partition: 0,
        }
    }

    /// Create a test record for basic execution tests
    pub fn basic_execution_record() -> StreamRecord {
        TestDataBuilder::user_record(1, "Test User", "test@example.com", "active")
    }

    /// Create a test record for window function tests
    pub fn window_test_record() -> StreamRecord {
        TestDataBuilder::ticker_record("AAPL", 150.0, 1000, 1640995200)
    }
}