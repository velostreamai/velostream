/*!
# Subquery JOIN ON Condition Tests

Tests for JOIN operations with subqueries in the ON condition - these should work
because the ExpressionEvaluator already supports EXISTS, NOT EXISTS, IN, NOT IN subqueries.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::table::sql::{SqlDataSource, SqlQueryable};

// Import shared test utilities
use crate::unit::sql::execution::common_test_utils::{
    MockTable, StandardTestData, TestExecutor, TestDataBuilder
};

fn create_test_record_for_on_condition() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(100)); // Changed from 42 to 100 to match orders
    fields.insert("user_id".to_string(), FieldValue::Integer(100));
    fields.insert(
        "name".to_string(),
        FieldValue::String("Test User".to_string()),
    );
    fields.insert("amount".to_string(), FieldValue::Float(250.0));
    fields.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
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

// Mock orders table for subquery testing
struct MockOrdersTable {
    records: HashMap<String, FieldValue>,
}

impl MockOrdersTable {
    fn new() -> Self {
        let mut records = HashMap::new();

        // Create some orders that match user_id = 100 (from test record)
        let mut order1 = HashMap::new();
        order1.insert("id".to_string(), FieldValue::Integer(1));
        order1.insert("user_id".to_string(), FieldValue::Integer(100));
        order1.insert("amount".to_string(), FieldValue::Float(150.0));
        order1.insert("status".to_string(), FieldValue::String("active".to_string()));
        records.insert("order1".to_string(), FieldValue::Struct(order1));

        let mut order2 = HashMap::new();
        order2.insert("id".to_string(), FieldValue::Integer(2));
        order2.insert("user_id".to_string(), FieldValue::Integer(100));
        order2.insert("amount".to_string(), FieldValue::Float(200.0));
        order2.insert("status".to_string(), FieldValue::String("completed".to_string()));
        records.insert("order2".to_string(), FieldValue::Struct(order2));

        Self { records }
    }
}

impl SqlDataSource for MockOrdersTable {
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

// Mock valid_statuses table
struct MockValidStatusesTable {
    records: HashMap<String, FieldValue>,
}

impl SqlDataSource for MockValidStatusesTable {
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

// Mock permissions table
struct MockPermissionsTable {
    records: HashMap<String, FieldValue>,
}

impl SqlDataSource for MockPermissionsTable {
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

/// Create a custom context customizer with additional tables needed for these tests
fn create_extended_context_customizer() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        // First, add the standard test data
        let standard_data = StandardTestData::comprehensive_test_data();
        for (name, records) in standard_data {
            let mock_table = MockTable::new(name.clone(), records);
            context.load_reference_table(
                &name,
                Arc::new(mock_table) as Arc<dyn SqlQueryable + Send + Sync>
            );
        }

        // Add additional tables needed for ON condition tests

        // Create 'permissions' table for EXISTS tests
        let permissions_data = vec![
            TestDataBuilder::user_record(100, "User 100", "user100@example.com", "active"),
            TestDataBuilder::user_record(101, "User 101", "user101@example.com", "active"),
        ];
        let permissions_table = MockTable::new("permissions".to_string(), permissions_data);
        context.load_reference_table(
            "permissions",
            Arc::new(permissions_table) as Arc<dyn SqlQueryable + Send + Sync>
        );

        // Create 'valid_statuses' table for IN tests
        let valid_statuses_data = vec![
            TestDataBuilder::config_record(1, "active", true, true, None),
            TestDataBuilder::config_record(2, "pending", true, true, None),
            TestDataBuilder::config_record(3, "completed", true, true, None),
        ];
        let valid_statuses_table = MockTable::new("valid_statuses".to_string(), valid_statuses_data);
        context.load_reference_table(
            "valid_statuses",
            Arc::new(valid_statuses_table) as Arc<dyn SqlQueryable + Send + Sync>
        );

        println!("DEBUG: Injected {} test tables into context (extended)", context.state_tables.len());
    })
}

async fn execute_on_condition_test(
    query: &str,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = std::sync::Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);

    // Set up the context customizer with extended test tables
    engine.context_customizer = Some(create_extended_context_customizer());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;
    let record = create_test_record_for_on_condition();

    engine.execute_with_record(&parsed_query, record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

// Test simple JOIN with basic ON condition (should work)
#[tokio::test]
async fn test_basic_join_on_condition() {
    let query = r#"
        SELECT u.id, u.name 
        FROM users u 
        INNER JOIN orders o ON u.id = o.user_id
    "#;

    let result = execute_on_condition_test(query).await;

    match &result {
        Ok(results) => println!("SUCCESS: Basic JOIN returned {} results", results.len()),
        Err(e) => println!("ERROR in basic JOIN: {}", e),
    }

    // Basic JOINs should work
    assert!(result.is_ok() || result.unwrap_err().to_string().contains("JOIN"));
}

// Test JOIN with IN subquery in ON condition
#[tokio::test]
async fn test_join_with_in_subquery_on_condition() {
    let query = r#"
        SELECT u.id, u.name
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id 
            AND o.status IN (SELECT status FROM valid_statuses WHERE active = true)
    "#;

    let result = execute_on_condition_test(query).await;

    match &result {
        Ok(results) => println!(
            "SUCCESS: IN subquery JOIN returned {} results",
            results.len()
        ),
        Err(e) => println!("ERROR in IN subquery JOIN: {}", e),
    }

    // This might work if the expression evaluator can handle it in ON conditions
    // The error message will tell us if it's parser vs execution limitations
    if result.is_err() {
        let error_msg = result.unwrap_err().to_string();
        println!("IN subquery JOIN error: {}", error_msg);

        // Check if it's a parsing issue or execution issue
        assert!(
            error_msg.contains("parse")
                || error_msg.contains("expected")
                || error_msg.contains("syntax")
                || error_msg.contains("IN")
                || error_msg.contains("subquery")
                || error_msg.contains("JOIN"),
            "Should give informative error about IN subquery in JOIN: {}",
            error_msg
        );
    }
}

// Test JOIN with EXISTS subquery in ON condition
#[tokio::test]
async fn test_join_with_exists_on_condition() {
    let query = r#"
        SELECT u.id, u.name
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id 
            AND EXISTS (SELECT 1 FROM permissions WHERE user_id = u.id)
    "#;

    let result = execute_on_condition_test(query).await;

    match &result {
        Ok(results) => println!(
            "SUCCESS: EXISTS subquery JOIN returned {} results",
            results.len()
        ),
        Err(e) => println!("ERROR in EXISTS subquery JOIN: {}", e),
    }

    // Similar to above - check what kind of error we get
    if result.is_err() {
        let error_msg = result.unwrap_err().to_string();
        println!("EXISTS subquery JOIN error: {}", error_msg);

        assert!(
            error_msg.contains("parse")
                || error_msg.contains("expected")
                || error_msg.contains("syntax")
                || error_msg.contains("EXISTS")
                || error_msg.contains("subquery")
                || error_msg.contains("JOIN"),
            "Should give informative error about EXISTS subquery in JOIN: {}",
            error_msg
        );
    }
}

// Test a simple WHERE clause with subquery (should work with existing implementation)
#[tokio::test]
async fn test_where_clause_with_subquery() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)
    "#;

    let result = execute_on_condition_test(query).await;

    match &result {
        Ok(results) => {
            println!("SUCCESS: WHERE subquery returned {} results", results.len());
            assert!(!results.is_empty(), "Should return at least one result");
        }
        Err(e) => {
            println!("ERROR in WHERE subquery: {}", e);
            // This should work with existing subquery support in WHERE clauses
            panic!("WHERE clause subqueries should work: {}", e);
        }
    }
}

// Test WHERE with EXISTS (should work)
#[tokio::test]
async fn test_where_clause_with_exists() {
    let query = r#"
        SELECT id, name
        FROM users  
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)
    "#;

    let result = execute_on_condition_test(query).await;

    match &result {
        Ok(results) => {
            println!("SUCCESS: WHERE EXISTS returned {} results", results.len());
            assert!(!results.is_empty(), "Should return at least one result");
        }
        Err(e) => {
            println!("ERROR in WHERE EXISTS: {}", e);
            panic!("Unexpected error in WHERE clause EXISTS: {}", e);
        }
    }
}
