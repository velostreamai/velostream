/*!
# Targeted Correlated EXISTS Test

Ultra-focused test to isolate the exact correlation resolution issue:
`EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)`

This test creates the minimal reproduction case to understand and fix
the correlation variable binding problem.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Import shared test utilities
use crate::unit::sql::execution::common_test_utils::MockTable;

/// Minimal context setup with only the essential data for correlation testing
fn create_minimal_correlation_context() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        // Create minimal orders table with specific user_id values
        let orders_data = vec![
            // Order for user_id = 999 (should match when users.id = 999)
            {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(1001));
                fields.insert("user_id".to_string(), FieldValue::Integer(999));
                fields.insert("amount".to_string(), FieldValue::Float(100.0));
                StreamRecord {
                    fields,
                    headers: HashMap::new(),
                    event_time: None,
                    timestamp: 1640995200000,
                    offset: 1,
                    partition: 0,
                }
            },
            // Order for user_id = 888 (should match when users.id = 888)
            {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(1002));
                fields.insert("user_id".to_string(), FieldValue::Integer(888));
                fields.insert("amount".to_string(), FieldValue::Float(200.0));
                StreamRecord {
                    fields,
                    headers: HashMap::new(),
                    event_time: None,
                    timestamp: 1640995200001,
                    offset: 2,
                    partition: 0,
                }
            },
        ];

        let orders_table = MockTable::new("orders".to_string(), orders_data);
        context.load_reference_table(
            "orders",
            Arc::new(orders_table)
                as Arc<dyn velostream::velostream::table::sql::SqlQueryable + Send + Sync>,
        );

        println!("🎯 Minimal correlation context: orders table loaded with user_ids [999, 888]");
    })
}

async fn execute_targeted_test(
    query: &str,
    test_record: StreamRecord,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Set up minimal context
    engine.context_customizer = Some(create_minimal_correlation_context());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;

    println!("🎯 Executing query: {}", query.trim());
    println!("🎯 Test record id: {:?}", test_record.fields.get("id"));

    engine
        .execute_with_record(&parsed_query, test_record)
        .await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

/// Create a test record with specific id for correlation testing
fn create_user_record_with_id(id: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert(
        "name".to_string(),
        FieldValue::String(format!("User_{}", id)),
    );
    fields.insert(
        "email".to_string(),
        FieldValue::String(format!("user{}@example.com", id)),
    );

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
    }
}

// Test 1: Correlated EXISTS with id=999 (should find match)
#[tokio::test]
async fn test_correlated_exists_id_999_should_match() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)
    "#;

    // Test record with id=999 (should match because orders has user_id=999)
    let test_record = create_user_record_with_id(999);

    let result = execute_targeted_test(query, test_record).await;

    println!("🎯 CORRELATION TEST 1: users.id=999 → should find orders.user_id=999");
    match &result {
        Ok(results) => {
            println!("🎯 Result: {} records returned", results.len());
            if results.is_empty() {
                println!(
                    "❌ CORRELATION FAILURE: users.id=999 not resolving to find orders.user_id=999"
                );
                println!("   This confirms the correlation resolution bug");

                // Log the specific correlation issue
                panic!(
                    "CORRELATION BUG CONFIRMED: \n\
                    - Query: EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)\n\
                    - users.id should resolve to: 999\n\
                    - Should find: orders record with user_id=999\n\
                    - Actual result: No records found\n\
                    - Issue: users.id correlation reference not being resolved"
                );
            } else {
                println!(
                    "✅ SUCCESS: Correlation working! Found {} record(s)",
                    results.len()
                );
                assert_eq!(results.len(), 1, "Should return exactly one user record");
            }
        }
        Err(e) => {
            panic!("Query execution failed: {}", e);
        }
    }
}

// Test 2: Correlated EXISTS with id=888 (should find match)
#[tokio::test]
async fn test_correlated_exists_id_888_should_match() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)
    "#;

    // Test record with id=888 (should match because orders has user_id=888)
    let test_record = create_user_record_with_id(888);

    let result = execute_targeted_test(query, test_record).await;

    println!("🎯 CORRELATION TEST 2: users.id=888 → should find orders.user_id=888");
    match &result {
        Ok(results) => {
            println!("🎯 Result: {} records returned", results.len());
            if results.is_empty() {
                println!(
                    "❌ CORRELATION FAILURE: users.id=888 not resolving to find orders.user_id=888"
                );
                panic!("Correlation resolution failing for id=888");
            } else {
                println!("✅ SUCCESS: Found matching record for id=888");
            }
        }
        Err(e) => {
            panic!("Query execution failed: {}", e);
        }
    }
}

// Test 3: Correlated EXISTS with id=777 (should NOT find match)
#[tokio::test]
async fn test_correlated_exists_id_777_should_not_match() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)
    "#;

    // Test record with id=777 (should NOT match because orders has no user_id=777)
    let test_record = create_user_record_with_id(777);

    let result = execute_targeted_test(query, test_record).await;

    println!("🎯 CORRELATION TEST 3: users.id=777 → should NOT find orders.user_id=777");
    match &result {
        Ok(results) => {
            println!("🎯 Result: {} records returned", results.len());
            if !results.is_empty() {
                println!("❌ UNEXPECTED: Found records when none should exist for id=777");
                panic!("Should not find records for non-existent user_id=777");
            } else {
                println!("✅ CORRECT: No records found for id=777 (as expected)");
            }
        }
        Err(e) => {
            panic!("Query execution failed: {}", e);
        }
    }
}

// Test 4: Non-correlated EXISTS baseline (should work)
#[tokio::test]
async fn test_non_correlated_exists_baseline() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 999)
    "#;

    // Any test record should work for non-correlated query
    let test_record = create_user_record_with_id(999);

    let result = execute_targeted_test(query, test_record).await;

    println!("🎯 BASELINE TEST: Non-correlated EXISTS (user_id = 999)");
    match &result {
        Ok(results) => {
            println!("🎯 Result: {} records returned", results.len());
            if results.is_empty() {
                panic!("Non-correlated EXISTS should work (baseline test failed)");
            } else {
                println!("✅ BASELINE WORKING: Non-correlated EXISTS returns results");
            }
        }
        Err(e) => {
            panic!("Baseline test failed: {}", e);
        }
    }
}

// Test 5: Debug the correlation variable resolution
#[tokio::test]
async fn test_correlation_variable_debug() {
    // This test uses a very explicit correlation to help debug the issue
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (
            SELECT 1
            FROM orders
            WHERE orders.user_id = users.id
        )
    "#;

    let test_record = create_user_record_with_id(999);

    let result = execute_targeted_test(query, test_record).await;

    println!("🎯 DEBUG TEST: Explicit table.column correlation (orders.user_id = users.id)");
    match &result {
        Ok(results) => {
            println!("🎯 Result: {} records returned", results.len());
            if results.is_empty() {
                println!("❌ Even explicit table.column correlation failing");
                println!("   This suggests the issue is in correlation context binding");
            } else {
                println!("✅ Explicit correlation working");
            }
        }
        Err(e) => {
            panic!("Debug test failed: {}", e);
        }
    }
}

// Test 6: Test correlation with different comparison operators
#[tokio::test]
async fn test_correlation_with_different_operators() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (
            SELECT 1
            FROM orders
            WHERE user_id > users.id - 1 AND user_id < users.id + 1
        )
    "#;

    let test_record = create_user_record_with_id(999);

    let result = execute_targeted_test(query, test_record).await;

    println!("🎯 OPERATOR TEST: Correlation with range operators (user_id > users.id - 1 AND user_id < users.id + 1)");
    match &result {
        Ok(results) => {
            println!("🎯 Result: {} records returned", results.len());
            if results.is_empty() {
                println!("❌ Complex correlation expressions also failing");
            } else {
                println!("✅ Complex correlation expressions working");
            }
        }
        Err(e) => {
            panic!("Operator test failed: {}", e);
        }
    }
}

// Test 7: Minimal reproduction case
#[tokio::test]
async fn test_minimal_reproduction_case() {
    println!("\n🔥 MINIMAL REPRODUCTION CASE FOR CORRELATION BUG 🔥");
    println!("==================================================");

    let query = "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)";
    let test_record = create_user_record_with_id(999);

    println!("Query: {}", query);
    println!("Test data:");
    println!("  - Input record: users.id = 999");
    println!("  - Orders table: contains record with user_id = 999");
    println!("Expected: users.id (999) should resolve in subquery, find match, return 1 record");

    let result = execute_targeted_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("Actual result: {} records returned", results.len());

            if results.is_empty() {
                println!("\n❌ BUG REPRODUCED: Correlation variable not being resolved");
                println!(
                    "🐛 Root cause: users.id in EXISTS subquery not binding to outer query value"
                );
                println!("💡 Fix needed: Correlation context resolution in EXISTS evaluation");

                // This is our target reproduction case
                panic!("MINIMAL REPRODUCTION: Correlation variable users.id not resolving to 999 in EXISTS subquery");
            } else {
                println!("\n✅ BUG FIXED: Correlation working correctly!");
            }
        }
        Err(e) => {
            panic!("Minimal reproduction test failed: {}", e);
        }
    }
}
