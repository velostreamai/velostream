/*!
# Correlated EXISTS Subquery Test

Focused test to isolate and diagnose the correlated subquery resolution issue
in EXISTS clauses. This test specifically checks if `users.id` references
are properly resolved to outer query values.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Import shared test utilities
use crate::unit::sql::execution::common_test_utils::{
    MockTable, StandardTestData, TestDataBuilder, TestExecutor,
};

/// Create a context customizer with specific test data for correlation testing
fn create_correlation_test_context() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        // Create specific test data for correlation testing

        // Users table with specific IDs
        let users_data = vec![
            TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active"),
            TestDataBuilder::user_record(200, "Bob", "bob@example.com", "active"),
            TestDataBuilder::user_record(300, "Charlie", "charlie@example.com", "inactive"),
        ];
        let users_table = MockTable::new("users".to_string(), users_data);
        context.load_reference_table("users", Arc::new(users_table));

        // Orders table with specific user_id relationships
        let orders_data = vec![
            TestDataBuilder::order_record(1001, 100, 250.50, "completed", Some(50)), // User 100 has orders
            TestDataBuilder::order_record(1002, 100, 175.25, "pending", Some(51)), // User 100 has orders
            TestDataBuilder::order_record(1003, 200, 500.00, "completed", Some(52)), // User 200 has orders
                                                                                     // User 300 has NO orders - important for testing
        ];
        let orders_table = MockTable::new("orders".to_string(), orders_data);
        context.load_reference_table("orders", Arc::new(orders_table));

        println!(
            "DEBUG: Correlation test context loaded with {} tables",
            context.state_tables.len()
        );
    })
}

async fn execute_correlation_test(
    query: &str,
    test_record: StreamRecord,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Set up the correlation test context
    engine.context_customizer = Some(create_correlation_test_context());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;

    engine
        .execute_with_record(&parsed_query, test_record)
        .await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

// Test 1: Basic non-correlated EXISTS (should work as baseline)
#[tokio::test]
async fn test_non_correlated_exists() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 100)
    "#;

    // Test with user record id=100 (should match because orders exist for user_id=100)
    let test_record = TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!(
                "✅ Non-correlated EXISTS returned {} results",
                results.len()
            );
            // This should work - EXISTS (SELECT 1 FROM orders WHERE user_id = 100) should find records
            assert!(
                !results.is_empty(),
                "Non-correlated EXISTS should return results when orders exist for user_id=100"
            );
        }
        Err(e) => {
            println!("❌ Non-correlated EXISTS failed: {}", e);
            panic!("Non-correlated EXISTS should work: {}", e);
        }
    }
}

// Test 2: Correlated EXISTS with exact match (the problematic case)
#[tokio::test]
async fn test_correlated_exists_exact_match() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)
    "#;

    // Test with user record id=100 (should match because orders exist with user_id=100)
    let test_record = TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!(
                "🔍 Correlated EXISTS (should match) returned {} results",
                results.len()
            );
            if results.is_empty() {
                println!("❌ ISSUE CONFIRMED: Correlated EXISTS failing to resolve users.id = 100");
                println!("   Query: WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)");
                println!("   Expected: users.id should resolve to 100, find matching orders");
                println!("   Actual: No results found - correlation resolution failing");

                // This is the exact issue we're diagnosing
                panic!("CORRELATION ISSUE: EXISTS subquery not resolving users.id to outer query value (100)");
            } else {
                println!("✅ Correlated EXISTS working correctly!");
                assert_eq!(results.len(), 1, "Should return exactly one user record");
            }
        }
        Err(e) => {
            println!("❌ Correlated EXISTS execution error: {}", e);
            panic!("Correlated EXISTS execution failed: {}", e);
        }
    }
}

// Test 3: Correlated EXISTS with no match (should return empty)
#[tokio::test]
async fn test_correlated_exists_no_match() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id)
    "#;

    // Test with user record id=300 (should NOT match because no orders exist with user_id=300)
    let test_record =
        TestDataBuilder::user_record(300, "Charlie", "charlie@example.com", "inactive");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!(
                "🔍 Correlated EXISTS (should NOT match) returned {} results",
                results.len()
            );
            if !results.is_empty() {
                println!("❌ Unexpected: Found results when none should exist for user_id=300");
                panic!("EXISTS should return empty for user_id=300 (no orders)");
            } else {
                println!("✅ Correlated EXISTS correctly returned no results for user_id=300");
            }
        }
        Err(e) => {
            println!("❌ Correlated EXISTS execution error: {}", e);
            panic!("Correlated EXISTS execution failed: {}", e);
        }
    }
}

// Test 4: Compare with working IN subquery (correlation baseline)
#[tokio::test]
async fn test_correlated_in_subquery_baseline() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)
    "#;

    // Test with user record id=100 (should match because orders exist with user_id=100 and amount > 100)
    let test_record = TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!(
                "✅ IN subquery (baseline) returned {} results",
                results.len()
            );
            assert!(!results.is_empty(), "IN subquery should work as baseline");
        }
        Err(e) => {
            println!("❌ IN subquery baseline failed: {}", e);
            panic!("IN subquery baseline should work: {}", e);
        }
    }
}

// Test 5: Direct table query (verify test data is loaded correctly)
#[tokio::test]
async fn test_table_data_verification() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE id = 100
    "#;

    let test_record = TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("✅ Direct table query returned {} results", results.len());
            assert!(
                !results.is_empty(),
                "Direct table query should return results"
            );

            // Verify the data
            if let Some(record) = results.first() {
                if let Some(id) = record.fields.get("id") {
                    println!("   Found user with id: {:?}", id);
                    assert_eq!(
                        id,
                        &FieldValue::Integer(100),
                        "Should find user with id=100"
                    );
                }
            }
        }
        Err(e) => {
            println!("❌ Direct table query failed: {}", e);
            panic!("Direct table query should work: {}", e);
        }
    }
}

// Test 6: Simplified EXISTS without correlation (debug step)
#[tokio::test]
async fn test_simplified_exists_debug() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders)
    "#;

    let test_record = TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("🔍 Simplified EXISTS returned {} results", results.len());
            if results.is_empty() {
                println!(
                    "❌ Even simplified EXISTS failing - deeper issue with EXISTS implementation"
                );
                panic!("Simplified EXISTS should work if orders table has any records");
            } else {
                println!("✅ Simplified EXISTS working - issue is specifically with correlation");
            }
        }
        Err(e) => {
            println!("❌ Simplified EXISTS failed: {}", e);
            panic!("Simplified EXISTS failed: {}", e);
        }
    }
}

// Test 7: Debug query to verify orders table content
#[tokio::test]
async fn test_orders_table_debug() {
    // This test helps verify that orders data is loaded correctly
    let query = r#"
        SELECT id, name
        FROM users
        WHERE id IN (SELECT user_id FROM orders)
    "#;

    let test_record = TestDataBuilder::user_record(100, "Alice", "alice@example.com", "active");

    let result = execute_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!(
                "🔍 Orders table debug query returned {} results",
                results.len()
            );
            if results.is_empty() {
                println!("❌ No users found with matching orders - data loading issue?");
                panic!("Should find users that have orders");
            } else {
                println!("✅ Orders table loaded correctly - found users with orders");
            }
        }
        Err(e) => {
            println!("❌ Orders table debug query failed: {}", e);
            panic!("Orders table debug query failed: {}", e);
        }
    }
}
