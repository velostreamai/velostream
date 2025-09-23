/*!
# Dynamic Correlation Resolution Test

Tests for dynamic table alias detection in correlated subqueries.
This validates that correlation works correctly regardless of the outer table name.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::parser::StreamingSqlParser;

// Import shared test utilities
use crate::unit::sql::execution::common_test_utils::MockTable;

/// Create a context customizer with specific test data for different table names
fn create_dynamic_correlation_context() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        // Create orders table for correlation testing with different outer table names
        let orders_data = vec![
            // Customer orders
            {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(1001));
                fields.insert("customer_id".to_string(), FieldValue::Integer(100));
                fields.insert("amount".to_string(), FieldValue::Float(250.0));
                StreamRecord {
                    fields,
                    headers: HashMap::new(),
                    event_time: None,
                    timestamp: 1640995200000,
                    offset: 1,
                    partition: 0,
                }
            },
            {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(1002));
                fields.insert("customer_id".to_string(), FieldValue::Integer(200));
                fields.insert("amount".to_string(), FieldValue::Float(150.0));
                StreamRecord {
                    fields,
                    headers: HashMap::new(),
                    event_time: None,
                    timestamp: 1640995200001,
                    offset: 2,
                    partition: 0,
                }
            },
            // Product reviews
            {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(2001));
                fields.insert("product_id".to_string(), FieldValue::Integer(300));
                fields.insert("rating".to_string(), FieldValue::Integer(5));
                StreamRecord {
                    fields,
                    headers: HashMap::new(),
                    event_time: None,
                    timestamp: 1640995200002,
                    offset: 3,
                    partition: 0,
                }
            },
            {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(2002));
                fields.insert("product_id".to_string(), FieldValue::Integer(400));
                fields.insert("rating".to_string(), FieldValue::Integer(4));
                StreamRecord {
                    fields,
                    headers: HashMap::new(),
                    event_time: None,
                    timestamp: 1640995200003,
                    offset: 4,
                    partition: 0,
                }
            },
        ];

        let orders_table = MockTable::new("orders".to_string(), orders_data);
        context.load_reference_table(
            "orders",
            Arc::new(orders_table) as Arc<dyn velostream::velostream::table::sql::SqlQueryable + Send + Sync>
        );

        println!("🎯 Dynamic correlation context: loaded orders table for dynamic correlation testing");
    })
}

async fn execute_dynamic_correlation_test(
    query: &str,
    test_record: StreamRecord,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    engine.context_customizer = Some(create_dynamic_correlation_context());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;

    engine.execute_with_record(&parsed_query, test_record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

/// Create a customer record
fn create_customer_record(id: i64, name: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("email".to_string(), FieldValue::String(format!("{}@example.com", name.to_lowercase())));

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
fn create_product_record(id: i64, name: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("price".to_string(), FieldValue::Float(99.99));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
    }
}

// Test 1: Dynamic correlation with customers table
#[tokio::test]
async fn test_dynamic_correlation_customers_table() {
    let query = r#"
        SELECT id, name
        FROM customers
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
    "#;

    // Test with customer id=100 (should find matching order)
    let test_record = create_customer_record(100, "Alice");

    let result = execute_dynamic_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("✅ Customers table correlation: {} results", results.len());
            assert!(!results.is_empty(), "Should find customer with orders");
        }
        Err(e) => {
            panic!("Customers table correlation test failed: {}", e);
        }
    }
}

// Test 2: Dynamic correlation with products table
#[tokio::test]
async fn test_dynamic_correlation_products_table() {
    let query = r#"
        SELECT id, name
        FROM products
        WHERE EXISTS (SELECT 1 FROM orders WHERE product_id = products.id)
    "#;

    // Test with product id=300 (should find matching review in orders)
    let test_record = create_product_record(300, "Laptop");

    let result = execute_dynamic_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("✅ Products table correlation: {} results", results.len());
            assert!(!results.is_empty(), "Should find product with reviews");
        }
        Err(e) => {
            panic!("Products table correlation test failed: {}", e);
        }
    }
}

// Test 3: Dynamic correlation with table alias 'c'
#[tokio::test]
async fn test_dynamic_correlation_with_customer_alias() {
    let query = r#"
        SELECT c.id, c.name
        FROM customers c
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)
    "#;

    let test_record = create_customer_record(200, "Bob");

    let result = execute_dynamic_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("✅ Customer alias 'c' correlation: {} results", results.len());
            assert!(!results.is_empty(), "Should resolve c.id correlation for customer_id=200");
        }
        Err(e) => {
            panic!("Customer alias correlation test failed: {}", e);
        }
    }
}

// Test 4: Dynamic correlation with table alias 'p'
#[tokio::test]
async fn test_dynamic_correlation_with_product_alias() {
    let query = r#"
        SELECT p.id, p.name
        FROM products p
        WHERE EXISTS (SELECT 1 FROM orders WHERE product_id = p.id)
    "#;

    let test_record = create_product_record(400, "Mouse");

    let result = execute_dynamic_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("✅ Product alias 'p' correlation: {} results", results.len());
            assert!(!results.is_empty(), "Should resolve p.id correlation for product_id=400");
        }
        Err(e) => {
            panic!("Product alias correlation test failed: {}", e);
        }
    }
}

// Test 5: Test that non-matching records return empty results
#[tokio::test]
async fn test_dynamic_correlation_no_match() {
    let query = r#"
        SELECT id, name
        FROM customers
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)
    "#;

    // Test with customer id=999 (should NOT find any matching orders)
    // The orders table only has customer_id values: [100, 200]
    let test_record = create_customer_record(999, "NoOrders");

    println!("🔍 No match test: Testing customer_id=999 (should NOT find any orders)");
    println!("🔍 Orders table contains customer_id: [100, 200] and product_id: [300, 400]");

    let result = execute_dynamic_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("🔍 No match test: {} results returned", results.len());
            if !results.is_empty() {
                println!("❌ UNEXPECTED: Found {} results when none expected", results.len());
                println!("   This suggests correlation may not be working correctly");
                println!("   or MockTable WHERE clause evaluation has issues");

                // Print the actual records to debug
                for (i, record) in results.iter().enumerate() {
                    println!("   Result {}: {:?}", i, record.fields);
                }
            }
            assert!(results.is_empty(), "Should not find customer without orders (customer_id=999 not in orders table)");
        }
        Err(e) => {
            panic!("No match test failed: {}", e);
        }
    }
}

// Test 6: Test that the old hardcoded approach still works (backward compatibility)
#[tokio::test]
async fn test_backward_compatibility_users_table() {
    let query = r#"
        SELECT id, name
        FROM users
        WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = users.id)
    "#;

    // Test with user that has customer_id match
    let test_record = create_customer_record(100, "BackwardCompat");

    let result = execute_dynamic_correlation_test(query, test_record).await;

    match &result {
        Ok(results) => {
            println!("✅ Backward compatibility: {} results", results.len());
            // This should work because the new system still recognizes "users" as a correlation table
            assert!(!results.is_empty(), "Backward compatibility maintained for users table");
        }
        Err(e) => {
            panic!("Backward compatibility test failed: {}", e);
        }
    }
}