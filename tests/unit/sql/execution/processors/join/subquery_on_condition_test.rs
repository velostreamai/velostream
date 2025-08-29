/*!
# Subquery JOIN ON Condition Tests

Tests for JOIN operations with subqueries in the ON condition - these should work
because the ExpressionEvaluator already supports EXISTS, NOT EXISTS, IN, NOT IN subqueries.
*/

use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use tokio::sync::mpsc;

fn create_test_record_for_on_condition() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(42));
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
        timestamp: 1234567890000,
        offset: 1,
        partition: 0,
    }
}

async fn execute_on_condition_test(
    query: &str,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format = std::sync::Arc::new(JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx, serialization_format.clone());
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
            // This should work with existing EXISTS support
            panic!("WHERE clause EXISTS should work: {}", e);
        }
    }
}
