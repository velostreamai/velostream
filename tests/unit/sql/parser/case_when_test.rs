/*!
# Tests for CASE WHEN Expressions and Conditional Aggregation

Comprehensive test suite for CASE WHEN expressions and their use in aggregation functions.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );
    fields.insert("amount".to_string(), FieldValue::Float(123.45));
    fields.insert("quantity".to_string(), FieldValue::Integer(42));
    fields.insert(
        "priority".to_string(),
        FieldValue::String("high".to_string()),
    );
    fields.insert("score".to_string(), FieldValue::Integer(85));
    fields.insert(
        "category".to_string(),
        FieldValue::String("premium".to_string()),
    );

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test-system".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1734652800000,
        offset: 100,
        partition: 0,
        event_time: None,
        topic: None,
        key: None,
    }
}

async fn execute_query(query: &str) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Execute the query with StreamRecord
    engine.execute_with_record(&parsed_query, &record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_case_when_parsing() {
    // Test basic CASE WHEN expression parsing
    let parser = StreamingSqlParser::new();

    let test_queries = vec![
        "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END as is_active FROM test_stream",
        "SELECT CASE WHEN amount > 100 THEN 'high' WHEN amount > 50 THEN 'medium' ELSE 'low' END as amount_tier FROM test_stream",
        "SELECT CASE WHEN priority = 'high' THEN amount * 2 ELSE amount END as adjusted_amount FROM test_stream",
    ];

    for query in test_queries {
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse query: {}", query);

        // Verify the query contains CASE expression
        match result.unwrap() {
            velostream::velostream::sql::ast::StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 1);
                match &fields[0] {
                    velostream::velostream::sql::ast::SelectField::Expression { expr, .. } => {
                        match expr {
                            velostream::velostream::sql::ast::Expr::Case {
                                when_clauses, ..
                            } => {
                                assert!(!when_clauses.is_empty(), "CASE should have WHEN clauses");
                            }
                            _ => panic!("Expected CASE expression, got: {:?}", expr),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}

#[tokio::test]
async fn test_simple_case_when_execution() {
    // Test basic CASE WHEN execution
    let results = execute_query(
        "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END as is_active FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let is_active = match results[0].fields.get("is_active") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for is_active"),
    };
    assert_eq!(is_active, 1); // status is 'active'
}

#[tokio::test]
async fn test_multiple_when_clauses() {
    // Test CASE with multiple WHEN clauses
    let results = execute_query(
        "SELECT CASE 
            WHEN amount > 200 THEN 'very_high'
            WHEN amount > 100 THEN 'high' 
            WHEN amount > 50 THEN 'medium'
            ELSE 'low' 
         END as amount_tier FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let amount_tier = match results[0].fields.get("amount_tier") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for amount_tier"),
    };
    assert_eq!(amount_tier, "high"); // amount is 123.45, so > 100
}

#[tokio::test]
async fn test_case_when_with_expressions() {
    // Test CASE WHEN with complex expressions
    let results = execute_query(
        "SELECT CASE 
            WHEN priority = 'high' THEN amount * 2 
            WHEN priority = 'medium' THEN amount * 1.5
            ELSE amount 
         END as adjusted_amount FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let adjusted_amount = match results[0].fields.get("adjusted_amount") {
        Some(FieldValue::Float(val)) => *val,
        _ => panic!("Expected number result for adjusted_amount"),
    };
    assert_eq!(adjusted_amount, 246.9); // priority is 'high', so amount * 2 = 123.45 * 2
}

#[tokio::test]
async fn test_case_when_without_else() {
    // Test CASE WHEN without ELSE clause (should return NULL)
    let results = execute_query(
        "SELECT CASE WHEN status = 'inactive' THEN 1 END as inactive_flag FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let inactive_flag = results[0].fields.get("inactive_flag");
    assert_eq!(inactive_flag, Some(&FieldValue::Null)); // status is 'active', not 'inactive'
}

#[tokio::test]
async fn test_conditional_aggregation_count() {
    // Test COUNT with CASE WHEN (conditional counting)
    let results = execute_query(
        "SELECT COUNT(CASE WHEN status = 'active' THEN 1 END) as active_count FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let active_count = match results[0].fields.get("active_count") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for active_count"),
    };
    assert_eq!(active_count, 1); // Should count 1 because status is 'active'
}

#[tokio::test]
async fn test_conditional_aggregation_sum() {
    // Test SUM with CASE WHEN (conditional summing)
    let results = execute_query(
        "SELECT SUM(CASE WHEN priority = 'high' THEN amount ELSE 0 END) as high_priority_total FROM test_stream"
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let high_priority_total = match results[0].fields.get("high_priority_total") {
        Some(FieldValue::Float(val)) => *val,
        _ => panic!("Expected number result for high_priority_total"),
    };
    assert_eq!(high_priority_total, 123.45); // Should sum amount because priority is 'high'
}

#[tokio::test]
async fn test_conditional_aggregation_avg() {
    // Test AVG with CASE WHEN (conditional averaging)
    let results = execute_query(
        "SELECT AVG(CASE WHEN score >= 80 THEN score END) as high_score_avg FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let high_score_avg = match results[0].fields.get("high_score_avg") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for high_score_avg"),
    };
    assert_eq!(high_score_avg, 85); // Should average score because score (85) >= 80
}

#[tokio::test]
async fn test_multiple_conditional_aggregations() {
    // Test multiple conditional aggregations in one query
    let results = execute_query(
        "SELECT 
            COUNT(CASE WHEN status = 'active' THEN 1 END) as active_count,
            SUM(CASE WHEN priority = 'high' THEN amount ELSE 0 END) as high_priority_sum,
            AVG(CASE WHEN category = 'premium' THEN score END) as premium_avg_score
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    let active_count = match results[0].fields.get("active_count") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for active_count"),
    };
    assert_eq!(active_count, 1);

    let high_priority_sum = match results[0].fields.get("high_priority_sum") {
        Some(FieldValue::Float(val)) => *val,
        _ => panic!("Expected number result for high_priority_sum"),
    };
    assert_eq!(high_priority_sum, 123.45);

    let premium_avg_score = match results[0].fields.get("premium_avg_score") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for premium_avg_score"),
    };
    assert_eq!(premium_avg_score, 85);
}

#[tokio::test]
async fn test_nested_case_expressions() {
    // Test nested CASE expressions
    let results = execute_query(
        "SELECT CASE 
            WHEN status = 'active' THEN 
                CASE WHEN priority = 'high' THEN 'active_high' ELSE 'active_normal' END
            ELSE 'inactive' 
         END as status_priority FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let status_priority = match results[0].fields.get("status_priority") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for status_priority"),
    };
    assert_eq!(status_priority, "active_high"); // status is 'active' and priority is 'high'
}

#[tokio::test]
async fn test_case_when_with_null_values() {
    // Test CASE WHEN handling NULL values
    let results = execute_query(
        "SELECT CASE 
            WHEN NULL IS NULL THEN 'null_detected'
            ELSE 'not_null' 
         END as null_check FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let null_check = match results[0].fields.get("null_check") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for null_check"),
    };
    assert_eq!(null_check, "null_detected");
}

#[tokio::test]
async fn test_case_when_boolean_results() {
    // Test CASE WHEN returning boolean values
    let results = execute_query(
        "SELECT CASE 
            WHEN amount > 100 THEN TRUE 
            ELSE FALSE 
         END as is_high_amount FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let is_high_amount = match results[0].fields.get("is_high_amount") {
        Some(FieldValue::Boolean(val)) => *val,
        _ => panic!("Expected boolean result for is_high_amount"),
    };
    assert!(is_high_amount); // amount (123.45) > 100
}

#[tokio::test]
async fn test_conditional_aggregation_listagg() {
    // Test LISTAGG with CASE WHEN
    let results = execute_query(
        "SELECT LISTAGG(CASE WHEN priority = 'high' THEN status END, ', ') as high_priority_statuses FROM test_stream"
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let high_priority_statuses = match results[0].fields.get("high_priority_statuses") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for high_priority_statuses"),
    };
    assert_eq!(high_priority_statuses, "active"); // Should include status because priority is 'high'
}

#[tokio::test]
async fn test_case_when_error_handling() {
    // Test error cases for CASE WHEN
    let parser = StreamingSqlParser::new();

    let invalid_queries = vec![
        "SELECT CASE WHEN status = 'active' FROM test", // Missing THEN and END
        "SELECT CASE THEN 'result' END FROM test",      // Missing WHEN
        "SELECT CASE WHEN status = 'active' THEN 'result' FROM test", // Missing END
        "SELECT CASE END FROM test",                    // Empty CASE
    ];

    for query in invalid_queries {
        let result = parser.parse(query);
        assert!(result.is_err(), "Query should have failed: {}", query);
    }
}

#[tokio::test]
async fn test_case_when_performance_patterns() {
    // Test common performance patterns with CASE WHEN
    let results = execute_query(
        "SELECT 
            SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_count,
            SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) as inactive_count,
            SUM(CASE WHEN priority = 'high' THEN amount ELSE 0 END) as high_priority_revenue,
            AVG(CASE WHEN category = 'premium' THEN score END) as premium_avg_score
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    // Verify all results
    let active_count = match results[0].fields.get("active_count") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for active_count"),
    };
    assert_eq!(active_count, 1);

    let inactive_count = match results[0].fields.get("inactive_count") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for inactive_count"),
    };
    assert_eq!(inactive_count, 0);

    let high_priority_revenue = match results[0].fields.get("high_priority_revenue") {
        Some(FieldValue::Float(val)) => *val,
        _ => panic!("Expected number result for high_priority_revenue"),
    };
    assert_eq!(high_priority_revenue, 123.45);

    let premium_avg_score = match results[0].fields.get("premium_avg_score") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for premium_avg_score"),
    };
    assert_eq!(premium_avg_score, 85);
}

#[tokio::test]
async fn test_case_when_with_comparison_operators() {
    // Test CASE WHEN with various comparison operators
    let results = execute_query(
        "SELECT 
            CASE WHEN amount >= 100 THEN 'gte_100' ELSE 'lt_100' END as amount_gte_check,
            CASE WHEN quantity <= 50 THEN 'lte_50' ELSE 'gt_50' END as quantity_lte_check,
            CASE WHEN score <> 90 THEN 'not_90' ELSE 'is_90' END as score_neq_check
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    let amount_gte_check = match results[0].fields.get("amount_gte_check") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for amount_gte_check"),
    };
    assert_eq!(amount_gte_check, "gte_100"); // amount (123.45) >= 100

    let quantity_lte_check = match results[0].fields.get("quantity_lte_check") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for quantity_lte_check"),
    };
    assert_eq!(quantity_lte_check, "lte_50"); // quantity (42) <= 50

    let score_neq_check = match results[0].fields.get("score_neq_check") {
        Some(FieldValue::String(val)) => val,
        _ => panic!("Expected string result for score_neq_check"),
    };
    assert_eq!(score_neq_check, "not_90"); // score (85) <> 90
}
