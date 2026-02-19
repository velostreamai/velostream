/*!
# Tests for New Engine Functions

Comprehensive test suite for SPLIT_PART, REGEXP_REPLACE, DELTA, JSON_EXISTS, JSON_QUERY,
and improved JSON path extraction.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::ast::Expr;
use velostream::velostream::sql::execution::aggregation::AggregateFunctions;
use velostream::velostream::sql::execution::internal::GroupAccumulator;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("amount".to_string(), FieldValue::Float(123.456));
    fields.insert("quantity".to_string(), FieldValue::Integer(42));
    fields.insert(
        "name".to_string(),
        FieldValue::String("hello-world-test".to_string()),
    );
    fields.insert(
        "email".to_string(),
        FieldValue::String("user@example.com".to_string()),
    );
    fields.insert(
        "csv_data".to_string(),
        FieldValue::String("alpha,beta,gamma,delta".to_string()),
    );
    fields.insert(
        "message".to_string(),
        FieldValue::String("Error 404: not found".to_string()),
    );
    fields.insert(
        "json_data".to_string(),
        FieldValue::String(
            r#"{"user":{"name":"Alice","age":30},"tags":["rust","sql"],"active":true}"#.to_string(),
        ),
    );
    fields.insert(
        "simple_json".to_string(),
        FieldValue::String(r#"{"key":"value","count":5}"#.to_string()),
    );

    StreamRecord {
        fields,
        headers: HashMap::new(),
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

    engine.execute_with_record(&parsed_query, &record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

// ============================================================================
// SPLIT_PART TESTS
// ============================================================================

#[tokio::test]
async fn test_split_part_basic() {
    let results = execute_query("SELECT SPLIT_PART(csv_data, ',', 1) as part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("part"),
        Some(&FieldValue::String("alpha".to_string()))
    );
}

#[tokio::test]
async fn test_split_part_middle() {
    let results = execute_query("SELECT SPLIT_PART(csv_data, ',', 3) as part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("part"),
        Some(&FieldValue::String("gamma".to_string()))
    );
}

#[tokio::test]
async fn test_split_part_last() {
    let results = execute_query("SELECT SPLIT_PART(csv_data, ',', 4) as part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("part"),
        Some(&FieldValue::String("delta".to_string()))
    );
}

#[tokio::test]
async fn test_split_part_out_of_bounds() {
    let results = execute_query("SELECT SPLIT_PART(csv_data, ',', 10) as part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("part"),
        Some(&FieldValue::String("".to_string()))
    );
}

#[tokio::test]
async fn test_split_part_zero_index() {
    // Index < 1 returns empty string (PostgreSQL semantics)
    let results = execute_query("SELECT SPLIT_PART(csv_data, ',', 0) as part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("part"),
        Some(&FieldValue::String("".to_string()))
    );
}

#[tokio::test]
async fn test_split_part_hyphen_delimiter() {
    let results = execute_query("SELECT SPLIT_PART(name, '-', 2) as part FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("part"),
        Some(&FieldValue::String("world".to_string()))
    );
}

// ============================================================================
// REGEXP_REPLACE TESTS
// ============================================================================

#[tokio::test]
async fn test_regexp_replace_first_match() {
    // Default: replace first match only
    let results =
        execute_query("SELECT REGEXP_REPLACE(message, '[0-9]+', 'NUM') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("Error NUM: not found".to_string()))
    );
}

#[tokio::test]
async fn test_regexp_replace_global() {
    let results = execute_query(
        "SELECT REGEXP_REPLACE(csv_data, '[a-z]+', 'X', 'g') as result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("X,X,X,X".to_string()))
    );
}

#[tokio::test]
async fn test_regexp_replace_case_insensitive() {
    let results = execute_query(
        "SELECT REGEXP_REPLACE(message, 'error', 'Warning', 'i') as result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("Warning 404: not found".to_string()))
    );
}

#[tokio::test]
async fn test_regexp_replace_global_case_insensitive() {
    let results =
        execute_query("SELECT REGEXP_REPLACE(name, '[a-z]', 'X', 'gi') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("XXXXX-XXXXX-XXXX".to_string()))
    );
}

#[tokio::test]
async fn test_regexp_replace_no_match() {
    let results = execute_query(
        "SELECT REGEXP_REPLACE(name, 'ZZZZZ', 'replaced') as result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("hello-world-test".to_string()))
    );
}

// ============================================================================
// DELTA AGGREGATE TESTS
// ============================================================================

#[test]
fn test_delta_aggregate_float() {
    let mut accumulator = GroupAccumulator::new();
    accumulator.count = 5;
    accumulator
        .mins
        .insert("price".to_string(), FieldValue::Float(10.0));
    accumulator
        .maxs
        .insert("price".to_string(), FieldValue::Float(50.0));

    let delta_expr = Expr::Function {
        name: "DELTA".to_string(),
        args: vec![Expr::Column("price".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("price", &delta_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::Float(40.0));
}

#[test]
fn test_delta_aggregate_integer() {
    let mut accumulator = GroupAccumulator::new();
    accumulator.count = 3;
    accumulator
        .mins
        .insert("count".to_string(), FieldValue::Integer(5));
    accumulator
        .maxs
        .insert("count".to_string(), FieldValue::Integer(100));

    let delta_expr = Expr::Function {
        name: "DELTA".to_string(),
        args: vec![Expr::Column("count".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("count", &delta_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::Integer(95));
}

#[test]
fn test_delta_aggregate_missing_field() {
    let accumulator = GroupAccumulator::new();

    let delta_expr = Expr::Function {
        name: "DELTA".to_string(),
        args: vec![Expr::Column("missing".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("missing", &delta_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::Null);
}

#[test]
fn test_delta_aggregate_single_value() {
    // When min == max, delta should be 0
    let mut accumulator = GroupAccumulator::new();
    accumulator.count = 1;
    accumulator
        .mins
        .insert("val".to_string(), FieldValue::Float(42.0));
    accumulator
        .maxs
        .insert("val".to_string(), FieldValue::Float(42.0));

    let delta_expr = Expr::Function {
        name: "DELTA".to_string(),
        args: vec![Expr::Column("val".to_string())],
    };

    let result =
        AggregateFunctions::compute_field_aggregate_value("val", &delta_expr, &accumulator)
            .unwrap();
    assert_eq!(result, FieldValue::Float(0.0));
}

// ============================================================================
// JSON_EXISTS TESTS
// ============================================================================

#[tokio::test]
async fn test_json_exists_valid_path() {
    let results =
        execute_query("SELECT JSON_EXISTS(json_data, '$.user.name') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_json_exists_missing_path() {
    let results =
        execute_query("SELECT JSON_EXISTS(json_data, '$.user.email') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Boolean(false))
    );
}

#[tokio::test]
async fn test_json_exists_nested_path() {
    let results =
        execute_query("SELECT JSON_EXISTS(json_data, '$.user') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_json_exists_root() {
    let results = execute_query("SELECT JSON_EXISTS(json_data, '$') as result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_json_exists_array_element() {
    let results =
        execute_query("SELECT JSON_EXISTS(json_data, '$.tags.0') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Boolean(true))
    );
}

// ============================================================================
// JSON_QUERY TESTS
// ============================================================================

#[tokio::test]
async fn test_json_query_object() {
    let results =
        execute_query("SELECT JSON_QUERY(json_data, '$.user') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    // Should return the nested object as a JSON string
    let result_str = match results[0].fields.get("result") {
        Some(FieldValue::String(s)) => s.clone(),
        other => panic!("Expected String, got {:?}", other),
    };
    let parsed: serde_json::Value = serde_json::from_str(&result_str).unwrap();
    assert_eq!(parsed["name"], "Alice");
    assert_eq!(parsed["age"], 30);
}

#[tokio::test]
async fn test_json_query_array() {
    let results =
        execute_query("SELECT JSON_QUERY(json_data, '$.tags') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    let result_str = match results[0].fields.get("result") {
        Some(FieldValue::String(s)) => s.clone(),
        other => panic!("Expected String, got {:?}", other),
    };
    let parsed: serde_json::Value = serde_json::from_str(&result_str).unwrap();
    assert!(parsed.is_array());
    assert_eq!(parsed[0], "rust");
    assert_eq!(parsed[1], "sql");
}

#[tokio::test]
async fn test_json_query_scalar_returns_null() {
    // JSON_QUERY should return Null for scalar values
    let results =
        execute_query("SELECT JSON_QUERY(json_data, '$.active') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].fields.get("result"), Some(&FieldValue::Null));
}

#[tokio::test]
async fn test_json_query_missing_path() {
    let results =
        execute_query("SELECT JSON_QUERY(json_data, '$.nonexistent') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].fields.get("result"), Some(&FieldValue::Null));
}

// ============================================================================
// IMPROVED JSON PATH EXTRACTION TESTS
// ============================================================================

#[tokio::test]
async fn test_json_extract_nested_path() {
    let results =
        execute_query("SELECT JSON_EXTRACT(json_data, '$.user.name') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("Alice".to_string()))
    );
}

#[tokio::test]
async fn test_json_extract_numeric_value() {
    let results =
        execute_query("SELECT JSON_EXTRACT(json_data, '$.user.age') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Integer(30))
    );
}

#[tokio::test]
async fn test_json_extract_boolean_value() {
    let results =
        execute_query("SELECT JSON_EXTRACT(json_data, '$.active') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Boolean(true))
    );
}

#[tokio::test]
async fn test_json_extract_missing_path() {
    let results = execute_query(
        "SELECT JSON_EXTRACT(json_data, '$.missing.path') as result FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].fields.get("result"), Some(&FieldValue::Null));
}

#[tokio::test]
async fn test_json_value_nested_path() {
    let results =
        execute_query("SELECT JSON_VALUE(simple_json, '$.key') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("value".to_string()))
    );
}

#[tokio::test]
async fn test_json_value_integer() {
    let results =
        execute_query("SELECT JSON_VALUE(simple_json, '$.count') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::Integer(5))
    );
}

#[tokio::test]
async fn test_json_extract_root() {
    let results = execute_query("SELECT JSON_EXTRACT(simple_json, '$') as result FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    // Root path returns the whole JSON string
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String(
            r#"{"key":"value","count":5}"#.to_string()
        ))
    );
}

#[tokio::test]
async fn test_json_extract_array_index() {
    let results =
        execute_query("SELECT JSON_EXTRACT(json_data, '$.tags.0') as result FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("result"),
        Some(&FieldValue::String("rust".to_string()))
    );
}
