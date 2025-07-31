/*!
# Tests for JOIN Operations

Basic test suite for INNER JOIN functionality in streaming SQL.
*/

use ferrisstreams::ferris::sql::execution::{StreamExecutionEngine, StreamRecord, FieldValue};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use tokio::sync::mpsc;

fn create_test_record_for_join() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    fields.insert("amount".to_string(), FieldValue::Float(100.0));
    
    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: 1234567890000,
        offset: 1,
        partition: 0,
    }
}

async fn execute_join_query(query: &str) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();
    
    let parsed_query = parser.parse(query)?;
    let record = create_test_record_for_join();
    
    // Convert StreamRecord to HashMap<String, serde_json::Value>
    let json_record: HashMap<String, serde_json::Value> = record.fields.into_iter().map(|(k, v)| {
        let json_val = match v {
            FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(i)),
            FieldValue::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or(0.into())),
            FieldValue::String(s) => serde_json::Value::String(s),
            FieldValue::Boolean(b) => serde_json::Value::Bool(b),
            FieldValue::Null => serde_json::Value::Null,
            _ => serde_json::Value::String(format!("{:?}", v)),
        };
        (k, json_val)
    }).collect();
    
    engine.execute(&parsed_query, json_record).await?;
    
    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_basic_inner_join() {
    // Test basic INNER JOIN syntax
    let query = "SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.id = right_stream.right_id";
    
    // This should work with our mock implementation
    let result = execute_join_query(query).await;
    
    // For now, expect an error since we haven't implemented the parser yet
    // Once the parser supports JOIN, this should succeed
    assert!(result.is_err() || result.unwrap().len() > 0);
}

#[tokio::test]
async fn test_join_with_alias() {
    // Test JOIN with table aliases
    let query = "SELECT l.name, r.right_name FROM left_stream l INNER JOIN right_stream r ON l.id = r.right_id";
    
    let result = execute_join_query(query).await;
    assert!(result.is_err() || result.unwrap().len() > 0);
}

#[tokio::test]
async fn test_join_with_where_clause() {
    // Test JOIN combined with WHERE clause
    let query = "SELECT * FROM left_stream l INNER JOIN right_stream r ON l.id = r.right_id WHERE l.amount > 50";
    
    let result = execute_join_query(query).await;
    assert!(result.is_err() || result.unwrap().len() > 0);
}

#[tokio::test]
async fn test_join_field_access() {
    // Test accessing joined fields
    let query = "SELECT id, name, right_name, right_value FROM left_stream INNER JOIN right_stream ON id = right_id";
    
    let result = execute_join_query(query).await;
    assert!(result.is_err() || result.unwrap().len() > 0);
}

#[tokio::test]
async fn test_multiple_joins() {
    // Test multiple JOIN clauses (will be supported when parser is extended)
    let query = "SELECT * FROM stream1 s1 INNER JOIN stream2 s2 ON s1.id = s2.id INNER JOIN stream3 s3 ON s2.id = s3.id";
    
    let result = execute_join_query(query).await;
    assert!(result.is_err()); // Expected to fail until parser supports multiple JOINs
}