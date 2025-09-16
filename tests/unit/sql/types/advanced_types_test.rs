/*!
# Tests for Advanced Data Types

Comprehensive test suite for ARRAY, MAP, and STRUCT data types and their operations.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record_with_advanced_types() -> StreamRecord {
    let mut fields = HashMap::new();

    // Basic fields
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));

    // Array field
    let array_data = vec![
        FieldValue::Integer(1),
        FieldValue::Integer(2),
        FieldValue::Integer(3),
    ];
    fields.insert("numbers".to_string(), FieldValue::Array(array_data));

    // Map field
    let mut map_data = HashMap::new();
    map_data.insert("key1".to_string(), FieldValue::String("value1".to_string()));
    map_data.insert("key2".to_string(), FieldValue::Integer(42));
    fields.insert("metadata".to_string(), FieldValue::Map(map_data));

    // Struct field
    let mut struct_data = HashMap::new();
    struct_data.insert(
        "first_name".to_string(),
        FieldValue::String("John".to_string()),
    );
    struct_data.insert(
        "last_name".to_string(),
        FieldValue::String("Doe".to_string()),
    );
    struct_data.insert("age".to_string(), FieldValue::Integer(30));
    fields.insert("person".to_string(), FieldValue::Struct(struct_data));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp: 1234567890000,
        offset: 1,
        partition: 0,
    }
}

async fn execute_query(
    query: &str,
) -> Result<Vec<HashMap<String, serde_json::Value>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let serialization_format =
        std::sync::Arc::new(velostream::velostream::serialization::JsonFormat);
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record_with_advanced_types();

    // Execute the query with internal record, including metadata
    engine.execute_with_record(&parsed_query, record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        // Convert StreamRecord to HashMap<String, serde_json::Value>
        let converted: HashMap<String, serde_json::Value> = result
            .fields
            .into_iter()
            .map(|(k, v)| {
                let json_val = match v {
                    FieldValue::Integer(i) => {
                        serde_json::Value::Number(serde_json::Number::from(i))
                    }
                    FieldValue::Float(f) => serde_json::Value::Number(
                        serde_json::Number::from_f64(f).unwrap_or(0.into()),
                    ),
                    FieldValue::String(s) => serde_json::Value::String(s),
                    FieldValue::Boolean(b) => serde_json::Value::Bool(b),
                    FieldValue::Null => serde_json::Value::Null,
                    _ => serde_json::Value::String(format!("{:?}", v)),
                };
                (k, json_val)
            })
            .collect();
        results.push(converted);
    }
    Ok(results)
}

#[tokio::test]
async fn test_array_functions() {
    // Test ARRAY creation
    let results = execute_query("SELECT ARRAY(1, 2, 3, 4) as new_array FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    // Note: The result would be serialized to JSON, so we'd need to check the structure

    // Test ARRAY_LENGTH
    let results = execute_query("SELECT ARRAY_LENGTH(numbers) as array_len FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["array_len"], 3);

    // Test ARRAY_CONTAINS
    let results =
        execute_query("SELECT ARRAY_CONTAINS(numbers, 2) as contains_two FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["contains_two"], true);

    let results =
        execute_query("SELECT ARRAY_CONTAINS(numbers, 5) as contains_five FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["contains_five"], false);

    // Test mixed type array
    let results = execute_query("SELECT ARRAY('hello', 42, true) as mixed_array FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_map_functions() {
    // Test MAP creation
    let results =
        execute_query("SELECT MAP('name', 'Alice', 'age', 25) as person_map FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);

    // Test MAP_KEYS
    let results = execute_query("SELECT MAP_KEYS(metadata) as meta_keys FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    // Keys should be returned as an array

    // Test MAP_VALUES
    let results = execute_query("SELECT MAP_VALUES(metadata) as meta_values FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    // Values should be returned as an array
}

#[tokio::test]
async fn test_struct_functions() {
    // Test STRUCT creation
    let results =
        execute_query("SELECT STRUCT('Alice', 30, true) as person_struct FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);

    // Test accessing struct fields (would need dot notation support)
    // This would require parser extensions: person.first_name
}

#[tokio::test]
async fn test_advanced_type_operations() {
    // Test CONCAT with arrays
    let results =
        execute_query("SELECT CONCAT('Numbers: ', numbers) as array_concat FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
    // Should get something like "Numbers: [1, 2, 3]"

    // Test CONCAT with maps
    let results =
        execute_query("SELECT CONCAT('Metadata: ', metadata) as map_concat FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);

    // Test CONCAT with structs
    let results =
        execute_query("SELECT CONCAT('Person: ', person) as struct_concat FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_nested_structures() {
    // Test array of maps
    let results = execute_query("SELECT ARRAY(MAP('id', 1, 'name', 'A'), MAP('id', 2, 'name', 'B')) as array_of_maps FROM test_stream").await.unwrap();
    assert_eq!(results.len(), 1);

    // Test map with array values
    let results = execute_query("SELECT MAP('numbers', ARRAY(1, 2, 3), 'letters', ARRAY('a', 'b', 'c')) as map_with_arrays FROM test_stream").await.unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_null_handling_advanced_types() {
    // Test ARRAY_LENGTH with null
    let results = execute_query("SELECT ARRAY_LENGTH(NULL) as null_array_len FROM test_stream")
        .await
        .unwrap();
    assert!(results[0]["null_array_len"].is_null());

    // Test ARRAY_CONTAINS with null array
    let results = execute_query("SELECT ARRAY_CONTAINS(NULL, 1) as null_contains FROM test_stream")
        .await
        .unwrap();
    assert!(results[0]["null_contains"].is_null());

    // Test MAP_KEYS with null
    let results = execute_query("SELECT MAP_KEYS(NULL) as null_map_keys FROM test_stream")
        .await
        .unwrap();
    assert!(results[0]["null_map_keys"].is_null());
}

#[tokio::test]
async fn test_type_errors() {
    // Test ARRAY_LENGTH with non-array
    let result =
        execute_query("SELECT ARRAY_LENGTH(name) as invalid_array_len FROM test_stream").await;
    assert!(result.is_err());

    // Test ARRAY_CONTAINS with non-array
    let result =
        execute_query("SELECT ARRAY_CONTAINS(name, 'test') as invalid_contains FROM test_stream")
            .await;
    assert!(result.is_err());

    // Test MAP_KEYS with non-map
    let result = execute_query("SELECT MAP_KEYS(name) as invalid_map_keys FROM test_stream").await;
    assert!(result.is_err());

    // Test MAP with odd number of arguments
    let result =
        execute_query("SELECT MAP('key1', 'value1', 'key2') as invalid_map FROM test_stream").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_complex_expressions() {
    // Test combining array functions
    let results = execute_query(
        "SELECT ARRAY_LENGTH(ARRAY(1, 2, 3, 4, 5)) as expr_array_len FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["expr_array_len"], 5);

    // Test array contains with expression
    let results = execute_query(
        "SELECT ARRAY_CONTAINS(ARRAY('a', 'b', 'c'), LOWER('A')) as expr_contains FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["expr_contains"], true);

    // Test map operations
    let results = execute_query("SELECT ARRAY_LENGTH(MAP_KEYS(MAP('a', 1, 'b', 2, 'c', 3))) as map_key_count FROM test_stream").await.unwrap();
    assert_eq!(results[0]["map_key_count"], 3);
}

#[tokio::test]
async fn test_comparison_operations() {
    // Arrays should be comparable
    let results =
        execute_query("SELECT ARRAY(1, 2, 3) = ARRAY(1, 2, 3) as arrays_equal FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results[0]["arrays_equal"], true);

    let results = execute_query(
        "SELECT ARRAY(1, 2, 3) = ARRAY(3, 2, 1) as arrays_not_equal FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["arrays_not_equal"], false);

    // Maps should be comparable
    let results = execute_query(
        "SELECT MAP('a', 1, 'b', 2) = MAP('b', 2, 'a', 1) as maps_equal FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["maps_equal"], true);
}

#[tokio::test]
async fn test_coalesce_with_advanced_types() {
    // Test COALESCE with arrays
    let results =
        execute_query("SELECT COALESCE(NULL, ARRAY(1, 2, 3)) as coalesce_array FROM test_stream")
            .await
            .unwrap();
    assert_eq!(results.len(), 1);

    // Test COALESCE with maps
    let results = execute_query(
        "SELECT COALESCE(NULL, MAP('key', 'value')) as coalesce_map FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);

    // Test NULLIF with arrays
    let results = execute_query(
        "SELECT NULLIF(ARRAY(1, 2, 3), ARRAY(1, 2, 3)) as nullif_array FROM test_stream",
    )
    .await
    .unwrap();
    assert!(results[0]["nullif_array"].is_null());
}

#[tokio::test]
async fn test_array_edge_cases() {
    // Empty array
    let results = execute_query("SELECT ARRAY() as empty_array FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    let results = execute_query("SELECT ARRAY_LENGTH(ARRAY()) as empty_array_len FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results[0]["empty_array_len"], 0);

    // Array with nulls
    let results = execute_query("SELECT ARRAY(1, NULL, 3) as array_with_nulls FROM test_stream")
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    let results = execute_query(
        "SELECT ARRAY_LENGTH(ARRAY(1, NULL, 3)) as array_with_nulls_len FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results[0]["array_with_nulls_len"], 3);
}

#[tokio::test]
async fn test_performance_with_large_structures() {
    // Test with larger arrays
    let results = execute_query("SELECT ARRAY_LENGTH(ARRAY(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)) as big_array_len FROM test_stream").await.unwrap();
    assert_eq!(results[0]["big_array_len"], 20);

    // Test deeply nested structures
    let results = execute_query(
        "SELECT ARRAY(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5, 6)) as nested_arrays FROM test_stream",
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
}
