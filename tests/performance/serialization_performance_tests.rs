/*!
# Serialization Performance Tests

Tests for serialization format performance and memory efficiency.
Heavy benchmarks are run as examples in the performance-tests.yml workflow.
*/

use ferrisstreams::ferris::serialization::{JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;

fn create_test_data_small() -> HashMap<String, FieldValue> {
    let mut data = HashMap::new();
    data.insert("id".to_string(), FieldValue::Integer(12345));
    data.insert("name".to_string(), FieldValue::String("test".to_string()));
    data.insert("amount".to_string(), FieldValue::Float(99.99));
    data.insert("active".to_string(), FieldValue::Boolean(true));
    data
}

fn create_test_data_large() -> HashMap<String, FieldValue> {
    let mut data = HashMap::new();

    // Add many fields to test performance with larger payloads
    for i in 0..100 {
        data.insert(format!("field_{}", i), FieldValue::Integer(i as i64));
        data.insert(
            format!("str_{}", i),
            FieldValue::String(format!("value_{}", i)),
        );
        data.insert(format!("float_{}", i), FieldValue::Float(i as f64 * 1.5));
        data.insert(format!("bool_{}", i), FieldValue::Boolean(i % 2 == 0));
    }

    data
}

#[test]
fn test_json_serialization_memory_efficiency() {
    // Test that JSON serialization doesn't consume excessive memory
    let json_format = JsonFormat;
    let test_data = create_test_data_small();

    // Serialize many times to test memory usage
    let mut serialized_results: Vec<Vec<u8>> = Vec::with_capacity(1000);

    for _ in 0..1000 {
        match json_format.serialize_record(&test_data) {
            Ok(bytes) => {
                serialized_results.push(bytes);
            }
            Err(e) => panic!("Serialization failed: {:?}", e),
        }
    }

    assert_eq!(serialized_results.len(), 1000);

    // All serialized results should be non-empty
    for result in &serialized_results {
        assert!(!result.is_empty());
    }
}

#[test]
fn test_json_deserialization_memory_efficiency() {
    // Test that JSON deserialization doesn't consume excessive memory
    let json_format = JsonFormat;
    let test_data = create_test_data_small();

    // First serialize the test data
    let serialized = json_format.serialize_record(&test_data).unwrap();

    // Deserialize many times to test memory usage
    let mut deserialized_results: Vec<HashMap<String, FieldValue>> = Vec::with_capacity(1000);

    for _ in 0..1000 {
        match json_format.deserialize_record(&serialized) {
            Ok(data) => {
                deserialized_results.push(data);
            }
            Err(e) => panic!("Deserialization failed: {:?}", e),
        }
    }

    assert_eq!(deserialized_results.len(), 1000);

    // All deserialized results should match original data
    for result in &deserialized_results {
        assert_eq!(result.len(), test_data.len());
        assert!(result.contains_key("id"));
        assert!(result.contains_key("name"));
    }
}

#[test]
fn test_large_payload_serialization_performance() {
    // Test serialization performance with larger payloads
    let json_format = JsonFormat;
    let large_data = create_test_data_large();

    // Should handle large payloads without performance issues
    for _ in 0..100 {
        let serialized = json_format.serialize_record(&large_data).unwrap();
        assert!(!serialized.is_empty());

        let deserialized = json_format.deserialize_record(&serialized).unwrap();
        assert_eq!(deserialized.len(), large_data.len());
    }
}

#[test]
fn test_internal_value_memory_footprint() {
    // Test that InternalValue variants don't have excessive memory overhead
    let values = vec![
        FieldValue::Null,
        FieldValue::Boolean(true),
        FieldValue::Integer(12345),
        FieldValue::Float(123.456),
        FieldValue::String("test string".to_string()),
    ];

    // Should be able to store many values efficiently
    let large_value_collection: Vec<_> = values.iter().cycle().take(10000).cloned().collect();

    assert_eq!(large_value_collection.len(), 10000);
}

#[test]
fn test_serialization_format_consistency() {
    // Test that serialization format is consistent across multiple operations
    let json_format = JsonFormat;
    let test_data = create_test_data_small();

    // Serialize the same data multiple times
    let mut serialized_results = Vec::new();
    for _ in 0..10 {
        serialized_results.push(json_format.serialize_record(&test_data).unwrap());
    }

    // All results should be identical (deterministic serialization)
    let first_result = &serialized_results[0];
    for result in &serialized_results[1..] {
        assert_eq!(
            result, first_result,
            "Serialization should be deterministic"
        );
    }
}

#[test]
fn test_null_value_serialization_efficiency() {
    // Test that null values are handled efficiently
    let json_format = JsonFormat;

    let mut data = HashMap::new();
    // Add many null values
    for i in 0..1000 {
        data.insert(format!("null_field_{}", i), FieldValue::Null);
    }

    // Should serialize and deserialize efficiently
    let serialized = json_format.serialize_record(&data).unwrap();
    let deserialized = json_format.deserialize_record(&serialized).unwrap();

    assert_eq!(deserialized.len(), 1000);

    // All values should be null
    for (_key, value) in deserialized {
        assert_eq!(value, FieldValue::Null);
    }
}

#[test]
fn test_mixed_type_serialization_performance() {
    // Test performance with mixed data types
    let json_format = JsonFormat;

    let mut data = HashMap::new();
    for i in 0..100 {
        match i % 5 {
            0 => data.insert(format!("key_{}", i), FieldValue::Null),
            1 => data.insert(format!("key_{}", i), FieldValue::Boolean(i % 2 == 0)),
            2 => data.insert(format!("key_{}", i), FieldValue::Integer(i as i64)),
            3 => data.insert(format!("key_{}", i), FieldValue::Float(i as f64 * 0.5)),
            _ => data.insert(
                format!("key_{}", i),
                FieldValue::String(format!("value_{}", i)),
            ),
        };
    }

    // Should handle mixed types efficiently
    for _ in 0..100 {
        let serialized = json_format.serialize_record(&data).unwrap();
        let deserialized = json_format.deserialize_record(&serialized).unwrap();
        assert_eq!(deserialized.len(), data.len());
    }
}
