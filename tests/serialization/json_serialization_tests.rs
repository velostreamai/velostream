/*!
# JSON Serialization Tests

Tests for JSON serialization format implementation, ensuring all FieldValue types
are properly converted to/from JSON format.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;

fn create_comprehensive_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Basic types
    record.insert(
        "string_field".to_string(),
        FieldValue::String("test_value".to_string()),
    );
    record.insert("integer_field".to_string(), FieldValue::Integer(42));
    record.insert("float_field".to_string(), FieldValue::Float(3.14159));
    record.insert("boolean_field".to_string(), FieldValue::Boolean(true));
    record.insert("null_field".to_string(), FieldValue::Null);

    // Array types
    record.insert(
        "string_array".to_string(),
        FieldValue::Array(vec![
            FieldValue::String("item1".to_string()),
            FieldValue::String("item2".to_string()),
            FieldValue::String("item3".to_string()),
        ]),
    );

    record.insert(
        "mixed_array".to_string(),
        FieldValue::Array(vec![
            FieldValue::Integer(1),
            FieldValue::Float(2.5),
            FieldValue::Boolean(false),
            FieldValue::Null,
        ]),
    );

    // Map type
    let mut map = HashMap::new();
    map.insert(
        "nested_string".to_string(),
        FieldValue::String("nested_value".to_string()),
    );
    map.insert("nested_number".to_string(), FieldValue::Integer(100));
    record.insert("map_field".to_string(), FieldValue::Map(map));

    // Struct type
    let mut struct_fields = HashMap::new();
    struct_fields.insert("id".to_string(), FieldValue::Integer(123));
    struct_fields.insert(
        "name".to_string(),
        FieldValue::String("John Doe".to_string()),
    );
    struct_fields.insert("active".to_string(), FieldValue::Boolean(true));
    record.insert(
        "struct_field".to_string(),
        FieldValue::Struct(struct_fields),
    );

    record
}

#[tokio::test]
async fn test_json_serialization_round_trip() {
    let format = JsonFormat;
    let original_record = create_comprehensive_test_record();

    // Serialize to bytes
    let serialized = format
        .serialize_record(&original_record)
        .expect("Serialization should succeed");

    // Deserialize back to record
    let deserialized_record = format
        .deserialize_record(&serialized)
        .expect("Deserialization should succeed");

    // Verify all fields match
    assert_eq!(original_record.len(), deserialized_record.len());

    for (key, original_value) in &original_record {
        let deserialized_value = deserialized_record.get(key).expect(&format!(
            "Key '{}' should exist in deserialized record",
            key
        ));
        assert_eq!(
            original_value, deserialized_value,
            "Value for key '{}' should match after round trip",
            key
        );
    }
}

#[tokio::test]
async fn test_json_to_execution_format() {
    let format = JsonFormat;
    let record = create_comprehensive_test_record();

    let execution_format = format
        .to_execution_format(&record)
        .expect("Conversion to execution format should succeed");

    // Verify conversions
    assert_eq!(
        execution_format.get("string_field"),
        Some(&InternalValue::String("test_value".to_string()))
    );
    assert_eq!(
        execution_format.get("integer_field"),
        Some(&InternalValue::Integer(42))
    );
    assert_eq!(
        execution_format.get("float_field"),
        Some(&InternalValue::Number(3.14159))
    );
    assert_eq!(
        execution_format.get("boolean_field"),
        Some(&InternalValue::Boolean(true))
    );
    assert_eq!(
        execution_format.get("null_field"),
        Some(&InternalValue::Null)
    );
}

#[tokio::test]
async fn test_json_from_execution_format() {
    let format = JsonFormat;

    let mut execution_data = HashMap::new();
    execution_data.insert(
        "test_string".to_string(),
        InternalValue::String("test".to_string()),
    );
    execution_data.insert("test_int".to_string(), InternalValue::Integer(42));
    execution_data.insert("test_float".to_string(), InternalValue::Number(3.14));
    execution_data.insert("test_bool".to_string(), InternalValue::Boolean(false));
    execution_data.insert("test_null".to_string(), InternalValue::Null);

    let record = format
        .from_execution_format(&execution_data)
        .expect("Conversion from execution format should succeed");

    assert_eq!(
        record.get("test_string"),
        Some(&FieldValue::String("test".to_string()))
    );
    assert_eq!(record.get("test_int"), Some(&FieldValue::Integer(42)));
    assert_eq!(record.get("test_float"), Some(&FieldValue::Float(3.14)));
    assert_eq!(record.get("test_bool"), Some(&FieldValue::Boolean(false)));
    assert_eq!(record.get("test_null"), Some(&FieldValue::Null));
}

#[tokio::test]
async fn test_json_nested_structures() {
    let format = JsonFormat;

    let mut inner_struct = HashMap::new();
    inner_struct.insert(
        "inner_field".to_string(),
        FieldValue::String("inner_value".to_string()),
    );

    let mut record = HashMap::new();
    record.insert(
        "nested_array".to_string(),
        FieldValue::Array(vec![
            FieldValue::Struct(inner_struct.clone()),
            FieldValue::Array(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
        ]),
    );

    // Test round trip
    let serialized = format
        .serialize_record(&record)
        .expect("Serialization should succeed");
    let deserialized = format
        .deserialize_record(&serialized)
        .expect("Deserialization should succeed");

    assert_eq!(record, deserialized);
}

#[tokio::test]
async fn test_json_error_handling() {
    let format = JsonFormat;

    // Test invalid JSON bytes
    let invalid_json = b"{ invalid json }";
    let result = format.deserialize_record(invalid_json);
    assert!(
        result.is_err(),
        "Invalid JSON should cause deserialization error"
    );

    // Test non-object JSON
    let non_object_json = b"\"not an object\"";
    let result = format.deserialize_record(non_object_json);
    assert!(
        result.is_err(),
        "Non-object JSON should cause deserialization error"
    );
}

#[tokio::test]
async fn test_json_format_name() {
    let format = JsonFormat;
    assert_eq!(format.format_name(), "JSON");
}

#[tokio::test]
async fn test_json_empty_record() {
    let format = JsonFormat;
    let empty_record = HashMap::new();

    let serialized = format
        .serialize_record(&empty_record)
        .expect("Empty record serialization should succeed");
    let deserialized = format
        .deserialize_record(&serialized)
        .expect("Empty record deserialization should succeed");

    assert!(deserialized.is_empty());
}

#[tokio::test]
async fn test_json_large_numbers() {
    let format = JsonFormat;

    let mut record = HashMap::new();
    record.insert("large_int".to_string(), FieldValue::Integer(i64::MAX));
    record.insert("small_int".to_string(), FieldValue::Integer(i64::MIN));
    record.insert("large_float".to_string(), FieldValue::Float(f64::MAX));
    record.insert("small_float".to_string(), FieldValue::Float(f64::MIN));

    let serialized = format
        .serialize_record(&record)
        .expect("Large number serialization should succeed");
    let deserialized = format
        .deserialize_record(&serialized)
        .expect("Large number deserialization should succeed");

    assert_eq!(record, deserialized);
}

#[tokio::test]
async fn test_json_unicode_strings() {
    let format = JsonFormat;

    let mut record = HashMap::new();
    record.insert(
        "unicode_field".to_string(),
        FieldValue::String("Hello 世界 🌍 café".to_string()),
    );
    record.insert(
        "emoji_field".to_string(),
        FieldValue::String("🚀🔥💻⚡️".to_string()),
    );

    let serialized = format
        .serialize_record(&record)
        .expect("Unicode serialization should succeed");
    let deserialized = format
        .deserialize_record(&serialized)
        .expect("Unicode deserialization should succeed");

    assert_eq!(record, deserialized);
}
