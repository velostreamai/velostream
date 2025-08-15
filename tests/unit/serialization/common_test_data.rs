/*!
# Common Test Data for Serialization Tests

Standardized test record creators and utilities shared across all serialization format tests.
*/

use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;
use std::f64::consts::PI;

/// Creates a basic test record with fundamental types
pub fn create_basic_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();
    record.insert("id".to_string(), FieldValue::Integer(123));
    record.insert(
        "name".to_string(),
        FieldValue::String("Test User".to_string()),
    );
    record.insert("active".to_string(), FieldValue::Boolean(true));
    record.insert("score".to_string(), FieldValue::Float(89.5));
    record
}

/// Creates a comprehensive test record with all supported types
pub fn create_comprehensive_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Basic types
    record.insert(
        "string_field".to_string(),
        FieldValue::String("test_value".to_string()),
    );
    record.insert("integer_field".to_string(), FieldValue::Integer(42));
    record.insert("float_field".to_string(), FieldValue::Float(PI));
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

/// Creates a test record with null-handling patterns
pub fn create_null_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();
    record.insert("id".to_string(), FieldValue::Integer(123));
    record.insert("optional_field".to_string(), FieldValue::Null);
    record.insert(
        "required_field".to_string(),
        FieldValue::String("required".to_string()),
    );
    record
}

/// Creates a test record with edge case values
pub fn create_edge_case_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Large numbers
    record.insert("large_int".to_string(), FieldValue::Integer(i64::MAX));
    record.insert("small_int".to_string(), FieldValue::Integer(i64::MIN));
    record.insert("large_float".to_string(), FieldValue::Float(f64::MAX));
    record.insert("small_float".to_string(), FieldValue::Float(f64::MIN));

    // Unicode strings
    record.insert(
        "unicode_field".to_string(),
        FieldValue::String("Hello 世界 🌍 café".to_string()),
    );
    record.insert(
        "emoji_field".to_string(),
        FieldValue::String("🚀🔥💻⚡️".to_string()),
    );

    // Empty collections
    record.insert("empty_array".to_string(), FieldValue::Array(vec![]));
    record.insert("empty_map".to_string(), FieldValue::Map(HashMap::new()));
    record.insert(
        "empty_struct".to_string(),
        FieldValue::Struct(HashMap::new()),
    );

    record
}

/// Creates a test record with large data volumes
pub fn create_large_data_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Large string data
    let large_string = "x".repeat(10000);
    record.insert("large_field".to_string(), FieldValue::String(large_string));

    // Large array
    let large_array: Vec<FieldValue> = (0..1000).map(|i| FieldValue::Integer(i)).collect();
    record.insert("large_array".to_string(), FieldValue::Array(large_array));

    // Large nested structure
    let mut large_map = HashMap::new();
    for i in 0..100 {
        large_map.insert(format!("key_{}", i), FieldValue::Integer(i));
    }
    record.insert("large_map".to_string(), FieldValue::Map(large_map));

    record
}

/// Creates a test record for complex nested structures
pub fn create_complex_nested_test_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Nested structs
    let mut inner_struct = HashMap::new();
    inner_struct.insert(
        "inner_field".to_string(),
        FieldValue::String("inner_value".to_string()),
    );

    let mut outer_struct = HashMap::new();
    outer_struct.insert(
        "outer_field".to_string(),
        FieldValue::String("outer_value".to_string()),
    );
    outer_struct.insert("inner_struct".to_string(), FieldValue::Struct(inner_struct));

    record.insert(
        "nested_struct".to_string(),
        FieldValue::Struct(outer_struct),
    );

    // Nested arrays
    record.insert(
        "nested_array".to_string(),
        FieldValue::Array(vec![
            FieldValue::Array(vec![FieldValue::Integer(1), FieldValue::Integer(2)]),
            FieldValue::Array(vec![
                FieldValue::String("a".to_string()),
                FieldValue::String("b".to_string()),
            ]),
        ]),
    );

    // Mixed nesting
    let mut complex_map = HashMap::new();
    complex_map.insert(
        "array_in_map".to_string(),
        FieldValue::Array(vec![
            FieldValue::Integer(1),
            FieldValue::Float(2.5),
            FieldValue::Boolean(true),
        ]),
    );
    record.insert("complex_map".to_string(), FieldValue::Map(complex_map));

    record
}

/// Standard Avro schema for basic tests
pub fn create_basic_avro_schema() -> &'static str {
    r#"
    {
        "type": "record",
        "name": "BasicRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "active", "type": "boolean"},
            {"name": "score", "type": "double"}
        ]
    }
    "#
}

/// Standard Avro schema for null handling tests
pub fn create_null_avro_schema() -> &'static str {
    r#"
    {
        "type": "record",
        "name": "NullRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "optional_field", "type": ["string", "null"]},
            {"name": "required_field", "type": "string"}
        ]
    }
    "#
}

/// Standard Avro schema for complex types
pub fn create_complex_avro_schema() -> &'static str {
    r#"
    {
        "type": "record",
        "name": "ComplexRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "metadata", "type": {"type": "map", "values": "string"}},
            {"name": "nested", "type": {
                "type": "record",
                "name": "NestedRecord",
                "fields": [
                    {"name": "value", "type": "string"},
                    {"name": "count", "type": "long"}
                ]
            }}
        ]
    }
    "#
}

/// Helper function to compare records while handling format-specific conversions
pub fn assert_records_equivalent(
    original: &HashMap<String, FieldValue>,
    deserialized: &HashMap<String, FieldValue>,
) {
    assert_eq!(original.len(), deserialized.len(), "Record length mismatch");

    for (key, original_value) in original {
        let deserialized_value = deserialized.get(key).expect(&format!(
            "Key '{}' should exist in deserialized record",
            key
        ));
        assert_field_values_equivalent(original_value, deserialized_value);
    }
}

/// Helper to compare field values with format-specific handling (like Struct -> Map conversion)
pub fn assert_field_values_equivalent(original: &FieldValue, deserialized: &FieldValue) {
    match (original, deserialized) {
        // Handle Struct -> Map conversion (common in JSON serialization)
        (FieldValue::Struct(original_map), FieldValue::Map(deserialized_map)) => {
            assert_eq!(original_map.len(), deserialized_map.len());
            for (key, orig_value) in original_map {
                let deser_value = deserialized_map
                    .get(key)
                    .expect(&format!("Key '{}' should exist in deserialized map", key));
                assert_field_values_equivalent(orig_value, deser_value);
            }
        }
        // Handle Map -> Struct conversion (in case the reverse happens)
        (FieldValue::Map(original_map), FieldValue::Struct(deserialized_map)) => {
            assert_eq!(original_map.len(), deserialized_map.len());
            for (key, orig_value) in original_map {
                let deser_value = deserialized_map.get(key).expect(&format!(
                    "Key '{}' should exist in deserialized struct",
                    key
                ));
                assert_field_values_equivalent(orig_value, deser_value);
            }
        }
        // Handle arrays that might contain structs
        (FieldValue::Array(original_array), FieldValue::Array(deserialized_array)) => {
            assert_eq!(original_array.len(), deserialized_array.len());
            for (orig_item, deser_item) in original_array.iter().zip(deserialized_array.iter()) {
                assert_field_values_equivalent(orig_item, deser_item);
            }
        }
        // Handle maps that might contain structs
        (FieldValue::Map(original_map), FieldValue::Map(deserialized_map)) => {
            assert_eq!(original_map.len(), deserialized_map.len());
            for (key, orig_value) in original_map {
                let deser_value = deserialized_map
                    .get(key)
                    .expect(&format!("Key '{}' should exist", key));
                assert_field_values_equivalent(orig_value, deser_value);
            }
        }
        // Handle structs that might contain nested structs/maps
        (FieldValue::Struct(original_map), FieldValue::Struct(deserialized_map)) => {
            assert_eq!(original_map.len(), deserialized_map.len());
            for (key, orig_value) in original_map {
                let deser_value = deserialized_map
                    .get(key)
                    .expect(&format!("Key '{}' should exist in struct", key));
                assert_field_values_equivalent(orig_value, deser_value);
            }
        }
        // For all other types, they should match exactly
        _ => {
            assert_eq!(original, deserialized, "Field value mismatch");
        }
    }
}

/// Test execution format conversion consistency
pub fn test_execution_format_round_trip(
    format: &dyn ferrisstreams::ferris::serialization::SerializationFormat,
    record: &HashMap<String, FieldValue>,
) -> Result<(), Box<dyn std::error::Error>> {
    let execution_format = format.to_execution_format(record)?;
    let converted_back = format.from_execution_format(&execution_format)?;
    assert_records_equivalent(record, &converted_back);
    Ok(())
}

/// Test serialization round trip with common assertions
pub fn test_serialization_round_trip(
    format: &dyn ferrisstreams::ferris::serialization::SerializationFormat,
    record: &HashMap<String, FieldValue>,
) -> Result<(), Box<dyn std::error::Error>> {
    let serialized = format.serialize_record(record)?;
    assert!(
        !serialized.is_empty(),
        "Serialized data should not be empty"
    );

    let deserialized = format.deserialize_record(&serialized)?;
    assert_records_equivalent(record, &deserialized);

    Ok(())
}
