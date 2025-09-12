/*!
# JSON Serialization Tests

Tests for JSON serialization format implementation, ensuring all FieldValue types
are properly converted to/from JSON format.
*/

use super::common_test_data::*;
use ferrisstreams::ferris::serialization::{JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::execution::types::StreamRecord;
use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;

#[tokio::test]
async fn test_json_serialization_round_trip() {
    let format = JsonFormat;
    let record = create_comprehensive_test_record();

    test_serialization_round_trip(&format, &record).expect("JSON round trip should succeed");
}

#[tokio::test]
async fn test_json_to_execution_format() {
    let format = JsonFormat;
    let record = create_comprehensive_test_record();

    test_execution_format_round_trip(&format, &record)
        .expect("JSON execution format round trip should succeed");
}

#[tokio::test]
async fn test_json_from_execution_format() {
    let format = JsonFormat;

    // Create a StreamRecord directly
    let mut fields = HashMap::new();
    fields.insert(
        "test_string".to_string(),
        FieldValue::String("test".to_string()),
    );
    fields.insert("test_int".to_string(), FieldValue::Integer(42));
    fields.insert(
        "test_float".to_string(),
        FieldValue::Float(std::f64::consts::PI),
    );
    fields.insert("test_bool".to_string(), FieldValue::Boolean(false));
    fields.insert("test_null".to_string(), FieldValue::Null);

    let stream_record = StreamRecord {
        fields: fields.clone(),
        timestamp: 1234567890,
        offset: 100,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    };

    // Serialize the StreamRecord's fields
    let serialized = format
        .serialize_record(&stream_record.fields)
        .expect("Serialization should succeed");

    // Deserialize back
    let deserialized = format
        .deserialize_record(&serialized)
        .expect("Deserialization should succeed");

    // Verify the round-trip
    assert_eq!(
        deserialized.get("test_string"),
        Some(&FieldValue::String("test".to_string()))
    );
    assert_eq!(deserialized.get("test_int"), Some(&FieldValue::Integer(42)));
    assert_eq!(
        deserialized.get("test_float"),
        Some(&FieldValue::Float(std::f64::consts::PI))
    );
    assert_eq!(
        deserialized.get("test_bool"),
        Some(&FieldValue::Boolean(false))
    );
    assert_eq!(deserialized.get("test_null"), Some(&FieldValue::Null));
}

#[tokio::test]
async fn test_json_nested_structures() {
    let format = JsonFormat;
    let record = create_complex_nested_test_record();

    test_serialization_round_trip(&format, &record)
        .expect("Nested structures round trip should succeed");
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

    test_serialization_round_trip(&format, &empty_record)
        .expect("Empty record round trip should succeed");
}

#[tokio::test]
async fn test_json_large_numbers() {
    let format = JsonFormat;
    let record = create_edge_case_test_record();

    // Extract large number fields for testing
    let mut large_number_record = HashMap::new();
    large_number_record.insert("large_int".to_string(), record["large_int"].clone());
    large_number_record.insert("small_int".to_string(), record["small_int"].clone());
    large_number_record.insert("large_float".to_string(), record["large_float"].clone());
    large_number_record.insert("small_float".to_string(), record["small_float"].clone());

    test_serialization_round_trip(&format, &large_number_record)
        .expect("Large numbers round trip should succeed");
}

#[tokio::test]
async fn test_json_unicode_strings() {
    let format = JsonFormat;
    let record = create_edge_case_test_record();

    // Extract unicode fields for testing
    let mut unicode_record = HashMap::new();
    unicode_record.insert("unicode_field".to_string(), record["unicode_field"].clone());
    unicode_record.insert("emoji_field".to_string(), record["emoji_field"].clone());

    test_serialization_round_trip(&format, &unicode_record)
        .expect("Unicode round trip should succeed");
}

#[tokio::test]
async fn test_json_large_data() {
    let format = JsonFormat;
    let record = create_large_data_test_record();

    test_serialization_round_trip(&format, &record).expect("Large data round trip should succeed");
}

#[tokio::test]
async fn test_json_null_handling() {
    let format = JsonFormat;
    let record = create_null_test_record();

    test_serialization_round_trip(&format, &record)
        .expect("Null handling round trip should succeed");
}

#[tokio::test]
async fn test_json_comprehensive_type_matrix() {
    // Test comprehensive type coverage - the baseline for all other formats
    let format = JsonFormat;
    let record = create_comprehensive_test_record();

    // Test serialization round trip
    test_serialization_round_trip(&format, &record)
        .expect("Comprehensive type matrix round trip should succeed");

    // Also test execution format round trip
    test_execution_format_round_trip(&format, &record)
        .expect("Comprehensive execution format round trip should succeed");
}
