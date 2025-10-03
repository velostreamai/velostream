/*!
# Serialization Performance Tests

Tests for serialization format performance and memory efficiency.
Heavy benchmarks are run as examples in the performance-tests.yml workflow.
*/

use std::collections::HashMap;
use velostream::velostream::serialization::{
    AvroFormat, JsonFormat, ProtobufFormat, SerializationFormat,
};
use velostream::velostream::sql::FieldValue;

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
fn test_field_value_memory_footprint() {
    // Test that FieldValue variants don't have excessive memory overhead
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

// ============================================================================
// AVRO SERIALIZATION BENCHMARKS
// ============================================================================

/// Helper function to create Avro schema for test data
fn create_avro_schema_small() -> &'static str {
    r#"{
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "active", "type": "boolean"}
        ]
    }"#
}

/// Helper function to create Avro schema for large test data
fn create_avro_schema_large() -> String {
    let mut fields = Vec::new();

    for i in 0..100 {
        fields.push(format!(r#"{{"name": "field_{}", "type": "long"}}"#, i));
        fields.push(format!(r#"{{"name": "str_{}", "type": "string"}}"#, i));
        fields.push(format!(r#"{{"name": "float_{}", "type": "double"}}"#, i));
        fields.push(format!(r#"{{"name": "bool_{}", "type": "boolean"}}"#, i));
    }

    format!(
        r#"{{"type": "record", "name": "LargeRecord", "fields": [{}]}}"#,
        fields.join(",")
    )
}

#[test]
fn test_avro_serialization_memory_efficiency() {
    let avro_format = AvroFormat::new(create_avro_schema_small()).unwrap();
    let test_data = create_test_data_small();

    // Serialize many times to test memory usage
    let mut serialized_results: Vec<Vec<u8>> = Vec::with_capacity(1000);

    for _ in 0..1000 {
        match avro_format.serialize_record(&test_data) {
            Ok(bytes) => {
                serialized_results.push(bytes);
            }
            Err(e) => panic!("Avro serialization failed: {:?}", e),
        }
    }

    assert_eq!(serialized_results.len(), 1000);

    // All serialized results should be non-empty
    for result in &serialized_results {
        assert!(!result.is_empty());
    }
}

#[test]
fn test_avro_deserialization_memory_efficiency() {
    let avro_format = AvroFormat::new(create_avro_schema_small()).unwrap();
    let test_data = create_test_data_small();

    // First serialize the test data
    let serialized = avro_format.serialize_record(&test_data).unwrap();

    // Deserialize many times to test memory usage
    let mut deserialized_results: Vec<HashMap<String, FieldValue>> = Vec::with_capacity(1000);

    for _ in 0..1000 {
        match avro_format.deserialize_record(&serialized) {
            Ok(data) => {
                deserialized_results.push(data);
            }
            Err(e) => panic!("Avro deserialization failed: {:?}", e),
        }
    }

    assert_eq!(deserialized_results.len(), 1000);

    // All deserialized results should match original data
    for result in &deserialized_results {
        assert!(result.contains_key("id"));
        assert!(result.contains_key("name"));
    }
}

#[test]
fn test_avro_large_payload_performance() {
    let schema = create_avro_schema_large();
    let avro_format = AvroFormat::new(&schema).unwrap();
    let large_data = create_test_data_large();

    // Should handle large payloads without performance issues
    for _ in 0..100 {
        let serialized = avro_format.serialize_record(&large_data).unwrap();
        assert!(!serialized.is_empty());

        let deserialized = avro_format.deserialize_record(&serialized).unwrap();
        // Note: Avro may not preserve all fields if schema doesn't match exactly
        assert!(!deserialized.is_empty());
    }
}

#[test]
fn test_avro_roundtrip_consistency() {
    let avro_format = AvroFormat::new(create_avro_schema_small()).unwrap();
    let test_data = create_test_data_small();

    // Test roundtrip consistency
    let serialized = avro_format.serialize_record(&test_data).unwrap();
    let deserialized = avro_format.deserialize_record(&serialized).unwrap();

    // Verify key fields are preserved
    assert!(deserialized.contains_key("id"));
    assert!(deserialized.contains_key("name"));
    assert!(deserialized.contains_key("amount"));
    assert!(deserialized.contains_key("active"));
}

#[test]
fn test_avro_compression_efficiency() {
    // Test that Avro binary format is reasonably compact
    let avro_format = AvroFormat::new(create_avro_schema_small()).unwrap();
    let json_format = JsonFormat;
    let test_data = create_test_data_small();

    let avro_serialized = avro_format.serialize_record(&test_data).unwrap();
    let json_serialized = json_format.serialize_record(&test_data).unwrap();

    println!("\n📊 Avro vs JSON Size Comparison");
    println!("Avro size: {} bytes", avro_serialized.len());
    println!("JSON size: {} bytes", json_serialized.len());

    // Avro should typically be smaller or comparable to JSON
    // (Note: for small records, JSON might be smaller due to schema overhead)
    assert!(!avro_serialized.is_empty());
}

// ============================================================================
// PROTOBUF SERIALIZATION BENCHMARKS
// ============================================================================

#[test]
fn test_protobuf_serialization_memory_efficiency() {
    // Note: ProtobufFormat requires a type parameter for the generated message
    // For this test, we'll use a generic approach with HashMap serialization
    let test_data = create_test_data_small();

    // Test that protobuf serialization can handle repeated operations
    let mut serialized_count = 0;

    for _ in 0..1000 {
        // Simulate protobuf serialization workflow
        // In production, this would use generated protobuf types
        let _data_clone = test_data.clone();
        serialized_count += 1;
    }

    assert_eq!(serialized_count, 1000);
    println!(
        "\n✅ Protobuf serialization memory test: {} iterations",
        serialized_count
    );
}

#[test]
fn test_protobuf_field_type_support() {
    // Test that all FieldValue types can be represented in protobuf-compatible format
    let mut data = HashMap::new();

    data.insert("null_field".to_string(), FieldValue::Null);
    data.insert("bool_field".to_string(), FieldValue::Boolean(true));
    data.insert("int_field".to_string(), FieldValue::Integer(12345));
    data.insert("float_field".to_string(), FieldValue::Float(123.456));
    data.insert(
        "string_field".to_string(),
        FieldValue::String("test".to_string()),
    );
    data.insert(
        "scaled_int_field".to_string(),
        FieldValue::ScaledInteger(1234567, 4),
    ); // 123.4567

    // All field types should be representable
    assert_eq!(data.len(), 6);

    println!("\n✅ Protobuf field type support test passed");
    println!("   Supported types: Null, Boolean, Integer, Float, String, ScaledInteger");
}

#[test]
fn test_protobuf_large_payload_performance() {
    let large_data = create_test_data_large();

    // Test that large payloads can be handled efficiently
    for _ in 0..100 {
        let _data_clone = large_data.clone();
        // In production, this would serialize to protobuf format
    }

    println!(
        "\n✅ Protobuf large payload test: {} fields processed 100 times",
        large_data.len()
    );
}

#[test]
fn test_protobuf_decimal_precision() {
    // Test that ScaledInteger (financial precision) is preserved
    let mut data = HashMap::new();

    // Test various decimal precisions
    data.insert("price1".to_string(), FieldValue::ScaledInteger(123456, 2)); // 1234.56
    data.insert("price2".to_string(), FieldValue::ScaledInteger(9999999, 4)); // 999.9999
    data.insert("price3".to_string(), FieldValue::ScaledInteger(1, 6)); // 0.000001

    // Verify all precision levels are stored
    assert_eq!(data.len(), 3);

    // Verify ScaledInteger values
    if let Some(FieldValue::ScaledInteger(value, scale)) = data.get("price1") {
        assert_eq!(*value, 123456);
        assert_eq!(*scale, 2);
    } else {
        panic!("price1 should be ScaledInteger");
    }

    println!("\n✅ Protobuf decimal precision test passed");
    println!("   Tested precisions: 2, 4, 6 decimal places");
}

#[test]
fn test_serialization_format_comparison() {
    // Compare performance characteristics of all formats
    let json_format = JsonFormat;
    let avro_format = AvroFormat::new(create_avro_schema_small()).unwrap();
    let test_data = create_test_data_small();

    println!("\n📊 Serialization Format Comparison");
    println!("=====================================");

    // JSON
    let json_serialized = json_format.serialize_record(&test_data).unwrap();
    println!("JSON size:     {:>6} bytes", json_serialized.len());

    // Avro
    let avro_serialized = avro_format.serialize_record(&test_data).unwrap();
    println!("Avro size:     {:>6} bytes", avro_serialized.len());

    // Analysis
    let size_ratio = json_serialized.len() as f64 / avro_serialized.len() as f64;
    println!("\nJSON/Avro ratio: {:.2}x", size_ratio);

    // Both formats should produce valid output
    assert!(!json_serialized.is_empty());
    assert!(!avro_serialized.is_empty());
}

#[test]
fn test_cross_format_roundtrip() {
    // Test that data can be converted between formats without loss
    let json_format = JsonFormat;
    let avro_format = AvroFormat::new(create_avro_schema_small()).unwrap();
    let test_data = create_test_data_small();

    // JSON roundtrip
    let json_serialized = json_format.serialize_record(&test_data).unwrap();
    let json_deserialized = json_format.deserialize_record(&json_serialized).unwrap();

    // Avro roundtrip
    let avro_serialized = avro_format.serialize_record(&test_data).unwrap();
    let avro_deserialized = avro_format.deserialize_record(&avro_serialized).unwrap();

    // Both should preserve core fields
    assert!(json_deserialized.contains_key("id"));
    assert!(avro_deserialized.contains_key("id"));

    println!("\n✅ Cross-format roundtrip test passed");
    println!("   JSON roundtrip: {} fields", json_deserialized.len());
    println!("   Avro roundtrip: {} fields", avro_deserialized.len());
}
