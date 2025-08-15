/*!
# Serialization Factory Tests

Tests for the SerializationFormatFactory and general serialization functionality.
*/

use super::common_test_data::*;
use ferrisstreams::ferris::serialization::SerializationFormatFactory;
use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;

#[tokio::test]
async fn test_factory_create_json_format() {
    let format =
        SerializationFormatFactory::create_format("json").expect("Should create JSON format");

    assert_eq!(format.format_name(), "JSON");
}

#[tokio::test]
async fn test_factory_create_json_format_case_insensitive() {
    let formats = ["JSON", "Json", "jSoN"];

    for format_name in &formats {
        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format for '{}'", format_name));
        assert_eq!(format.format_name(), "JSON");
    }
}

#[tokio::test]
async fn test_factory_create_unknown_format() {
    let result = SerializationFormatFactory::create_format("unknown_format");
    assert!(result.is_err(), "Unknown format should return error");
}

#[tokio::test]
async fn test_factory_supported_formats() {
    let supported = SerializationFormatFactory::supported_formats();

    // JSON should always be supported
    assert!(
        supported.contains(&"json"),
        "JSON should be in supported formats"
    );

    // Check that the list is not empty
    assert!(
        !supported.is_empty(),
        "Supported formats should not be empty"
    );

    // Verify each format can be created
    for format_name in supported {
        let format = SerializationFormatFactory::create_format(format_name);
        assert!(
            format.is_ok(),
            "Should be able to create supported format '{}'",
            format_name
        );
    }
}

#[tokio::test]
async fn test_factory_default_format() {
    let format = SerializationFormatFactory::default_format();
    assert_eq!(format.format_name(), "JSON");

    // Test that default format works
    let mut record = HashMap::new();
    record.insert("test".to_string(), FieldValue::String("value".to_string()));

    let serialized = format
        .serialize_record(&record)
        .expect("Default format serialization should work");
    let deserialized = format
        .deserialize_record(&serialized)
        .expect("Default format deserialization should work");

    assert_eq!(record, deserialized);
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn test_factory_create_avro_format() {
    let format = SerializationFormatFactory::create_format("avro")
        .expect("Should create Avro format when feature is enabled");

    assert_eq!(format.format_name(), "Avro");
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn test_factory_create_custom_avro_format() {
    let schema = r#"
    {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }
    "#;

    let format = SerializationFormatFactory::create_avro_format(schema)
        .expect("Should create custom Avro format");

    assert_eq!(format.format_name(), "Avro");
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn test_factory_create_avro_format_with_evolution() {
    let writer_schema = r#"
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }
    "#;

    let reader_schema = r#"
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let format =
        SerializationFormatFactory::create_avro_format_with_schemas(writer_schema, reader_schema)
            .expect("Should create Avro format with schema evolution");

    assert_eq!(format.format_name(), "Avro");
}

#[cfg(feature = "protobuf")]
#[tokio::test]
async fn test_factory_create_protobuf_format() {
    let format_proto = SerializationFormatFactory::create_format("protobuf")
        .expect("Should create Protobuf format when feature is enabled");
    assert_eq!(format_proto.format_name(), "Protobuf");

    let format_proto_alias = SerializationFormatFactory::create_format("proto")
        .expect("Should create Protobuf format with alias");
    assert_eq!(format_proto_alias.format_name(), "Protobuf");
}

#[cfg(feature = "protobuf")]
#[tokio::test]
async fn test_factory_create_typed_protobuf_format() {
    let format = SerializationFormatFactory::create_protobuf_format::<()>();
    assert_eq!(format.format_name(), "Protobuf");
}

#[tokio::test]
async fn test_format_interface_consistency() {
    let supported_formats = SerializationFormatFactory::supported_formats();

    for format_name in supported_formats {
        // Skip Avro in consistency test as it requires a specific schema
        if format_name == "avro" {
            continue;
        }

        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format '{}'", format_name));

        // Test basic functionality with standardized test data
        let test_record = create_basic_test_record();

        // Test serialization round trip
        test_serialization_round_trip(format.as_ref(), &test_record).expect(&format!(
            "Round trip should succeed for format '{}'",
            format_name
        ));

        // Test execution format round trip
        test_execution_format_round_trip(format.as_ref(), &test_record).expect(&format!(
            "Execution format round trip should succeed for format '{}'",
            format_name
        ));

        // Verify format name is not empty
        assert!(
            !format.format_name().is_empty(),
            "Format name should not be empty for '{}'",
            format_name
        );
    }
}

#[tokio::test]
async fn test_multiple_formats_independence() {
    // Create multiple format instances
    let json_format1 =
        SerializationFormatFactory::create_format("json").expect("Should create first JSON format");
    let json_format2 = SerializationFormatFactory::create_format("json")
        .expect("Should create second JSON format");

    // Test that they work independently
    let mut record1 = HashMap::new();
    record1.insert(
        "test1".to_string(),
        FieldValue::String("value1".to_string()),
    );

    let mut record2 = HashMap::new();
    record2.insert(
        "test2".to_string(),
        FieldValue::String("value2".to_string()),
    );

    let serialized1 = json_format1
        .serialize_record(&record1)
        .expect("First format should work");
    let serialized2 = json_format2
        .serialize_record(&record2)
        .expect("Second format should work");

    let deserialized1 = json_format1
        .deserialize_record(&serialized1)
        .expect("First format deserialization should work");
    let deserialized2 = json_format2
        .deserialize_record(&serialized2)
        .expect("Second format deserialization should work");

    assert_eq!(record1, deserialized1);
    assert_eq!(record2, deserialized2);
}

#[tokio::test]
async fn test_factory_error_messages() {
    let result = SerializationFormatFactory::create_format("nonexistent");

    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Unknown format") || error_msg.contains("Unsupported"),
                "Error message should be descriptive: {}",
                error_msg
            );
        }
        Ok(_) => panic!("Should return error for nonexistent format"),
    }
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn test_avro_factory_error_handling() {
    let invalid_schema = "{ this is not valid json }";
    let result = SerializationFormatFactory::create_avro_format(invalid_schema);

    assert!(result.is_err(), "Invalid schema should cause error");

    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("schema") || error_msg.contains("Avro"),
        "Error message should mention schema or Avro: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_comprehensive_type_matrix_across_formats() {
    // Test that all supported formats handle the same comprehensive type set
    let supported_formats = SerializationFormatFactory::supported_formats();
    let comprehensive_record = create_comprehensive_test_record();

    for format_name in supported_formats {
        // Skip Avro in comprehensive test as it requires specific schema
        if format_name == "avro" {
            continue;
        }

        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format '{}'", format_name));

        // Test comprehensive type matrix
        test_serialization_round_trip(format.as_ref(), &comprehensive_record).expect(&format!(
            "Comprehensive type matrix should work for '{}'",
            format_name
        ));

        test_execution_format_round_trip(format.as_ref(), &comprehensive_record).expect(&format!(
            "Comprehensive execution format should work for '{}'",
            format_name
        ));
    }
}

#[tokio::test]
async fn test_edge_cases_across_formats() {
    // Test edge cases across all formats
    let supported_formats = SerializationFormatFactory::supported_formats();
    let edge_case_record = create_edge_case_test_record();

    for format_name in supported_formats {
        // Skip Avro as it requires specific schema
        if format_name == "avro" {
            continue;
        }

        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format '{}'", format_name));

        // Test edge cases (extract compatible fields for testing)
        let mut test_record = HashMap::new();
        test_record.insert(
            "unicode_field".to_string(),
            edge_case_record["unicode_field"].clone(),
        );
        test_record.insert(
            "emoji_field".to_string(),
            edge_case_record["emoji_field"].clone(),
        );
        test_record.insert(
            "large_int".to_string(),
            edge_case_record["large_int"].clone(),
        );
        test_record.insert(
            "small_int".to_string(),
            edge_case_record["small_int"].clone(),
        );

        test_serialization_round_trip(format.as_ref(), &test_record)
            .expect(&format!("Edge cases should work for '{}'", format_name));
    }
}

#[tokio::test]
async fn test_large_data_across_formats() {
    // Test large data handling across all formats
    let supported_formats = SerializationFormatFactory::supported_formats();
    let large_data_record = create_large_data_test_record();

    for format_name in supported_formats {
        // Skip Avro as it requires specific schema
        if format_name == "avro" {
            continue;
        }

        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format '{}'", format_name));

        test_serialization_round_trip(format.as_ref(), &large_data_record)
            .expect(&format!("Large data should work for '{}'", format_name));
    }
}

#[tokio::test]
async fn test_null_handling_consistency_across_formats() {
    // Test null handling consistency across all formats
    let supported_formats = SerializationFormatFactory::supported_formats();
    let null_record = create_null_test_record();

    for format_name in supported_formats {
        // Skip Avro as it requires specific schema for null handling
        if format_name == "avro" {
            continue;
        }

        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format '{}'", format_name));

        test_serialization_round_trip(format.as_ref(), &null_record)
            .expect(&format!("Null handling should work for '{}'", format_name));
    }
}
