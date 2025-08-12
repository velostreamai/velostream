/*!
# Serialization Factory Tests

Tests for the SerializationFormatFactory and general serialization functionality.
*/

use ferrisstreams::ferris::serialization::{SerializationFormat, SerializationFormatFactory};
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
        let format = SerializationFormatFactory::create_format(format_name)
            .expect(&format!("Should create format '{}'", format_name));

        // Test basic functionality
        let mut test_record = HashMap::new();
        test_record.insert("id".to_string(), FieldValue::Integer(123));
        test_record.insert("name".to_string(), FieldValue::String("test".to_string()));
        test_record.insert("active".to_string(), FieldValue::Boolean(true));
        test_record.insert("score".to_string(), FieldValue::Float(95.5));
        test_record.insert("null_field".to_string(), FieldValue::Null);

        // Test serialization
        let serialized = format.serialize_record(&test_record);
        assert!(
            serialized.is_ok(),
            "Serialization should succeed for format '{}'",
            format_name
        );

        let serialized_bytes = serialized.unwrap();
        assert!(
            !serialized_bytes.is_empty(),
            "Serialized data should not be empty for format '{}'",
            format_name
        );

        // Test deserialization
        let deserialized = format.deserialize_record(&serialized_bytes);
        assert!(
            deserialized.is_ok(),
            "Deserialization should succeed for format '{}'",
            format_name
        );

        let deserialized_record = deserialized.unwrap();
        assert!(
            !deserialized_record.is_empty(),
            "Deserialized record should not be empty for format '{}'",
            format_name
        );

        // Test execution format conversion
        let to_execution = format.to_execution_format(&test_record);
        assert!(
            to_execution.is_ok(),
            "Conversion to execution format should succeed for format '{}'",
            format_name
        );

        let execution_data = to_execution.unwrap();
        let from_execution = format.from_execution_format(&execution_data);
        assert!(
            from_execution.is_ok(),
            "Conversion from execution format should succeed for format '{}'",
            format_name
        );

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
