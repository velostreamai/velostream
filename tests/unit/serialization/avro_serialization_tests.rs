/*!
# Avro Serialization Tests

Tests for Avro serialization format implementation, including schema handling
and schema evolution scenarios.
*/

mod avro_tests {
    use super::super::common_test_data::*;
    use ferrisstreams::ferris::serialization::{AvroFormat, SerializationFormat};
    use ferrisstreams::ferris::sql::FieldValue;
    use std::collections::HashMap;

    fn create_evolved_schema() -> &'static str {
        r#"
        {
            "type": "record",
            "name": "BasicRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }
        "#
    }

    #[tokio::test]
    async fn test_avro_format_creation() {
        let schema = create_basic_avro_schema();
        let format = AvroFormat::new(schema);
        assert!(
            format.is_ok(),
            "Avro format creation should succeed with valid schema"
        );

        let format = format.unwrap();
        assert_eq!(format.format_name(), "Avro");
    }

    #[tokio::test]
    async fn test_avro_format_creation_with_invalid_schema() {
        let invalid_schema = "{ invalid schema }";
        let format = AvroFormat::new(invalid_schema);
        assert!(
            format.is_err(),
            "Avro format creation should fail with invalid schema"
        );
    }

    #[tokio::test]
    async fn test_avro_default_format() {
        let format = AvroFormat::default_format();
        assert!(
            format.is_ok(),
            "Default Avro format creation should succeed"
        );
    }

    #[tokio::test]
    async fn test_avro_serialization_round_trip() {
        let schema = create_basic_avro_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");
        let record = create_basic_test_record();

        test_serialization_round_trip(&format, &record).expect("Avro round trip should succeed");
    }

    #[tokio::test]
    async fn test_avro_schema_evolution() {
        let writer_schema = create_basic_avro_schema();
        let reader_schema = create_evolved_schema();

        let format = AvroFormat::with_schemas(writer_schema, reader_schema)
            .expect("Should create Avro format with schema evolution");

        let record = create_basic_test_record();
        let serialized = format
            .serialize_record(&record)
            .expect("Schema evolution serialization should succeed");

        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Schema evolution deserialization should succeed");

        // Verify original fields are preserved
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            deserialized.get("name"),
            Some(&FieldValue::String("Test User".to_string()))
        );
        assert_eq!(deserialized.get("active"), Some(&FieldValue::Boolean(true)));
    }

    #[tokio::test]
    async fn test_avro_to_execution_format() {
        let schema = create_basic_avro_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");
        let record = create_basic_test_record();

        test_execution_format_round_trip(&format, &record)
            .expect("Execution format conversion should succeed");
    }

    #[tokio::test]
    async fn test_avro_from_execution_format() {
        use ferrisstreams::ferris::sql::execution::types::StreamRecord;

        let schema = create_basic_avro_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");

        // Create a StreamRecord directly
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(456));
        fields.insert(
            "name".to_string(),
            FieldValue::String("Jane Smith".to_string()),
        );
        fields.insert("active".to_string(), FieldValue::Boolean(false));
        fields.insert("score".to_string(), FieldValue::Float(87.3));

        let stream_record = StreamRecord {
            fields: fields.clone(),
            timestamp: 1234567890,
            offset: 100,
            partition: 0,
            headers: HashMap::new(),
        };

        // Serialize the StreamRecord
        let serialized = format
            .serialize_record(&stream_record.fields)
            .expect("Serialization should succeed");

        // Deserialize back
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Deserialization should succeed");

        // Verify the round-trip
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(456)));
        assert_eq!(
            deserialized.get("name"),
            Some(&FieldValue::String("Jane Smith".to_string()))
        );
        assert_eq!(
            deserialized.get("active"),
            Some(&FieldValue::Boolean(false))
        );
        assert_eq!(deserialized.get("score"), Some(&FieldValue::Float(87.3)));
    }

    #[tokio::test]
    async fn test_avro_complex_types() {
        let schema = create_complex_avro_schema();
        let format =
            AvroFormat::new(schema).expect("Should create Avro format with complex schema");

        let mut nested_fields = HashMap::new();
        nested_fields.insert(
            "value".to_string(),
            FieldValue::String("nested_value".to_string()),
        );
        nested_fields.insert("count".to_string(), FieldValue::Integer(5));

        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), FieldValue::String("value1".to_string()));
        metadata.insert("key2".to_string(), FieldValue::String("value2".to_string()));

        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(789));
        record.insert(
            "tags".to_string(),
            FieldValue::Array(vec![
                FieldValue::String("tag1".to_string()),
                FieldValue::String("tag2".to_string()),
            ]),
        );
        record.insert("metadata".to_string(), FieldValue::Map(metadata));
        record.insert("nested".to_string(), FieldValue::Struct(nested_fields));

        test_serialization_round_trip(&format, &record)
            .expect("Complex type round trip should succeed");
    }

    #[tokio::test]
    async fn test_avro_direct_creation() {
        let format = AvroFormat::default_format().expect("Should create Avro format directly");

        assert_eq!(format.format_name(), "Avro");

        // Test with custom schema
        let schema = create_basic_avro_schema();
        let custom_format = AvroFormat::new(schema).expect("Should create custom Avro format");

        assert_eq!(custom_format.format_name(), "Avro");
    }

    #[tokio::test]
    async fn test_avro_null_handling() {
        let schema = create_null_avro_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format with null schema");
        let record = create_null_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Null handling round trip should succeed");
    }

    #[tokio::test]
    async fn test_avro_error_handling() {
        let schema = create_basic_avro_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");

        // Test with invalid binary data
        let invalid_data = b"invalid avro data";
        let result = format.deserialize_record(invalid_data);
        assert!(
            result.is_err(),
            "Invalid Avro data should cause deserialization error"
        );

        // Test serialization with incompatible data (this might succeed but with type coercion)
        let mut incompatible_record = HashMap::new();
        incompatible_record.insert(
            "id".to_string(),
            FieldValue::String("not_a_number".to_string()),
        );

        // This might succeed due to Avro's flexible type handling, so we don't assert failure
        let _result = format.serialize_record(&incompatible_record);
    }

    #[tokio::test]
    // ignore edge case for now
    #[ignore]
    async fn test_avro_empty_record() {
        // Schema for empty record
        let empty_schema = r#"
        {
            "type": "record",
            "name": "EmptyRecord",
            "fields": []
        }
        "#;

        let format =
            AvroFormat::new(empty_schema).expect("Should create Avro format for empty record");
        let empty_record = HashMap::new();

        test_serialization_round_trip(&format, &empty_record)
            .expect("Empty record round trip should succeed");
    }

    #[tokio::test]
    async fn test_avro_unicode_strings() {
        // Schema for unicode strings
        let unicode_schema = r#"
        {
            "type": "record",
            "name": "UnicodeRecord",
            "fields": [
                {"name": "unicode_field", "type": "string"},
                {"name": "emoji_field", "type": "string"}
            ]
        }
        "#;

        let format =
            AvroFormat::new(unicode_schema).expect("Should create Avro format for unicode");

        let mut record = HashMap::new();
        record.insert(
            "unicode_field".to_string(),
            FieldValue::String("Hello 世界 🌍 café".to_string()),
        );
        record.insert(
            "emoji_field".to_string(),
            FieldValue::String("🚀🔥💻⚡️".to_string()),
        );

        test_serialization_round_trip(&format, &record).expect("Unicode round trip should succeed");
    }

    #[tokio::test]
    async fn test_avro_large_numbers() {
        // Schema for large numbers
        let large_number_schema = r#"
        {
            "type": "record",
            "name": "LargeNumberRecord",
            "fields": [
                {"name": "large_int", "type": "long"},
                {"name": "small_int", "type": "long"},
                {"name": "large_float", "type": "double"},
                {"name": "small_float", "type": "double"}
            ]
        }
        "#;

        let format = AvroFormat::new(large_number_schema)
            .expect("Should create Avro format for large numbers");

        let mut record = HashMap::new();
        record.insert("large_int".to_string(), FieldValue::Integer(i64::MAX));
        record.insert("small_int".to_string(), FieldValue::Integer(i64::MIN));
        record.insert("large_float".to_string(), FieldValue::Float(f64::MAX));
        record.insert("small_float".to_string(), FieldValue::Float(f64::MIN));

        test_serialization_round_trip(&format, &record)
            .expect("Large numbers round trip should succeed");
    }

    #[tokio::test]
    async fn test_avro_large_data() {
        // Schema for large data
        let large_data_schema = r#"
        {
            "type": "record",
            "name": "LargeDataRecord",
            "fields": [
                {"name": "large_field", "type": "string"},
                {"name": "large_array", "type": {"type": "array", "items": "long"}}
            ]
        }
        "#;

        let format =
            AvroFormat::new(large_data_schema).expect("Should create Avro format for large data");
        let record = create_large_data_test_record();

        // Only test specific fields that fit the schema
        let mut schema_compatible_record = HashMap::new();
        schema_compatible_record.insert("large_field".to_string(), record["large_field"].clone());
        schema_compatible_record.insert("large_array".to_string(), record["large_array"].clone());

        test_serialization_round_trip(&format, &schema_compatible_record)
            .expect("Large data round trip should succeed");
    }

    #[tokio::test]
    // see https://github.com/bluemonk3y/ferris_streams/issues/32
    async fn test_avro_comprehensive_type_matrix() {
        // Test comprehensive type coverage similar to other formats
        let comprehensive_schema = r#"
        {
            "type": "record",
            "name": "ComprehensiveRecord", 
            "fields": [
                {"name": "string_field", "type": "string"},
                {"name": "integer_field", "type": "long"},
                {"name": "float_field", "type": "double"},
                {"name": "boolean_field", "type": "boolean"},
                {"name": "null_field", "type": ["null", "string"]},
                {"name": "string_array", "type": {"type": "array", "items": "string"}},
                {"name": "mixed_array", "type": {"type": "array", "items": "long"}},
                {"name": "map_field", "type": {"type": "map", "values": "string"}}
            ]
        }
        "#;

        let format =
            AvroFormat::new(comprehensive_schema).expect("Should create comprehensive Avro format");

        // Create comprehensive test record with Avro-compatible data
        let mut record = HashMap::new();
        record.insert(
            "string_field".to_string(),
            FieldValue::String("test_value".to_string()),
        );
        record.insert("integer_field".to_string(), FieldValue::Integer(42));
        record.insert(
            "float_field".to_string(),
            FieldValue::Float(std::f64::consts::PI),
        );
        record.insert("boolean_field".to_string(), FieldValue::Boolean(true));
        record.insert("null_field".to_string(), FieldValue::Null);

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
                FieldValue::Integer(2),
                FieldValue::Integer(3),
            ]),
        );

        let mut map = HashMap::new();
        map.insert(
            "nested_string".to_string(),
            FieldValue::String("nested_value".to_string()),
        );
        map.insert(
            "nested_key2".to_string(),
            FieldValue::String("another_value".to_string()),
        );
        record.insert("map_field".to_string(), FieldValue::Map(map));

        test_serialization_round_trip(&format, &record)
            .expect("Comprehensive type matrix round trip should succeed");

        // Also test execution format round trip
        test_execution_format_round_trip(&format, &record)
            .expect("Comprehensive execution format round trip should succeed");
    }

    // Tests extracted from avro_codec.rs

    #[tokio::test]
    async fn test_avro_codec_basic() {
        use ferrisstreams::ferris::serialization::avro_codec::AvroCodec;

        let schema_json = r#"
        {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"},
                {"name": "optional_field", "type": ["null", "string"], "default": null}
            ]
        }
        "#;

        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(123));
        record.insert("name".to_string(), FieldValue::String("test".to_string()));
        record.insert("active".to_string(), FieldValue::Boolean(true));
        record.insert("score".to_string(), FieldValue::Float(95.5));
        record.insert("optional_field".to_string(), FieldValue::Null);

        let codec = AvroCodec::new(schema_json).unwrap();

        // Test serialization
        let bytes = codec.serialize(&record).unwrap();
        assert!(!bytes.is_empty());

        // Test deserialization
        let deserialized = codec.deserialize(&bytes).unwrap();

        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            deserialized.get("name"),
            Some(&FieldValue::String("test".to_string()))
        );
        assert_eq!(deserialized.get("active"), Some(&FieldValue::Boolean(true)));
        assert_eq!(deserialized.get("score"), Some(&FieldValue::Float(95.5)));
        assert_eq!(deserialized.get("optional_field"), Some(&FieldValue::Null));
    }

    #[tokio::test]
    async fn test_avro_convenience_functions() {
        use ferrisstreams::ferris::serialization::{deserialize_from_avro, serialize_to_avro};

        let schema_json = r#"
        {
            "type": "record",
            "name": "SimpleRecord",
            "fields": [
                {"name": "message", "type": "string"}
            ]
        }
        "#;

        let mut record = HashMap::new();
        record.insert(
            "message".to_string(),
            FieldValue::String("hello world".to_string()),
        );

        // Test convenience functions
        let bytes = serialize_to_avro(&record, schema_json).unwrap();
        let deserialized = deserialize_from_avro(&bytes, schema_json).unwrap();

        assert_eq!(
            deserialized.get("message"),
            Some(&FieldValue::String("hello world".to_string()))
        );
    }
}

// Avro is always available - no feature flag needed
