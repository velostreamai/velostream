/*!
# Protobuf Serialization Tests

Tests for Protocol Buffers serialization format implementation.
*/

#[cfg(feature = "protobuf")]
mod protobuf_tests {
    use ferrisstreams::ferris::serialization::{
        ProtobufFormat, SerializationFormat, SerializationFormatFactory,
    };
    use ferrisstreams::ferris::sql::FieldValue;
    use std::collections::HashMap;

    fn create_test_record() -> HashMap<String, FieldValue> {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(123));
        record.insert(
            "name".to_string(),
            FieldValue::String("Test User".to_string()),
        );
        record.insert("active".to_string(), FieldValue::Boolean(true));
        record.insert("score".to_string(), FieldValue::Float(89.5));
        record.insert(
            "tags".to_string(),
            FieldValue::Array(vec![
                FieldValue::String("tag1".to_string()),
                FieldValue::String("tag2".to_string()),
            ]),
        );
        record
    }

    #[tokio::test]
    async fn test_protobuf_format_creation() {
        let format = ProtobufFormat::<()>::new();
        assert_eq!(format.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_serialization_round_trip() {
        let format = ProtobufFormat::<()>::new();
        let record = create_test_record();

        // Serialize to bytes
        let serialized = format
            .serialize_record(&record)
            .expect("Protobuf serialization should succeed");
        assert!(
            !serialized.is_empty(),
            "Serialized data should not be empty"
        );

        // Deserialize back to record
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Protobuf deserialization should succeed");

        // Verify fields (protobuf implementation currently uses JSON underneath for generic support)
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            deserialized.get("name"),
            Some(&FieldValue::String("Test User".to_string()))
        );
        assert_eq!(deserialized.get("active"), Some(&FieldValue::Boolean(true)));

        // Float comparison with tolerance
        if let Some(FieldValue::Float(score)) = deserialized.get("score") {
            assert!(
                (score - 89.5).abs() < 0.001,
                "Score should be approximately 89.5"
            );
        } else {
            panic!("Score field should be present and be a float");
        }
    }

    #[tokio::test]
    async fn test_protobuf_to_execution_format() {
        let format = ProtobufFormat::<()>::new();
        let record = create_test_record();

        let execution_format = format
            .to_execution_format(&record)
            .expect("Conversion to execution format should succeed");

        assert!(execution_format.contains_key("id"));
        assert!(execution_format.contains_key("name"));
        assert!(execution_format.contains_key("active"));
        assert!(execution_format.contains_key("score"));
        assert!(execution_format.contains_key("tags"));
    }

    #[tokio::test]
    async fn test_protobuf_from_execution_format() {
        use ferrisstreams::ferris::serialization::InternalValue;

        let format = ProtobufFormat::<()>::new();

        let mut execution_data = HashMap::new();
        execution_data.insert("user_id".to_string(), InternalValue::Integer(456));
        execution_data.insert(
            "username".to_string(),
            InternalValue::String("jane_doe".to_string()),
        );
        execution_data.insert("is_admin".to_string(), InternalValue::Boolean(false));
        execution_data.insert("rating".to_string(), InternalValue::Number(92.7));

        let record = format
            .from_execution_format(&execution_data)
            .expect("Conversion from execution format should succeed");

        assert_eq!(record.get("user_id"), Some(&FieldValue::Integer(456)));
        assert_eq!(
            record.get("username"),
            Some(&FieldValue::String("jane_doe".to_string()))
        );
        assert_eq!(record.get("is_admin"), Some(&FieldValue::Boolean(false)));
        assert_eq!(record.get("rating"), Some(&FieldValue::Float(92.7)));
    }

    #[tokio::test]
    async fn test_protobuf_complex_types() {
        let format = ProtobufFormat::<()>::new();

        let mut metadata = HashMap::new();
        metadata.insert("version".to_string(), FieldValue::String("1.0".to_string()));
        metadata.insert("env".to_string(), FieldValue::String("prod".to_string()));

        let mut nested_struct = HashMap::new();
        nested_struct.insert("nested_id".to_string(), FieldValue::Integer(789));
        nested_struct.insert(
            "nested_value".to_string(),
            FieldValue::String("nested".to_string()),
        );

        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(123));
        record.insert("metadata".to_string(), FieldValue::Map(metadata));
        record.insert("nested".to_string(), FieldValue::Struct(nested_struct));
        record.insert(
            "numbers".to_string(),
            FieldValue::Array(vec![
                FieldValue::Integer(1),
                FieldValue::Integer(2),
                FieldValue::Integer(3),
            ]),
        );

        let serialized = format
            .serialize_record(&record)
            .expect("Complex type serialization should succeed");
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Complex type deserialization should succeed");

        // Verify basic field
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(123)));

        // Verify complex fields exist
        assert!(
            deserialized.contains_key("metadata"),
            "Metadata field should exist"
        );
        assert!(
            deserialized.contains_key("nested"),
            "Nested field should exist"
        );
        assert!(
            deserialized.contains_key("numbers"),
            "Numbers array should exist"
        );
    }

    #[tokio::test]
    async fn test_protobuf_factory_creation() {
        let format_proto = SerializationFormatFactory::create_format("protobuf")
            .expect("Should create Protobuf format via factory");
        assert_eq!(format_proto.format_name(), "Protobuf");

        let format_proto_alias = SerializationFormatFactory::create_format("proto")
            .expect("Should create Protobuf format via factory with 'proto' alias");
        assert_eq!(format_proto_alias.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_null_handling() {
        let format = ProtobufFormat::<()>::new();

        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(123));
        record.insert("optional_field".to_string(), FieldValue::Null);
        record.insert(
            "required_field".to_string(),
            FieldValue::String("required".to_string()),
        );

        let serialized = format
            .serialize_record(&record)
            .expect("Null field serialization should succeed");
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Null field deserialization should succeed");

        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(deserialized.get("optional_field"), Some(&FieldValue::Null));
        assert_eq!(
            deserialized.get("required_field"),
            Some(&FieldValue::String("required".to_string()))
        );
    }

    #[tokio::test]
    async fn test_protobuf_empty_record() {
        let format = ProtobufFormat::<()>::new();
        let empty_record = HashMap::new();

        let serialized = format
            .serialize_record(&empty_record)
            .expect("Empty record serialization should succeed");
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Empty record deserialization should succeed");

        assert!(
            deserialized.is_empty(),
            "Deserialized record should be empty"
        );
    }

    #[tokio::test]
    async fn test_protobuf_large_data() {
        let format = ProtobufFormat::<()>::new();

        let mut record = HashMap::new();

        // Create large string data
        let large_string = "x".repeat(10000);
        record.insert(
            "large_field".to_string(),
            FieldValue::String(large_string.clone()),
        );

        // Create large array
        let large_array: Vec<FieldValue> = (0..1000).map(|i| FieldValue::Integer(i)).collect();
        record.insert("large_array".to_string(), FieldValue::Array(large_array));

        let serialized = format
            .serialize_record(&record)
            .expect("Large data serialization should succeed");
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Large data deserialization should succeed");

        assert_eq!(
            deserialized.get("large_field"),
            Some(&FieldValue::String(large_string))
        );

        if let Some(FieldValue::Array(arr)) = deserialized.get("large_array") {
            assert_eq!(arr.len(), 1000, "Array should have 1000 elements");
        } else {
            panic!("Large array should be present");
        }
    }

    #[tokio::test]
    async fn test_protobuf_error_handling() {
        let format = ProtobufFormat::<()>::new();

        // Test with invalid binary data (non-JSON for our generic implementation)
        let invalid_data = b"invalid protobuf data that is not json";
        let result = format.deserialize_record(invalid_data);
        assert!(
            result.is_err(),
            "Invalid protobuf data should cause deserialization error"
        );

        // Test with malformed JSON (since our generic implementation uses JSON)
        let malformed_json = b"{ malformed json";
        let result = format.deserialize_record(malformed_json);
        assert!(
            result.is_err(),
            "Malformed JSON should cause deserialization error"
        );
    }

    #[tokio::test]
    async fn test_protobuf_type_specific_creation() {
        // Test creating protobuf format with specific type
        let format = SerializationFormatFactory::create_protobuf_format::<()>();
        assert_eq!(format.format_name(), "Protobuf");
    }
}

// If Protobuf feature is not enabled, provide placeholder tests
#[cfg(not(feature = "protobuf"))]
mod protobuf_tests {
    #[tokio::test]
    async fn test_protobuf_feature_not_enabled() {
        // This test just verifies that the feature flag is working correctly
        assert!(true, "Protobuf feature is not enabled, tests are skipped");
    }
}
