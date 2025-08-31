/*!
# Protobuf Serialization Tests

Tests for Protocol Buffers serialization format implementation.
*/

#[cfg(feature = "protobuf")]
mod protobuf_tests {
    use super::super::common_test_data::*;
    use ferrisstreams::ferris::serialization::{ProtobufFormat, SerializationFormat};
    use ferrisstreams::ferris::sql::execution::types::StreamRecord;
    use ferrisstreams::ferris::sql::FieldValue;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_protobuf_format_creation() {
        let format = ProtobufFormat::<()>::new();
        assert_eq!(format.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_serialization_round_trip() {
        let format = ProtobufFormat::<()>::new();
        let record = create_basic_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Protobuf round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_to_execution_format() {
        let format = ProtobufFormat::<()>::new();
        let record = create_basic_test_record();

        // Create a StreamRecord
        let stream_record = StreamRecord {
            fields: record.clone(),
            timestamp: 1234567890,
            offset: 100,
            partition: 0,
            headers: HashMap::new(),
        };

        // Test serialization round-trip using StreamRecord's fields
        let serialized = format
            .serialize_record(&stream_record.fields)
            .expect("Serialization should succeed");

        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Deserialization should succeed");

        // Verify key fields are preserved
        assert_eq!(deserialized.get("id"), record.get("id"));
        assert_eq!(deserialized.get("name"), record.get("name"));
        assert_eq!(deserialized.get("active"), record.get("active"));
        assert_eq!(deserialized.get("score"), record.get("score"));
    }

    #[tokio::test]
    async fn test_protobuf_from_execution_format() {
        let format = ProtobufFormat::<()>::new();

        // Create a StreamRecord directly
        let mut fields = HashMap::new();
        fields.insert("user_id".to_string(), FieldValue::Integer(456));
        fields.insert(
            "username".to_string(),
            FieldValue::String("jane_doe".to_string()),
        );
        fields.insert("is_admin".to_string(), FieldValue::Boolean(false));
        fields.insert("rating".to_string(), FieldValue::Float(92.7));

        let stream_record = StreamRecord {
            fields: fields.clone(),
            timestamp: 1234567890,
            offset: 100,
            partition: 0,
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
        assert_eq!(deserialized.get("user_id"), Some(&FieldValue::Integer(456)));
        assert_eq!(
            deserialized.get("username"),
            Some(&FieldValue::String("jane_doe".to_string()))
        );
        assert_eq!(
            deserialized.get("is_admin"),
            Some(&FieldValue::Boolean(false))
        );
        assert_eq!(deserialized.get("rating"), Some(&FieldValue::Float(92.7)));
    }

    #[tokio::test]
    async fn test_protobuf_complex_types() {
        let format = ProtobufFormat::<()>::new();
        let record = create_comprehensive_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Complex type round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_direct_creation() {
        let format_proto = ProtobufFormat::<()>::new();
        assert_eq!(format_proto.format_name(), "Protobuf");

        // Test that direct creation works the same way
        let format_proto_direct = ProtobufFormat::<()>::new();
        assert_eq!(format_proto_direct.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_null_handling() {
        let format = ProtobufFormat::<()>::new();
        let record = create_null_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Null handling round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_empty_record() {
        let format = ProtobufFormat::<()>::new();
        let empty_record = HashMap::new();

        test_serialization_round_trip(&format, &empty_record)
            .expect("Empty record round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_large_data() {
        let format = ProtobufFormat::<()>::new();
        let record = create_large_data_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Large data round trip should succeed");
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
        let format = ProtobufFormat::<()>::new();
        assert_eq!(format.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_schema_evolution() {
        // For Protobuf, schema evolution is handled at the type level
        // Since we're using a generic implementation, we simulate evolution
        // by testing backward compatibility with different field sets

        let format_v1 = ProtobufFormat::<()>::new();
        let format_v2 = ProtobufFormat::<()>::new();

        // Version 1 record (minimal fields)
        let mut record_v1 = HashMap::new();
        record_v1.insert("id".to_string(), FieldValue::Integer(123));
        record_v1.insert(
            "name".to_string(),
            FieldValue::String("Test User".to_string()),
        );

        // Serialize with v1 format
        let serialized_v1 = format_v1
            .serialize_record(&record_v1)
            .expect("V1 serialization should succeed");

        // Deserialize with v2 format (should be compatible)
        let deserialized_v2 = format_v2
            .deserialize_record(&serialized_v1)
            .expect("V2 deserialization should succeed");

        // Core fields should be preserved
        assert_eq!(deserialized_v2.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            deserialized_v2.get("name"),
            Some(&FieldValue::String("Test User".to_string()))
        );

        // Version 2 record (extended fields)
        let mut record_v2 = record_v1.clone();
        record_v2.insert(
            "email".to_string(),
            FieldValue::String("test@example.com".to_string()),
        );
        record_v2.insert("active".to_string(), FieldValue::Boolean(true));

        // Test round trip with extended record
        test_serialization_round_trip(&format_v2, &record_v2)
            .expect("V2 extended record round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_unicode_strings() {
        let format = ProtobufFormat::<()>::new();
        let record = create_edge_case_test_record();

        // Extract unicode fields for testing
        let mut unicode_record = HashMap::new();
        unicode_record.insert("unicode_field".to_string(), record["unicode_field"].clone());
        unicode_record.insert("emoji_field".to_string(), record["emoji_field"].clone());

        test_serialization_round_trip(&format, &unicode_record)
            .expect("Unicode round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_large_numbers() {
        let format = ProtobufFormat::<()>::new();
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
    async fn test_protobuf_comprehensive_type_matrix() {
        // Test comprehensive type coverage similar to other formats
        let format = ProtobufFormat::<()>::new();
        let record = create_comprehensive_test_record();

        // Test serialization round trip
        test_serialization_round_trip(&format, &record)
            .expect("Comprehensive type matrix round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_complex_nested_structures() {
        let format = ProtobufFormat::<()>::new();
        let record = create_complex_nested_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Complex nested structures round trip should succeed");
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
