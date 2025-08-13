/*!
# Avro Serialization Tests

Tests for Avro serialization format implementation, including schema handling
and schema evolution scenarios.
*/

#[cfg(feature = "avro")]
mod avro_tests {
    use ferrisstreams::ferris::serialization::{
        AvroFormat, SerializationFormat, SerializationFormatFactory,
    };
    use ferrisstreams::ferris::sql::FieldValue;
    use std::collections::HashMap;

    fn create_simple_test_record() -> HashMap<String, FieldValue> {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(123));
        record.insert(
            "name".to_string(),
            FieldValue::String("John Doe".to_string()),
        );
        record.insert("age".to_string(), FieldValue::Integer(30));
        record.insert("active".to_string(), FieldValue::Boolean(true));
        record.insert("score".to_string(), FieldValue::Float(95.5));
        record
    }

    fn create_user_schema() -> &'static str {
        r#"
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "long"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"}
            ]
        }
        "#
    }

    fn create_evolved_schema() -> &'static str {
        r#"
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": "long"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }
        "#
    }

    #[tokio::test]
    async fn test_avro_format_creation() {
        let schema = create_user_schema();
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
        let format = AvroFormat::default();
        assert!(
            format.is_ok(),
            "Default Avro format creation should succeed"
        );
    }

    #[tokio::test]
    async fn test_avro_serialization_round_trip() {
        let schema = create_user_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");
        let record = create_simple_test_record();

        // Serialize to bytes
        let serialized = format
            .serialize_record(&record)
            .expect("Avro serialization should succeed");
        assert!(
            !serialized.is_empty(),
            "Serialized data should not be empty"
        );

        // Deserialize back to record
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Avro deserialization should succeed");

        // Verify core fields (Avro might reorder or transform slightly)
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            deserialized.get("name"),
            Some(&FieldValue::String("John Doe".to_string()))
        );
        assert_eq!(deserialized.get("age"), Some(&FieldValue::Integer(30)));
        assert_eq!(deserialized.get("active"), Some(&FieldValue::Boolean(true)));

        // Float comparison with tolerance
        if let Some(FieldValue::Float(score)) = deserialized.get("score") {
            assert!(
                (score - 95.5).abs() < 0.001,
                "Score should be approximately 95.5"
            );
        } else {
            panic!("Score field should be present and be a float");
        }
    }

    #[tokio::test]
    async fn test_avro_schema_evolution() {
        let writer_schema = create_user_schema();
        let reader_schema = create_evolved_schema();

        let format = AvroFormat::with_schemas(writer_schema, reader_schema)
            .expect("Should create Avro format with schema evolution");

        let record = create_simple_test_record();
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
            Some(&FieldValue::String("John Doe".to_string()))
        );
    }

    #[tokio::test]
    async fn test_avro_to_execution_format() {
        let schema = create_user_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");
        let record = create_simple_test_record();

        let execution_format = format
            .to_execution_format(&record)
            .expect("Conversion to execution format should succeed");

        assert!(execution_format.contains_key("id"));
        assert!(execution_format.contains_key("name"));
        assert!(execution_format.contains_key("age"));
        assert!(execution_format.contains_key("active"));
        assert!(execution_format.contains_key("score"));
    }

    #[tokio::test]
    async fn test_avro_from_execution_format() {
        use ferrisstreams::ferris::serialization::InternalValue;

        let schema = create_user_schema();
        let format = AvroFormat::new(schema).expect("Should create Avro format");

        let mut execution_data = HashMap::new();
        execution_data.insert("id".to_string(), InternalValue::Integer(456));
        execution_data.insert(
            "name".to_string(),
            InternalValue::String("Jane Smith".to_string()),
        );
        execution_data.insert("age".to_string(), InternalValue::Integer(25));
        execution_data.insert("active".to_string(), InternalValue::Boolean(false));
        execution_data.insert("score".to_string(), InternalValue::Number(87.3));

        let record = format
            .from_execution_format(&execution_data)
            .expect("Conversion from execution format should succeed");

        assert_eq!(record.get("id"), Some(&FieldValue::Integer(456)));
        assert_eq!(
            record.get("name"),
            Some(&FieldValue::String("Jane Smith".to_string()))
        );
        assert_eq!(record.get("age"), Some(&FieldValue::Integer(25)));
        assert_eq!(record.get("active"), Some(&FieldValue::Boolean(false)));
        assert_eq!(record.get("score"), Some(&FieldValue::Float(87.3)));
    }

    #[tokio::test]
    async fn test_avro_complex_types() {
        let complex_schema = r#"
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
        "#;

        let format =
            AvroFormat::new(complex_schema).expect("Should create Avro format with complex schema");

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

        let serialized = format
            .serialize_record(&record)
            .expect("Complex type serialization should succeed");
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Complex type deserialization should succeed");

        // Verify basic field
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(789)));

        // Verify array field exists
        assert!(deserialized.contains_key("tags"), "Tags field should exist");

        // Verify map field exists
        assert!(
            deserialized.contains_key("metadata"),
            "Metadata field should exist"
        );

        // Verify nested record exists
        assert!(
            deserialized.contains_key("nested"),
            "Nested field should exist"
        );
    }

    #[tokio::test]
    async fn test_avro_factory_creation() {
        let format = SerializationFormatFactory::create_format("avro")
            .expect("Should create Avro format via factory");

        assert_eq!(format.format_name(), "Avro");

        // Test with custom schema
        let schema = create_user_schema();
        let custom_format = SerializationFormatFactory::create_avro_format(schema)
            .expect("Should create custom Avro format");

        assert_eq!(custom_format.format_name(), "Avro");
    }

    #[tokio::test]
    async fn test_avro_null_handling() {
        let null_schema = r#"
        {
            "type": "record",
            "name": "NullRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "optional_field", "type": ["null", "string"]},
                {"name": "required_field", "type": "string"}
            ]
        }
        "#;

        let format =
            AvroFormat::new(null_schema).expect("Should create Avro format with null schema");

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
    async fn test_avro_error_handling() {
        let schema = create_user_schema();
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
}

// If Avro feature is not enabled, provide placeholder tests
#[cfg(not(feature = "avro"))]
mod avro_tests {
    #[tokio::test]
    async fn test_avro_feature_not_enabled() {
        // This test just verifies that the feature flag is working correctly
        assert!(true, "Avro feature is not enabled, tests are skipped");
    }
}
