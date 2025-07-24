use crate::ferris::kafka::common::*;

#[test]
fn test_json_serializer_basic() {
    let user = TestUser {
        id: 123,
        name: "Alice Johnson".to_string(),
        email: Some("alice@example.com".to_string()),
    };

    let serializer = JsonSerializer;
    
    let bytes = serializer.serialize(&user).expect("Failed to serialize");
    assert!(!bytes.is_empty());

    let deserialized: TestUser = serializer.deserialize(&bytes).expect("Failed to deserialize");
    assert_eq!(user, deserialized);
}

#[test]
fn test_json_serializer_optional_fields() {
    let user = TestUser {
        id: 456,
        name: "Bob Smith".to_string(),
        email: None,
    };

    let serializer = JsonSerializer;
    
    let bytes = serializer.serialize(&user).expect("Failed to serialize");
    let deserialized: TestUser = serializer.deserialize(&bytes).expect("Failed to deserialize");
    
    assert_eq!(user, deserialized);
    assert_eq!(deserialized.email, None);
}

#[test]
fn test_json_serializer_different_types() {
    let serializer = JsonSerializer;

    // String
    let string_data = "test string".to_string();
    let bytes = serializer.serialize(&string_data).expect("Failed to serialize string");
    let deserialized: String = serializer.deserialize(&bytes).expect("Failed to deserialize string");
    assert_eq!(string_data, deserialized);

    // Number
    let number_data = 42u32;
    let bytes = serializer.serialize(&number_data).expect("Failed to serialize number");
    let deserialized: u32 = serializer.deserialize(&bytes).expect("Failed to deserialize number");
    assert_eq!(number_data, deserialized);

    // Vector
    let vec_data = vec![1, 2, 3, 4, 5];
    let bytes = serializer.serialize(&vec_data).expect("Failed to serialize vec");
    let deserialized: Vec<i32> = serializer.deserialize(&bytes).expect("Failed to deserialize vec");
    assert_eq!(vec_data, deserialized);
}

#[test]
fn test_json_serializer_nested_structs() {
    let nested = NestedStruct {
        outer: "outer value".to_string(),
        inner: InnerStruct {
            value: 100,
            flag: true,
        },
        numbers: vec![1, 2, 3, 4, 5],
    };

    let serializer = JsonSerializer;
    
    let bytes = serializer.serialize(&nested).expect("Failed to serialize nested");
    let deserialized: NestedStruct = serializer.deserialize(&bytes).expect("Failed to deserialize nested");
    
    assert_eq!(nested, deserialized);
    assert_eq!(deserialized.inner.value, 100);
    assert_eq!(deserialized.inner.flag, true);
}

#[test]
fn test_json_serializer_error_cases() {
    let serializer = JsonSerializer;
    
    // Invalid JSON
    let invalid_json = b"{ invalid json }";
    let result: Result<TestUser, _> = serializer.deserialize(invalid_json);
    assert!(result.is_err());

    // Empty input
    let empty_input = b"";
    let result: Result<TestUser, _> = serializer.deserialize(empty_input);
    assert!(result.is_err());
}

// Feature-gated serializer tests

#[cfg(feature = "protobuf")]
mod protobuf_tests {
    use super::*;
    use ferrisstreams::ferris::kafka::ProtoSerializer;
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct TestProtoUser {
        #[prost(uint64, tag = "1")]
        pub id: u64,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(string, optional, tag = "3")]
        pub email: Option<String>,
    }

    #[test]
    fn test_proto_serializer() {
        let user = TestProtoUser {
            id: 789,
            name: "Charlie Brown".to_string(),
            email: Some("charlie@example.com".to_string()),
        };

        let serializer = ProtoSerializer::<TestProtoUser>::new();
        
        let bytes = serializer.serialize(&user).expect("Failed to serialize proto");
        let deserialized: TestProtoUser = serializer.deserialize(&bytes).expect("Failed to deserialize proto");
        
        assert_eq!(user, deserialized);
    }
}

#[cfg(feature = "avro")]
mod avro_tests {
    use super::*;
    use ferrisstreams::ferris::kafka::AvroSerializer;
    use apache_avro::{Schema as AvroSchema, types::Value as AvroValue};

    #[test]
    fn test_avro_serializer() {
        let schema_str = r#"
        {
            "type": "record",
            "name": "TestUser",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        }
        "#;

        let schema = AvroSchema::parse_str(schema_str).expect("Failed to parse schema");
        let serializer = AvroSerializer::new(schema);

        let user_value = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(999)),
            ("name".to_string(), AvroValue::String("Dave Wilson".to_string())),
            ("email".to_string(), AvroValue::Union(1, Box::new(AvroValue::String("dave@example.com".to_string())))),
        ]);

        let bytes = serializer.serialize(&user_value).expect("Failed to serialize avro");
        let deserialized: AvroValue = serializer.deserialize(&bytes).expect("Failed to deserialize avro");
        
        assert_eq!(user_value, deserialized);
    }
}