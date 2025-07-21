#[cfg(test)]
mod serialization_tests {
    use ferrisstreams::ferris::kafka::{
        KafkaSerialize,
        to_json, from_json, to_proto, from_proto, to_avro, from_avro,
        JsonSerializer, Serializer
    };
    // Conditionally import ProtoSerializer and AvroSerializer
    #[cfg(feature = "protobuf")]
    use ferrisstreams::ferris::kafka::ProtoSerializer;
    #[cfg(feature = "avro")]
    use ferrisstreams::ferris::kafka::AvroSerializer;

    use serde::{Serialize, Deserialize};
    use prost::{Message};
    use apache_avro::{Schema as AvroSchema, types::Value as AvroValue};

    // Test struct for JSON serialization
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestUser {
        id: u64,
        name: String,
        email: Option<String>,
    }

    // Define a simple protobuf message
    #[derive(Clone, PartialEq, Message)]
    pub struct TestProtoUser {
        #[prost(uint64, tag = "1")]
        pub id: u64,
        #[prost(string, tag = "2")]
        pub name: String,
        #[prost(string, optional, tag = "3")]
        pub email: Option<String>,
    }

    #[tokio::test]
    async fn test_json_serialization() {
        let user = TestUser {
            id: 1,
            name: "John Doe".to_string(),
            email: Some("john@example.com".to_string()),
        };

        // Test KafkaSerialize trait
        let bytes = user.to_bytes().unwrap();
        
        // Test direct functions
        let bytes2 = to_json(&user).unwrap();
        assert_eq!(bytes, bytes2);
        
        // Test deserialization
        let deserialized: TestUser = from_json(&bytes).unwrap();
        assert_eq!(user, deserialized);
    }

    #[tokio::test]
    async fn test_proto_serialization() {
        let user = TestProtoUser {
            id: 1,
            name: "John Doe".to_string(),
            email: Some("john@example.com".to_string()),
        };

        // Test serialization
        let bytes = to_proto(&user).unwrap();
        
        // Test deserialization
        let deserialized: TestProtoUser = from_proto(&bytes).unwrap();
        assert_eq!(user, deserialized);
    }

    #[tokio::test]
    async fn test_avro_serialization() {
        // Define Avro schema
        let schema_str = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }"#;
        
        let schema = AvroSchema::parse_str(schema_str).unwrap();
        
        // Create Avro record
        let record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            ("name".to_string(), AvroValue::String("John Doe".to_string())),
            ("email".to_string(), AvroValue::Union(
                1, Box::new(AvroValue::String("john@example.com".to_string()))
            ))
        ]);
        
        // Test serialization
        let bytes = to_avro(&record, &schema).unwrap();
        
        // Test deserialization
        let deserialized = from_avro(&bytes, &schema).unwrap();
        
        // For Avro, we need to manually check fields as direct comparison doesn't work well
        if let AvroValue::Record(fields) = deserialized {
            assert_eq!(fields.len(), 3);
            if let AvroValue::Long(id) = &fields[0].1 {
                assert_eq!(*id, 1);
            } else {
                panic!("Expected Long value for id");
            }
            if let AvroValue::String(name) = &fields[1].1 {
                assert_eq!(name, "John Doe");
            } else {
                panic!("Expected String value for name");
            }
        } else {
            panic!("Expected Record");
        }
    }

    #[tokio::test]
    async fn demo_test_serializers() {
        // JSON Serializer demo
        let json_serializer = JsonSerializer;
        let user = TestUser {
            id: 1,
            name: "John Doe".to_string(),
            email: Some("john@example.com".to_string()),
        };

        // Serialize with the JSON serializer
        let json_bytes = json_serializer.serialize(&user).unwrap();

        // Deserialize with the JSON serializer
        let json_deserialized: TestUser = json_serializer.deserialize(&json_bytes).unwrap();
        assert_eq!(user, json_deserialized);

        // Protocol Buffers Serializer demo
        let proto_serializer = ProtoSerializer::<TestProtoUser>::new();
        let proto_user = TestProtoUser {
            id: 1,
            name: "John Doe".to_string(),
            email: Some("john@example.com".to_string()),
        };

        // Serialize with the Protocol Buffers serializer
        let proto_bytes = proto_serializer.serialize(&proto_user).unwrap();

        // Deserialize with the Protocol Buffers serializer
        let proto_deserialized = proto_serializer.deserialize(&proto_bytes).unwrap();
        assert_eq!(proto_user, proto_deserialized);

        // Avro Serializer demo
        let schema_str = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]}
            ]
        }"#;

        let schema = AvroSchema::parse_str(schema_str).unwrap();
        let avro_serializer = AvroSerializer::new(schema);

        let avro_record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Long(1)),
            ("name".to_string(), AvroValue::String("John Doe".to_string())),
            ("email".to_string(), AvroValue::Union(
                1, Box::new(AvroValue::String("john@example.com".to_string()))
            ))
        ]);

        // Serialize with the Avro serializer
        let avro_bytes = avro_serializer.serialize(&avro_record).unwrap();

        // Deserialize with the Avro serializer
        let avro_deserialized = avro_serializer.deserialize(&avro_bytes).unwrap();

        // Verify the Avro deserialization worked
        if let AvroValue::Record(fields) = avro_deserialized {
            assert_eq!(fields.len(), 3);
            if let AvroValue::Long(id) = &fields[0].1 {
                assert_eq!(*id, 1);
            } else {
                panic!("Expected Long value for id");
            }
        } else {
            panic!("Expected Record");
        }

        // Demo using serializer with KafkaProducer
        // In a real scenario, you would do:
        //
        // let producer = KafkaProducer::new("localhost:9092", "my-topic").unwrap();
        // producer.send_with_serializer(None, &user, &json_serializer, None).await.unwrap();
        // producer.send_with_serializer(None, &proto_user, &proto_serializer, None).await.unwrap();
        // producer.send_with_serializer(None, &avro_record, &avro_serializer, None).await.unwrap();
    }
}
