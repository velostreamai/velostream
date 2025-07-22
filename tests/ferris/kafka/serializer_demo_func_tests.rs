#[cfg(test)]
mod serializer_demo_tests {
    use std::time::Duration;
    use serde::{Serialize, Deserialize};
    use serial_test::serial;

    use ferrisstreams::ferris::kafka::{
        KafkaProducer, KafkaConsumer,
        KafkaSerialize, SerializationError,
        JsonSerializer, Serializer
    };

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct User {
        id: u64,
        name: String,
        email: Option<String>,
    }

    #[tokio::test]
    #[serial]
    async fn demo_json_serializer() {
        // This test demonstrates how to use JsonSerializer with Kafka

        // Create a producer and consumer
        let broker = "localhost:9092";
        let topic = "json-serializer-demo";

        let producer = KafkaProducer::<ferrisstreams::ferris::kafka::LoggingProducerContext>::new(broker, topic).expect("Failed to create producer");
        let consumer = KafkaConsumer::new(broker, "demo-group");
        consumer.subscribe(&[topic]);

        // Create a serializer
        let json_serializer = JsonSerializer;

        // Create test data
        let user = User {
            id: 1,
            name: "Alice Smith".to_string(),
            email: Some("alice@example.com".to_string()),
        };

        // Send message using the serializer
        producer.send_with_serializer(
            Some("user-1"),
            &user,
            &json_serializer,
            None
        ).await.expect("Failed to send message");

        // Consume message using the serializer
        let (received_user, key) = consumer.poll_with_serializer(
            Duration::from_secs(5),
            &json_serializer
        ).await.expect("Failed to receive message");

        // Verify the message
        assert_eq!(user, received_user);
        assert_eq!(key.map(|k| String::from_utf8(k).unwrap()), Some("user-1".to_string()));

        println!("Successfully demonstrated JSON serialization");
    }

    #[cfg(feature = "protobuf")]
    mod protobuf_tests {
        use super::*;
        use prost::Message;
        use serial_test::serial;
        // Use public re-export instead of private module
        #[cfg(feature = "protobuf")]
        use ferrisstreams::ferris::kafka::ProtoSerializer;

        #[derive(Clone, PartialEq, Message)]
        pub struct ProtoUser {
            #[prost(uint64, tag = "1")]
            pub id: u64,
            #[prost(string, tag = "2")]
            pub name: String,
            #[prost(string, optional, tag = "3")]
            pub email: Option<String>,
        }

        #[tokio::test]
        #[serial]
        async fn demo_proto_serializer() {
            // This test demonstrates how to use ProtoSerializer with Kafka

            // Create a producer and consumer
            let broker = "localhost:9092";
            let topic = "proto-serializer-demo";

            let producer = KafkaProducer::<ferrisstreams::ferris::kafka::LoggingProducerContext>::new(broker, topic).expect("Failed to create producer");
            let consumer = KafkaConsumer::new(broker, "demo-group");
            consumer.subscribe(&[topic]);

            // Create a serializer
            let proto_serializer = ProtoSerializer::<ProtoUser>::new();

            // Create test data
            let user = ProtoUser {
                id: 2,
                name: "Bob Johnson".to_string(),
                email: Some("bob@example.com".to_string()),
            };

            // Send message using the serializer
            producer.send_with_serializer(
                Some("user-2"),
                &user,
                &proto_serializer,
                None
            ).await.expect("Failed to send message");

            // Consume message using the serializer
            let (received_user, key) = consumer.poll_with_serializer(
                Duration::from_secs(5),
                &proto_serializer
            ).await.expect("Failed to receive message");

            // Verify the message
            assert_eq!(user, received_user);
            assert_eq!(key.map(|k| String::from_utf8(k).unwrap()), Some("user-2".to_string()));

            println!("Successfully demonstrated Protocol Buffers serialization");
        }
    }

    #[cfg(feature = "avro")]
    mod avro_tests {
        use super::*;
        use serial_test::serial;
        use apache_avro::{
            types::Value as AvroValue,
            Schema as AvroSchema,
        };
        // Use public re-export instead of private module
        #[cfg(feature = "avro")]
        use ferrisstreams::ferris::kafka::AvroSerializer;

        #[tokio::test]
        #[serial]
        async fn demo_avro_serializer() {
            // This test demonstrates how to use AvroSerializer with Kafka

            // Create a producer and consumer
            let broker = "localhost:9092";
            let topic = "avro-serializer-demo";

            let producer = KafkaProducer::<ferrisstreams::ferris::kafka::LoggingProducerContext>::new(broker, topic).expect("Failed to create producer");
            let consumer = KafkaConsumer::new(broker, "demo-group");
            consumer.subscribe(&[topic]);

            // Create Avro schema
            let schema_str = r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": ["null", "string"]}
                ]
            }"#;

            let schema = AvroSchema::parse_str(schema_str).expect("Failed to parse Avro schema");

            // Create a serializer
            let avro_serializer = AvroSerializer::new(schema.clone());

            // Create test data as Avro record
            let user = AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Long(3)),
                ("name".to_string(), AvroValue::String("Charlie Davis".to_string())),
                ("email".to_string(), AvroValue::Union(
                    1, Box::new(AvroValue::String("charlie@example.com".to_string()))
                ))
            ]);

            // Send message using the serializer
            producer.send_with_serializer(
                Some("user-3"),
                &user,
                &avro_serializer,
                None
            ).await.expect("Failed to send message");

            // Consume message using the serializer
            let (received_user, key) = consumer.poll_with_serializer(
                Duration::from_secs(5),
                &avro_serializer
            ).await.expect("Failed to receive message");

            // Helper function to extract values from Avro records
            fn get_field_value<'a>(record: &'a AvroValue, field_name: &str) -> &'a AvroValue {
                if let AvroValue::Record(fields) = record {
                    for (name, value) in fields {
                        if name == field_name {
                            return value;
                        }
                    }
                }
                panic!("Field not found: {}", field_name);
            }

            // Verify the message
            assert_eq!(get_field_value(&user, "id"), get_field_value(&received_user, "id"));
            assert_eq!(get_field_value(&user, "name"), get_field_value(&received_user, "name"));
            assert_eq!(key.map(|k| String::from_utf8(k).unwrap()), Some("user-3".to_string()));

            println!("Successfully demonstrated Avro serialization");
        }
    }

    // Custom serializer example
    mod custom_serializer_tests {
        use super::*;
        use serial_test::serial;
        // Using the public re-exports instead of private module
        use ferrisstreams::ferris::kafka::{SerializationError, Serializer};

        // A very simple custom serializer that uses CSV format
        struct CsvSerializer;

        impl Serializer<User> for CsvSerializer {
            fn serialize(&self, value: &User) -> Result<Vec<u8>, SerializationError> {
                let email = value.email.as_deref().unwrap_or("");
                let csv = format!("{},{},{}", value.id, value.name, email);
                Ok(csv.into_bytes())
            }

            fn deserialize(&self, bytes: &[u8]) -> Result<User, SerializationError> {
                let csv = String::from_utf8(bytes.to_vec())
                    .map_err(|e| SerializationError::Schema(e.to_string()))?;

                let parts: Vec<&str> = csv.split(',').collect();
                if parts.len() != 3 {
                    return Err(SerializationError::Schema(
                        "CSV must have exactly 3 parts".to_string()
                    ));
                }

                let id = parts[0].parse::<u64>()
                    .map_err(|e| SerializationError::Schema(e.to_string()))?;

                let name = parts[1].to_string();

                let email = if parts[2].is_empty() {
                    None
                } else {
                    Some(parts[2].to_string())
                };

                Ok(User { id, name, email })
            }
        }

        #[tokio::test]
        #[serial]
        async fn demo_custom_serializer() {
            // This test demonstrates how to use a custom serializer with Kafka

            // Create a producer and consumer
            let broker = "localhost:9092";
            let topic = "custom-serializer-demo";

            let producer = KafkaProducer::<ferrisstreams::ferris::kafka::LoggingProducerContext>::new(broker, topic).expect("Failed to create producer");
            let consumer = KafkaConsumer::new(broker, "demo-group");
            consumer.subscribe(&[topic]);

            // Create a serializer
            let csv_serializer = CsvSerializer;

            // Create test data
            let user = User {
                id: 4,
                name: "David Wilson".to_string(),
                email: Some("david@example.com".to_string()),
            };

            // Send message using the serializer
            producer.send_with_serializer(
                Some("user-4"),
                &user,
                &csv_serializer,
                None
            ).await.expect("Failed to send message");

            // Consume message using the serializer
            let (received_user, key) = consumer.poll_with_serializer(
                Duration::from_secs(5),
                &csv_serializer
            ).await.expect("Failed to receive message");

            // Verify the message
            assert_eq!(user, received_user);
            assert_eq!(key.map(|k| String::from_utf8(k).unwrap()), Some("user-4".to_string()));

            println!("Successfully demonstrated custom CSV serialization");
        }
    }
}
