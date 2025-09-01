//! Tests for Phase 2 Configurable Kafka Consumer functionality
//!
//! These tests validate the ConfigurableKafkaConsumerBuilder and related
//! functionality for runtime serialization format selection.

use ferrisstreams::ferris::kafka::{
    configurable_consumer::ConfigurableKafkaConsumerBuilder,
    serialization_format::{SerializationConfig, SerializationFormat},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestMessage {
    id: u32,
    content: String,
    timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    amount: f64,
    status: String,
}

#[cfg(test)]
mod configurable_consumer_tests {
    use super::*;

    #[test]
    fn test_configurable_consumer_builder_creation() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        );

        // Verify default formats
        assert_eq!(*builder.key_format(), SerializationFormat::Json);
        assert_eq!(*builder.value_format(), SerializationFormat::Json);
    }

    #[test]
    fn test_configurable_consumer_builder_with_formats() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        )
        .with_key_format(SerializationFormat::String)
        .with_value_format(SerializationFormat::Json);

        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
    }

    #[test]
    fn test_configurable_consumer_builder_fluent_api() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, OrderEvent>::new(
            "kafka-cluster:9092",
            "order-processing-group",
        )
        .with_key_format(SerializationFormat::String)
        .with_value_format(SerializationFormat::Json);

        // Test that the fluent API maintains all settings
        assert_eq!(builder.brokers, "kafka-cluster:9092");
        assert_eq!(builder.group_id, "order-processing-group");
        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
    }

    #[test]
    fn test_configurable_consumer_builder_from_sql_config_basic() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "json".to_string());

        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::from_sql_config(
            "localhost:9092",
            "test-group",
            &sql_params,
        )
        .expect("Failed to create builder from SQL config");

        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
        assert!(builder.serialization_config.is_some());
    }

    #[test]
    fn test_configurable_consumer_builder_from_sql_config_complex() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "bytes".to_string());
        sql_params.insert("value.serializer".to_string(), "string".to_string());
        sql_params.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        sql_params.insert("auto.offset.reset".to_string(), "earliest".to_string());
        sql_params.insert("enable.auto.commit".to_string(), "false".to_string());

        let builder = ConfigurableKafkaConsumerBuilder::<Vec<u8>, String>::from_sql_config(
            "localhost:9092",
            "complex-consumer-group",
            &sql_params,
        )
        .expect("Failed to create builder from SQL config");

        assert_eq!(builder.key_format, SerializationFormat::Bytes);
        assert_eq!(builder.value_format, SerializationFormat::String);

        // Verify configuration is captured
        let config = builder.serialization_config.unwrap();
        assert_eq!(
            config.custom_properties.get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
        assert_eq!(
            config.custom_properties.get("auto.offset.reset"),
            Some(&"earliest".to_string())
        );
        assert_eq!(
            config.custom_properties.get("enable.auto.commit"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn test_configurable_consumer_builder_from_sql_config_invalid() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "invalid_format".to_string());

        let result = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::from_sql_config(
            "localhost:9092",
            "test-group",
            &sql_params,
        );

        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Unsupported serialization format"));
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_configurable_consumer_builder_avro_key_serialization() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        )
        .with_avro_key_serialization("http://localhost:8081", "orders-key");

        if let SerializationFormat::Avro {
            schema_registry_url,
            subject,
        } = &builder.key_format
        {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "orders-key");
        } else {
            panic!("Expected Avro serialization format for key");
        }
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_configurable_consumer_builder_avro_value_serialization() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        )
        .with_avro_value_serialization("http://schema-registry:8081", "orders-value");

        if let SerializationFormat::Avro {
            schema_registry_url,
            subject,
        } = &builder.value_format
        {
            assert_eq!(schema_registry_url, "http://schema-registry:8081");
            assert_eq!(subject, "orders-value");
        } else {
            panic!("Expected Avro serialization format for value");
        }
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn test_configurable_consumer_builder_protobuf_serialization() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        )
        .with_protobuf_key_serialization("com.example.OrderKey")
        .with_protobuf_value_serialization("com.example.OrderValue");

        if let SerializationFormat::Protobuf { message_type } = &builder.key_format {
            assert_eq!(message_type, "com.example.OrderKey");
        } else {
            panic!("Expected Protobuf serialization format for key");
        }

        if let SerializationFormat::Protobuf { message_type } = &builder.value_format {
            assert_eq!(message_type, "com.example.OrderValue");
        } else {
            panic!("Expected Protobuf serialization format for value");
        }
    }

    #[test]
    fn test_configurable_consumer_builder_serialization_config() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "json".to_string());

        let serialization_config = SerializationConfig::from_sql_params(&sql_params).unwrap();

        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        )
        .with_serialization_config(serialization_config);

        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
        assert!(builder.serialization_config.is_some());
    }

    #[test]
    fn test_configurable_consumer_builder_method_chaining() {
        // Test that all builder methods can be chained together
        let builder = ConfigurableKafkaConsumerBuilder::<String, OrderEvent>::new(
            "localhost:9092",
            "test-group",
        )
        .with_key_format(SerializationFormat::String)
        .with_value_format(SerializationFormat::Json);

        // Further chaining should work
        let _final_builder = builder.with_key_format(SerializationFormat::Bytes);

        // This mainly tests that the API compiles and types work correctly
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_configurable_consumer_builder_avro_from_sql() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "avro".to_string());
        sql_params.insert(
            "schema.registry.url".to_string(),
            "http://localhost:8081".to_string(),
        );
        sql_params.insert(
            "value.subject".to_string(),
            "customer-events-value".to_string(),
        );

        let builder =
            ConfigurableKafkaConsumerBuilder::<String, serde_json::Value>::from_sql_config(
                "localhost:9092",
                "customer-events-group",
                &sql_params,
            )
            .expect("Failed to create builder from SQL config");

        assert_eq!(builder.key_format, SerializationFormat::String);

        if let SerializationFormat::Avro {
            schema_registry_url,
            subject,
        } = &builder.value_format
        {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "customer-events-value");
        } else {
            panic!("Expected Avro serialization format for value");
        }
    }

    #[test]
    fn test_configurable_consumer_different_key_value_types() {
        // Test with different key-value type combinations

        // String key, JSON value
        let _builder1 = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "group1",
        );

        // Integer key, String value
        let _builder2 =
            ConfigurableKafkaConsumerBuilder::<i64, String>::new("localhost:9092", "group2");

        // Bytes key, JSON value
        let _builder3 = ConfigurableKafkaConsumerBuilder::<Vec<u8>, OrderEvent>::new(
            "localhost:9092",
            "group3",
        );

        // JSON key, JSON value
        let _builder4 =
            ConfigurableKafkaConsumerBuilder::<serde_json::Value, serde_json::Value>::new(
                "localhost:9092",
                "group4",
            );
    }

    #[test]
    fn test_configurable_consumer_builder_realistic_scenarios() {
        // Scenario 1: Financial transaction processing
        let mut financial_params = HashMap::new();
        financial_params.insert("key.serializer".to_string(), "string".to_string());
        financial_params.insert("value.serializer".to_string(), "json".to_string());
        financial_params.insert("auto.offset.reset".to_string(), "earliest".to_string());
        financial_params.insert("enable.auto.commit".to_string(), "false".to_string());
        financial_params.insert("isolation.level".to_string(), "read_committed".to_string());

        #[derive(Serialize, Deserialize, Debug)]
        struct Transaction {
            transaction_id: String,
            amount: String, // Using string for exact decimal precision
            currency: String,
            timestamp: i64,
        }

        let _financial_builder =
            ConfigurableKafkaConsumerBuilder::<String, Transaction>::from_sql_config(
                "financial-kafka:9092",
                "transaction-processor-group",
                &financial_params,
            )
            .expect("Failed to create financial consumer builder");

        // Scenario 2: IoT sensor data processing
        let mut iot_params = HashMap::new();
        iot_params.insert("key.serializer".to_string(), "bytes".to_string());
        iot_params.insert("value.serializer".to_string(), "json".to_string());
        iot_params.insert("fetch.min.bytes".to_string(), "1048576".to_string()); // 1MB
        iot_params.insert("max.poll.records".to_string(), "1000".to_string());

        #[derive(Serialize, Deserialize, Debug)]
        struct SensorReading {
            sensor_id: String,
            temperature: f64,
            humidity: f64,
            timestamp: i64,
        }

        let _iot_builder =
            ConfigurableKafkaConsumerBuilder::<Vec<u8>, SensorReading>::from_sql_config(
                "iot-cluster:9092",
                "sensor-data-group",
                &iot_params,
            )
            .expect("Failed to create IoT consumer builder");

        // Scenario 3: Log aggregation
        let mut log_params = HashMap::new();
        log_params.insert("key.serializer".to_string(), "string".to_string());
        log_params.insert("value.serializer".to_string(), "string".to_string());
        log_params.insert("session.timeout.ms".to_string(), "30000".to_string());
        log_params.insert("heartbeat.interval.ms".to_string(), "3000".to_string());

        let _log_builder = ConfigurableKafkaConsumerBuilder::<String, String>::from_sql_config(
            "log-cluster:9092",
            "log-aggregation-group",
            &log_params,
        )
        .expect("Failed to create log consumer builder");
    }

    #[test]
    fn test_configurable_consumer_builder_error_handling() {
        // Test various error conditions

        // Invalid serialization format
        let mut invalid_params = HashMap::new();
        invalid_params.insert("key.serializer".to_string(), "xml".to_string());

        let result = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::from_sql_config(
            "localhost:9092",
            "test-group",
            &invalid_params,
        );
        assert!(result.is_err());

        // Missing required Avro parameters (if avro feature enabled)
        #[cfg(feature = "avro")]
        {
            let mut avro_params = HashMap::new();
            avro_params.insert("value.serializer".to_string(), "avro".to_string());
            // Missing schema.registry.url and value.subject

            let result = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::from_sql_config(
                "localhost:9092",
                "test-group",
                &avro_params,
            );

            if let Ok(builder) = result {
                // Configuration parsing might succeed, but validation should fail
                let config = builder.serialization_config.unwrap();
                assert!(config.validate().is_err());
            }
        }
    }

    #[test]
    fn test_configurable_consumer_format_accessors() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        )
        .with_key_format(SerializationFormat::Bytes)
        .with_value_format(SerializationFormat::String);

        // Test that we can access the formats
        assert_eq!(builder.key_format, SerializationFormat::Bytes);
        assert_eq!(builder.value_format, SerializationFormat::String);
    }
}
