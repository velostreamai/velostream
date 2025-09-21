//! Tests for Phase 2 Configurable Kafka Producer functionality
//!
//! These tests validate the ConfigurableKafkaProducerBuilder and related
//! functionality for runtime serialization format selection.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use velostream::velostream::kafka::{
    configurable_producer::ConfigurableKafkaProducerBuilder,
    serialization_format::{SerializationConfig, SerializationFormat},
};
use velostream::ProducerBuilder;

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
mod configurable_producer_tests {
    use super::*;

    #[test]
    fn test_configurable_producer_builder_creation() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
        );

        // Verify default formats
        assert_eq!(builder.key_format, SerializationFormat::Json);
        assert_eq!(builder.value_format, SerializationFormat::Json);
        assert_eq!(builder.brokers, "localhost:9092");
        assert_eq!(builder.default_topic, "test-topic");
    }

    #[test]
    fn test_configurable_producer_builder_with_formats() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
        )
        .with_key_format(SerializationFormat::String)
        .with_value_format(SerializationFormat::Json);

        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
    }

    #[test]
    fn test_configurable_producer_builder_fluent_api() {
        let builder = ConfigurableKafkaProducerBuilder::<String, OrderEvent>::new(
            "kafka-cluster:9092",
            "order-events",
        )
        .with_key_format(SerializationFormat::String)
        .with_value_format(SerializationFormat::Json);

        // Test that the fluent API maintains all settings
        assert_eq!(builder.brokers, "kafka-cluster:9092");
        assert_eq!(builder.default_topic, "order-events");
        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
    }

    #[test]
    fn test_configurable_producer_builder_from_sql_config_basic() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "json".to_string());

        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::from_sql_config(
            "localhost:9092",
            "test-topic",
            &sql_params,
        )
        .expect("Failed to create builder from SQL config");

        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
        assert!(builder.serialization_config.is_some());
    }

    #[test]
    fn test_configurable_producer_builder_from_sql_config_complex() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "bytes".to_string());
        sql_params.insert("value.serializer".to_string(), "string".to_string());
        sql_params.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        sql_params.insert("acks".to_string(), "all".to_string());
        sql_params.insert("compression.type".to_string(), "snappy".to_string());
        sql_params.insert("batch.size".to_string(), "32768".to_string());
        sql_params.insert("linger.ms".to_string(), "10".to_string());
        sql_params.insert("enable.idempotence".to_string(), "true".to_string());

        let builder = ConfigurableKafkaProducerBuilder::<Vec<u8>, String>::from_sql_config(
            "localhost:9092",
            "high-throughput-topic",
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
            config.custom_properties.get("acks"),
            Some(&"all".to_string())
        );
        assert_eq!(
            config.custom_properties.get("compression.type"),
            Some(&"snappy".to_string())
        );
        assert_eq!(
            config.custom_properties.get("batch.size"),
            Some(&"32768".to_string())
        );
        assert_eq!(
            config.custom_properties.get("linger.ms"),
            Some(&"10".to_string())
        );
        assert_eq!(
            config.custom_properties.get("enable.idempotence"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn test_configurable_producer_builder_from_sql_config_invalid() {
        let mut sql_params = HashMap::new();
        sql_params.insert(
            "value.serializer".to_string(),
            "unsupported_format".to_string(),
        );

        let result = ConfigurableKafkaProducerBuilder::<String, TestMessage>::from_sql_config(
            "localhost:9092",
            "test-topic",
            &sql_params,
        );

        assert!(result.is_err());
        // let _err = result.unwrap_err(); // Result is an error case
    }

    #[test]
    fn test_configurable_producer_builder_avro_key_serialization() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
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

    #[test]
    fn test_configurable_producer_builder_avro_value_serialization() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
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

    #[test]
    fn test_configurable_producer_builder_protobuf_serialization() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
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
    fn test_configurable_producer_builder_serialization_config() {
        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string());
        sql_params.insert("value.serializer".to_string(), "json".to_string());

        let serialization_config = SerializationConfig::from_sql_params(&sql_params).unwrap();

        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
        )
        .with_serialization_config(serialization_config);

        assert_eq!(builder.key_format, SerializationFormat::String);
        assert_eq!(builder.value_format, SerializationFormat::Json);
        assert!(builder.serialization_config.is_some());
    }

    #[test]
    fn test_configurable_producer_builder_method_chaining() {
        // Test that all builder methods can be chained together
        let builder =
            ConfigurableKafkaProducerBuilder::<String, OrderEvent>::new("localhost:9092", "orders")
                .with_key_format(SerializationFormat::String)
                .with_value_format(SerializationFormat::Json);

        // Further chaining should work
        let _final_builder = builder.with_key_format(SerializationFormat::Bytes);

        // This mainly tests that the API compiles and types work correctly
    }

    #[test]
    fn test_configurable_producer_builder_avro_from_sql() {
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
            ConfigurableKafkaProducerBuilder::<String, serde_json::Value>::from_sql_config(
                "localhost:9092",
                "customer-events",
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
    fn test_configurable_producer_different_key_value_types() {
        // Test with different key-value type combinations

        // String key, JSON value
        let _builder1 = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "topic1",
        );

        // Integer key, String value
        let _builder2 =
            ConfigurableKafkaProducerBuilder::<i64, String>::new("localhost:9092", "topic2");

        // Bytes key, JSON value
        let _builder3 = ConfigurableKafkaProducerBuilder::<Vec<u8>, OrderEvent>::new(
            "localhost:9092",
            "topic3",
        );

        // JSON key, JSON value
        let _builder4 =
            ConfigurableKafkaProducerBuilder::<serde_json::Value, serde_json::Value>::new(
                "localhost:9092",
                "topic4",
            );
    }

    #[test]
    fn test_configurable_producer_builder_realistic_scenarios() {
        // Scenario 1: Financial transaction publishing
        let mut financial_params = HashMap::new();
        financial_params.insert("key.serializer".to_string(), "string".to_string());
        financial_params.insert("value.serializer".to_string(), "json".to_string());
        financial_params.insert("acks".to_string(), "all".to_string());
        financial_params.insert("enable.idempotence".to_string(), "true".to_string());
        financial_params.insert("compression.type".to_string(), "lz4".to_string());
        financial_params.insert("retries".to_string(), "2147483647".to_string()); // Max retries
        financial_params.insert(
            "max.in.flight.requests.per.connection".to_string(),
            "5".to_string(),
        );

        #[derive(Serialize, Deserialize, Debug)]
        struct FinancialTransaction {
            transaction_id: String,
            amount: String, // Using string for exact decimal precision
            currency: String,
            timestamp: i64,
            account_id: String,
        }

        let _financial_builder =
            ConfigurableKafkaProducerBuilder::<String, FinancialTransaction>::from_sql_config(
                "financial-kafka:9092",
                "transactions",
                &financial_params,
            )
            .expect("Failed to create financial producer builder");

        // Scenario 2: High-throughput IoT data publishing
        let mut iot_params = HashMap::new();
        iot_params.insert("key.serializer".to_string(), "bytes".to_string());
        iot_params.insert("value.serializer".to_string(), "json".to_string());
        iot_params.insert("batch.size".to_string(), "65536".to_string()); // 64KB batches
        iot_params.insert("linger.ms".to_string(), "100".to_string()); // 100ms batching
        iot_params.insert("compression.type".to_string(), "snappy".to_string());
        iot_params.insert("buffer.memory".to_string(), "134217728".to_string()); // 128MB buffer

        #[derive(Serialize, Deserialize, Debug)]
        struct SensorData {
            sensor_id: String,
            measurements: Vec<f64>,
            timestamp: i64,
            location: String,
        }

        let _iot_builder =
            ConfigurableKafkaProducerBuilder::<Vec<u8>, SensorData>::from_sql_config(
                "iot-cluster:9092",
                "sensor-data",
                &iot_params,
            )
            .expect("Failed to create IoT producer builder");

        // Scenario 3: Low-latency event publishing
        let mut low_latency_params = HashMap::new();
        low_latency_params.insert("key.serializer".to_string(), "string".to_string());
        low_latency_params.insert("value.serializer".to_string(), "string".to_string());
        low_latency_params.insert("acks".to_string(), "1".to_string()); // Leader acknowledgment only
        low_latency_params.insert("batch.size".to_string(), "1".to_string()); // No batching
        low_latency_params.insert("linger.ms".to_string(), "0".to_string()); // Immediate send

        let _low_latency_builder =
            ConfigurableKafkaProducerBuilder::<String, String>::from_sql_config(
                "low-latency-cluster:9092",
                "events",
                &low_latency_params,
            )
            .expect("Failed to create low-latency producer builder");
    }

    #[test]
    fn test_configurable_producer_builder_error_handling() {
        // Test various error conditions

        // Invalid serialization format
        let mut invalid_params = HashMap::new();
        invalid_params.insert("key.serializer".to_string(), "yaml".to_string());

        let result = ConfigurableKafkaProducerBuilder::<String, TestMessage>::from_sql_config(
            "localhost:9092",
            "test-topic",
            &invalid_params,
        );
        assert!(result.is_err());

        {
            let mut avro_params = HashMap::new();
            avro_params.insert("value.serializer".to_string(), "avro".to_string());
            // Missing schema.registry.url and value.subject

            let result = ConfigurableKafkaProducerBuilder::<String, TestMessage>::from_sql_config(
                "localhost:9092",
                "test-topic",
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
    fn test_configurable_producer_format_accessors() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
        )
        .with_key_format(SerializationFormat::Bytes)
        .with_value_format(SerializationFormat::String);

        // Test that we can access the formats
        assert_eq!(builder.key_format, SerializationFormat::Bytes);
        assert_eq!(builder.value_format, SerializationFormat::String);
    }

    #[test]
    fn test_configurable_producer_performance_presets_simulation() {
        // Test configurations that would be used with different performance presets

        // High Throughput preset equivalent
        let mut high_throughput_params = HashMap::new();
        high_throughput_params.insert("key.serializer".to_string(), "string".to_string());
        high_throughput_params.insert("value.serializer".to_string(), "json".to_string());
        high_throughput_params.insert("batch.size".to_string(), "131072".to_string()); // 128KB
        high_throughput_params.insert("linger.ms".to_string(), "100".to_string());
        high_throughput_params.insert("compression.type".to_string(), "lz4".to_string());
        high_throughput_params.insert("acks".to_string(), "1".to_string());

        let _high_throughput_builder =
            ConfigurableKafkaProducerBuilder::<String, TestMessage>::from_sql_config(
                "localhost:9092",
                "high-throughput-topic",
                &high_throughput_params,
            )
            .expect("Failed to create high throughput builder");

        // Low Latency preset equivalent
        let mut low_latency_params = HashMap::new();
        low_latency_params.insert("key.serializer".to_string(), "bytes".to_string());
        low_latency_params.insert("value.serializer".to_string(), "bytes".to_string());
        low_latency_params.insert("batch.size".to_string(), "1".to_string());
        low_latency_params.insert("linger.ms".to_string(), "0".to_string());
        low_latency_params.insert("acks".to_string(), "1".to_string());

        let _low_latency_builder =
            ConfigurableKafkaProducerBuilder::<Vec<u8>, Vec<u8>>::from_sql_config(
                "localhost:9092",
                "low-latency-topic",
                &low_latency_params,
            )
            .expect("Failed to create low latency builder");

        // Max Durability preset equivalent
        let mut max_durability_params = HashMap::new();
        max_durability_params.insert("key.serializer".to_string(), "string".to_string());
        max_durability_params.insert("value.serializer".to_string(), "json".to_string());
        max_durability_params.insert("acks".to_string(), "all".to_string());
        max_durability_params.insert("enable.idempotence".to_string(), "true".to_string());
        max_durability_params.insert("retries".to_string(), "2147483647".to_string());
        max_durability_params.insert(
            "max.in.flight.requests.per.connection".to_string(),
            "5".to_string(),
        );

        let _max_durability_builder =
            ConfigurableKafkaProducerBuilder::<String, TestMessage>::from_sql_config(
                "localhost:9092",
                "durable-topic",
                &max_durability_params,
            )
            .expect("Failed to create max durability builder");
    }

    #[test]
    fn test_configurable_producer_builder_with_all_serialization_formats() {
        // Test that all serialization formats can be configured

        // JSON
        let _json_builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "json-topic",
        )
        .with_value_format(SerializationFormat::Json);

        // String
        let _string_builder = ConfigurableKafkaProducerBuilder::<String, String>::new(
            "localhost:9092",
            "string-topic",
        )
        .with_value_format(SerializationFormat::String);

        // Bytes
        let _bytes_builder = ConfigurableKafkaProducerBuilder::<Vec<u8>, Vec<u8>>::new(
            "localhost:9092",
            "bytes-topic",
        )
        .with_value_format(SerializationFormat::Bytes);

        {
            let _avro_builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
                "localhost:9092",
                "avro-topic",
            )
            .with_avro_value_serialization("http://localhost:8081", "test-subject");
        }

        {
            let _protobuf_builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
                "localhost:9092",
                "protobuf-topic",
            )
            .with_protobuf_value_serialization("com.example.TestMessage");
        }
    }
}
