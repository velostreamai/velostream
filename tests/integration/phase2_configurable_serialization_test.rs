//! Integration tests for Phase 2 Enhanced Kafka Integration
//!
//! These tests validate the complete configurable serialization system from
//! SQL WITH clause parsing to consumer/producer creation.

use velostream::velostream::kafka::{
    configurable_consumer::ConfigurableKafkaConsumerBuilder,
    configurable_producer::ConfigurableKafkaProducerBuilder,
    serialization_format::{SerializationConfig, SerializationFactory, SerializationFormat},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct CustomerSpendingEvent {
    customer_id: String,
    merchant_category: String,
    transaction_count: i64,
    total_spent: String, // Using string for DECIMAL precision
    avg_transaction: String,
    window_start: String,
    window_end: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TransactionEvent {
    transaction_id: String,
    customer_id: String,
    amount: String, // DECIMAL as string
    currency: String,
    timestamp: String,
    merchant_category: String,
}

#[cfg(test)]
mod phase2_integration_tests {
    use super::*;

    #[test]
    fn test_sql_with_clause_to_consumer_integration() {
        // Simulate SQL WITH clause parameters from a CREATE STREAM statement
        // CREATE STREAM ... FROM KAFKA_TOPIC('...') WITH (...)
        let mut sql_with_params = HashMap::new();
        sql_with_params.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        sql_with_params.insert("key.serializer".to_string(), "string".to_string());
        sql_with_params.insert("value.serializer".to_string(), "json".to_string());
        sql_with_params.insert("auto.offset.reset".to_string(), "earliest".to_string());
        sql_with_params.insert("enable.auto.commit".to_string(), "false".to_string());

        // Parse SQL parameters into serialization config
        let serialization_config = SerializationConfig::from_sql_params(&sql_with_params)
            .expect("Failed to parse SQL parameters");

        // Validate configuration
        assert!(serialization_config.validate().is_ok());

        // Create consumer builder from SQL config
        let consumer_builder = ConfigurableKafkaConsumerBuilder::<String, CustomerSpendingEvent>::from_sql_config(
            "localhost:9092",
            "customer-spending-group",
            &sql_with_params,
        ).expect("Failed to create consumer from SQL config");

        // Verify configuration was applied correctly
        assert_eq!(consumer_builder.key_format, SerializationFormat::String);
        assert_eq!(consumer_builder.value_format, SerializationFormat::Json);
        
        let config = consumer_builder.serialization_config.unwrap();
        assert_eq!(config.custom_properties.get("bootstrap.servers"), Some(&"localhost:9092".to_string()));
        assert_eq!(config.custom_properties.get("auto.offset.reset"), Some(&"earliest".to_string()));
        assert_eq!(config.custom_properties.get("enable.auto.commit"), Some(&"false".to_string()));
    }

    #[test]
    fn test_sql_with_clause_to_producer_integration() {
        // Simulate SQL WITH clause parameters from an INTO KAFKA_TOPIC statement
        // SELECT ... INTO KAFKA_TOPIC('...') WITH (...)
        let mut sql_with_params = HashMap::new();
        sql_with_params.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        sql_with_params.insert("key.serializer".to_string(), "string".to_string());
        sql_with_params.insert("value.serializer".to_string(), "json".to_string());
        sql_with_params.insert("acks".to_string(), "all".to_string());
        sql_with_params.insert("compression.type".to_string(), "snappy".to_string());
        sql_with_params.insert("enable.idempotence".to_string(), "true".to_string());

        // Parse SQL parameters into serialization config
        let serialization_config = SerializationConfig::from_sql_params(&sql_with_params)
            .expect("Failed to parse SQL parameters");

        // Validate configuration
        assert!(serialization_config.validate().is_ok());

        // Create producer builder from SQL config
        let producer_builder = ConfigurableKafkaProducerBuilder::<String, CustomerSpendingEvent>::from_sql_config(
            "localhost:9092",
            "customer-spending-aggregated",
            &sql_with_params,
        ).expect("Failed to create producer from SQL config");

        // Verify configuration was applied correctly
        assert_eq!(producer_builder.key_format, SerializationFormat::String);
        assert_eq!(producer_builder.value_format, SerializationFormat::Json);
        
        let config = producer_builder.serialization_config.unwrap();
        assert_eq!(config.custom_properties.get("bootstrap.servers"), Some(&"localhost:9092".to_string()));
        assert_eq!(config.custom_properties.get("acks"), Some(&"all".to_string()));
        assert_eq!(config.custom_properties.get("compression.type"), Some(&"snappy".to_string()));
        assert_eq!(config.custom_properties.get("enable.idempotence"), Some(&"true".to_string()));
    }

    #[test]
    fn test_avro_sql_with_clause_integration() {
        // Test Avro serialization configuration via SQL WITH clause
        let mut sql_with_params = HashMap::new();
        sql_with_params.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        sql_with_params.insert("key.serializer".to_string(), "string".to_string());
        sql_with_params.insert("value.serializer".to_string(), "avro".to_string());
        sql_with_params.insert("schema.registry.url".to_string(), "http://localhost:8081".to_string());
        sql_with_params.insert("value.subject".to_string(), "customer-spending-value".to_string());

        // Test with consumer
        let consumer_builder = ConfigurableKafkaConsumerBuilder::<String, serde_json::Value>::from_sql_config(
            "localhost:9092",
            "avro-consumer-group",
            &sql_with_params,
        ).expect("Failed to create Avro consumer from SQL config");

        assert_eq!(consumer_builder.key_format, SerializationFormat::String);
        if let SerializationFormat::Avro { schema_registry_url, subject } = &consumer_builder.value_format {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "customer-spending-value");
        } else {
            panic!("Expected Avro serialization format");
        }

        // Test with producer
        let producer_builder = ConfigurableKafkaProducerBuilder::<String, serde_json::Value>::from_sql_config(
            "localhost:9092",
            "avro-topic",
            &sql_with_params,
        ).expect("Failed to create Avro producer from SQL config");

        assert_eq!(producer_builder.key_format, SerializationFormat::String);
        if let SerializationFormat::Avro { schema_registry_url, subject } = &producer_builder.value_format {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "customer-spending-value");
        } else {
            panic!("Expected Avro serialization format");
        }
    }

    #[test]
    fn test_complete_pipeline_sql_configuration() {
        // Test a complete pipeline configuration as it would appear in SQL

        // Step 1: Source configuration (reading from Kafka)
        let mut source_sql_params = HashMap::new();
        source_sql_params.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        source_sql_params.insert("key.serializer".to_string(), "string".to_string());
        source_sql_params.insert("value.serializer".to_string(), "json".to_string());
        source_sql_params.insert("auto.offset.reset".to_string(), "earliest".to_string());

        let source_consumer = ConfigurableKafkaConsumerBuilder::<String, TransactionEvent>::from_sql_config(
            "localhost:9092",
            "transaction-processor",
            &source_sql_params,
        ).expect("Failed to create source consumer");

        // Step 2: Sink configuration (writing to Kafka)
        let mut sink_sql_params = HashMap::new();
        sink_sql_params.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
        sink_sql_params.insert("key.serializer".to_string(), "string".to_string());
        sink_sql_params.insert("value.serializer".to_string(), "json".to_string());
        sink_sql_params.insert("acks".to_string(), "all".to_string());
        sink_sql_params.insert("enable.idempotence".to_string(), "true".to_string());

        let sink_producer = ConfigurableKafkaProducerBuilder::<String, CustomerSpendingEvent>::from_sql_config(
            "localhost:9092",
            "customer-spending-aggregated",
            &sink_sql_params,
        ).expect("Failed to create sink producer");

        // Verify both are configured correctly
        assert_eq!(source_consumer.key_format, SerializationFormat::String);
        assert_eq!(source_consumer.value_format, SerializationFormat::Json);
        assert_eq!(sink_producer.key_format, SerializationFormat::String);
        assert_eq!(sink_producer.value_format, SerializationFormat::Json);
    }

    #[test]
    fn test_serialization_format_validation_in_pipeline() {
        // Test that serialization formats are properly validated

        // Valid configuration should pass
        let mut valid_params = HashMap::new();
        valid_params.insert("key.serializer".to_string(), "string".to_string());
        valid_params.insert("value.serializer".to_string(), "json".to_string());

        let valid_config = SerializationConfig::from_sql_params(&valid_params)
            .expect("Valid config should parse successfully");
        assert!(valid_config.validate().is_ok());
        assert!(SerializationFactory::validate_format(&valid_config.key_format()).is_ok());
        assert!(SerializationFactory::validate_format(&valid_config.value_format()).is_ok());

        // Invalid format should fail
        let mut invalid_params = HashMap::new();
        invalid_params.insert("value.serializer".to_string(), "xml".to_string());

        let invalid_result = SerializationConfig::from_sql_params(&invalid_params);
        assert!(invalid_result.is_err());
    }

    #[test]
    fn test_different_serialization_combinations() {
        // Test various combinations of key/value serialization formats

        let test_cases = vec![
            ("string", "json"),
            ("string", "string"),
            ("bytes", "json"), 
            ("bytes", "bytes"),
            ("json", "json"),
            ("json", "string"),
        ];

        for (key_format, value_format) in test_cases {
            let mut sql_params = HashMap::new();
            sql_params.insert("key.serializer".to_string(), key_format.to_string());
            sql_params.insert("value.serializer".to_string(), value_format.to_string());

            let config = SerializationConfig::from_sql_params(&sql_params)
                .expect(&format!("Failed to parse config for {}/{}", key_format, value_format));

            assert!(config.validate().is_ok(), 
                "Validation failed for {}/{}", key_format, value_format);

            // Test that both consumer and producer can be created
            let _consumer = ConfigurableKafkaConsumerBuilder::<serde_json::Value, serde_json::Value>::from_sql_config(
                "localhost:9092",
                "test-group",
                &sql_params,
            ).expect(&format!("Failed to create consumer for {}/{}", key_format, value_format));

            let _producer = ConfigurableKafkaProducerBuilder::<serde_json::Value, serde_json::Value>::from_sql_config(
                "localhost:9092", 
                "test-topic",
                &sql_params,
            ).expect(&format!("Failed to create producer for {}/{}", key_format, value_format));
        }
    }

    #[test]
    fn test_financial_precision_configuration() {
        // Test configuration specifically for financial use cases requiring DECIMAL precision

        let mut financial_params = HashMap::new();
        financial_params.insert("bootstrap.servers".to_string(), "financial-kafka:9092".to_string());
        financial_params.insert("key.serializer".to_string(), "string".to_string());
        financial_params.insert("value.serializer".to_string(), "json".to_string());
        // Financial-specific Kafka settings for reliability
        financial_params.insert("acks".to_string(), "all".to_string());
        financial_params.insert("enable.idempotence".to_string(), "true".to_string());
        financial_params.insert("retries".to_string(), "2147483647".to_string());
        financial_params.insert("max.in.flight.requests.per.connection".to_string(), "5".to_string());
        financial_params.insert("compression.type".to_string(), "lz4".to_string());

        #[derive(Serialize, Deserialize, Debug)]
        struct FinancialEvent {
            account_id: String,
            transaction_id: String,
            amount: String, // DECIMAL as string for exact precision
            currency: String,
            balance: String, // DECIMAL as string for exact precision
        }

        let financial_consumer = ConfigurableKafkaConsumerBuilder::<String, FinancialEvent>::from_sql_config(
            "financial-kafka:9092",
            "financial-processor",
            &financial_params,
        ).expect("Failed to create financial consumer");

        let financial_producer = ConfigurableKafkaProducerBuilder::<String, FinancialEvent>::from_sql_config(
            "financial-kafka:9092",
            "processed-financial-events",
            &financial_params,
        ).expect("Failed to create financial producer");

        // Verify JSON serialization (which preserves string-based DECIMAL precision)
        assert_eq!(financial_consumer.value_format, SerializationFormat::Json);
        assert_eq!(financial_producer.value_format, SerializationFormat::Json);

        // Verify financial-specific Kafka settings are captured
        let consumer_config = financial_consumer.serialization_config.unwrap();
        assert_eq!(consumer_config.custom_properties.get("acks"), Some(&"all".to_string()));
        assert_eq!(consumer_config.custom_properties.get("enable.idempotence"), Some(&"true".to_string()));

        let producer_config = financial_producer.serialization_config.unwrap();
        assert_eq!(producer_config.custom_properties.get("compression.type"), Some(&"lz4".to_string()));
        assert_eq!(producer_config.custom_properties.get("retries"), Some(&"2147483647".to_string()));
    }

    #[test]
    fn test_high_throughput_configuration() {
        // Test configuration for high-throughput scenarios

        let mut high_throughput_params = HashMap::new();
        high_throughput_params.insert("bootstrap.servers".to_string(), "iot-cluster:9092".to_string());
        high_throughput_params.insert("key.serializer".to_string(), "bytes".to_string());
        high_throughput_params.insert("value.serializer".to_string(), "json".to_string());
        // High-throughput specific settings
        high_throughput_params.insert("batch.size".to_string(), "131072".to_string()); // 128KB
        high_throughput_params.insert("linger.ms".to_string(), "100".to_string());
        high_throughput_params.insert("compression.type".to_string(), "snappy".to_string());
        high_throughput_params.insert("buffer.memory".to_string(), "134217728".to_string()); // 128MB

        #[derive(Serialize, Deserialize, Debug)]
        struct IoTSensorData {
            sensor_id: String,
            readings: Vec<f64>,
            timestamp: i64,
        }

        let iot_producer = ConfigurableKafkaProducerBuilder::<Vec<u8>, IoTSensorData>::from_sql_config(
            "iot-cluster:9092",
            "sensor-data",
            &high_throughput_params,
        ).expect("Failed to create IoT producer");

        // Verify bytes key serialization for compact keys
        assert_eq!(iot_producer.key_format, SerializationFormat::Bytes);
        assert_eq!(iot_producer.value_format, SerializationFormat::Json);

        // Verify high-throughput settings
        let config = iot_producer.serialization_config.unwrap();
        assert_eq!(config.custom_properties.get("batch.size"), Some(&"131072".to_string()));
        assert_eq!(config.custom_properties.get("linger.ms"), Some(&"100".to_string()));
        assert_eq!(config.custom_properties.get("compression.type"), Some(&"snappy".to_string()));
    }

    #[test]
    fn test_sql_parameter_case_sensitivity() {
        // Test that SQL parameter names are handled correctly

        let mut sql_params = HashMap::new();
        sql_params.insert("key.serializer".to_string(), "string".to_string()); // Should work
        sql_params.insert("KEY.SERIALIZER".to_string(), "bytes".to_string()); // Should be ignored
        sql_params.insert("value_serializer".to_string(), "json".to_string()); // Alternative format should work

        let config = SerializationConfig::from_sql_params(&sql_params)
            .expect("Failed to parse SQL parameters");

        // Should use the correctly cased parameters
        assert_eq!(config.key_format(), SerializationFormat::String); // Not Bytes
        assert_eq!(config.value_format(), SerializationFormat::Json);
    }

    #[test]
    fn test_error_propagation_in_pipeline() {
        // Test that errors are properly propagated through the pipeline

        // Invalid serialization format should fail early
        let mut invalid_params = HashMap::new();
        invalid_params.insert("key.serializer".to_string(), "unsupported".to_string());

        let consumer_result = ConfigurableKafkaConsumerBuilder::<String, TransactionEvent>::from_sql_config(
            "localhost:9092",
            "test-group",
            &invalid_params,
        );
        assert!(consumer_result.is_err());

        let producer_result = ConfigurableKafkaProducerBuilder::<String, TransactionEvent>::from_sql_config(
            "localhost:9092",
            "test-topic",
            &invalid_params,
        );
        assert!(producer_result.is_err());

        // Both should have similar error messages
        let consumer_error = format!("{}", consumer_result.unwrap_err());
        let producer_error = format!("{}", producer_result.unwrap_err());
        
        assert!(consumer_error.contains("Unsupported serialization format"));
        assert!(producer_error.contains("Unsupported serialization format"));
    }
}