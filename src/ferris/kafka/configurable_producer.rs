//! Enhanced Kafka Producer with configurable serialization for Phase 2
//!
//! This module provides the ConfigurableKafkaProducerBuilder that supports runtime-configurable
//! serialization formats, enabling users to select JSON, Avro, Protobuf, etc. via
//! SQL WITH clauses or programmatic configuration.

use crate::ferris::kafka::kafka_producer_def_context::LoggingProducerContext;
use crate::ferris::kafka::{
    kafka_error::ProducerError,
    producer_config::ProducerConfig,
    serialization::{JsonSerializer, SerializationError},
    serialization_format::{SerializationConfig, SerializationFactory, SerializationFormat},
    Headers, KafkaProducer,
};
use rdkafka::{
    error::KafkaError,
    // producer::{ProducerContext},
};
use serde::Serialize;
use std::{collections::HashMap, marker::PhantomData};

/// Enhanced Kafka Producer Builder with configurable serialization support
///
/// This builder enables runtime selection of serialization formats, replacing the
/// compile-time hardcoded serializer approach with flexible configuration.
///
/// # Examples
///
/// ## Basic JSON Serialization
/// ```rust,no_run
/// use ferrisstreams::ferris::kafka::{ConfigurableKafkaProducerBuilder, SerializationFormat};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct MyMessage {
///     id: u32,
///     content: String,
/// }
///
/// let producer = ConfigurableKafkaProducerBuilder::<String, MyMessage>::new(
///     "localhost:9092",
///     "my-topic"
/// )
/// .with_key_format(SerializationFormat::String)
/// .with_value_format(SerializationFormat::Json)
/// .build()
/// .expect("Failed to create producer");
/// ```
///
/// ## SQL WITH Clause Configuration
/// ```rust,no_run
/// use std::collections::HashMap;
/// use ferrisstreams::ferris::kafka::{ConfigurableKafkaProducerBuilder, SerializationConfig};
///
/// let mut sql_params = HashMap::new();
/// sql_params.insert("key.serializer".to_string(), "string".to_string());
/// sql_params.insert("value.serializer".to_string(), "avro".to_string());
/// sql_params.insert("schema.registry.url".to_string(), "http://localhost:8081".to_string());
/// sql_params.insert("value.subject".to_string(), "orders-value".to_string());
///
/// let config = SerializationConfig::from_sql_params(&sql_params)
///     .expect("Failed to parse SQL parameters");
///
/// let producer = ConfigurableKafkaProducerBuilder::<String, serde_json::Value>::new(
///     "localhost:9092",
///     "my-topic"
/// )
/// .with_serialization_config(config)
/// .build()
/// .expect("Failed to create producer");
/// ```
pub struct ConfigurableKafkaProducerBuilder<K, V>
where
    K: Serialize + 'static,
    V: Serialize + 'static,
{
    pub brokers: String,
    pub default_topic: String,
    pub key_format: SerializationFormat,
    pub value_format: SerializationFormat,
    producer_config: Option<ProducerConfig>,
    pub serialization_config: Option<SerializationConfig>,
    // Context will be LoggingProducerContext by default
    // _context: Option<LoggingProducerContext>,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V> ConfigurableKafkaProducerBuilder<K, V>
where
    K: Serialize + 'static,
    V: Serialize + 'static,
{
    /// Create a new configurable producer builder
    ///
    /// By default, both key and value will use JSON serialization, providing
    /// backward compatibility with existing code.
    pub fn new(brokers: &str, default_topic: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            default_topic: default_topic.to_string(),
            key_format: SerializationFormat::Json,
            value_format: SerializationFormat::Json,
            producer_config: None,
            serialization_config: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Create a new builder from SQL WITH clause parameters
    ///
    /// This is the primary method for creating producers from SQL queries,
    /// parsing serialization configuration from the WITH parameters.
    pub fn from_sql_config(
        brokers: &str,
        default_topic: &str,
        sql_params: &HashMap<String, String>,
    ) -> Result<Self, SerializationError> {
        let serialization_config = SerializationConfig::from_sql_params(sql_params)?;
        serialization_config.validate()?;

        let mut builder = Self::new(brokers, default_topic);
        builder.key_format = serialization_config.key_format();
        builder.value_format = serialization_config.value_format();
        builder.serialization_config = Some(serialization_config);

        Ok(builder)
    }
}

impl<K, V> ConfigurableKafkaProducerBuilder<K, V>
where
    K: Serialize + 'static,
    V: Serialize + 'static,
{
    /// Set the key serialization format
    pub fn with_key_format(mut self, format: SerializationFormat) -> Self {
        self.key_format = format;
        self
    }

    /// Set the value serialization format
    pub fn with_value_format(mut self, format: SerializationFormat) -> Self {
        self.value_format = format;
        self
    }

    /// Configure Avro serialization for keys with Schema Registry
    pub fn with_avro_key_serialization(mut self, schema_registry_url: &str, subject: &str) -> Self {
        self.key_format = SerializationFormat::Avro {
            schema_registry_url: schema_registry_url.to_string(),
            subject: subject.to_string(),
        };
        self
    }

    /// Configure Avro serialization for values with Schema Registry
    pub fn with_avro_value_serialization(
        mut self,
        schema_registry_url: &str,
        subject: &str,
    ) -> Self {
        self.value_format = SerializationFormat::Avro {
            schema_registry_url: schema_registry_url.to_string(),
            subject: subject.to_string(),
        };
        self
    }

    /// Configure Protobuf serialization for keys
    pub fn with_protobuf_key_serialization(mut self, message_type: &str) -> Self {
        self.key_format = SerializationFormat::Protobuf {
            message_type: message_type.to_string(),
        };
        self
    }

    /// Configure Protobuf serialization for values
    pub fn with_protobuf_value_serialization(mut self, message_type: &str) -> Self {
        self.value_format = SerializationFormat::Protobuf {
            message_type: message_type.to_string(),
        };
        self
    }

    /// Set producer configuration
    pub fn with_producer_config(mut self, config: ProducerConfig) -> Self {
        self.producer_config = Some(config);
        self
    }

    /// Set serialization configuration (parsed from SQL WITH clauses)
    pub fn with_serialization_config(mut self, config: SerializationConfig) -> Self {
        self.key_format = config.key_format();
        self.value_format = config.value_format();
        self.serialization_config = Some(config);
        self
    }

    // Context methods can be added later for advanced use cases
    /*
    pub fn with_context<NewC>(self, context: NewC) -> ConfigurableKafkaProducerBuilder<K, V>
    where
        NewC: ProducerContext + 'static,
    {
        // Implementation for custom contexts can be added when needed
        todo!("Custom context support not yet implemented")
    }
    */

    /// Build the configurable producer
    ///
    /// This method creates the appropriate serializers based on the configured formats
    /// and constructs the KafkaProducer with proper serialization support.
    pub fn build(self) -> Result<ConfigurableKafkaProducer<K, V>, KafkaError> {
        // Validate serialization formats
        SerializationFactory::validate_format(&self.key_format)
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
        SerializationFactory::validate_format(&self.value_format)
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

        // For Phase 2 Step 1, we'll create a simplified producer that works with JSON internally
        // Create a basic JSON producer
        let json_producer = if let Some(config) = self.producer_config {
            KafkaProducer::<serde_json::Value, serde_json::Value, JsonSerializer, JsonSerializer>::with_config(config, JsonSerializer, JsonSerializer)?
        } else {
            KafkaProducer::<serde_json::Value, serde_json::Value, JsonSerializer, JsonSerializer>::new(&self.brokers, &self.default_topic, JsonSerializer, JsonSerializer)?
        };

        Ok(ConfigurableKafkaProducer {
            inner_producer: json_producer,
            key_format: self.key_format,
            value_format: self.value_format,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }
}

/// Configurable Kafka Producer with runtime serialization format support
///
/// This producer wraps the existing KafkaProducer and provides serialization format
/// conversion at runtime, enabling the same producer to work with different data formats
/// based on configuration.
pub struct ConfigurableKafkaProducer<K, V>
where
    K: Serialize + 'static,
    V: Serialize + 'static,
{
    inner_producer: KafkaProducer<
        serde_json::Value,
        serde_json::Value,
        JsonSerializer,
        JsonSerializer,
        LoggingProducerContext,
    >,
    key_format: SerializationFormat,
    value_format: SerializationFormat,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V> ConfigurableKafkaProducer<K, V>
where
    K: Serialize + 'static,
    V: Serialize + 'static,
{
    /// Send a message to the default topic with configurable serialization
    pub async fn send(
        &self,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        // Convert key to JSON format for internal producer
        let json_key = if let Some(k) = key {
            Some(self.convert_key_to_json(k)?)
        } else {
            None
        };

        // Convert value to JSON format for internal producer
        let json_value = self.convert_value_to_json(value)?;

        // Send using internal producer
        self.inner_producer
            .send(json_key.as_ref(), &json_value, headers, timestamp)
            .await
    }

    /// Send a message to a specific topic with configurable serialization
    pub async fn send_to_topic(
        &self,
        topic: &str,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        // Convert key to JSON format for internal producer
        let json_key = if let Some(k) = key {
            Some(self.convert_key_to_json(k)?)
        } else {
            None
        };

        // Convert value to JSON format for internal producer
        let json_value = self.convert_value_to_json(value)?;

        // Send using internal producer
        self.inner_producer
            .send_to_topic(topic, json_key.as_ref(), &json_value, headers, timestamp)
            .await
    }

    /// Get the key serialization format
    pub fn key_format(&self) -> &SerializationFormat {
        &self.key_format
    }

    /// Get the value serialization format
    pub fn value_format(&self) -> &SerializationFormat {
        &self.value_format
    }

    /// Flush any pending messages
    pub fn flush(&self, timeout_ms: u64) -> Result<(), KafkaError> {
        self.inner_producer.flush(timeout_ms)
    }

    /// Convert key from the configured format to JSON
    fn convert_key_to_json(&self, key: &K) -> Result<serde_json::Value, ProducerError> {
        match &self.key_format {
            SerializationFormat::Json => {
                // Direct conversion from K to JSON Value
                serde_json::to_value(key).map_err(|e| {
                    ProducerError::SerializationError(SerializationError::json_error(
                        "Failed to convert key to JSON value",
                        e,
                    ))
                })
            }
            SerializationFormat::String => {
                // For string format, serialize as JSON string
                let json_str = serde_json::to_string(key).map_err(|e| {
                    ProducerError::SerializationError(SerializationError::json_error(
                        "Failed to convert key to JSON string",
                        e,
                    ))
                })?;
                Ok(serde_json::Value::String(
                    json_str.trim_matches('"').to_string(),
                ))
            }
            _ => {
                // For other formats, we'll implement proper conversion in later phases
                Err(ProducerError::SerializationError(
                    SerializationError::UnsupportedType(format!(
                        "Key format '{}' not yet fully implemented in Phase 2 Step 1",
                        self.key_format
                    )),
                ))
            }
        }
    }

    /// Convert value from the configured format to JSON
    fn convert_value_to_json(&self, value: &V) -> Result<serde_json::Value, ProducerError> {
        match &self.value_format {
            SerializationFormat::Json => {
                // Direct conversion from V to JSON Value
                serde_json::to_value(value).map_err(|e| {
                    ProducerError::SerializationError(SerializationError::json_error(
                        "Failed to convert value to JSON value",
                        e,
                    ))
                })
            }
            SerializationFormat::String => {
                // For string format, serialize as JSON string
                let json_str = serde_json::to_string(value).map_err(|e| {
                    ProducerError::SerializationError(SerializationError::json_error(
                        "Failed to convert value to JSON string",
                        e,
                    ))
                })?;
                Ok(serde_json::Value::String(
                    json_str.trim_matches('"').to_string(),
                ))
            }
            _ => {
                // For other formats, we'll implement proper conversion in later phases
                Err(ProducerError::SerializationError(
                    SerializationError::UnsupportedType(format!(
                        "Value format '{}' not yet fully implemented in Phase 2 Step 1",
                        self.value_format
                    )),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestMessage {
        id: u32,
        content: String,
    }

    #[test]
    fn test_configurable_producer_builder_creation() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
        );

        // Verify default formats
        assert_eq!(builder.key_format, SerializationFormat::Json);
        assert_eq!(builder.value_format, SerializationFormat::Json);
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
    fn test_sql_config_parsing() {
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
    }

    #[test]
    fn test_avro_configuration() {
        let builder = ConfigurableKafkaProducerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-topic",
        )
        .with_avro_value_serialization("http://localhost:8081", "test-subject");

        if let SerializationFormat::Avro {
            schema_registry_url,
            subject,
        } = &builder.value_format
        {
            assert_eq!(schema_registry_url, "http://localhost:8081");
            assert_eq!(subject, "test-subject");
        } else {
            panic!("Expected Avro serialization format");
        }
    }
}
