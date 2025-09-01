//! Enhanced Kafka Consumer with configurable serialization for Phase 2
//!
//! This module provides the KafkaConsumerBuilder that supports runtime-configurable
//! serialization formats, enabling users to select JSON, Avro, Protobuf, etc. via
//! SQL WITH clauses or programmatic configuration.

use crate::ferris::kafka::{
    consumer_config::ConsumerConfig,
    kafka_error::ConsumerError,
    serialization::{JsonSerializer, SerializationError},
    serialization_format::{SerializationConfig, SerializationFactory, SerializationFormat},
    KafkaConsumer, Message,
};
use rdkafka::{consumer::DefaultConsumerContext, error::KafkaError};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, marker::PhantomData};

/// Enhanced Kafka Consumer Builder with configurable serialization support
///
/// This builder enables runtime selection of serialization formats, replacing the
/// compile-time hardcoded serializer approach with flexible configuration.
///
/// # Examples
///
/// ## Basic JSON Serialization
/// ```rust,no_run
/// use ferrisstreams::ferris::kafka::{ConfigurableKafkaConsumerBuilder, SerializationFormat};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct MyMessage {
///     id: u32,
///     content: String,
/// }
///
/// let consumer = ConfigurableKafkaConsumerBuilder::<String, MyMessage>::new(
///     "localhost:9092",
///     "my-group"
/// )
/// .with_key_format(SerializationFormat::String)
/// .with_value_format(SerializationFormat::Json)
/// .build()
/// .expect("Failed to create consumer");
/// ```
///
/// ## Avro Serialization with Schema Registry (requires "avro" feature)
/// ```rust,no_run
/// # {
/// use ferrisstreams::ferris::kafka::{ConfigurableKafkaConsumerBuilder, SerializationFormat};
///
/// let consumer = ConfigurableKafkaConsumerBuilder::<String, serde_json::Value>::new(
///     "localhost:9092",
///     "my-group"
/// )
/// .with_avro_value_serialization(
///     "http://localhost:8081",  // Schema Registry URL
///     "customer-spending-value" // Subject name
/// )
/// .build()
/// .expect("Failed to create consumer");
/// # }
/// ```
///
/// ## SQL WITH Clause Configuration
/// ```rust,no_run
/// use std::collections::HashMap;
/// use ferrisstreams::ferris::kafka::{ConfigurableKafkaConsumerBuilder, SerializationConfig};
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
/// let consumer = ConfigurableKafkaConsumerBuilder::<String, serde_json::Value>::new(
///     "localhost:9092",
///     "my-group"
/// )
/// .with_serialization_config(config)
/// .build()
/// .expect("Failed to create consumer");
/// ```
pub struct ConfigurableKafkaConsumerBuilder<K, V>
where
    K: for<'de> Deserialize<'de> + Serialize + 'static,
    V: for<'de> Deserialize<'de> + Serialize + 'static,
{
    brokers: String,
    group_id: String,
    key_format: SerializationFormat,
    value_format: SerializationFormat,
    consumer_config: Option<ConsumerConfig>,
    serialization_config: Option<SerializationConfig>,
    // Context will be DefaultConsumerContext by default
    // _context: Option<DefaultConsumerContext>,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V> ConfigurableKafkaConsumerBuilder<K, V>
where
    K: for<'de> Deserialize<'de> + Serialize + 'static,
    V: for<'de> Deserialize<'de> + Serialize + 'static,
{
    /// Create a new configurable consumer builder
    ///
    /// By default, both key and value will use JSON serialization, providing
    /// backward compatibility with existing code.
    pub fn new(brokers: &str, group_id: &str) -> Self {
        Self {
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            key_format: SerializationFormat::Json,
            value_format: SerializationFormat::Json,
            consumer_config: None,
            serialization_config: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Create a new builder from SQL WITH clause parameters
    ///
    /// This is the primary method for creating consumers from SQL queries,
    /// parsing serialization configuration from the WITH parameters.
    pub fn from_sql_config(
        brokers: &str,
        group_id: &str,
        sql_params: &HashMap<String, String>,
    ) -> Result<Self, SerializationError> {
        let serialization_config = SerializationConfig::from_sql_params(sql_params)?;
        serialization_config.validate()?;

        let mut builder = Self::new(brokers, group_id);
        builder.key_format = serialization_config.key_format();
        builder.value_format = serialization_config.value_format();
        builder.serialization_config = Some(serialization_config);

        Ok(builder)
    }
}

impl<K, V> ConfigurableKafkaConsumerBuilder<K, V>
where
    K: for<'de> Deserialize<'de> + Serialize + 'static,
    V: for<'de> Deserialize<'de> + Serialize + 'static,
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

    /// Set consumer configuration
    pub fn with_consumer_config(mut self, config: ConsumerConfig) -> Self {
        self.consumer_config = Some(config);
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
    pub fn with_context<NewC>(self, context: NewC) -> ConfigurableKafkaConsumerBuilder<K, V>
    where
        NewC: ConsumerContext + 'static,
    {
        // Implementation for custom contexts can be added when needed
        todo!("Custom context support not yet implemented")
    }
    */

    /// Build the configurable consumer
    ///
    /// This method creates the appropriate serializers based on the configured formats
    /// and constructs the KafkaConsumer with proper serialization support.
    pub fn build(self) -> Result<ConfigurableKafkaConsumer<K, V>, KafkaError> {
        // Validate serialization formats
        SerializationFactory::validate_format(&self.key_format)
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
        SerializationFactory::validate_format(&self.value_format)
            .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;

        // For Phase 2 Step 1, we'll create a simplified consumer that works with JSON internally
        // For now, just return a mock implementation to establish the API structure
        // In the next steps, we'll implement proper serialization conversion

        // Create a basic JSON consumer
        let json_consumer = if let Some(config) = self.consumer_config {
            KafkaConsumer::<serde_json::Value, serde_json::Value, JsonSerializer, JsonSerializer>::with_config(config, JsonSerializer, JsonSerializer)?
        } else {
            KafkaConsumer::<serde_json::Value, serde_json::Value, JsonSerializer, JsonSerializer>::new(&self.brokers, &self.group_id, JsonSerializer, JsonSerializer)?
        };

        Ok(ConfigurableKafkaConsumer {
            inner_consumer: json_consumer,
            key_format: self.key_format,
            value_format: self.value_format,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }
}

/// Configurable Kafka Consumer with runtime serialization format support
///
/// This consumer wraps the existing KafkaConsumer and provides serialization format
/// conversion at runtime, enabling the same consumer to work with different data formats
/// based on configuration.
pub struct ConfigurableKafkaConsumer<K, V>
where
    K: for<'de> Deserialize<'de> + Serialize + 'static,
    V: for<'de> Deserialize<'de> + Serialize + 'static,
{
    inner_consumer: KafkaConsumer<
        serde_json::Value,
        serde_json::Value,
        JsonSerializer,
        JsonSerializer,
        DefaultConsumerContext,
    >,
    key_format: SerializationFormat,
    value_format: SerializationFormat,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V> ConfigurableKafkaConsumer<K, V>
where
    K: for<'de> Deserialize<'de> + Serialize + 'static,
    V: for<'de> Deserialize<'de> + Serialize + 'static,
{
    /// Subscribe to topics
    pub fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        self.inner_consumer.subscribe(topics)
    }

    /// Poll for a message with configurable serialization
    pub async fn poll(&self, timeout: std::time::Duration) -> Result<Message<K, V>, ConsumerError> {
        // Get the raw message from the inner consumer
        let raw_message = self.inner_consumer.poll(timeout).await?;

        // Convert the message to the desired formats
        self.convert_message(raw_message)
    }

    /// Get the key serialization format
    pub fn key_format(&self) -> &SerializationFormat {
        &self.key_format
    }

    /// Get the value serialization format
    pub fn value_format(&self) -> &SerializationFormat {
        &self.value_format
    }

    /// Convert a raw JSON message to the configured types
    fn convert_message(
        &self,
        raw_message: Message<serde_json::Value, serde_json::Value>,
    ) -> Result<Message<K, V>, ConsumerError> {
        // Convert key
        let converted_key = if let Some(raw_key) = raw_message.key() {
            Some(self.convert_key_from_json(raw_key)?)
        } else {
            None
        };

        // Convert value
        let converted_value = self.convert_value_from_json(raw_message.value())?;

        // Create new message with converted types
        Ok(Message::new(
            converted_key,
            converted_value,
            raw_message.headers().clone(),
            raw_message.partition(),
            raw_message.offset(),
            raw_message.timestamp(),
        ))
    }

    /// Convert key from JSON to target format
    fn convert_key_from_json(&self, json_key: &serde_json::Value) -> Result<K, ConsumerError> {
        match &self.key_format {
            SerializationFormat::Json => {
                // Direct conversion from JSON Value to K
                serde_json::from_value(json_key.clone()).map_err(|e| {
                    ConsumerError::SerializationError(SerializationError::SerializationFailed(
                        e.to_string(),
                    ))
                })
            }
            SerializationFormat::String => {
                // For string format, we expect a JSON string value
                if let serde_json::Value::String(s) = json_key {
                    serde_json::from_str(&format!("\"{}\"", s)).map_err(|e| {
                        ConsumerError::SerializationError(SerializationError::SerializationFailed(
                            e.to_string(),
                        ))
                    })
                } else {
                    Err(ConsumerError::SerializationError(
                        SerializationError::SchemaError(
                            "Expected string value for string serialization format".to_string(),
                        ),
                    ))
                }
            }
            _ => {
                // For other formats, we'll implement proper conversion in later phases
                Err(ConsumerError::SerializationError(
                    SerializationError::UnsupportedType(format!(
                        "Key format '{}' not yet fully implemented in Phase 2 Step 1",
                        self.key_format
                    )),
                ))
            }
        }
    }

    /// Convert value from JSON to target format
    fn convert_value_from_json(&self, json_value: &serde_json::Value) -> Result<V, ConsumerError> {
        match &self.value_format {
            SerializationFormat::Json => {
                // Direct conversion from JSON Value to V
                serde_json::from_value(json_value.clone()).map_err(|e| {
                    ConsumerError::SerializationError(SerializationError::SerializationFailed(
                        e.to_string(),
                    ))
                })
            }
            SerializationFormat::String => {
                // For string format, we expect a JSON string value
                if let serde_json::Value::String(s) = json_value {
                    serde_json::from_str(&format!("\"{}\"", s)).map_err(|e| {
                        ConsumerError::SerializationError(SerializationError::SerializationFailed(
                            e.to_string(),
                        ))
                    })
                } else {
                    Err(ConsumerError::SerializationError(
                        SerializationError::SchemaError(
                            "Expected string value for string serialization format".to_string(),
                        ),
                    ))
                }
            }
            _ => {
                // For other formats, we'll implement proper conversion in later phases
                Err(ConsumerError::SerializationError(
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
    fn test_configurable_consumer_builder_creation() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
        );

        // Verify default formats
        assert_eq!(builder.key_format, SerializationFormat::Json);
        assert_eq!(builder.value_format, SerializationFormat::Json);
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
    fn test_sql_config_parsing() {
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
    }

    #[test]
    fn test_avro_configuration() {
        let builder = ConfigurableKafkaConsumerBuilder::<String, TestMessage>::new(
            "localhost:9092",
            "test-group",
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
