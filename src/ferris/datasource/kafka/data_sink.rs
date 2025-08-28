//! Kafka data sink implementation

use crate::ferris::datasource::{DataSink, DataWriter, SinkConfig, SinkMetadata};
use crate::ferris::kafka::serialization::JsonSerializer;
use crate::ferris::kafka::KafkaProducer;
use crate::ferris::schema::Schema;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::writer::{DynamicKafkaWriter, KafkaDataWriter};

#[cfg(feature = "avro")]
use crate::ferris::kafka::serialization::AvroSerializer;
#[cfg(feature = "avro")]
use apache_avro::types::Value as AvroValue;

/// Kafka DataSink implementation
pub struct KafkaDataSink {
    brokers: String,
    topic: String,
    config: HashMap<String, String>,
}

impl KafkaDataSink {
    /// Create a new Kafka data sink
    pub fn new(brokers: String, topic: String) -> Self {
        Self {
            brokers,
            topic,
            config: HashMap::new(),
        }
    }

    /// Create a producer based on the serialization format in config
    fn create_producer_from_config(
        &self,
    ) -> Result<DynamicKafkaWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization formats from config (default to JSON if not specified)
        let key_format = self
            .config
            .get("key.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");
        let value_format = self
            .config
            .get("value.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");

        match (key_format, value_format) {
            ("json", "json") => {
                let producer = KafkaProducer::<String, Value, _, _>::new(
                    &self.brokers,
                    &self.topic,
                    JsonSerializer,
                    JsonSerializer,
                )?;
                Ok(DynamicKafkaWriter::JsonWriter(KafkaDataWriter {
                    producer,
                    topic: self.topic.clone(),
                }))
            }
            #[cfg(feature = "avro")]
            ("json", "avro") => {
                use apache_avro::Schema as AvroSchema;

                // For now, use a generic schema - in production this would come from Schema Registry
                let schema_str = r#"
                {
                    "type": "record",
                    "name": "GenericRecord",
                    "fields": []
                }
                "#;
                let schema = AvroSchema::parse_str(schema_str)
                    .map_err(|e| format!("Failed to parse Avro schema: {}", e))?;
                let avro_serializer = AvroSerializer::new(schema);

                let producer = KafkaProducer::<String, AvroValue, _, _>::new(
                    &self.brokers,
                    &self.topic,
                    JsonSerializer,
                    avro_serializer,
                )?;
                Ok(DynamicKafkaWriter::AvroWriter(KafkaDataWriter {
                    producer,
                    topic: self.topic.clone(),
                }))
            }
            #[cfg(feature = "protobuf")]
            ("json", "protobuf") | ("json", "proto") => {
                // For protobuf, we use Vec<u8> as the value type with JsonSerializer
                // The actual protobuf serialization would happen at a higher level
                let producer = KafkaProducer::<String, Vec<u8>, _, _>::new(
                    &self.brokers,
                    &self.topic,
                    JsonSerializer,
                    JsonSerializer, // Use JSON serializer for raw bytes
                )?;
                Ok(DynamicKafkaWriter::ProtobufWriter(KafkaDataWriter {
                    producer,
                    topic: self.topic.clone(),
                }))
            }
            _ => {
                // Default to JSON for unsupported combinations
                let producer = KafkaProducer::<String, Value, _, _>::new(
                    &self.brokers,
                    &self.topic,
                    JsonSerializer,
                    JsonSerializer,
                )?;
                Ok(DynamicKafkaWriter::JsonWriter(KafkaDataWriter {
                    producer,
                    topic: self.topic.clone(),
                }))
            }
        }
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }
}

#[async_trait]
impl DataSink for KafkaDataSink {
    async fn initialize(
        &mut self,
        config: SinkConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SinkConfig::Kafka {
                brokers,
                topic,
                properties,
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.config = properties;
                Ok(())
            }
            _ => Err(Box::new(KafkaDataSourceError::Configuration(
                "Expected Kafka configuration".to_string(),
            ))),
        }
    }

    async fn validate_schema(
        &self,
        _schema: &Schema,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Kafka is schema-flexible, so we accept any schema
        // In practice, this could validate against Schema Registry
        Ok(())
    }

    async fn create_writer(
        &self,
    ) -> Result<Box<dyn DataWriter>, Box<dyn std::error::Error + Send + Sync>> {
        // Create the appropriate producer based on serialization config
        let writer = self.create_producer_from_config().map_err(|e| {
            Box::new(KafkaDataSourceError::Configuration(format!(
                "Failed to create producer: {}",
                e
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(writer))
    }

    fn supports_transactions(&self) -> bool {
        true // Kafka supports exactly-once semantics
    }

    fn supports_upsert(&self) -> bool {
        false // Kafka is append-only, upserts require application logic
    }

    fn metadata(&self) -> SinkMetadata {
        SinkMetadata {
            sink_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_transactions: true,
            supports_upsert: false,
            supports_schema_evolution: true,
            capabilities: vec![
                "exactly_once".to_string(),
                "partitioning".to_string(),
                "headers".to_string(),
                "high_throughput".to_string(),
            ],
        }
    }
}
