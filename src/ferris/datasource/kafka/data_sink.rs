//! Kafka data sink implementation

use crate::ferris::datasource::{DataSink, DataWriter, SinkConfig, SinkMetadata};
use crate::ferris::schema::Schema;
use async_trait::async_trait;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::reader::SerializationFormat;
use super::writer::KafkaDataWriter;

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

    /// Create a unified writer based on the serialization format in config
    async fn create_unified_writer(
        &self,
    ) -> Result<KafkaDataWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization format from config (default to JSON if not specified)
        let value_format = self
            .config
            .get("value.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");

        // Map config value to SerializationFormat enum
        let format = match value_format {
            "json" => SerializationFormat::Json,
            #[cfg(feature = "avro")]
            "avro" => SerializationFormat::Avro,
            #[cfg(feature = "protobuf")]
            "protobuf" | "proto" => SerializationFormat::Protobuf,
            "auto" => SerializationFormat::Auto,
            _ => SerializationFormat::Json, // Default fallback
        };

        // Get key field for message partitioning
        let key_field = self.config.get("key.field").cloned();

        // Create unified writer
        KafkaDataWriter::new(&self.brokers, self.topic.clone(), format, key_field).await
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
        // Create the unified writer
        let writer = self.create_unified_writer().await.map_err(|e| {
            Box::new(KafkaDataSourceError::Configuration(format!(
                "Failed to create writer: {}",
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
