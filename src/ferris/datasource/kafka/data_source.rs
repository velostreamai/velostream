//! Kafka data source implementation

use crate::ferris::datasource::{DataReader, DataSource, SourceConfig, SourceMetadata};
use crate::ferris::kafka::serialization::JsonSerializer;
use crate::ferris::kafka::KafkaConsumer;
use crate::ferris::schema::{FieldDefinition, Schema};
use crate::ferris::sql::ast::DataType;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::reader::{DynamicKafkaReader, KafkaDataReader};

#[cfg(feature = "avro")]
use crate::ferris::kafka::serialization::AvroSerializer;

/// Kafka DataSource implementation
pub struct KafkaDataSource {
    brokers: String,
    topic: String,
    group_id: Option<String>,
    config: HashMap<String, String>,
}

impl KafkaDataSource {
    /// Create a new Kafka data source
    pub fn new(brokers: String, topic: String) -> Self {
        Self {
            brokers,
            topic,
            group_id: None,
            config: HashMap::new(),
        }
    }

    /// Set the consumer group ID
    pub fn with_group_id(mut self, group_id: String) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    /// Create a consumer based on the serialization format in config
    fn create_consumer_from_config(
        &self,
        group_id: &str,
    ) -> Result<DynamicKafkaReader, Box<dyn std::error::Error + Send + Sync>> {
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
                let consumer = KafkaConsumer::<String, Value, _, _>::new(
                    &self.brokers,
                    group_id,
                    JsonSerializer,
                    JsonSerializer,
                )?;
                consumer.subscribe(&[&self.topic])?;
                Ok(DynamicKafkaReader::JsonReader(KafkaDataReader {
                    consumer,
                    topic: self.topic.clone(),
                }))
            }
            #[cfg(feature = "avro")]
            ("json", "avro") => {
                use apache_avro::types::Value as AvroValue;
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

                let consumer = KafkaConsumer::<String, AvroValue, _, _>::new(
                    &self.brokers,
                    group_id,
                    JsonSerializer,
                    avro_serializer,
                )?;
                consumer.subscribe(&[&self.topic])?;
                Ok(DynamicKafkaReader::AvroReader(KafkaDataReader {
                    consumer,
                    topic: self.topic.clone(),
                }))
            }
            #[cfg(feature = "protobuf")]
            ("json", "protobuf") | ("json", "proto") => {
                // For protobuf, we use Vec<u8> as the value type with JsonSerializer
                // The actual protobuf deserialization would happen at a higher level
                let consumer = KafkaConsumer::<String, Vec<u8>, _, _>::new(
                    &self.brokers,
                    group_id,
                    JsonSerializer,
                    JsonSerializer, // Use JSON serializer for raw bytes
                )?;
                consumer.subscribe(&[&self.topic])?;
                Ok(DynamicKafkaReader::ProtobufReader(KafkaDataReader {
                    consumer,
                    topic: self.topic.clone(),
                }))
            }
            _ => {
                // Default to JSON for unsupported combinations
                let consumer = KafkaConsumer::<String, Value, _, _>::new(
                    &self.brokers,
                    group_id,
                    JsonSerializer,
                    JsonSerializer,
                )?;
                consumer.subscribe(&[&self.topic])?;
                Ok(DynamicKafkaReader::JsonReader(KafkaDataReader {
                    consumer,
                    topic: self.topic.clone(),
                }))
            }
        }
    }
}

#[async_trait]
impl DataSource for KafkaDataSource {
    async fn initialize(
        &mut self,
        config: SourceConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SourceConfig::Kafka {
                brokers,
                topic,
                group_id,
                properties,
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.group_id = group_id;
                self.config = properties;
                Ok(())
            }
            _ => Err(Box::new(KafkaDataSourceError::Configuration(
                "Expected Kafka configuration".to_string(),
            ))),
        }
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        // For Kafka, we'll return a generic schema since message format is flexible
        // In practice, this could integrate with Schema Registry
        let fields = vec![
            FieldDefinition::required("key".to_string(), DataType::String),
            FieldDefinition::required("value".to_string(), DataType::String),
            FieldDefinition::required("timestamp".to_string(), DataType::Timestamp),
            FieldDefinition::required("offset".to_string(), DataType::Integer),
            FieldDefinition::required("partition".to_string(), DataType::Integer),
        ];

        Ok(Schema::new(fields))
    }

    async fn create_reader(
        &self,
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        let group_id = self.group_id.as_ref().ok_or_else(|| {
            Box::new(KafkaDataSourceError::Configuration(
                "Group ID required for consumer".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // Create the appropriate consumer based on serialization config
        let reader = self.create_consumer_from_config(group_id)?;

        Ok(Box::new(reader))
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_batch(&self) -> bool {
        true // Kafka can be used for batch processing
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            source_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: true,
            capabilities: vec![
                "real_time".to_string(),
                "exactly_once".to_string(),
                "schema_registry".to_string(),
                "headers".to_string(),
                "partitioning".to_string(),
            ],
        }
    }
}
