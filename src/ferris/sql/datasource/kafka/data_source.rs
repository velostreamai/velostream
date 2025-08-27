//! Kafka data source implementation

use crate::ferris::kafka::serialization::JsonSerializer;
use crate::ferris::kafka::KafkaConsumer;
use crate::ferris::sql::ast::DataType;
use crate::ferris::sql::datasource::{DataReader, DataSource, SourceConfig, SourceMetadata};
use crate::ferris::schema::{FieldDefinition, Schema};
use async_trait::async_trait;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::reader::KafkaDataReader;

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
                ..
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.group_id = group_id;
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

        let consumer = KafkaConsumer::<String, String, _, _>::new(
            &self.brokers,
            group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .map_err(|e| {
            Box::new(KafkaDataSourceError::from(e)) as Box<dyn std::error::Error + Send + Sync>
        })?;

        consumer.subscribe(&[&self.topic]).map_err(|e| {
            Box::new(KafkaDataSourceError::from(e)) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(KafkaDataReader {
            consumer,
            topic: self.topic.clone(),
        }))
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
