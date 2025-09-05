//! Kafka data sink implementation

use crate::ferris::datasource::{DataSink, DataWriter, SinkConfig, SinkMetadata};
use crate::ferris::schema::Schema;
use async_trait::async_trait;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
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

    /// Create a Kafka data sink from properties
    pub fn from_properties(props: &HashMap<String, String>, job_name: &str) -> Self {
        // Helper function to get property with sink. prefix fallback
        let get_sink_prop = |key: &str| {
            props
                .get(&format!("sink.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        let brokers = get_sink_prop("brokers")
            .or_else(|| get_sink_prop("bootstrap.servers"))
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = get_sink_prop("topic").unwrap_or_else(|| format!("{}_output", job_name));

        // Create filtered config with sink. properties
        let mut sink_config = HashMap::new();
        for (key, value) in props.iter() {
            if key.starts_with("sink.") {
                // Remove sink. prefix for the config map
                let config_key = key.strip_prefix("sink.").unwrap().to_string();
                sink_config.insert(config_key, value.clone());
            } else if !key.starts_with("source.") && !props.contains_key(&format!("sink.{}", key)) {
                // Include unprefixed properties only if there's no prefixed version and it's not a source property
                sink_config.insert(key.clone(), value.clone());
            }
        }

        Self {
            brokers,
            topic,
            config: sink_config,
        }
    }

    /// Create a unified writer based on the serialization format in config
    async fn create_unified_writer(
        &self,
    ) -> Result<KafkaDataWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization format from config (default to JSON if not specified)
        let _value_format = self
            .config
            .get("value.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");

        // Map config value to SerializationFormat enum
        // Format is now handled by KafkaDataWriter::from_properties

        // Create writer with properties-based configuration
        KafkaDataWriter::from_properties(&self.brokers, self.topic.clone(), &self.config).await
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    // Getter methods for testing
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
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
