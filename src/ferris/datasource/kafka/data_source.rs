//! Kafka data source implementation

use crate::ferris::datasource::{DataReader, DataSource, SourceConfig, SourceMetadata};
use crate::ferris::schema::{FieldDefinition, Schema};
use crate::ferris::sql::ast::DataType;
use async_trait::async_trait;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::reader::{KafkaDataReader, SerializationFormat};

/// Kafka DataSource implementation
pub struct KafkaDataSource {
    brokers: String,
    topic: String,
    group_id: Option<String>,
    config: HashMap<String, String>,
}

impl KafkaDataSource {
    /// Create a Kafka data source from properties
    pub fn from_properties(
        props: &HashMap<String, String>,
        default_topic: &str,
        job_name: &str,
    ) -> Self {
        // Helper function to get property with source. prefix fallback
        let get_source_prop = |key: &str| {
            props
                .get(&format!("source.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        let brokers = get_source_prop("brokers")
            .or_else(|| get_source_prop("bootstrap.servers"))
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = get_source_prop("topic").unwrap_or_else(|| default_topic.to_string());
        let group_id =
            get_source_prop("group_id").unwrap_or_else(|| format!("ferris-sql-{}", job_name));

        // Create filtered config with source. properties
        let mut source_config = HashMap::new();
        for (key, value) in props.iter() {
            if key.starts_with("source.") {
                // Remove source. prefix for the config map
                let config_key = key.strip_prefix("source.").unwrap().to_string();
                source_config.insert(config_key, value.clone());
            } else if !key.starts_with("sink.") && !props.contains_key(&format!("source.{}", key)) {
                // Include unprefixed properties only if there's no prefixed version and it's not a sink property
                source_config.insert(key.clone(), value.clone());
            }
        }

        Self {
            brokers,
            topic,
            group_id: Some(group_id),
            config: source_config,
        }
    }

    /// Apply BatchConfig settings to Kafka consumer properties
    fn apply_batch_config_to_kafka_properties(
        &self,
        kafka_config: &mut std::collections::HashMap<String, String>,
        batch_config: &crate::ferris::datasource::BatchConfig,
    ) {
        use crate::ferris::datasource::BatchStrategy;

        // Only apply batch settings if batching is enabled
        if !batch_config.enable_batching {
            return;
        }

        // Map our BatchConfig to Kafka consumer properties
        match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => {
                // max.poll.records - controls how many records are returned in each poll()
                let max_records = size.min(&batch_config.max_batch_size).to_string();
                kafka_config.insert("max.poll.records".to_string(), max_records);
            }
            BatchStrategy::TimeWindow(duration) => {
                // fetch.max.wait.ms - max time to wait for fetch.min.bytes
                kafka_config.insert(
                    "fetch.max.wait.ms".to_string(),
                    duration.as_millis().to_string(),
                );
                // Conservative max.poll.records for time-based batching
                kafka_config.insert(
                    "max.poll.records".to_string(),
                    (batch_config.max_batch_size / 2).to_string(),
                );
            }
            BatchStrategy::AdaptiveSize {
                min_size,
                max_size: _,
                target_latency,
            } => {
                // Use min_size as conservative starting point
                kafka_config.insert("max.poll.records".to_string(), min_size.to_string());
                kafka_config.insert(
                    "fetch.max.wait.ms".to_string(),
                    target_latency.as_millis().to_string(),
                );
                // Set fetch.min.bytes to encourage batching
                kafka_config.insert("fetch.min.bytes".to_string(), "10240".to_string());
                // 10KB
            }
            BatchStrategy::MemoryBased(target_bytes) => {
                // fetch.min.bytes - minimum data to fetch in a request
                kafka_config.insert("fetch.min.bytes".to_string(), target_bytes.to_string());
                // Conservative timeout and max records
                kafka_config.insert(
                    "fetch.max.wait.ms".to_string(),
                    batch_config.batch_timeout.as_millis().to_string(),
                );
                kafka_config.insert(
                    "max.poll.records".to_string(),
                    (batch_config.max_batch_size / 4).to_string(),
                );
            }
        }

        // Always set session.timeout.ms and heartbeat.interval.ms for reliable batching
        kafka_config
            .entry("session.timeout.ms".to_string())
            .or_insert("30000".to_string());
        kafka_config
            .entry("heartbeat.interval.ms".to_string())
            .or_insert("3000".to_string());

        // Optimize for throughput when batching is enabled
        kafka_config
            .entry("fetch.max.bytes".to_string())
            .or_insert("52428800".to_string()); // 50MB
    }
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

    /// Generate SourceConfig from current state
    pub fn to_source_config(&self) -> SourceConfig {
        SourceConfig::Kafka {
            brokers: self.brokers.clone(),
            topic: self.topic.clone(),
            group_id: self.group_id.clone(),
            properties: self.config.clone(),
            batch_config: Default::default(),
        }
    }

    // Getter methods for testing
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn group_id(&self) -> &Option<String> {
        &self.group_id
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Self-initialize with current configuration
    pub async fn self_initialize(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.to_source_config();
        self.initialize(config).await
    }

    /// Create a consumer based on the serialization format in config
    async fn create_unified_reader(
        &self,
        group_id: &str,
        batch_size: Option<usize>,
    ) -> Result<KafkaDataReader, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization format from config (default to JSON if not specified)
        let value_format = self
            .config
            .get("value.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");

        // Parse format using FromStr with proper error handling
        let format = {
            use std::str::FromStr;
            SerializationFormat::from_str(value_format).unwrap_or(SerializationFormat::Json)
        };

        // Extract schema from config based on format
        let schema = self.extract_schema_for_format(&format)?;

        // Create unified reader with detected format and schema
        KafkaDataReader::new_with_schema(
            &self.brokers,
            self.topic.clone(),
            group_id,
            format,
            batch_size,
            schema.as_deref(), // Pass schema if available
        )
        .await
    }

    /// Extract schema from configuration based on serialization format
    fn extract_schema_for_format(
        &self,
        format: &SerializationFormat,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        match format {
            SerializationFormat::Avro { .. } => {
                // Look for Avro schema in various config keys (common patterns)
                let schema = self
                    .config
                    .get("avro.schema")
                    .or_else(|| self.config.get("value.avro.schema"))
                    .or_else(|| self.config.get("schema.avro"))
                    .or_else(|| self.config.get("avro_schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path
                    if let Some(schema_file) = self
                        .config
                        .get("avro.schema.file")
                        .or_else(|| self.config.get("schema.file"))
                        .or_else(|| self.config.get("avro_schema_file"))
                    {
                        return self.load_schema_from_file(schema_file);
                    }
                }

                Ok(schema)
            }
            SerializationFormat::Protobuf { .. } => {
                // Look for Protobuf schema in various config keys
                let schema = self
                    .config
                    .get("protobuf.schema")
                    .or_else(|| self.config.get("value.protobuf.schema"))
                    .or_else(|| self.config.get("schema.protobuf"))
                    .or_else(|| self.config.get("protobuf_schema"))
                    .or_else(|| self.config.get("proto.schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path
                    if let Some(schema_file) = self
                        .config
                        .get("protobuf.schema.file")
                        .or_else(|| self.config.get("proto.schema.file"))
                        .or_else(|| self.config.get("schema.file"))
                        .or_else(|| self.config.get("protobuf_schema_file"))
                    {
                        return self.load_schema_from_file(schema_file);
                    }
                }

                Ok(schema)
            }
            SerializationFormat::Json
            | SerializationFormat::Bytes
            | SerializationFormat::String => {
                // JSON doesn't require schema, but allow optional schema for validation
                let schema = self
                    .config
                    .get("json.schema")
                    .or_else(|| self.config.get("schema.json"))
                    .cloned();
                Ok(schema)
            }
        }
    }

    /// Load schema content from a file path
    fn load_schema_from_file(
        &self,
        file_path: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        use std::fs;

        match fs::read_to_string(file_path) {
            Ok(content) => Ok(Some(content)),
            Err(e) => Err(format!("Failed to load schema from file '{}': {}", file_path, e).into()),
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
                batch_config,
                ..
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.group_id = group_id;

                // Start with provided properties
                let mut kafka_config = properties;

                // Add batch configuration to Kafka consumer properties
                self.apply_batch_config_to_kafka_properties(&mut kafka_config, &batch_config);

                self.config = kafka_config;
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

        // Extract batch size from config if available
        let batch_size = self
            .config
            .get("max.poll.records")
            .and_then(|s| s.parse::<usize>().ok());

        // Create the unified reader
        let reader = self.create_unified_reader(group_id, batch_size).await?;

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
