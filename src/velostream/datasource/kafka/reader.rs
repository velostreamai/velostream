//! Unified Kafka data reader implementation

use crate::velostream::datasource::{
    BatchConfig, BatchStrategy, DataReader, EventTimeConfig, SourceOffset,
};
// FR-081 Phase 2B: Use kafka_fast_consumer (BaseConsumer-based) directly
use crate::velostream::kafka::{
    Message, consumer_config::ConsumerTier, kafka_error::ConsumerError,
    kafka_fast_consumer::Consumer as FastConsumer, serialization::StringSerializer,
};

// Removed unused imports - AvroSerializer and ProtoSerializer don't exist
// Using AvroCodec and ProtobufCodec directly instead
use crate::velostream::serialization::{SerializationCodec, json_codec::JsonCodec};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
use log::{error, info, warn};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Import the unified SerializationFormat from the main module
pub use crate::velostream::kafka::serialization_format::SerializationFormat;

// Type alias to reduce complexity warnings
type MessageStream<'a> = std::pin::Pin<
    Box<
        dyn futures::Stream<
                Item = Result<Message<String, HashMap<String, FieldValue>>, ConsumerError>,
            > + Send
            + 'a,
    >,
>;

/// Unified Kafka DataReader that handles all serialization formats
/// FR-081 Phase 2B: Now uses BaseConsumer-based fast consumer with configurable performance tiers
pub struct KafkaDataReader {
    consumer: Arc<
        FastConsumer<String, HashMap<String, FieldValue>, StringSerializer, SerializationCodec>,
    >,
    tier: ConsumerTier,
    batch_config: BatchConfig,
    event_time_config: Option<EventTimeConfig>,
    topic: String,
    // State for adaptive batching
    current_batch_start: Option<Instant>,
    adaptive_state: AdaptiveBatchState,
}

/// State tracking for adaptive batch sizing
#[derive(Debug, Clone)]
struct AdaptiveBatchState {
    current_size: usize,
    recent_latencies: Vec<Duration>,
    last_adjustment: Instant,
}

impl AdaptiveBatchState {
    fn new(initial_size: usize) -> Self {
        Self {
            current_size: initial_size,
            recent_latencies: Vec::with_capacity(10),
            last_adjustment: Instant::now(),
        }
    }

    fn record_latency(&mut self, latency: Duration) {
        self.recent_latencies.push(latency);
        if self.recent_latencies.len() > 10 {
            self.recent_latencies.remove(0);
        }
    }

    fn average_latency(&self) -> Option<Duration> {
        if self.recent_latencies.is_empty() {
            None
        } else {
            let total: Duration = self.recent_latencies.iter().sum();
            Some(total / self.recent_latencies.len() as u32)
        }
    }
}

impl KafkaDataReader {
    /// FAIL FAST: Validate topic configuration to prevent misconfiguration
    ///
    /// This validates that the topic name is properly configured and not a placeholder.
    /// Aligned with KafkaDataWriter's topic validation for consistency.
    fn validate_topic_configuration(topic: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Check for empty topic name - ALWAYS fail
        if topic.is_empty() {
            return Err(format!(
                "CONFIGURATION ERROR: Kafka source topic name is empty.\n\
                 \n\
                 A valid Kafka topic name MUST be configured. Please configure via:\n\
                 1. YAML config file: 'topic.name: <topic_name>'\n\
                 2. SQL properties: '<source_name>.topic = <topic_name>'\n\
                 3. Direct parameter when creating KafkaDataReader\n\
                 \n\
                 This validation prevents misconfiguration of data sources."
            )
            .into());
        }

        // Warn about suspicious topic names that might indicate misconfiguration
        let suspicious_names = [
            "default",
            "test",
            "temp",
            "placeholder",
            "undefined",
            "null",
            "none",
            "example",
            "my-topic",
            "topic-name",
        ];

        if suspicious_names.contains(&topic.to_lowercase().as_str()) {
            return Err(format!(
                "CONFIGURATION ERROR: Kafka source configured with suspicious topic name '{}'.\n\
                 \n\
                 This is a common placeholder/fallback value that indicates configuration \
                 was not properly loaded.\n\
                 \n\
                 Valid topic names should be:\n\
                 1. Extracted from source name in SQL: CREATE STREAM <source_name> ...\n\
                 2. Configured in YAML: 'topic: <topic_name>' or 'topic.name: <topic_name>'\n\
                 \n\
                 Common misconfiguration causes:\n\
                 - YAML file not found or not loaded\n\
                 - Missing 'topic' or 'topic.name' in YAML\n\
                 - Hardcoded fallback value not updated\n\
                 \n\
                 This validation prevents misconfiguration of data sources.",
                topic
            )
            .into());
        }

        info!(
            "KafkaDataReader: Topic validation passed - will read from topic '{}'",
            topic
        );

        Ok(())
    }

    /// Parse serialization format string with proper error handling
    ///
    /// Tries multiple property key conventions for compatibility:
    /// - value.serializer (Kafka convention)
    /// - schema.value.serializer
    /// - serializer.format
    /// - format (fallback)
    fn parse_format_from_properties(properties: &HashMap<String, String>) -> SerializationFormat {
        use std::str::FromStr;

        let format_str = properties
            .get("value.serializer")
            .or_else(|| properties.get("schema.value.serializer"))
            .or_else(|| properties.get("serializer.format"))
            .or_else(|| properties.get("format"))
            .map(|s| s.as_str())
            .unwrap_or("json");

        SerializationFormat::from_str(format_str).unwrap_or(SerializationFormat::Json)
    }

    /// Extract schema from properties based on serialization format
    ///
    /// Supports inline schemas and schema file paths, with multiple property key conventions
    fn extract_schema_from_properties(
        format: &SerializationFormat,
        properties: &HashMap<String, String>,
    ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        match format {
            SerializationFormat::Avro { .. } => {
                // Look for Avro schema in various config keys (dot notation preferred, underscore fallback)
                let schema = properties
                    .get("avro.schema")
                    .or_else(|| properties.get("value.avro.schema"))
                    .or_else(|| properties.get("schema.avro"))
                    .or_else(|| properties.get("avro_schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path (dot notation preferred, underscore fallback)
                    if let Some(schema_file) = properties
                        .get("avro.schema.file")
                        .or_else(|| properties.get("schema.value.schema.file"))
                        .or_else(|| properties.get("value.schema.file"))
                        .or_else(|| properties.get("schema.file"))
                        .or_else(|| properties.get("avro_schema_file"))
                        .or_else(|| properties.get("schema_file"))
                    {
                        return Self::load_schema_from_file(schema_file);
                    }
                }
                Ok(schema)
            }
            SerializationFormat::Protobuf { .. } => {
                // Look for Protobuf schema in various config keys (dot notation preferred, underscore fallback)
                let schema = properties
                    .get("protobuf.schema")
                    .or_else(|| properties.get("value.protobuf.schema"))
                    .or_else(|| properties.get("schema.protobuf"))
                    .or_else(|| properties.get("protobuf_schema"))
                    .or_else(|| properties.get("proto.schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path (dot notation preferred, underscore fallback)
                    if let Some(schema_file) = properties
                        .get("protobuf.schema.file")
                        .or_else(|| properties.get("proto.schema.file"))
                        .or_else(|| properties.get("schema.value.schema.file"))
                        .or_else(|| properties.get("value.schema.file"))
                        .or_else(|| properties.get("schema.file"))
                        .or_else(|| properties.get("protobuf_schema_file"))
                        .or_else(|| properties.get("schema_file"))
                    {
                        return Self::load_schema_from_file(schema_file);
                    }
                }
                Ok(schema)
            }
            SerializationFormat::Json
            | SerializationFormat::Bytes
            | SerializationFormat::String => {
                // JSON doesn't require schema, but allow optional validation schema
                let schema = properties
                    .get("json.schema")
                    .or_else(|| properties.get("schema.json"))
                    .cloned();
                Ok(schema)
            }
        }
    }

    /// Load schema content from file path
    fn load_schema_from_file(
        file_path: &str,
    ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        use std::fs;
        match fs::read_to_string(file_path) {
            Ok(content) => {
                info!("KafkaDataReader: Loaded schema from file: {}", file_path);
                Ok(Some(content))
            }
            Err(e) => Err(format!("Failed to load schema from file '{}': {}", file_path, e).into()),
        }
    }

    /// Create a serialization codec based on format and schema, with robust error handling
    fn create_serialization_codec(
        format: &SerializationFormat,
        schema_json: Option<&str>,
    ) -> Result<SerializationCodec, Box<dyn Error + Send + Sync>> {
        use crate::velostream::serialization::helpers;

        match format {
            // JSON is always available - the default fallback
            SerializationFormat::Json => Ok(SerializationCodec::Json(JsonCodec::new())),

            // Avro requires schema
            SerializationFormat::Avro {
                schema_registry_url: _,
                subject: _,
            } => {
                if let Some(schema) = schema_json {
                    let avro_codec = helpers::create_avro_codec(Some(schema))?;
                    Ok(SerializationCodec::Avro(avro_codec))
                } else {
                    Err("Avro serialization requires a schema. Please provide schema JSON or use JSON format as fallback.".into())
                }
            }

            // Protobuf with optional schema
            SerializationFormat::Protobuf { message_type: _ } => {
                if schema_json.is_some() {
                    let protobuf_codec = helpers::create_protobuf_codec(schema_json)?;
                    Ok(SerializationCodec::Protobuf(protobuf_codec))
                } else {
                    // For protobuf, we can create a default schema codec
                    let protobuf_codec = helpers::create_protobuf_codec(None)?;
                    Ok(SerializationCodec::Protobuf(protobuf_codec))
                }
            }

            // Bytes and String formats are converted to JSON for structured data
            SerializationFormat::Bytes | SerializationFormat::String => {
                // For structured data processing, we use JSON as the internal format
                // The bytes/string will be converted to FieldValue::String during processing
                Ok(SerializationCodec::Json(JsonCodec::new()))
            }
        }
    }

    /// Create a new Kafka data reader with optional schema and batch configuration
    pub async fn new_with_schema(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_config: Option<BatchConfig>,
        passed_schema_json: Option<&str>,
        event_time_config: Option<EventTimeConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schema_and_config(
            brokers,
            topic,
            group_id,
            format,
            batch_config,
            passed_schema_json,
            event_time_config,
            &HashMap::new(), // No additional config properties
        )
        .await
    }

    /// Create a new Kafka data reader with schema and additional consumer properties
    pub async fn new_with_schema_and_config(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_config: Option<BatchConfig>,
        passed_schema_json: Option<&str>,
        event_time_config: Option<EventTimeConfig>,
        consumer_properties: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // FAIL FAST: Validate topic is properly configured
        Self::validate_topic_configuration(&topic)?;

        // Create codec using robust factory pattern
        let codec = Self::create_serialization_codec(&format, passed_schema_json)?;

        let batch_config = batch_config.unwrap_or_default();

        // Configure Kafka consumer based on BatchConfig
        let mut consumer_config =
            crate::velostream::kafka::consumer_config::ConsumerConfig::new(brokers, group_id);

        // Apply consumer properties from YAML config FIRST (before batch config)
        Self::apply_consumer_properties(&mut consumer_config, consumer_properties);

        // Then apply batch config (which may override some properties)
        Self::apply_batch_config_to_consumer(&mut consumer_config, &batch_config);

        // FR-081 Phase 2B: Select optimal performance tier based on batch strategy
        // Performance-focused tier selection for CPU and memory efficiency
        let tier = consumer_config.performance_tier.take().unwrap_or_else(|| {
            match &batch_config.strategy {
                BatchStrategy::FixedSize(size) if *size > 100 => {
                    info!("FR-081: Auto-selecting Buffered tier (50-75K msg/s) for large batch size: {}", size);
                    ConsumerTier::Buffered { batch_size: (*size).min(1000) }
                },
                BatchStrategy::MemoryBased(_) => {
                    info!("FR-081: Auto-selecting Buffered tier (50-75K msg/s) for memory-based batching");
                    ConsumerTier::Buffered { batch_size: 500 }
                },
                _ => {
                    info!("FR-081: Using Standard tier (10-15K msg/s, low CPU/memory) for Kafka consumer");
                    ConsumerTier::Standard
                }
            }
        });

        // Log the applied consumer configuration
        Self::log_consumer_config(&consumer_config, &batch_config);

        // FR-081 Phase 2B: Create fast consumer (BaseConsumer-based, low overhead)
        let consumer = Arc::new(FastConsumer::<
            String,
            HashMap<String, FieldValue>,
            StringSerializer,
            SerializationCodec,
        >::with_config(
            consumer_config, StringSerializer, codec
        )?);

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let initial_size = match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => *size,
            BatchStrategy::AdaptiveSize { min_size, .. } => *min_size,
            BatchStrategy::LowLatency { max_batch_size, .. } => *max_batch_size,
            _ => 100,
        };

        Ok(Self {
            consumer,
            tier,
            batch_config,
            event_time_config,
            topic,
            current_batch_start: None,
            adaptive_state: AdaptiveBatchState::new(initial_size),
        })
    }

    /// Create a new Kafka data reader with BatchConfig (preferred method)
    pub async fn new_with_batch_config(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_config: BatchConfig,
        passed_schema_json: Option<&str>,
        event_time_config: Option<EventTimeConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schema_and_config(
            brokers,
            topic,
            group_id,
            format,
            Some(batch_config),
            passed_schema_json,
            event_time_config,
            &HashMap::new(), // No additional config properties
        )
        .await
    }

    /// Create a new Kafka data reader with BatchConfig and consumer properties
    pub async fn new_with_batch_config_and_properties(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_config: BatchConfig,
        passed_schema_json: Option<&str>,
        event_time_config: Option<EventTimeConfig>,
        consumer_properties: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schema_and_config(
            brokers,
            topic,
            group_id,
            format,
            Some(batch_config),
            passed_schema_json,
            event_time_config,
            consumer_properties,
        )
        .await
    }

    /// Create a new Kafka data reader with JSON format (convenience method)
    pub async fn new_json(
        brokers: &str,
        topic: String,
        group_id: &str,
        batch_size: Option<usize>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let batch_config = batch_size.map(|size| BatchConfig {
            strategy: BatchStrategy::FixedSize(size),
            ..Default::default()
        });

        Self::new_with_schema(
            brokers,
            topic,
            group_id,
            SerializationFormat::Json,
            batch_config,
            None,
            None,
        )
        .await
    }

    /// Create a new Kafka data reader with format from string (with error handling)
    pub async fn new_from_format_string(
        brokers: &str,
        topic: String,
        group_id: &str,
        format_str: &str,
        batch_size: Option<usize>,
        schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        use std::str::FromStr;

        // Parse format string with proper error handling
        let format = SerializationFormat::from_str(format_str)
            .map_err(|e| format!("Invalid serialization format '{}': {}", format_str, e))?;

        let batch_config = batch_size.map(|size| BatchConfig {
            strategy: BatchStrategy::FixedSize(size),
            ..Default::default()
        });

        Self::new_with_schema(
            brokers,
            topic,
            group_id,
            format,
            batch_config,
            schema_json,
            None,
        )
        .await
    }

    /// Create a new Kafka data reader from HashMap properties (similar to KafkaDataWriter pattern)
    ///
    /// Extracts format and schema from properties, enabling unified configuration management.
    pub async fn from_properties(
        brokers: &str,
        topic: String,
        group_id: &str,
        properties: &HashMap<String, String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Extract format from properties
        let format = Self::parse_format_from_properties(properties);

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::new_with_schema_and_config(
            brokers,
            topic,
            group_id,
            format,
            None,
            schema.as_deref(),
            None,
            properties,
        )
        .await
    }

    /// Create a new Kafka data reader from HashMap properties with batch configuration
    pub async fn from_properties_with_batch_config(
        brokers: &str,
        topic: String,
        group_id: &str,
        properties: &HashMap<String, String>,
        batch_config: BatchConfig,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Extract format from properties
        let format = Self::parse_format_from_properties(properties);

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::new_with_schema_and_config(
            brokers,
            topic,
            group_id,
            format,
            Some(batch_config),
            schema.as_deref(),
            None,
            properties,
        )
        .await
    }

    /// Create a new Kafka data reader from HashMap properties with batch configuration and event time config
    pub async fn from_properties_with_batch_and_event_time(
        brokers: &str,
        topic: String,
        group_id: &str,
        properties: &HashMap<String, String>,
        batch_config: BatchConfig,
        event_time_config: Option<EventTimeConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Extract format from properties
        let format = Self::parse_format_from_properties(properties);

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::new_with_schema_and_config(
            brokers,
            topic,
            group_id,
            format,
            Some(batch_config),
            schema.as_deref(),
            event_time_config,
            properties,
        )
        .await
    }
}

#[async_trait]
impl DataReader for KafkaDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        if !self.batch_config.enable_batching {
            return self.read_single().await;
        }

        match &self.batch_config.strategy {
            BatchStrategy::FixedSize(size) => self.read_fixed_size(*size).await,
            BatchStrategy::TimeWindow(duration) => self.read_time_window(*duration).await,
            BatchStrategy::AdaptiveSize {
                min_size,
                max_size,
                target_latency,
            } => {
                self.read_adaptive(*min_size, *max_size, *target_latency)
                    .await
            }
            BatchStrategy::MemoryBased(max_bytes) => self.read_memory_based(*max_bytes).await,
            BatchStrategy::LowLatency {
                max_batch_size,
                max_wait_time,
                eager_processing,
            } => {
                self.read_low_latency(*max_batch_size, *max_wait_time, *eager_processing)
                    .await
            }
            BatchStrategy::MegaBatch { batch_size, .. } => {
                // High-throughput mega-batch processing (Phase 4 optimization)
                self.read_fixed_size(*batch_size).await
            }
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.consumer
            .commit()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        match offset {
            SourceOffset::Kafka {
                partition: _,
                offset: _,
            } => {
                // Note: velo_streams KafkaConsumer doesn't directly expose seek functionality
                // This would need to be implemented by accessing the underlying rdkafka consumer
                // For now, return an error indicating this is not yet implemented
                Err("Seek operation not yet implemented for velo_streams KafkaConsumer".into())
            }
            _ => Err("Invalid offset type for Kafka source".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // For Kafka, we always potentially have more data unless the consumer is closed
        Ok(true)
    }

    // Transaction support methods (Kafka exactly-once semantics)
    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // Kafka transactions would be handled at the producer level
        // For consumers, we use manual commit mode for exactly-once processing
        Ok(true)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.commit().await
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // For Kafka consumers, we can't really "abort" a read transaction
        // The best we can do is not commit the offsets
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        true
    }
}

impl KafkaDataReader {
    /// Apply consumer properties from YAML config to Kafka consumer configuration
    fn apply_consumer_properties(
        consumer_config: &mut crate::velostream::kafka::consumer_config::ConsumerConfig,
        properties: &HashMap<String, String>,
    ) {
        use crate::velostream::kafka::consumer_config::OffsetReset;

        log::info!(
            "Applying {} consumer properties from config",
            properties.len()
        );

        for (key, value) in properties.iter() {
            // Skip properties that have special handling or are metadata
            if key.starts_with("schema.")
                || key.starts_with("value.")
                || key.starts_with("datasource.")
                || key == "topic"  // topic is not a Kafka consumer property
                || key == "consumer.group"  // consumer.group is metadata, group.id is the actual property
                || key == "performance_profile"
            // performance_profile is metadata, not a Kafka property
            {
                log::debug!("  Skipping property (schema/metadata): {} = {}", key, value);
                continue;
            }

            log::debug!("  Applying consumer property: {} = {}", key, value);

            // Map common property names to ConsumerConfig fields
            match key.as_str() {
                "bootstrap.servers" => {
                    // Already set via constructor
                    log::debug!("    (bootstrap.servers handled by constructor)");
                }
                "group.id" => {
                    // Already set via constructor
                    log::debug!("    (group.id handled by constructor)");
                }
                "auto.offset.reset" => {
                    consumer_config.auto_offset_reset = match value.as_str() {
                        "earliest" => OffsetReset::Earliest,
                        "latest" => OffsetReset::Latest,
                        "none" => OffsetReset::None,
                        _ => OffsetReset::Earliest, // Default to earliest
                    };
                }
                "enable.auto.commit" => {
                    consumer_config.enable_auto_commit = value.parse().unwrap_or(true);
                }
                "auto.commit.interval.ms" => {
                    if let Ok(millis) = value.parse() {
                        consumer_config.auto_commit_interval =
                            std::time::Duration::from_millis(millis);
                    }
                }
                "session.timeout.ms" => {
                    if let Ok(millis) = value.parse() {
                        consumer_config.session_timeout = std::time::Duration::from_millis(millis);
                    }
                }
                "heartbeat.interval.ms" => {
                    if let Ok(millis) = value.parse() {
                        consumer_config.heartbeat_interval =
                            std::time::Duration::from_millis(millis);
                    }
                }
                "max.poll.records" => {
                    if let Ok(records) = value.parse() {
                        consumer_config.max_poll_records = records;
                    }
                }
                "fetch.min.bytes" => {
                    if let Ok(bytes) = value.parse() {
                        consumer_config.fetch_min_bytes = bytes;
                    }
                }
                "fetch.max.wait.ms" => {
                    if let Ok(millis) = value.parse() {
                        consumer_config.fetch_max_wait = std::time::Duration::from_millis(millis);
                    }
                }
                "max.partition.fetch.bytes" => {
                    if let Ok(bytes) = value.parse() {
                        consumer_config.max_partition_fetch_bytes = bytes;
                    }
                }
                _ => {
                    // Add other properties to custom config (via common.custom_config)
                    log::debug!("    (adding as custom property)");
                    consumer_config
                        .common
                        .custom_config
                        .insert(key.clone(), value.clone());
                }
            }
        }
    }

    /// Apply BatchConfig settings to Kafka consumer configuration
    fn apply_batch_config_to_consumer(
        consumer_config: &mut crate::velostream::kafka::consumer_config::ConsumerConfig,
        batch_config: &BatchConfig,
    ) {
        if !batch_config.enable_batching {
            // Disable batching - use minimal settings
            consumer_config.max_poll_records = 1;
            return;
        }

        // Configure consumer based on batch strategy
        match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => {
                // Set max_poll_records to batch size, capped by max_batch_size
                let poll_records = (*size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;

                // Use reasonable fetch settings for fixed size batching
                consumer_config.fetch_min_bytes = 1; // Don't wait for more data
                consumer_config.fetch_max_wait = std::time::Duration::from_millis(100);
                // Short wait
            }
            BatchStrategy::TimeWindow(duration) => {
                // For time-based batching, use a reasonable poll size and longer fetch wait
                consumer_config.max_poll_records = (batch_config.max_batch_size / 2) as u32;
                consumer_config.fetch_max_wait = *duration;
                consumer_config.fetch_min_bytes = 1; // Accept any amount of data
            }
            BatchStrategy::AdaptiveSize {
                min_size,
                max_size: _,
                target_latency,
            } => {
                // Start conservatively with min_size
                let poll_records = (*min_size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;

                // Use target latency as fetch wait time
                consumer_config.fetch_max_wait = *target_latency;
                consumer_config.fetch_min_bytes = 1;
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                // For memory-based batching, use conservative polling
                // Estimate ~1KB per record as baseline
                let estimated_records = (*max_bytes / 1024).min(batch_config.max_batch_size);
                consumer_config.max_poll_records = estimated_records as u32;

                // Use memory target to set fetch limits
                consumer_config.max_partition_fetch_bytes =
                    (*max_bytes as u32).min(50 * 1024 * 1024); // Cap at 50MB
                consumer_config.fetch_min_bytes = 1;
                consumer_config.fetch_max_wait = batch_config.batch_timeout;
            }

            // Ultra-low latency consumer settings
            //     fetch.min.bytes: 1           # Don't wait for batches
            //     fetch_max_wait_ms: 1         # 1ms maximum wait
            //     max_poll_records: 10         # Process small batches quickly
            // session_timeout_ms: 6000     # Fast failure detection
            //     heartbeat.interval.ms: 2000  # Frequent heartbeats
            // auto_offset_reset: "latest"  # Start from newest messages
            BatchStrategy::LowLatency {
                max_batch_size,
                max_wait_time,
                eager_processing: _,
            } => {
                // Optimize for minimal latency with very small batches
                let poll_records = (*max_batch_size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;

                // Use aggressive timeout settings for low latency
                consumer_config.fetch_max_wait = *max_wait_time;
                consumer_config.fetch_min_bytes = 1; // Don't wait for data to accumulate
                consumer_config.max_partition_fetch_bytes = 64 * 1024; // Small fetch size (64KB)

                // Enable low-level optimizations for latency
                consumer_config.enable_auto_commit = true;
                consumer_config.auto_commit_interval = std::time::Duration::from_millis(50);
                // Frequent commits
            }
            BatchStrategy::MegaBatch { batch_size, .. } => {
                // High-throughput mega-batch processing (Phase 4 optimization)
                let poll_records = (*batch_size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;

                // Optimize for high throughput
                consumer_config.fetch_min_bytes = 1_048_576; // 1MB minimum fetch
                consumer_config.fetch_max_wait = std::time::Duration::from_millis(500);
                consumer_config.max_partition_fetch_bytes = 10 * 1024 * 1024; // 10MB per partition
            }
        }

        // Apply general batch timeout
        consumer_config.max_poll_interval = batch_config
            .batch_timeout
            .max(std::time::Duration::from_secs(30));
    }

    /// Log the consumer configuration for debugging and monitoring
    fn log_consumer_config(
        consumer_config: &crate::velostream::kafka::consumer_config::ConsumerConfig,
        batch_config: &BatchConfig,
    ) {
        info!("=== Kafka Consumer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);

        info!("Applied Consumer Settings:");
        info!("  - max_poll_records: {}", consumer_config.max_poll_records);
        info!("  - fetch_min_bytes: {}", consumer_config.fetch_min_bytes);
        info!("  - fetch_max_wait: {:?}", consumer_config.fetch_max_wait);
        info!(
            "  - max_partition_fetch_bytes: {}",
            consumer_config.max_partition_fetch_bytes
        );
        info!(
            "  - enable_auto_commit: {}",
            consumer_config.enable_auto_commit
        );
        info!(
            "  - auto_commit_interval: {:?}",
            consumer_config.auto_commit_interval
        );
        info!(
            "  - max_poll_interval: {:?}",
            consumer_config.max_poll_interval
        );
        info!("  - brokers: {}", consumer_config.brokers());
        info!("  - group_id: {}", consumer_config.group_id);
        info!("=====================================");
    }

    /// Read a single record (when batching is disabled)
    /// FR-081 Phase 2B: Performance-optimized using tier-appropriate streaming
    async fn read_single(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        use futures::StreamExt;
        use tokio::time::timeout;

        let timeout_duration = self.batch_config.batch_timeout;

        // FR-081: Use appropriate streaming method based on tier for optimal CPU/memory
        let mut stream: MessageStream<'_> = match &self.tier {
            ConsumerTier::Standard => {
                // Direct polling - lowest CPU overhead (~2-5%)
                Box::pin(self.consumer.stream())
            }
            ConsumerTier::Buffered { batch_size } => {
                // Batched polling - better throughput, moderate CPU (~3-8%)
                Box::pin(self.consumer.buffered_stream(*batch_size))
            }
            ConsumerTier::Dedicated => {
                // Dedicated thread - maximum throughput, higher CPU (~10-15%)
                // Convert DedicatedKafkaStream to futures::Stream
                let consumer_arc = Arc::clone(&self.consumer);
                let mut dedicated = consumer_arc.dedicated_stream();
                Box::pin(futures::stream::poll_fn(move |_cx| {
                    std::task::Poll::Ready(dedicated.next())
                }))
            }
        };

        match timeout(timeout_duration, stream.next()).await {
            Ok(Some(Ok(message))) => {
                let record = self.create_stream_record(message)?;
                Ok(vec![record])
            }
            Ok(Some(Err(_))) | Ok(None) | Err(_) => Ok(vec![]), // Timeout or error - return empty batch
        }
    }

    /// Read fixed number of records
    /// FR-081 Phase 2B: Performance-optimized batch reading using tier-appropriate streaming
    ///
    /// Polls the Kafka consumer until either:
    /// - Target batch size is reached
    /// - No more messages are available (consumer returns None)
    /// - A consumer error occurs
    ///
    /// The Kafka consumer's fetch.max.wait.ms controls how long each poll waits for data.
    async fn read_fixed_size(
        &mut self,
        size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        use futures::StreamExt;

        let target_size = size.min(self.batch_config.max_batch_size);
        let mut records = Vec::with_capacity(target_size);

        log::debug!(
            "🔍 FR-081: read_fixed_size using {:?} tier for {} records",
            self.tier,
            target_size
        );

        // FR-081: Use buffered streaming for batch reads (memory efficient)
        let mut stream: MessageStream<'_> = match &self.tier {
            ConsumerTier::Buffered { batch_size } => {
                // Already optimized for batching - use native batch size
                Box::pin(self.consumer.buffered_stream(*batch_size))
            }
            _ => {
                // Standard/Dedicated: Use buffered stream with target size for better performance
                Box::pin(self.consumer.buffered_stream(target_size))
            }
        };

        // Poll until we have enough records or no more are available
        while records.len() < target_size {
            match stream.next().await {
                Some(Ok(message)) => {
                    log::trace!(
                        "🔍 read_fixed_size: received message from partition {} offset {}",
                        message.partition(),
                        message.offset()
                    );
                    let record = self.create_stream_record(message)?;
                    records.push(record);
                }
                Some(Err(e)) => {
                    // Consumer error - return what we have or propagate if empty
                    log::error!(
                        "🚨 Consumer error on topic '{}': {:?}, records so far: {}",
                        self.topic,
                        e,
                        records.len()
                    );
                    if records.is_empty() {
                        return Err(Box::new(e));
                    }
                    break;
                }
                None => {
                    // No more messages available - return what we have
                    log::debug!(
                        "🔍 read_fixed_size: no more messages, returning {} records",
                        records.len()
                    );
                    break;
                }
            }
        }

        log::debug!("🔍 read_fixed_size: returning {} records", records.len());
        Ok(records)
    }

    /// Read records within a time window
    /// FR-081 Phase 2B: Performance-optimized time-windowed batch reading
    async fn read_time_window(
        &mut self,
        duration: Duration,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        use futures::StreamExt;
        use tokio::time::timeout;

        let mut records = Vec::new();
        let start_time = Instant::now();
        let poll_timeout = Duration::from_millis(100); // Short poll timeout for time-based batching

        // Initialize batch start time if not set
        if self.current_batch_start.is_none() {
            self.current_batch_start = Some(start_time);
        }

        // FR-081: Use standard streaming for time windows (low CPU, predictable latency)
        let mut stream: MessageStream<'_> = Box::pin(self.consumer.stream());

        while start_time.elapsed() < duration && records.len() < self.batch_config.max_batch_size {
            match timeout(poll_timeout, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    let record = self.create_stream_record(message)?;
                    records.push(record);
                }
                Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                    // Continue trying until time window expires
                    if records.is_empty() && start_time.elapsed() < duration {
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        // Reset batch start time for next batch
        self.current_batch_start = None;
        Ok(records)
    }

    /// Read records with adaptive batch sizing
    async fn read_adaptive(
        &mut self,
        min_size: usize,
        max_size: usize,
        target_latency: Duration,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let batch_start = Instant::now();
        let current_size = self
            .adaptive_state
            .current_size
            .clamp(min_size, max_size.min(self.batch_config.max_batch_size));

        // Read current adaptive batch size
        let records = self.read_fixed_size(current_size).await?;
        let batch_latency = batch_start.elapsed();

        // Record latency for adaptive adjustment
        self.adaptive_state.record_latency(batch_latency);

        // Adjust batch size every 10 seconds
        if self.adaptive_state.last_adjustment.elapsed() > Duration::from_secs(10) {
            if let Some(avg_latency) = self.adaptive_state.average_latency() {
                if avg_latency > target_latency && current_size > min_size {
                    // Too slow, reduce batch size
                    self.adaptive_state.current_size = (current_size as f64 * 0.8) as usize;
                    self.adaptive_state.current_size =
                        self.adaptive_state.current_size.max(min_size);
                } else if avg_latency < target_latency / 2 && current_size < max_size {
                    // Too fast, increase batch size
                    self.adaptive_state.current_size = (current_size as f64 * 1.2) as usize;
                    self.adaptive_state.current_size =
                        self.adaptive_state.current_size.min(max_size);
                }
            }
            self.adaptive_state.last_adjustment = Instant::now();
        }

        Ok(records)
    }

    /// Read records up to a memory limit (approximate)
    /// FR-081 Phase 2B: Performance-optimized memory-bounded batch reading
    async fn read_memory_based(
        &mut self,
        max_bytes: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        use futures::StreamExt;
        use tokio::time::timeout;

        let mut records = Vec::new();
        let mut estimated_size = 0usize;
        let timeout_duration = Duration::from_millis(1000);

        // FR-081: Use buffered streaming for memory-based batching (better throughput)
        let mut stream: MessageStream<'_> = Box::pin(self.consumer.buffered_stream(100));

        while estimated_size < max_bytes && records.len() < self.batch_config.max_batch_size {
            match timeout(timeout_duration, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    let record = self.create_stream_record(message)?;

                    // Rough estimate: 24 bytes overhead + field data
                    let record_size = 24
                        + record
                            .fields
                            .iter()
                            .map(|(k, v)| k.len() + self.estimate_field_size(v))
                            .sum::<usize>();

                    if estimated_size + record_size > max_bytes && !records.is_empty() {
                        // Would exceed memory limit, return current batch
                        break;
                    }

                    estimated_size += record_size;
                    records.push(record);
                }
                Ok(Some(Err(_))) | Ok(None) | Err(_) => {
                    // Timeout or error - return what we have
                    if !records.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(records)
    }

    /// Read records with low-latency optimization
    /// FR-081 Phase 2B: Performance-optimized low-latency batch reading
    async fn read_low_latency(
        &mut self,
        max_batch_size: usize,
        max_wait_time: Duration,
        eager_processing: bool,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        use futures::StreamExt;
        use tokio::time::timeout;

        let mut records = Vec::with_capacity(max_batch_size.min(self.batch_config.max_batch_size));
        let start_time = Instant::now();

        // FR-081: Use standard streaming for low-latency (minimal overhead, predictable timing)
        let mut stream: MessageStream<'_> = Box::pin(self.consumer.stream());

        // Low-latency strategy: prioritize immediate processing over batch completeness
        while records.len() < max_batch_size && start_time.elapsed() < max_wait_time {
            let poll_timeout = if eager_processing {
                // Immediate timeout for eager processing
                Duration::from_millis(0)
            } else {
                // Minimal timeout
                std::cmp::min(max_wait_time, Duration::from_millis(1))
            };

            match timeout(poll_timeout, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    let record = self.create_stream_record(message)?;
                    records.push(record);

                    // For eager processing, return immediately after first record
                    if eager_processing && !records.is_empty() {
                        break;
                    }
                }
                Ok(Some(Err(e))) => {
                    // Consumer error - log and break (prioritize low latency)
                    warn!("Consumer error in low-latency mode: {:?}", e);
                    break;
                }
                Ok(None) => {
                    // Stream ended - break immediately
                    log::debug!("Stream ended in low-latency mode");
                    break;
                }
                Err(_) => {
                    // Timeout - break immediately (low latency = no waiting)
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Helper method to create a StreamRecord from a Kafka message
    fn create_stream_record(
        &self,
        mut message: crate::velostream::kafka::message::Message<
            String,
            HashMap<String, FieldValue>,
        >,
    ) -> Result<StreamRecord, Box<dyn Error + Send + Sync>> {
        let mut fields = message.take_value();

        // Add message key to fields map if present
        if let Some(key) = message.take_key() {
            fields.insert("key".to_string(), FieldValue::String(key));
        } else {
            fields.insert("key".to_string(), FieldValue::Null);
        }

        // Extract event_time if configured
        let event_time = if let Some(config) = &self.event_time_config {
            use crate::velostream::datasource::extract_event_time;
            match extract_event_time(&fields, config) {
                Ok(dt) => Some(dt),
                Err(e) => {
                    log::warn!("Failed to extract event_time: {}. Falling back to None", e);
                    None
                }
            }
        } else {
            None
        };

        let kafka_timestamp = message.timestamp();
        let record_timestamp =
            kafka_timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        log::debug!(
            "KafkaReader: Creating StreamRecord with timestamp={} (kafka_ts={:?}, offset={}, partition={})",
            record_timestamp,
            kafka_timestamp,
            message.offset(),
            message.partition()
        );

        Ok(StreamRecord {
            fields,
            timestamp: record_timestamp,
            offset: message.offset(),
            partition: message.partition(),
            headers: message.take_headers().into_map(),
            event_time,
        })
    }

    /// Estimate memory size of a FieldValue
    fn estimate_field_size(&self, field: &FieldValue) -> usize {
        match field {
            FieldValue::String(s) => s.len(),
            FieldValue::Integer(_) => 8,
            FieldValue::Float(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::ScaledInteger(_, _) => 16,
            FieldValue::Timestamp(_) => 16,
            FieldValue::Date(_) => 8,
            FieldValue::Decimal(_) => 16,
            FieldValue::Null => 0,
            FieldValue::Interval { .. } => 16,
            FieldValue::Array(arr) => {
                24 + arr
                    .iter()
                    .map(|v| self.estimate_field_size(v))
                    .sum::<usize>()
            }
            FieldValue::Map(map) => {
                24 + map
                    .iter()
                    .map(|(k, v)| k.len() + self.estimate_field_size(v))
                    .sum::<usize>()
            }
            FieldValue::Struct(s) => {
                24 + s
                    .iter()
                    .map(|(k, v)| k.len() + self.estimate_field_size(v))
                    .sum::<usize>()
            }
        }
    }
}
