//! Unified Kafka data writer implementation
//!
//! Supports two producer modes:
//! - **Async mode** (default): High-throughput with `BaseProducer` and dedicated poll thread
//! - **Transactional mode**: Exactly-once semantics with `TransactionalPolledProducer`
//!
//! Transactional mode is automatically enabled when `transactional.id` is provided in properties.

use crate::velostream::datasource::config::unified::{ConfigFactory, ConfigLogger};
use crate::velostream::datasource::{BatchConfig, DataWriter};
use crate::velostream::kafka::common_config::apply_broker_address_family;
use crate::velostream::kafka::kafka_fast_producer::{PolledProducer, TransactionalPolledProducer};
use crate::velostream::serialization::helpers::{
    create_avro_codec, create_protobuf_codec, field_value_to_json,
};
use crate::velostream::sql::execution::types::StreamRecord;
use async_trait::async_trait;
use rdkafka::{
    ClientConfig,
    producer::{BaseProducer, BaseRecord, DefaultProducerContext, Producer},
};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Internal enum to hold either async BaseProducer or TransactionalPolledProducer
///
/// This enables KafkaDataWriter to support both high-throughput async mode
/// and exactly-once transactional mode based on configuration.
enum ProducerKind {
    /// Async mode: BaseProducer with dedicated poll thread for high throughput
    Async {
        producer: Arc<BaseProducer<DefaultProducerContext>>,
        poll_stop: Arc<AtomicBool>,
        poll_thread: Option<JoinHandle<()>>,
    },
    /// Transactional mode: TransactionalPolledProducer for exactly-once semantics
    Transactional(TransactionalPolledProducer),
}

use crate::velostream::kafka::serialization_format::SerializationFormat;

// Import the codec implementations via type aliases
use crate::velostream::serialization::avro_codec::AvroCodec;
use crate::velostream::serialization::protobuf_codec::ProtobufCodec;

/// Unified Kafka DataWriter that handles all serialization formats
///
/// Supports two producer modes based on configuration:
///
/// ## Async Mode (default)
/// Uses `BaseProducer` for maximum throughput:
/// - Sync send() queues messages immediately (non-blocking)
/// - Internal librdkafka batching optimizes network I/O
/// - **Dedicated poll thread** for async delivery confirmation
/// - One producer instance, many sends = highest performance
///
/// ## Transactional Mode
/// Automatically enabled when `transactional.id` is provided in properties:
/// - Uses `TransactionalPolledProducer` for exactly-once semantics
/// - All operations serialized through dedicated manager thread
/// - `init_transactions()` called automatically at construction
/// - Proper `enable.idempotence=true` and `acks=all` settings
///
/// ## Configuration
/// To enable transactional mode, include `transactional.id` in properties:
/// ```yaml
/// transactional.id: "my-app-producer-1"
/// ```
pub struct KafkaDataWriter {
    /// Internal producer - either async BaseProducer or TransactionalPolledProducer
    producer_kind: ProducerKind,
    topic: String,
    format: SerializationFormat,
    /// Primary key fields from SQL PRIMARY KEY annotation (for Kafka message key)
    /// Multiple fields are joined with pipe delimiter for compound keys
    primary_keys: Option<Vec<String>>,
    avro_codec: Option<AvroCodec>,
    protobuf_codec: Option<ProtobufCodec>,
    /// Track if we've warned about null keys (warn once per writer instance)
    warned_null_key: std::sync::atomic::AtomicBool,
}

impl KafkaDataWriter {
    /// Create a new Kafka data writer with schema configuration
    /// This is the unified method that handles all format requirements
    pub async fn new_with_config(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        primary_keys: Option<Vec<String>>,
        schema: Option<&str>, // Unified schema parameter
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            primary_keys,
            schema,
            &HashMap::new(),
            None,
        )
        .await
    }

    /// Create from HashMap properties (similar to KafkaDataReader pattern)
    pub async fn from_properties(
        brokers: &str,
        topic: String,
        properties: &HashMap<String, String>,
        primary_keys: Option<Vec<String>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Extract format from properties
        let format_str = properties
            .get("value.serializer")
            .or_else(|| properties.get("schema.value.serializer"))
            .or_else(|| properties.get("serializer.format"))
            .or_else(|| properties.get("format"))
            .map(|s| s.as_str())
            .unwrap_or("json");

        let format = Self::parse_serialization_format(format_str);

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            primary_keys,
            schema.as_deref(),
            properties,
            None,
        )
        .await
    }

    /// Create from HashMap properties with batch configuration optimizations
    pub async fn from_properties_with_batch_config(
        brokers: &str,
        topic: String,
        properties: &HashMap<String, String>,
        batch_config: BatchConfig,
        primary_keys: Option<Vec<String>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Extract format from properties
        let format_str = properties
            .get("value.serializer")
            .or_else(|| properties.get("schema.value.serializer"))
            .or_else(|| properties.get("serializer.format"))
            .or_else(|| properties.get("format"))
            .map(|s| s.as_str())
            .unwrap_or("json");

        let format = Self::parse_serialization_format(format_str);

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            primary_keys,
            schema.as_deref(),
            properties,
            Some(batch_config),
        )
        .await
    }

    /// Internal method with schema validation and batch configuration support
    async fn create_with_schema_validation_and_batch_config(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        primary_keys: Option<Vec<String>>,
        schema: Option<&str>,
        properties: &HashMap<String, String>,
        batch_config: Option<BatchConfig>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // FAIL FAST: Validate topic is properly configured
        Self::validate_topic_configuration(&topic)?;

        // Validate schema requirements based on format
        Self::validate_schema_requirements(&format, schema)?;

        // Filter properties to separate metadata/schema from producer config
        let (valid_props, skipped_props) = Self::filter_producer_properties(properties);
        Self::log_producer_properties(&valid_props, &skipped_props);

        // Create optimized producer configuration using unified system
        let mut filtered_properties = HashMap::new();
        for (key, value) in valid_props {
            filtered_properties.insert(key, value);
        }

        // Check for transactional.id to determine producer mode
        let transactional_id = properties
            .get("transactional.id")
            .or_else(|| properties.get("transactional_id"))
            .cloned();

        // Initialize codec based on format using optimized single codec creation
        let (avro_codec, protobuf_codec) = match &format {
            SerializationFormat::Avro { .. } => {
                let codec = Self::create_avro_codec_with_defaults(schema)?;
                (Some(codec), None)
            }
            SerializationFormat::Protobuf { .. } => {
                let codec = Self::create_protobuf_codec_with_defaults(schema)?;
                (None, Some(codec))
            }
            _ => (None, None),
        };

        // Create appropriate producer based on transactional.id presence
        let producer_kind = if let Some(txn_id) = transactional_id.clone() {
            // Transactional mode: use TransactionalPolledProducer
            log::info!(
                "KafkaDataWriter: Creating TRANSACTIONAL producer for topic '{}' with transactional.id='{}'",
                topic,
                txn_id
            );

            // Try to create transactional producer with 10-second timeout
            match TransactionalPolledProducer::with_config(
                brokers,
                &txn_id,
                Duration::from_secs(30), // init_transactions timeout
                Some(&filtered_properties),
            ) {
                Ok(txn_producer) => {
                    log::info!("KafkaDataWriter: Transactional producer initialized successfully");
                    ProducerKind::Transactional(txn_producer)
                }
                Err(e) => {
                    // Check if fallback is allowed (for testing with testcontainers)
                    let allow_fallback = std::env::var("VELOSTREAM_ALLOW_TRANSACTIONAL_FALLBACK")
                        .map(|v| v == "true" || v == "1")
                        .unwrap_or(false);

                    if allow_fallback {
                        log::warn!(
                            "KafkaDataWriter: Transactional producer init failed for topic '{}': {}. \
                             Falling back to non-transactional (VELOSTREAM_ALLOW_TRANSACTIONAL_FALLBACK=true). \
                             WARNING: Exactly-once semantics are NOT guaranteed!",
                            topic,
                            e
                        );
                        Self::create_non_transactional_producer(
                            brokers,
                            &topic,
                            batch_config.as_ref(),
                            &filtered_properties,
                        )?
                    } else {
                        // Default: fail if transactional mode was requested but init fails
                        // This ensures exactly-once guarantees are not silently broken
                        return Err(format!(
                            "Transactional producer initialization failed for topic '{}': {}. \
                             Transactional mode was explicitly requested (transactional.id='{}') but \
                             init_transactions() failed. This would break exactly-once semantics. \
                             To allow fallback to non-transactional mode (for testing), set \
                             VELOSTREAM_ALLOW_TRANSACTIONAL_FALLBACK=true",
                            topic, e, txn_id
                        ).into());
                    }
                }
            }
        } else {
            // Non-transactional mode: use the helper to create async producer
            Self::create_non_transactional_producer(
                brokers,
                &topic,
                batch_config.as_ref(),
                &filtered_properties,
            )?
        };

        let is_transactional = matches!(producer_kind, ProducerKind::Transactional(_));
        let writer = Self {
            producer_kind,
            topic: topic.clone(),
            format,
            primary_keys, // No default - if not configured, records use round-robin partitioning
            avro_codec,
            protobuf_codec,
            warned_null_key: std::sync::atomic::AtomicBool::new(false),
        };

        log::info!(
            "KafkaDataWriter: Initialized writer for topic '{}' with format={:?}, primary_keys={:?}, transactional={}",
            topic,
            writer.format,
            writer.primary_keys,
            is_transactional
        );

        Ok(writer)
    }

    /// Create a non-transactional async producer
    ///
    /// This is used both for normal non-transactional mode and as a fallback
    /// when transactional producer initialization fails.
    fn create_non_transactional_producer(
        brokers: &str,
        topic: &str,
        batch_config: Option<&crate::velostream::datasource::BatchConfig>,
        properties: &HashMap<String, String>,
    ) -> Result<ProducerKind, Box<dyn Error + Send + Sync>> {
        // Filter out transactional properties that would conflict with non-transactional mode
        let mut filtered_props: HashMap<String, String> = properties
            .iter()
            .filter(|(k, _)| {
                !k.starts_with("transactional.")
                    && k.as_str() != "transactional.id"
                    && k.as_str() != "enable.idempotence"
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Explicitly set non-transactional settings
        filtered_props.insert("enable.idempotence".to_string(), "false".to_string());

        // Async mode: use BaseProducer with poll thread for max throughput
        let producer_config = if let Some(batch_config) = batch_config {
            let config =
                ConfigFactory::create_kafka_producer_config(brokers, &filtered_props, batch_config);
            ConfigLogger::log_kafka_producer_config(&config, batch_config, topic, brokers);
            config
        } else {
            let mut config = HashMap::new();
            config.insert("bootstrap.servers".to_string(), brokers.to_string());
            for (key, value) in filtered_props.iter() {
                config.insert(key.clone(), value.clone());
            }
            config
        };

        // Create Kafka ClientConfig from our optimized configuration
        let mut client_config = ClientConfig::new();
        for (key, value) in &producer_config {
            client_config.set(key, value);
        }

        // Add high-throughput BaseProducer settings (if not already set by batch config)
        if !producer_config.contains_key("queue.buffering.max.kbytes") {
            client_config.set("queue.buffering.max.kbytes", "1048576"); // 1GB queue
        }
        if !producer_config.contains_key("queue.buffering.max.messages") {
            client_config.set("queue.buffering.max.messages", "500000");
        }
        if !producer_config.contains_key("batch.size") {
            client_config.set("batch.size", "1048576"); // 1MB batches
        }
        if !producer_config.contains_key("linger.ms") {
            client_config.set("linger.ms", "5"); // Small delay for batching
        }
        if !producer_config.contains_key("compression.type") {
            client_config.set("compression.type", "lz4");
        }
        if !producer_config.contains_key("acks") {
            client_config.set("acks", "1"); // Leader only (faster flush)
        }
        if !producer_config.contains_key("enable.idempotence") {
            client_config.set("enable.idempotence", "false"); // Disable for max throughput
        }

        // Configure broker address family (env: VELOSTREAM_BROKER_ADDRESS_FAMILY, default: v4)
        apply_broker_address_family(&mut client_config);

        // Create high-performance BaseProducer (sync, internal batching)
        let producer: BaseProducer<DefaultProducerContext> = client_config.create()?;
        let producer = Arc::new(producer);

        // Create poll control and spawn initial poll thread
        let poll_stop = Arc::new(AtomicBool::new(false));
        let poll_thread = Self::spawn_poll_thread(
            Arc::clone(&producer),
            Arc::clone(&poll_stop),
            topic.to_string(),
        );

        log::info!(
            "KafkaDataWriter: Created ASYNC producer for topic '{}' with dedicated poll thread",
            topic
        );

        Ok(ProducerKind::Async {
            producer,
            poll_stop,
            poll_thread: Some(poll_thread),
        })
    }

    /// FAIL FAST: Validate topic configuration to prevent silent data loss
    ///
    /// This prevents the catastrophic bug where records are processed successfully
    /// but written to a misconfigured topic that doesn't exist, causing silent data
    /// loss with no error messages.
    fn validate_topic_configuration(topic: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Check for empty topic name - ALWAYS fail
        if topic.is_empty() {
            return Err("CONFIGURATION ERROR: Kafka sink topic name is empty.\n\
                 \n\
                 A valid Kafka topic name MUST be configured. Please configure via:\n\
                 1. YAML config file: 'topic.name: <topic_name>'\n\
                 2. SQL properties: '<sink_name>.topic = <topic_name>'\n\
                 3. Direct parameter when creating KafkaDataWriter\n\
                 \n\
                 This validation prevents silent data loss from writing to misconfigured topics."
                .into());
        }

        // Warn about suspicious topic names that might indicate misconfiguration
        // These are common fallback/placeholder values that suggest config wasn't loaded
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
                "CONFIGURATION ERROR: Kafka sink configured with suspicious topic name '{}'.\n\
                 \n\
                 This is a common placeholder/fallback value that indicates configuration \
                 was not properly loaded.\n\
                 \n\
                 Valid topic names should be:\n\
                 1. Extracted from sink name in SQL: CREATE STREAM <sink_name> ...\n\
                 2. Configured in YAML: 'topic: <topic_name>' or 'topic.name: <topic_name>'\n\
                 \n\
                 Common misconfiguration causes:\n\
                 - YAML file not found or not loaded\n\
                 - Missing 'topic' or 'topic.name' in YAML\n\
                 - Hardcoded fallback value not updated\n\
                 \n\
                 This validation prevents silent data loss from writing to misconfigured topics.",
                topic
            )
            .into());
        }

        log::info!(
            "KafkaDataWriter: Topic validation passed - will write to topic '{}'",
            topic
        );

        Ok(())
    }

    /// Validate that required schemas are provided for formats that need them
    fn validate_schema_requirements(
        format: &SerializationFormat,
        schema: Option<&str>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match format {
            SerializationFormat::Avro { .. } => {
                // Avro can work with a default schema if none provided
                Ok(())
            }
            SerializationFormat::Protobuf { .. } => {
                if schema.is_none() {
                    // Protobuf can use default schema for backward compatibility
                    // but log a warning
                    log::warn!("No Protobuf schema provided, using default generic schema");
                }
                Ok(())
            }
            SerializationFormat::Json
            | SerializationFormat::Bytes
            | SerializationFormat::String => {
                // JSON doesn't require schemas
                Ok(())
            }
        }
    }

    /// Create Avro codec with schema and defaults for writer (wrapper around helper)
    fn create_avro_codec_with_defaults(
        schema: Option<&str>,
    ) -> Result<AvroCodec, Box<dyn Error + Send + Sync>> {
        if let Some(schema_json) = schema {
            create_avro_codec(Some(schema_json))
        } else {
            // Use a generic schema for StreamRecord (writer-specific behavior)
            let default_schema = Self::get_default_avro_schema();
            create_avro_codec(Some(default_schema))
        }
    }

    /// Create Protobuf codec with schema and defaults for writer (wrapper around helper)
    fn create_protobuf_codec_with_defaults(
        schema: Option<&str>,
    ) -> Result<ProtobufCodec, Box<dyn Error + Send + Sync>> {
        if let Some(proto_schema) = schema {
            log::debug!(
                "KafkaDataWriter: Creating Protobuf codec with provided schema ({} bytes)",
                proto_schema.len()
            );
            create_protobuf_codec(Some(proto_schema))
        } else {
            // Use default schema for backward compatibility (writer-specific behavior)
            log::warn!(
                "KafkaDataWriter: No Protobuf schema provided, using default generic schema"
            );
            Ok(ProtobufCodec::new_with_default_schema())
        }
    }

    /// Extract schema from properties based on serialization format
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
                log::debug!(
                    "KafkaDataWriter: Extracting Protobuf schema, {} properties available",
                    properties.len()
                );

                let schema = properties
                    .get("protobuf.schema")
                    .or_else(|| properties.get("value.protobuf.schema"))
                    .or_else(|| properties.get("schema.protobuf"))
                    .or_else(|| properties.get("protobuf_schema"))
                    .or_else(|| properties.get("proto.schema"))
                    .cloned();

                if schema.is_some() {
                    log::debug!("KafkaDataWriter: Found inline protobuf schema");
                    return Ok(schema);
                }

                // Check for schema file path (dot notation preferred, underscore fallback)
                log::debug!("KafkaDataWriter: No inline schema, checking for schema file...");
                if let Some(schema_file) = properties
                    .get("protobuf.schema.file")
                    .or_else(|| properties.get("proto.schema.file"))
                    .or_else(|| properties.get("schema.value.schema.file"))
                    .or_else(|| properties.get("value.schema.file"))
                    .or_else(|| properties.get("schema.file"))
                    .or_else(|| properties.get("protobuf_schema_file"))
                    .or_else(|| properties.get("schema_file"))
                {
                    log::info!(
                        "KafkaDataWriter: Loading protobuf schema from file: {}",
                        schema_file
                    );
                    return Self::load_schema_from_file(schema_file);
                }

                log::warn!(
                    "KafkaDataWriter: No protobuf schema or schema file found in properties"
                );
                Ok(None)
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
        log::debug!(
            "KafkaDataWriter: Attempting to load schema from file: {}",
            file_path
        );
        match fs::read_to_string(file_path) {
            Ok(content) => {
                log::info!(
                    "KafkaDataWriter: Successfully loaded schema from file '{}' ({} bytes)",
                    file_path,
                    content.len()
                );
                Ok(Some(content))
            }
            Err(e) => {
                log::error!(
                    "KafkaDataWriter: Failed to load schema from file '{}': {}",
                    file_path,
                    e
                );
                Err(format!("Failed to load schema from file '{}': {}", file_path, e).into())
            }
        }
    }

    /// Parse serialization format string
    fn parse_serialization_format(format_str: &str) -> SerializationFormat {
        use std::str::FromStr;
        SerializationFormat::from_str(format_str).unwrap_or(SerializationFormat::Json)
    }

    /// Get default Avro schema for generic StreamRecord
    fn get_default_avro_schema() -> &'static str {
        r#"{
            "type": "record",
            "name": "StreamRecord",
            "fields": [
                {"name": "timestamp", "type": "long"},
                {"name": "offset", "type": "long"},
                {"name": "partition", "type": "int"},
                {"name": "data", "type": ["null", "string"], "default": null}
            ]
        }"#
    }

    // =========================================================================
    // Poll Thread Management
    // =========================================================================

    /// Spawn a new poll thread for async delivery confirmation
    ///
    /// The poll thread runs poll(0) to process delivery callbacks with adaptive
    /// backoff based on `in_flight_count()`: tight-loops when messages are in
    /// flight, backs off to 1ms then 10ms sleep when idle.
    /// This is stopped during flush() to avoid contention, then restarted after.
    fn spawn_poll_thread(
        producer: Arc<BaseProducer<DefaultProducerContext>>,
        poll_stop: Arc<AtomicBool>,
        topic: String,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            log::debug!("KafkaDataWriter: Poll thread started for topic '{}'", topic);
            let mut idle_streak: u32 = 0;
            while !poll_stop.load(Ordering::Relaxed) {
                producer.poll(Duration::from_millis(0));

                if producer.in_flight_count() == 0 {
                    idle_streak = idle_streak.saturating_add(1);
                    let sleep_ms = match idle_streak {
                        0..=100 => 0,
                        101..=1000 => 1,
                        _ => 10,
                    };
                    if sleep_ms > 0 {
                        thread::sleep(Duration::from_millis(sleep_ms));
                    }
                } else {
                    idle_streak = 0;
                }
            }
            // Drain remaining callbacks before exit
            producer.poll(Duration::from_millis(100));
            log::debug!("KafkaDataWriter: Poll thread stopped for topic '{}'", topic);
        })
    }

    /// Stop the poll thread and wait for it to exit (Async mode only)
    ///
    /// Must be called before transaction operations to prevent contention
    /// with the transaction coordinator.
    fn stop_poll_thread(&mut self) {
        if let ProducerKind::Async {
            poll_stop,
            poll_thread,
            ..
        } = &mut self.producer_kind
        {
            poll_stop.store(true, Ordering::SeqCst);
            if let Some(handle) = poll_thread.take() {
                let _ = handle.join();
            }
        }
        // TransactionalPolledProducer manages its own thread - no action needed
    }

    /// Restart the poll thread after transaction operations complete (Async mode only)
    fn restart_poll_thread(&mut self) {
        if let ProducerKind::Async {
            producer,
            poll_stop,
            poll_thread,
        } = &mut self.producer_kind
        {
            *poll_stop = Arc::new(AtomicBool::new(false));
            *poll_thread = Some(Self::spawn_poll_thread(
                Arc::clone(producer),
                Arc::clone(poll_stop),
                self.topic.clone(),
            ));
        }
        // TransactionalPolledProducer manages its own thread - no action needed
    }

    /// Check if this writer is in transactional mode
    fn is_transactional(&self) -> bool {
        matches!(self.producer_kind, ProducerKind::Transactional(_))
    }

    /// Internal: Send a record to the producer (Async mode only)
    ///
    /// For Async mode, directly calls producer.send().
    /// For Transactional mode, this method should not be called - use send_transactional() instead.
    #[allow(clippy::result_large_err)] // Error type is from rdkafka and cannot be changed
    fn send_async_record<'a, K, P>(
        &self,
        record: BaseRecord<'a, K, P>,
    ) -> Result<(), (rdkafka::error::KafkaError, BaseRecord<'a, K, P>)>
    where
        K: rdkafka::message::ToBytes + ?Sized,
        P: rdkafka::message::ToBytes + ?Sized,
    {
        match &self.producer_kind {
            ProducerKind::Async { producer, .. } => producer.send(record),
            ProducerKind::Transactional(_) => {
                // Should not reach here - transactional mode uses send_transactional
                panic!(
                    "send_async_record called on transactional producer - use send_transactional instead"
                )
            }
        }
    }

    /// Internal: Send a record via TransactionalPolledProducer
    ///
    /// TransactionalPolledProducer has to copy data to send to its manager thread,
    /// so we need to provide owned data.
    fn send_transactional(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &[u8],
        timestamp_ms: Option<i64>,
    ) -> Result<(), rdkafka::error::KafkaError> {
        match &self.producer_kind {
            ProducerKind::Transactional(txn_producer) => {
                // Build a BaseRecord for the TransactionalPolledProducer
                let mut record = BaseRecord::to(topic).payload(payload);
                if let Some(k) = key {
                    record = record.key(k);
                }
                if let Some(ts) = timestamp_ms {
                    record = record.timestamp(ts);
                }
                match txn_producer.send(record) {
                    Ok(()) => Ok(()),
                    Err((err, _)) => Err(err),
                }
            }
            ProducerKind::Async { .. } => {
                panic!(
                    "send_transactional called on async producer - use send_async_record instead"
                )
            }
        }
    }

    /// Internal: Flush the producer (works with both modes)
    fn flush_producer(&mut self, timeout: Duration) -> Result<(), rdkafka::error::KafkaError> {
        match &mut self.producer_kind {
            ProducerKind::Async { producer, .. } => producer.flush(timeout),
            ProducerKind::Transactional(txn_producer) => txn_producer.flush(timeout),
        }
    }

    // =========================================================================
    // Key Extraction and Serialization
    // =========================================================================

    /// Extract message key from StreamRecord
    ///
    /// Priority:
    /// 1. record.key - set by GROUP BY queries or PRIMARY KEY annotation
    /// 2. primary_keys - field names from SQL PRIMARY KEY annotation
    /// 3. None - null key (round-robin partitioning)
    ///
    /// Key format:
    /// - Single key: raw value (e.g., "AAPL")
    /// - Compound key: pipe-delimited (e.g., "US|Widget")
    fn extract_key(&self, record: &StreamRecord) -> Option<String> {
        // Priority 1: Use record.key if set (from GROUP BY or PRIMARY KEY)
        if let Some(ref key_value) = record.key {
            return Some(key_value.to_key_string());
        }

        // Priority 2: Use configured primary_keys from SQL PRIMARY KEY annotation
        if let Some(ref key_fields) = self.primary_keys {
            if key_fields.len() == 1 {
                // Single key field
                self.extract_single_key_value(record, &key_fields[0])
            } else {
                // Compound key - extract each field and join with pipe delimiter
                let key_parts: Vec<String> = key_fields
                    .iter()
                    .filter_map(|field_name| self.extract_single_key_value(record, field_name))
                    .collect();

                if key_parts.len() == key_fields.len() {
                    // All fields found - return pipe-delimited key
                    Some(key_parts.join("|"))
                } else {
                    // Some fields missing
                    if !self
                        .warned_null_key
                        .swap(true, std::sync::atomic::Ordering::Relaxed)
                    {
                        log::warn!(
                            "Writing record with NULL key to topic '{}' (some key fields missing from compound key {:?}). \
                             Records with NULL keys use round-robin partitioning.",
                            self.topic,
                            key_fields
                        );
                    }
                    None
                }
            }
        } else {
            // No primary_keys configured at all
            if !self
                .warned_null_key
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                log::warn!(
                    "Writing records with NULL keys to topic '{}' (no PRIMARY KEY configured). \
                     Records with NULL keys use round-robin partitioning.",
                    self.topic
                );
            }
            None
        }
    }

    /// Extract a single field value as a key string
    fn extract_single_key_value(&self, record: &StreamRecord, field_name: &str) -> Option<String> {
        match record.fields.get(field_name) {
            Some(value) => Some(value.to_key_string()),
            None => {
                if !self
                    .warned_null_key
                    .swap(true, std::sync::atomic::Ordering::Relaxed)
                {
                    log::warn!(
                        "Writing record with NULL key to topic '{}' (PRIMARY KEY field '{}' is null/missing). \
                         Records with NULL keys use round-robin partitioning.",
                        self.topic,
                        field_name
                    );
                }
                None
            }
        }
    }

    /// Convert StreamRecord to appropriate payload format (consolidated serialization logic)
    fn serialize_payload(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        match self.format {
            SerializationFormat::Json => self.serialize_json(record),
            SerializationFormat::Avro { .. } => self.serialize_avro(record),
            SerializationFormat::Protobuf { .. } => self.serialize_protobuf(record),
            SerializationFormat::Bytes | SerializationFormat::String => self.serialize_json(record), // Default to JSON
        }
    }

    /// Serialize to JSON format using direct serialization (no intermediate Value allocation)
    ///
    /// Uses sonic-rs SIMD-accelerated serialization directly on record.fields.
    /// This is consistent with Avro/Protobuf serialization - all formats serialize
    /// the fields HashMap as-is, preserving any upstream metadata (like _timestamp).
    ///
    /// Note: We do NOT inject _timestamp, _offset, _partition here because:
    /// - _offset and _partition are meaningless to downstream consumers (they have their own)
    /// - _timestamp should be preserved from upstream if present in fields
    fn serialize_json(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        // Direct serialization with sonic-rs SIMD acceleration
        // Same approach as Avro/Protobuf - just serialize the fields
        sonic_rs::to_vec(&record.fields).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    /// Serialize to Avro format
    fn serialize_avro(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        if let Some(codec) = &self.avro_codec {
            // Use proper Avro serialization with just the fields (no metadata)
            codec
                .serialize(&record.fields)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
        } else {
            // Fallback to JSON if codec not available
            self.serialize_json(record)
        }
    }

    /// Serialize to Protobuf format
    fn serialize_protobuf(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        if let Some(codec) = &self.protobuf_codec {
            // Use proper Protobuf serialization with just the fields (no metadata)
            codec
                .serialize(&record.fields)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
        } else {
            // Fallback to JSON if codec not available
            self.serialize_json(record)
        }
    }

    /// Build JSON payload with optional metadata (consolidated JSON conversion)
    fn build_json_payload(
        &self,
        record: &StreamRecord,
        include_metadata: bool,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let mut json_obj = serde_json::Map::new();

        // Convert all fields to JSON (key field included for complete payloads)
        for (field_name, field_value) in &record.fields {
            let json_value = field_value_to_json(field_value)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
            json_obj.insert(field_name.clone(), json_value);
        }

        // Add metadata fields for JSON format only
        if include_metadata {
            self.add_metadata_to_json(&mut json_obj, record);
        }

        Ok(Value::Object(json_obj))
    }

    /// Add metadata fields to JSON object
    fn add_metadata_to_json(
        &self,
        json_obj: &mut serde_json::Map<String, Value>,
        record: &StreamRecord,
    ) {
        json_obj.insert(
            "_timestamp".to_string(),
            Value::Number(serde_json::Number::from(record.timestamp)),
        );
        json_obj.insert(
            "_offset".to_string(),
            Value::Number(serde_json::Number::from(record.offset)),
        );
        json_obj.insert(
            "_partition".to_string(),
            Value::Number(serde_json::Number::from(record.partition)),
        );
    }

    /// Convert headers from StreamRecord to Kafka headers
    fn convert_headers(&self, headers: &HashMap<String, String>) -> Vec<(String, Vec<u8>)> {
        headers
            .iter()
            .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
            .collect()
    }

    /// Valid librdkafka producer configuration properties (allowlist approach)
    /// Source: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    fn valid_producer_properties() -> std::collections::HashSet<&'static str> {
        [
            // Global configuration
            "bootstrap.servers",
            "client.id",
            "metadata.broker.list",
            "message.max.bytes",
            "message.copy.max.bytes",
            "receive.message.max.bytes",
            "max.in.flight.requests.per.connection",
            "max.in.flight",
            "topic.metadata.refresh.interval.ms",
            "metadata.max.age.ms",
            "topic.metadata.refresh.fast.interval.ms",
            "topic.metadata.refresh.sparse",
            "topic.metadata.propagation.max.ms",
            "topic.blacklist",
            "debug",
            "socket.timeout.ms",
            "socket.blocking.max.ms",
            "socket.send.buffer.bytes",
            "socket.receive.buffer.bytes",
            "socket.keepalive.enable",
            "socket.nagle.disable",
            "socket.max.fails",
            "broker.address.ttl",
            "broker.address.family",
            "reconnect.backoff.jitter.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
            "statistics.interval.ms",
            "log_level",
            "log.queue",
            "log.thread.name",
            "log.connection.close",
            "api.version.request",
            "api.version.request.timeout.ms",
            "api.version.fallback.ms",
            "broker.version.fallback",
            // Security
            "security.protocol",
            "ssl.cipher.suites",
            "ssl.curves.list",
            "ssl.sigalgs.list",
            "ssl.key.location",
            "ssl.key.password",
            "ssl.key.pem",
            "ssl.certificate.location",
            "ssl.certificate.pem",
            "ssl.ca.location",
            "ssl.ca.pem",
            "ssl.ca.certificate.stores",
            "ssl.crl.location",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.providers",
            "ssl.engine.location",
            "enable.ssl.certificate.verification",
            "ssl.endpoint.identification.algorithm",
            // SASL
            "sasl.mechanisms",
            "sasl.mechanism",
            "sasl.kerberos.service.name",
            "sasl.kerberos.principal",
            "sasl.kerberos.kinit.cmd",
            "sasl.kerberos.keytab",
            "sasl.kerberos.min.time.before.relogin",
            "sasl.username",
            "sasl.password",
            "sasl.oauthbearer.config",
            "enable.sasl.oauthbearer.unsecure.jwt",
            "sasl.oauthbearer.method",
            "sasl.oauthbearer.client.id",
            "sasl.oauthbearer.client.secret",
            "sasl.oauthbearer.scope",
            "sasl.oauthbearer.extensions",
            "sasl.oauthbearer.token.endpoint.url",
            // Producer specific
            "transactional.id",
            "transaction.timeout.ms",
            "enable.idempotence",
            "enable.gapless.guarantee",
            "queue.buffering.max.messages",
            "queue.buffering.max.kbytes",
            "queue.buffering.max.ms",
            "linger.ms",
            "message.send.max.retries",
            "retries",
            "retry.backoff.ms",
            "retry.backoff.max.ms",
            "queue.buffering.backpressure.threshold",
            "compression.codec",
            "compression.type",
            "compression.level",
            "batch.num.messages",
            "batch.size",
            "delivery.report.only.error",
            "sticky.partitioning.linger.ms",
            "request.required.acks",
            "acks",
            "request.timeout.ms",
            "message.timeout.ms",
            "delivery.timeout.ms",
            "partitioner",
            "partitioner.consistent.random",
            "msg_order_cmp",
            "produce.offset.report",
            // Client rack
            "client.rack",
            // Interceptors
            "plugin.library.paths",
        ]
        .into_iter()
        .collect()
    }

    /// Filter producer properties using allowlist of valid librdkafka properties
    ///
    /// Only passes through properties that are known valid librdkafka producer configuration.
    /// All other properties (schema, format, custom) are skipped.
    fn filter_producer_properties(
        properties: &HashMap<String, String>,
    ) -> (Vec<(String, String)>, Vec<String>) {
        let valid_rdkafka_props = Self::valid_producer_properties();
        let mut valid_props = Vec::new();
        let mut skipped_props = Vec::new();

        for (key, value) in properties.iter() {
            let key_lower = key.to_lowercase();

            if valid_rdkafka_props.contains(key_lower.as_str()) {
                log::debug!(
                    "KafkaDataWriter: Applying producer property: {} = {}",
                    key,
                    value
                );
                valid_props.push((key.clone(), value.clone()));
            } else {
                log::debug!(
                    "KafkaDataWriter: Skipping non-rdkafka property: {} = {}",
                    key,
                    value
                );
                skipped_props.push(key.clone());
            }
        }

        (valid_props, skipped_props)
    }

    /// Log producer properties configuration
    fn log_producer_properties(valid_props: &[(String, String)], skipped_props: &[String]) {
        if !valid_props.is_empty() || !skipped_props.is_empty() {
            log::info!("KafkaDataWriter: Producer Properties Configuration");
            log::info!("  Applied properties ({}):", valid_props.len());
            for (key, value) in valid_props {
                log::info!("    • {}: {}", key, value);
            }

            if !skipped_props.is_empty() {
                log::debug!("  Skipped properties ({}):", skipped_props.len());
                for key in skipped_props {
                    log::debug!("    • {} (metadata/schema/datasource)", key);
                }
            }
        }
    }
}

#[async_trait]
impl DataWriter for KafkaDataWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Per-record logging at TRACE level to avoid hot path overhead
        log::trace!(
            "KafkaDataWriter: Sending record to topic '{}', format={:?}",
            self.topic,
            self.format
        );

        // Extract key for partitioning
        let key = self.extract_key(&record);

        // Serialize payload based on format
        let payload = self.serialize_payload(&record).map_err(|e| {
            log::error!(
                "🚨 SERIALIZATION FAILURE on topic '{}' with format {:?}: {:?}",
                self.topic,
                self.format,
                e
            );
            e
        })?;

        // Convert headers and build owned headers collection, preserving all headers
        let headers = self.convert_headers(&record.headers);

        // Build Kafka record using BaseRecord for high-performance sync send
        let mut kafka_record = BaseRecord::to(&self.topic).payload(&payload);

        // Propagate event_time as the Kafka message timestamp so downstream
        // consumers receive the correct event time via the message metadata.
        // Without this, consumers default to the broker's CreateTime (wall clock).
        if let Some(event_time) = record.event_time {
            kafka_record = kafka_record.timestamp(event_time.timestamp_millis());
        }

        if let Some(key_str) = &key {
            kafka_record = kafka_record.key(key_str);
        }

        // Add headers - accumulate all headers into a single OwnedHeaders to preserve them all
        if !headers.is_empty() {
            let mut owned_headers = rdkafka::message::OwnedHeaders::new();
            for (header_key, header_value) in headers {
                owned_headers = owned_headers.insert(rdkafka::message::Header {
                    key: &header_key,
                    value: Some(&header_value),
                });
            }
            kafka_record = kafka_record.headers(owned_headers);
        }

        // Per-record logging at TRACE level to avoid hot path overhead
        log::trace!(
            "KafkaDataWriter: Sending record to topic '{}' with key {:?}, payload_size={} bytes, format={:?}",
            self.topic,
            key,
            payload.len(),
            self.format
        );

        // Send record - different handling for async vs transactional modes
        let send_result = if self.is_transactional() {
            // Transactional mode: use dedicated send method
            self.send_transactional(
                &self.topic.clone(),
                key.as_deref(),
                &payload,
                record.event_time.map(|et| et.timestamp_millis()),
            )
            .map_err(|e| (e, ()))
        } else {
            // Async mode: use BaseProducer directly
            match &self.producer_kind {
                ProducerKind::Async { producer, .. } => {
                    producer.send(kafka_record).map_err(|(e, _)| (e, ()))
                }
                ProducerKind::Transactional(_) => unreachable!(),
            }
        };

        match send_result {
            Ok(()) => {
                // Message queued successfully - flush to ensure delivery for single writes
                self.flush_producer(Duration::from_secs(5))?;

                log::trace!(
                    "KafkaDataWriter: Successfully sent record to topic '{}'",
                    self.topic
                );
                Ok(())
            }
            Err((kafka_error, _)) => {
                log::error!(
                    "KafkaDataWriter: Failed to queue record to topic '{}': {:?}",
                    self.topic,
                    kafka_error
                );
                log::error!(
                    "KafkaDataWriter: Failed record details - key: {:?}, payload_size: {} bytes, format: {:?}",
                    key,
                    payload.len(),
                    self.format
                );
                Err(Box::new(kafka_error))
            }
        }
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let batch_size = records.len();
        if batch_size == 0 {
            return Ok(());
        }

        log::debug!(
            "KafkaDataWriter: Starting high-performance batch write of {} records to topic '{}', format={:?}",
            batch_size,
            self.topic,
            self.format
        );

        let start = std::time::Instant::now();
        let mut queued = 0usize;
        let mut errors = Vec::new();

        // Phase 1: Queue ALL messages to producer (non-blocking, no awaits!)
        // BaseProducer.send() returns immediately - messages are queued in librdkafka's internal buffer
        for (index, record_arc) in records.iter().enumerate() {
            // Serialize payload
            let payload = match self.serialize_payload(record_arc) {
                Ok(p) => p,
                Err(e) => {
                    log::error!(
                        "🚨 SERIALIZATION FAILURE at record {}/{} on topic '{}': {:?}",
                        index + 1,
                        batch_size,
                        self.topic,
                        e
                    );
                    errors.push((index, format!("serialization: {:?}", e)));
                    continue;
                }
            };

            // Extract key
            let key = self.extract_key(record_arc);

            // Build BaseRecord for high-performance sync send
            let mut kafka_record = BaseRecord::to(&self.topic).payload(&payload);

            // Propagate event_time as Kafka message timestamp
            if let Some(event_time) = record_arc.event_time {
                kafka_record = kafka_record.timestamp(event_time.timestamp_millis());
            }

            if let Some(key_str) = &key {
                kafka_record = kafka_record.key(key_str);
            }

            // Add headers if present
            let headers = self.convert_headers(&record_arc.headers);
            if !headers.is_empty() {
                let mut owned_headers = rdkafka::message::OwnedHeaders::new();
                for (header_key, header_value) in headers {
                    owned_headers = owned_headers.insert(rdkafka::message::Header {
                        key: &header_key,
                        value: Some(&header_value),
                    });
                }
                kafka_record = kafka_record.headers(owned_headers);
            }

            // Queue message - returns immediately (non-blocking!)
            // Different handling for async vs transactional modes
            let send_result = if self.is_transactional() {
                // Transactional mode: use dedicated send method
                self.send_transactional(
                    &self.topic.clone(),
                    key.as_deref(),
                    &payload,
                    record_arc.event_time.map(|et| et.timestamp_millis()),
                )
            } else {
                // Async mode: use BaseProducer directly
                match &self.producer_kind {
                    ProducerKind::Async { producer, .. } => {
                        producer.send(kafka_record).map_err(|(e, _)| e)
                    }
                    ProducerKind::Transactional(_) => unreachable!(),
                }
            };

            match send_result {
                Ok(()) => {
                    queued += 1;
                }
                Err(kafka_error) => {
                    log::error!(
                        "🚨 QUEUE FAILED at record {}/{} on topic '{}': {:?}",
                        index + 1,
                        batch_size,
                        self.topic,
                        kafka_error
                    );
                    errors.push((index, format!("queue: {:?}", kafka_error)));
                }
            }
        }

        let queue_time = start.elapsed();

        // Flush behavior depends on transactional mode:
        // - Transactional: flush synchronously for exactly-once semantics
        // - Non-transactional (async): poll thread handles delivery, no blocking flush
        let flush_time = if self.is_transactional() {
            let flush_start = std::time::Instant::now();
            let flush_result = self.flush_producer(Duration::from_secs(30));

            if let Err(e) = flush_result {
                log::error!(
                    "🚨 FLUSH FAILED after queuing {} records to topic '{}': {:?}",
                    queued,
                    self.topic,
                    e
                );
                return Err(Box::new(e));
            }
            flush_start.elapsed()
        } else {
            // Async mode: poll thread handles delivery, no blocking flush
            Duration::ZERO
        };

        let total_time = queue_time + flush_time;
        let rate = if total_time.as_secs_f64() > 0.0 {
            queued as f64 / total_time.as_secs_f64()
        } else {
            0.0
        };

        if self.is_transactional() {
            log::debug!(
                "KafkaDataWriter: Batch complete (transactional) - {}/{} records to '{}' in {:.2}ms (queue: {:.2}ms, flush: {:.2}ms) = {:.0} rec/s",
                queued,
                batch_size,
                self.topic,
                total_time.as_millis(),
                queue_time.as_millis(),
                flush_time.as_millis(),
                rate
            );
        } else {
            log::debug!(
                "KafkaDataWriter: Batch queued (async) - {}/{} records to '{}' in {:.2}ms = {:.0} rec/s",
                queued,
                batch_size,
                self.topic,
                queue_time.as_millis(),
                rate
            );
        }

        if !errors.is_empty() {
            log::warn!(
                "KafkaDataWriter: {} errors in batch of {} records",
                errors.len(),
                batch_size
            );
        }

        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Kafka doesn't have native update semantics - treat as write
        self.write(record).await
    }

    async fn delete(&mut self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Send tombstone record (empty payload) for deletion
        log::debug!(
            "KafkaDataWriter: Sending tombstone record for key '{}' to topic '{}'",
            key,
            self.topic
        );

        let empty_payload: &[u8] = &[];

        // Different handling for async vs transactional modes
        let send_result = if self.is_transactional() {
            // Transactional mode: use dedicated send method
            self.send_transactional(&self.topic.clone(), Some(key), empty_payload, None)
        } else {
            // Async mode: use BaseProducer directly
            let kafka_record = BaseRecord::to(&self.topic).key(key).payload(empty_payload);
            match &self.producer_kind {
                ProducerKind::Async { producer, .. } => {
                    producer.send(kafka_record).map_err(|(e, _)| e)
                }
                ProducerKind::Transactional(_) => unreachable!(),
            }
        };

        match send_result {
            Ok(()) => {
                self.flush_producer(Duration::from_secs(5))?;

                log::debug!(
                    "KafkaDataWriter: Successfully sent tombstone for key '{}' to topic '{}'",
                    key,
                    self.topic
                );
                Ok(())
            }
            Err(kafka_error) => {
                log::error!(
                    "KafkaDataWriter: Failed to send tombstone for key '{}' to topic '{}': {:?}",
                    key,
                    self.topic,
                    kafka_error
                );
                Err(Box::new(kafka_error))
            }
        }
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Flush pending messages to Kafka
        log::debug!(
            "KafkaDataWriter: Flushing pending messages to topic '{}'",
            self.topic
        );

        match self.flush_producer(Duration::from_secs(30)) {
            Ok(()) => {
                log::debug!(
                    "KafkaDataWriter: Successfully flushed all pending messages to topic '{}'",
                    self.topic
                );
                Ok(())
            }
            Err(e) => {
                log::error!(
                    "KafkaDataWriter: Failed to flush pending messages to topic '{}': {:?}",
                    self.topic,
                    e
                );
                Err(Box::new(e) as Box<dyn Error + Send + Sync>)
            }
        }
    }

    // Transaction support methods
    //
    // Transactional mode: TransactionalPolledProducer handles everything internally
    // Async mode: Not configured for transactions - will return error
    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        match &mut self.producer_kind {
            ProducerKind::Transactional(txn_producer) => {
                // TransactionalPolledProducer already has init_transactions() called at construction
                // Just begin the transaction
                log::debug!(
                    "KafkaDataWriter: Beginning transaction for topic '{}' (transactional mode)",
                    self.topic
                );

                match txn_producer.begin_transaction() {
                    Ok(()) => {
                        log::debug!(
                            "KafkaDataWriter: Transaction begun successfully for topic '{}'",
                            self.topic
                        );
                        Ok(true)
                    }
                    Err(e) => {
                        log::error!(
                            "KafkaDataWriter: Failed to begin transaction for topic '{}': {:?}",
                            self.topic,
                            e
                        );
                        Err(Box::new(e) as Box<dyn Error + Send + Sync>)
                    }
                }
            }
            ProducerKind::Async { .. } => {
                // Async mode without transactional.id cannot do transactions
                log::error!(
                    "KafkaDataWriter: Cannot begin transaction - writer was created without transactional.id. \
                     Configure 'transactional.id' in properties to enable transaction support."
                );
                Err("Transaction support requires transactional.id configuration".into())
            }
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // For non-transactional producers, commit is equivalent to flush
        self.flush().await
    }

    /// Commit the current transaction
    ///
    /// Note: This should only be called if begin_transaction() returned true
    /// and the producer was created with transactional.id
    /// For non-transactional producers, this will return an error
    async fn commit_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &mut self.producer_kind {
            ProducerKind::Transactional(txn_producer) => {
                log::debug!(
                    "KafkaDataWriter: Committing transaction for topic '{}'",
                    self.topic
                );

                match txn_producer.commit_transaction(Duration::from_secs(60)) {
                    Ok(()) => {
                        log::debug!(
                            "KafkaDataWriter: Transaction committed successfully for topic '{}'",
                            self.topic
                        );
                        Ok(())
                    }
                    Err(e) => {
                        log::error!(
                            "KafkaDataWriter: Failed to commit transaction for topic '{}': {:?}",
                            self.topic,
                            e
                        );
                        Err(Box::new(e) as Box<dyn Error + Send + Sync>)
                    }
                }
            }
            ProducerKind::Async { .. } => {
                log::error!(
                    "KafkaDataWriter: Cannot commit transaction - writer was created without transactional.id"
                );
                Err("Transaction support requires transactional.id configuration".into())
            }
        }
    }

    /// Abort the current transaction
    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &mut self.producer_kind {
            ProducerKind::Transactional(txn_producer) => {
                log::debug!(
                    "KafkaDataWriter: Aborting transaction for topic '{}'",
                    self.topic
                );

                match txn_producer.abort_transaction(Duration::from_secs(60)) {
                    Ok(()) => {
                        log::debug!(
                            "KafkaDataWriter: Transaction aborted successfully for topic '{}'",
                            self.topic
                        );
                        Ok(())
                    }
                    Err(e) => {
                        log::error!(
                            "KafkaDataWriter: Failed to abort transaction for topic '{}': {:?}",
                            self.topic,
                            e
                        );
                        Err(Box::new(e) as Box<dyn Error + Send + Sync>)
                    }
                }
            }
            ProducerKind::Async { .. } => {
                log::error!(
                    "KafkaDataWriter: Cannot abort transaction - writer was created without transactional.id"
                );
                Err("Transaction support requires transactional.id configuration".into())
            }
        }
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Non-transactional producers can't rollback
        // Best effort: flush what we can
        self.flush().await
    }

    fn supports_transactions(&self) -> bool {
        // Only report transaction support if the producer was created in transactional mode
        // If we fell back to non-transactional mode, we should not claim transaction support
        self.is_transactional()
    }
}

/// Ensure poll thread is stopped and producer is flushed on drop
impl Drop for KafkaDataWriter {
    fn drop(&mut self) {
        log::debug!(
            "KafkaDataWriter: Dropping writer for topic '{}'",
            self.topic
        );

        match &mut self.producer_kind {
            ProducerKind::Async {
                producer,
                poll_stop,
                poll_thread,
            } => {
                log::debug!(
                    "KafkaDataWriter: Stopping async poll thread for topic '{}'",
                    self.topic
                );

                // Signal poll thread to stop
                poll_stop.store(true, Ordering::Relaxed);

                // Wait for poll thread to finish
                if let Some(Err(e)) = poll_thread.take().map(|h| h.join()) {
                    log::warn!(
                        "KafkaDataWriter: Poll thread panicked during shutdown: {:?}",
                        e
                    );
                }

                // Final flush to ensure all queued messages are delivered
                if let Err(e) = producer.flush(Duration::from_secs(5)) {
                    log::warn!(
                        "KafkaDataWriter: Final flush failed during drop for topic '{}': {:?}",
                        self.topic,
                        e
                    );
                }
            }
            ProducerKind::Transactional(_) => {
                // TransactionalPolledProducer has its own Drop implementation
                // that handles cleanup, including aborting pending transactions
                log::debug!(
                    "KafkaDataWriter: TransactionalPolledProducer will handle cleanup for topic '{}'",
                    self.topic
                );
            }
        }

        log::debug!(
            "KafkaDataWriter: Successfully dropped writer for topic '{}'",
            self.topic
        );
    }
}
