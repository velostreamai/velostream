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
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use rdkafka::{
    ClientConfig,
    producer::{BaseProducer, BaseRecord, DefaultProducerContext, Producer},
};
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
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

/// A wrapper for direct JSON serialization of StreamRecord fields with metadata.
///
/// This avoids the intermediate serde_json::Value allocation, providing 2-4x faster
/// serialization by directly writing to the output.
struct DirectJsonPayload<'a> {
    fields: &'a HashMap<String, FieldValue>,
    exclude_key: Option<&'a str>,
    timestamp: i64,
    offset: i64,
    partition: i32,
}

impl<'a> Serialize for DirectJsonPayload<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Count fields: all fields minus excluded + 3 metadata fields
        let excluded_count = if let Some(key) = self.exclude_key {
            if self.fields.contains_key(key) { 1 } else { 0 }
        } else {
            0
        };
        let field_count = self.fields.len() - excluded_count + 3; // +3 for metadata

        let mut map = serializer.serialize_map(Some(field_count))?;

        // Serialize all fields directly (excluding key field if set)
        for (field_name, field_value) in self.fields {
            if let Some(key) = self.exclude_key {
                if field_name == key {
                    continue;
                }
            }
            map.serialize_entry(field_name, field_value)?;
        }

        // Add metadata fields
        map.serialize_entry("_timestamp", &self.timestamp)?;
        map.serialize_entry("_offset", &self.offset)?;
        map.serialize_entry("_partition", &self.partition)?;

        map.end()
    }
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
    key_field: Option<String>, // Field name to use as message key
    avro_codec: Option<AvroCodec>,
    protobuf_codec: Option<ProtobufCodec>,
}

impl KafkaDataWriter {
    /// Create a new Kafka data writer with schema configuration
    /// This is the unified method that handles all format requirements
    pub async fn new_with_config(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        key_field: Option<String>,
        schema: Option<&str>, // Unified schema parameter
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            key_field,
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

        // Extract key field configuration
        let key_field = properties
            .get("key.field")
            .or_else(|| properties.get("message.key.field"))
            .or_else(|| properties.get("schema.key.field"))
            .cloned();

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            key_field,
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

        // Extract key field configuration
        let key_field = properties
            .get("key.field")
            .or_else(|| properties.get("message.key.field"))
            .or_else(|| properties.get("schema.key.field"))
            .cloned();

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            key_field,
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
        key_field: Option<String>,
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
        let producer_kind = if let Some(txn_id) = transactional_id {
            // Transactional mode: use TransactionalPolledProducer
            log::info!(
                "KafkaDataWriter: Creating TRANSACTIONAL producer for topic '{}' with transactional.id='{}'",
                topic,
                txn_id
            );

            let txn_producer = TransactionalPolledProducer::with_config(
                brokers,
                &txn_id,
                Duration::from_secs(30), // init_transactions timeout
                Some(&filtered_properties),
            )?;

            log::info!(
                "KafkaDataWriter: Transactional producer initialized successfully (enable.idempotence=true, acks=all)"
            );

            ProducerKind::Transactional(txn_producer)
        } else {
            // Async mode: use BaseProducer with poll thread for max throughput
            let producer_config = if let Some(batch_config) = batch_config {
                let config = ConfigFactory::create_kafka_producer_config(
                    brokers,
                    &filtered_properties,
                    &batch_config,
                );
                ConfigLogger::log_kafka_producer_config(&config, &batch_config, &topic, brokers);
                config
            } else {
                let mut config = HashMap::new();
                config.insert("bootstrap.servers".to_string(), brokers.to_string());
                for (key, value) in filtered_properties.iter() {
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
                topic.clone(),
            );

            log::info!(
                "KafkaDataWriter: Created ASYNC producer for topic '{}' with dedicated poll thread",
                topic
            );

            ProducerKind::Async {
                producer,
                poll_stop,
                poll_thread: Some(poll_thread),
            }
        };

        let is_transactional = matches!(producer_kind, ProducerKind::Transactional(_));
        let writer = Self {
            producer_kind,
            topic: topic.clone(),
            format,
            key_field: key_field.or(Some("key".to_string())), // Default to "key" field
            avro_codec,
            protobuf_codec,
        };

        log::info!(
            "KafkaDataWriter: Initialized writer for topic '{}' with format={:?}, key_field={:?}, transactional={}",
            topic,
            writer.format,
            writer.key_field,
            is_transactional
        );

        Ok(writer)
    }

    /// FAIL FAST: Validate topic configuration to prevent silent data loss
    ///
    /// This prevents the catastrophic bug where records are processed successfully
    /// but written to a misconfigured topic that doesn't exist, causing silent data
    /// loss with no error messages.
    fn validate_topic_configuration(topic: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Check for empty topic name - ALWAYS fail
        if topic.is_empty() {
            return Err(format!(
                "CONFIGURATION ERROR: Kafka sink topic name is empty.\n\
                 \n\
                 A valid Kafka topic name MUST be configured. Please configure via:\n\
                 1. YAML config file: 'topic.name: <topic_name>'\n\
                 2. SQL properties: '<sink_name>.topic = <topic_name>'\n\
                 3. Direct parameter when creating KafkaDataWriter\n\
                 \n\
                 This validation prevents silent data loss from writing to misconfigured topics."
            )
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
            create_protobuf_codec(Some(proto_schema))
        } else {
            // Use default schema for backward compatibility (writer-specific behavior)
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
            Ok(content) => Ok(Some(content)),
            Err(e) => Err(format!("Failed to load schema from file '{}': {}", file_path, e).into()),
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
    /// The poll thread runs poll(0) in a tight loop to process delivery callbacks.
    /// This is stopped during flush() to avoid contention, then restarted after.
    fn spawn_poll_thread(
        producer: Arc<BaseProducer<DefaultProducerContext>>,
        poll_stop: Arc<AtomicBool>,
        topic: String,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            log::debug!("KafkaDataWriter: Poll thread started for topic '{}'", topic);
            while !poll_stop.load(Ordering::Relaxed) {
                // poll(0) = process callbacks immediately, no blocking
                // This runs at 100% CPU but is required for maximum throughput
                // to keep up with high-speed message production.
                producer.poll(Duration::from_millis(0));
            }
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
    ) -> Result<(), rdkafka::error::KafkaError> {
        match &self.producer_kind {
            ProducerKind::Transactional(txn_producer) => {
                // Build a BaseRecord for the TransactionalPolledProducer
                let mut record = BaseRecord::to(topic).payload(payload);
                if let Some(k) = key {
                    record = record.key(k);
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

    /// Extract message key from StreamRecord fields
    fn extract_key(&self, record: &StreamRecord) -> Option<String> {
        if let Some(key_field) = &self.key_field {
            match record.fields.get(key_field) {
                Some(FieldValue::String(s)) => Some(s.clone()),
                Some(FieldValue::Integer(i)) => Some(i.to_string()),
                Some(FieldValue::Float(f)) => Some(f.to_string()),
                Some(FieldValue::ScaledInteger(val, scale)) => {
                    // Format as decimal string
                    let divisor = 10_i64.pow(*scale as u32);
                    let integer_part = val / divisor;
                    let fractional_part = (val % divisor).abs();
                    if fractional_part == 0 {
                        Some(integer_part.to_string())
                    } else {
                        let frac_str =
                            format!("{:0width$}", fractional_part, width = *scale as usize);
                        let frac_trimmed = frac_str.trim_end_matches('0');
                        if frac_trimmed.is_empty() {
                            Some(integer_part.to_string())
                        } else {
                            Some(format!("{}.{}", integer_part, frac_trimmed))
                        }
                    }
                }
                Some(FieldValue::Boolean(b)) => Some(b.to_string()),
                Some(FieldValue::Null) | None => None,
                _ => None,
            }
        } else {
            None
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
    /// Uses sonic-rs SIMD-accelerated serialization.
    ///
    /// Performance: 2-4x faster than building serde_json::Value first because:
    /// - No intermediate Value tree allocation
    /// - No string cloning for field values
    /// - Direct serialization from FieldValue to bytes
    /// - SIMD-accelerated output
    fn serialize_json(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        // Create direct payload wrapper - no intermediate allocation
        let payload = DirectJsonPayload {
            fields: &record.fields,
            exclude_key: self.key_field.as_deref(),
            timestamp: record.timestamp,
            offset: record.offset,
            partition: record.partition,
        };

        // Direct serialization with sonic-rs SIMD acceleration
        sonic_rs::to_vec(&payload).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
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

        // Convert all fields to JSON, excluding key field if used as message key
        for (field_name, field_value) in &record.fields {
            if self.should_exclude_field_from_payload(field_name) {
                continue;
            }

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

    /// Check if field should be excluded from payload (e.g., key field used as message key)
    fn should_exclude_field_from_payload(&self, field_name: &str) -> bool {
        if let Some(key_field) = &self.key_field {
            field_name == key_field
        } else {
            false
        }
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

    /// Filter and validate producer properties (aligned with KafkaDataReader pattern)
    ///
    /// Skips metadata, schema, and datasource properties that shouldn't be passed to Kafka producer.
    /// This prevents unknown properties from being silently ignored by the Kafka producer.
    fn filter_producer_properties(
        properties: &HashMap<String, String>,
    ) -> (Vec<(String, String)>, Vec<String>) {
        let mut valid_props = Vec::new();
        let mut skipped_props = Vec::new();

        // Properties that are metadata or handled separately
        let skip_prefixes = [
            "schema.",     // Schema configuration (handled separately)
            "value.",      // Value-specific config (Kafka will interpret these)
            "datasource.", // Datasource-specific config (consumer side)
            "datasink.",   // Datasink-specific config (topic creation, delivery profiles, etc.)
            "avro.",       // Avro schema (handled separately)
            "protobuf.",   // Protobuf schema (handled separately)
            "proto.",      // Proto schema (handled separately)
            "json.",       // JSON schema (handled separately)
        ];

        let skip_exact = [
            "topic",                   // Topic is not a producer property
            "consumer.group",          // Consumer group is not a producer property
            "key.field",               // Key field extraction (handled separately)
            "message.key.field",       // Key field extraction (handled separately)
            "schema.key.field",        // Key field extraction (handled separately)
            "performance_profile",     // Performance profile is metadata
            "format",                  // Format is handled separately
            "serializer.format",       // Format is handled separately
            "value.serializer",        // Format is handled separately
            "schema.value.serializer", // Format is handled separately
            "type", // Sink type identifier from SQL WITH clause (e.g., 'kafka_sink')
        ];

        for (key, value) in properties.iter() {
            let key_lower = key.to_lowercase();

            // Check skip prefixes
            if skip_prefixes
                .iter()
                .any(|prefix| key_lower.starts_with(prefix))
            {
                log::debug!(
                    "KafkaDataWriter: Skipping property (schema/metadata): {} = {}",
                    key,
                    value
                );
                skipped_props.push(key.clone());
                continue;
            }

            // Check exact skip matches
            if skip_exact.contains(&key.as_str()) {
                log::debug!(
                    "KafkaDataWriter: Skipping property (metadata): {} = {}",
                    key,
                    value
                );
                skipped_props.push(key.clone());
                continue;
            }

            // Valid producer property
            log::debug!(
                "KafkaDataWriter: Applying producer property: {} = {}",
                key,
                value
            );
            valid_props.push((key.clone(), value.clone()));
        }

        (valid_props, skipped_props)
    }

    /// Log producer properties configuration
    fn log_producer_properties(valid_props: &[(String, String)], skipped_props: &[String]) {
        if !valid_props.is_empty() || !skipped_props.is_empty() {
            log::info!("KafkaDataWriter: Producer Properties Configuration");
            log::info!("  Valid properties: {}", valid_props.len());
            for (key, value) in valid_props {
                log::debug!("    • {}: {}", key, value);
            }

            if !skipped_props.is_empty() {
                log::info!("  Skipped properties: {}", skipped_props.len());
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
            self.send_transactional(&self.topic.clone(), key.as_deref(), &payload)
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
                self.send_transactional(&self.topic.clone(), key.as_deref(), &payload)
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
            self.send_transactional(&self.topic.clone(), Some(key), empty_payload)
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
        true
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
                if let Some(handle) = poll_thread.take() {
                    if let Err(e) = handle.join() {
                        log::warn!(
                            "KafkaDataWriter: Poll thread panicked during shutdown: {:?}",
                            e
                        );
                    }
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
