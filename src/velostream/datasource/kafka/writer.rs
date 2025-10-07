//! Unified Kafka data writer implementation

use crate::velostream::datasource::config::unified::{ConfigFactory, ConfigLogger};
use crate::velostream::datasource::{BatchConfig, DataWriter};
use crate::velostream::serialization::helpers::{
    create_avro_codec, create_protobuf_codec, field_value_to_json,
};
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use crate::velostream::kafka::serialization_format::SerializationFormat;

// Import the codec implementations via type aliases
use crate::velostream::serialization::avro_codec::AvroCodec;
use crate::velostream::serialization::protobuf_codec::ProtobufCodec;

/// Unified Kafka DataWriter that handles all serialization formats
pub struct KafkaDataWriter {
    producer: FutureProducer,
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
            .or_else(|| properties.get("serializer.format"))
            .or_else(|| properties.get("format"))
            .map(|s| s.as_str())
            .unwrap_or("json");

        let format = Self::parse_serialization_format(format_str);

        // Extract key field configuration
        let key_field = properties
            .get("key.field")
            .or_else(|| properties.get("message.key.field"))
            .cloned();

        // Extract schema based on format
        let schema = Self::extract_schema_from_properties(&format, properties)?;

        Self::create_with_schema_validation_and_batch_config(
            brokers,
            topic,
            format,
            key_field,
            schema.as_deref(),
            &HashMap::new(),
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
            .or_else(|| properties.get("serializer.format"))
            .or_else(|| properties.get("format"))
            .map(|s| s.as_str())
            .unwrap_or("json");

        let format = Self::parse_serialization_format(format_str);

        // Extract key field configuration
        let key_field = properties
            .get("key.field")
            .or_else(|| properties.get("message.key.field"))
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

        // Create optimized producer configuration using unified system
        let producer_config = if let Some(batch_config) = batch_config {
            let config =
                ConfigFactory::create_kafka_producer_config(brokers, properties, &batch_config);
            ConfigLogger::log_kafka_producer_config(&config, &batch_config, &topic, brokers);
            config
        } else {
            let mut config = HashMap::new();
            config.insert("bootstrap.servers".to_string(), brokers.to_string());
            // Apply user properties
            for (key, value) in properties.iter() {
                config.insert(key.clone(), value.clone());
            }
            config
        };

        // Create Kafka ClientConfig from our optimized configuration
        let mut client_config = ClientConfig::new();
        for (key, value) in &producer_config {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config.create()?;

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

        let writer = Self {
            producer,
            topic: topic.clone(),
            format,
            key_field: key_field.or(Some("key".to_string())), // Default to "key" field
            avro_codec,
            protobuf_codec,
        };

        log::info!(
            "KafkaDataWriter: Initialized writer for topic '{}' with format={:?}, key_field={:?}",
            topic,
            writer.format,
            writer.key_field
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
            log::warn!(
                "SUSPICIOUS TOPIC NAME: Kafka sink configured to write to topic '{}'. \
                 This is a common placeholder/fallback value that may indicate \
                 configuration was not properly loaded. If this is intentional, ignore this warning. \
                 Otherwise, check that your config file is being read correctly.",
                topic
            );
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

    /// Serialize to JSON format
    fn serialize_json(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let json_obj = self.build_json_payload(record, true)?; // Include metadata for JSON
        let json_str = serde_json::to_string(&json_obj)?;
        Ok(json_str.into_bytes())
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
}

#[async_trait]
impl DataWriter for KafkaDataWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        log::debug!(
            "KafkaDataWriter: Sending record to topic '{}', format={:?}",
            self.topic,
            self.format
        );

        // Extract key for partitioning
        let key = self.extract_key(&record);

        // Serialize payload based on format
        let payload = self.serialize_payload(&record)?;

        // Convert headers
        let headers = self.convert_headers(&record.headers);

        // Build Kafka record
        let mut kafka_record = FutureRecord::to(&self.topic).payload(&payload);

        if let Some(key_str) = &key {
            kafka_record = kafka_record.key(key_str);
        }

        // Add headers
        for (header_key, header_value) in headers {
            kafka_record = kafka_record.headers(rdkafka::message::OwnedHeaders::new().insert(
                rdkafka::message::Header {
                    key: &header_key,
                    value: Some(&header_value),
                },
            ));
        }

        // Send to Kafka with comprehensive debug logging
        log::debug!("KafkaDataWriter: Sending record to topic '{}' with key {:?}, payload_size={} bytes, format={:?}", 
                   self.topic, key, payload.len(), self.format);

        match self
            .producer
            .send(kafka_record, Duration::from_secs(5))
            .await
        {
            Ok(delivery) => {
                log::debug!("KafkaDataWriter: Successfully sent record to topic '{}', partition={}, offset={}", 
                           self.topic, delivery.partition, delivery.offset);
                Ok(())
            }
            Err((kafka_error, _)) => {
                log::error!(
                    "KafkaDataWriter: Failed to send record to topic '{}': {:?}",
                    self.topic,
                    kafka_error
                );
                log::error!("KafkaDataWriter: Failed record details - key: {:?}, payload_size: {} bytes, format: {:?}", 
                           key, payload.len(), self.format);
                Err(Box::new(kafka_error))
            }
        }
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        log::debug!(
            "KafkaDataWriter: Starting batch write of {} records to topic '{}', format={:?}",
            records.len(),
            self.topic,
            self.format
        );

        let batch_size = records.len();
        let mut successful_writes = 0;

        // Process each record individually to avoid lifetime issues
        for (index, record) in records.into_iter().enumerate() {
            log::debug!(
                "KafkaDataWriter: Processing record {}/{} in batch for topic '{}'",
                index + 1,
                batch_size,
                self.topic
            );

            match self.write(record).await {
                Ok(()) => {
                    successful_writes += 1;
                }
                Err(e) => {
                    log::error!(
                        "KafkaDataWriter: Batch failed at record {}/{} in topic '{}': {:?}",
                        index + 1,
                        batch_size,
                        self.topic,
                        e
                    );
                    log::error!("KafkaDataWriter: Batch statistics - {}/{} records successfully written before failure", 
                               successful_writes, batch_size);
                    return Err(e);
                }
            }
        }

        log::debug!(
            "KafkaDataWriter: Successfully completed batch write of {}/{} records to topic '{}'",
            successful_writes,
            batch_size,
            self.topic
        );
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
        // Send tombstone record (null payload) for deletion
        log::debug!(
            "KafkaDataWriter: Sending tombstone record for key '{}' to topic '{}'",
            key,
            self.topic
        );
        let kafka_record: FutureRecord<'_, _, ()> = FutureRecord::to(&self.topic).key(key);
        // No payload = tombstone

        match self
            .producer
            .send(kafka_record, Duration::from_secs(5))
            .await
        {
            Ok(delivery) => {
                log::debug!("KafkaDataWriter: Successfully sent tombstone for key '{}' to topic '{}', partition={}, offset={}", 
                           key, self.topic, delivery.partition, delivery.offset);
                Ok(())
            }
            Err((kafka_error, _)) => {
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
        match self.producer.flush(Duration::from_secs(5)) {
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
    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // Initialize transactions first if not already done
        match self.producer.init_transactions(Duration::from_secs(5)) {
            Ok(()) => {
                // Now begin the transaction
                match self.producer.begin_transaction() {
                    Ok(()) => Ok(true),
                    Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
                }
            }
            Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
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
        // self.producer.send_offsets_to_transaction(offsets, cgm, timeout)
        self.producer
            .commit_transaction(Duration::from_secs(60))
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.producer
            .abort_transaction(Duration::from_secs(60))
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
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
