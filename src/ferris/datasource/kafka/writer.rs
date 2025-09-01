//! Unified Kafka data writer implementation

use crate::ferris::datasource::DataWriter;
use crate::ferris::serialization::helpers::{field_value_to_json, create_avro_codec, create_protobuf_codec};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use super::reader::SerializationFormat; // Reuse the same format enum

// Import the codec implementations via type aliases
use crate::ferris::serialization::avro_codec::AvroCodec;
use crate::ferris::serialization::protobuf_codec::ProtobufCodec;

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
        Self::create_with_schema_validation(brokers, topic, format, key_field, schema).await
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

        Self::create_with_schema_validation(brokers, topic, format, key_field, schema.as_deref())
            .await
    }

    /// Internal method with schema validation (consolidated from multiple constructors)
    async fn create_with_schema_validation(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        key_field: Option<String>,
        schema: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Validate schema requirements based on format
        Self::validate_schema_requirements(&format, schema)?;
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.ms", "100")
            .set("batch.num.messages", "1000");

        let producer: FutureProducer = config.create()?;

        // Initialize codec based on format using optimized single codec creation
        let (avro_codec, protobuf_codec) = match &format {
            SerializationFormat::Avro => {
                let codec = Self::create_avro_codec_with_defaults(schema)?;
                (Some(codec), None)
            }
            SerializationFormat::Protobuf => {
                let codec = Self::create_protobuf_codec_with_defaults(schema)?;
                (None, Some(codec))
            }
            _ => (None, None),
        };

        Ok(Self {
            producer,
            topic,
            format,
            key_field: key_field.or(Some("key".to_string())), // Default to "key" field
            avro_codec,
            protobuf_codec,
        })
    }

    /// Validate that required schemas are provided for formats that need them
    fn validate_schema_requirements(
        format: &SerializationFormat,
        schema: Option<&str>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match format {
            SerializationFormat::Avro => {
                // Avro can work with a default schema if none provided
                Ok(())
            }
            SerializationFormat::Protobuf => {
                if schema.is_none() {
                    // Protobuf can use default schema for backward compatibility
                    // but log a warning
                    log::warn!("No Protobuf schema provided, using default generic schema");
                }
                Ok(())
            }
            SerializationFormat::Json | SerializationFormat::Auto => {
                // JSON doesn't require schemas
                Ok(())
            }
        }
    }

    /// Create Avro codec with schema and defaults for writer (wrapper around helper)
    fn create_avro_codec_with_defaults(schema: Option<&str>) -> Result<AvroCodec, Box<dyn Error + Send + Sync>> {
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
            SerializationFormat::Avro => {
                // Look for Avro schema in various config keys
                let schema = properties
                    .get("avro.schema")
                    .or_else(|| properties.get("value.avro.schema"))
                    .or_else(|| properties.get("schema.avro"))
                    .or_else(|| properties.get("avro_schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path
                    if let Some(schema_file) = properties
                        .get("avro.schema.file")
                        .or_else(|| properties.get("schema.file"))
                        .or_else(|| properties.get("avro_schema_file"))
                    {
                        return Self::load_schema_from_file(schema_file);
                    }
                }
                Ok(schema)
            }
            SerializationFormat::Protobuf => {
                // Look for Protobuf schema in various config keys
                let schema = properties
                    .get("protobuf.schema")
                    .or_else(|| properties.get("value.protobuf.schema"))
                    .or_else(|| properties.get("schema.protobuf"))
                    .or_else(|| properties.get("protobuf_schema"))
                    .or_else(|| properties.get("proto.schema"))
                    .cloned();

                if schema.is_none() {
                    // Check for schema file path
                    if let Some(schema_file) = properties
                        .get("protobuf.schema.file")
                        .or_else(|| properties.get("proto.schema.file"))
                        .or_else(|| properties.get("schema.file"))
                        .or_else(|| properties.get("protobuf_schema_file"))
                    {
                        return Self::load_schema_from_file(schema_file);
                    }
                }
                Ok(schema)
            }
            SerializationFormat::Json | SerializationFormat::Auto => {
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
        match format_str.to_lowercase().as_str() {
            "json" => SerializationFormat::Json,
            "avro" => SerializationFormat::Avro,
            "protobuf" | "proto" => SerializationFormat::Protobuf,
            "auto" => SerializationFormat::Auto,
            _ => SerializationFormat::Json, // Default fallback
        }
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
            SerializationFormat::Avro => self.serialize_avro(record),
            SerializationFormat::Protobuf => self.serialize_protobuf(record),
            SerializationFormat::Auto => self.serialize_json(record), // Default to JSON
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

        // Send to Kafka
        match self
            .producer
            .send(kafka_record, Duration::from_secs(5))
            .await
        {
            Ok(_) => Ok(()),
            Err((kafka_error, _)) => Err(Box::new(kafka_error)),
        }
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Process each record individually to avoid lifetime issues
        for record in records {
            self.write(record).await?;
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
        // Send tombstone record (null payload) for deletion
        let kafka_record: FutureRecord<'_, _, ()> = FutureRecord::to(&self.topic).key(key);
        // No payload = tombstone

        match self
            .producer
            .send(kafka_record, Duration::from_secs(5))
            .await
        {
            Ok(_) => Ok(()),
            Err((kafka_error, _)) => Err(Box::new(kafka_error)),
        }
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Flush pending messages to Kafka
        self.producer
            .flush(Duration::from_secs(5))
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
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
