//! Unified Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::kafka::{serialization::StringSerializer, KafkaConsumer};

// Removed unused imports - AvroSerializer and ProtoSerializer don't exist
// Using AvroCodec and ProtobufCodec directly instead
use crate::ferris::serialization::{json_codec::JsonCodec, SerializationCodec};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

// Import the unified SerializationFormat from the main module
pub use crate::ferris::kafka::serialization_format::SerializationFormat;

/// Unified Kafka DataReader that handles all serialization formats
pub struct KafkaDataReader {
    consumer:
        KafkaConsumer<String, HashMap<String, FieldValue>, StringSerializer, SerializationCodec>,
    batch_size: usize,
}

impl KafkaDataReader {
    /// Create a serialization codec based on format and schema, with robust error handling
    fn create_serialization_codec(
        format: &SerializationFormat,
        schema_json: Option<&str>,
    ) -> Result<SerializationCodec, Box<dyn Error + Send + Sync>> {
        use crate::ferris::serialization::helpers;

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

    /// Create a new Kafka data reader with optional schema (Avro or Protobuf)
    pub async fn new_with_schema(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_size: Option<usize>,
        passed_schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create codec using robust factory pattern
        let codec = Self::create_serialization_codec(&format, passed_schema_json)?;

        let consumer = KafkaConsumer::new(brokers, group_id, StringSerializer, codec)?;
        consumer
            .subscribe(&vec![topic.as_str()])
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(Self {
            consumer,
            batch_size: batch_size.unwrap_or(100),
        })
    }

    /// Create a new Kafka data reader with JSON format (convenience method)
    pub async fn new_json(
        brokers: &str,
        topic: String,
        group_id: &str,
        batch_size: Option<usize>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schema(
            brokers,
            topic,
            group_id,
            SerializationFormat::Json,
            batch_size,
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

        Self::new_with_schema(brokers, topic, group_id, format, batch_size, schema_json).await
    }
}

#[async_trait]
impl DataReader for KafkaDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            let timeout = Duration::from_millis(1000);

            match self.consumer.poll(timeout).await {
                Ok(mut message) => {
                    // The message already contains HashMap<String, FieldValue> as the value
                    let mut fields = message.take_value();

                    // Add message key to fields map if present
                    if let Some(key) = message.take_key() {
                        fields.insert("key".to_string(), FieldValue::String(key));
                    } else {
                        fields.insert("key".to_string(), FieldValue::Null);
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: message.take_headers().into_map(),
                    };

                    records.push(record);
                }
                Err(_) => {
                    // Timeout or error - break if we have some records, otherwise continue
                    if !records.is_empty() {
                        break;
                    }
                    // For empty batch, continue trying up to batch_size attempts
                }
            }
        }

        Ok(records)
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
                // Note: ferris_streams KafkaConsumer doesn't directly expose seek functionality
                // This would need to be implemented by accessing the underlying rdkafka consumer
                // For now, return an error indicating this is not yet implemented
                Err("Seek operation not yet implemented for ferris_streams KafkaConsumer".into())
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
