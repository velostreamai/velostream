//! Unified Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::kafka::{
    serialization::{BytesSerializer, JsonSerializer, StringSerializer},
    KafkaConsumer,
};

// Removed unused imports - AvroSerializer and ProtoSerializer don't exist
// Using AvroCodec and ProtobufCodec directly instead
use crate::ferris::serialization::helpers::json_to_field_value;

use crate::ferris::serialization::avro_codec::{create_avro_serializer, AvroCodec};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
// Note: rdkafka imports no longer needed since we use ferris_streams KafkaConsumer
use crate::ferris::serialization::{create_protobuf_serializer, ProtobufCodec};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

/// Supported serialization formats for Kafka messages
#[derive(Debug, Clone, PartialEq)]
pub enum SerializationFormat {
    Json,
    Avro,
    Protobuf,
    /// Auto-detect format from message headers or content
    Auto,
}

/// Consumer wrapper that handles different serialization formats
enum ConsumerWrapper {
    Json(KafkaConsumer<String, String, StringSerializer, JsonSerializer>),
    Bytes(KafkaConsumer<String, Vec<u8>, StringSerializer, BytesSerializer>),
    Avro(KafkaConsumer<String, HashMap<String, FieldValue>, StringSerializer, AvroCodec>),
    Protobuf(KafkaConsumer<String, HashMap<String, FieldValue>, StringSerializer, ProtobufCodec>),
}

/// Unified Kafka DataReader that handles all serialization formats  
pub struct KafkaDataReader {
    consumer: ConsumerWrapper,
    topic: String,
    format: SerializationFormat,
    batch_size: usize,
}

impl KafkaDataReader {
    /// Create a new Kafka data reader (deprecated - use new_with_config instead)
    #[deprecated(note = "Use new_with_config which properly handles schema requirements")]
    pub async fn new(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_size: Option<usize>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // For backwards compatibility, but warn about schema requirements
        match format {
            SerializationFormat::Avro | SerializationFormat::Protobuf => {
                return Err(format!(
                    "{:?} format requires schema - use new_with_schema() instead",
                    format
                )
                .into());
            }
            _ => {}
        }
        Self::new_with_schema(brokers, topic, group_id, format, batch_size, None).await
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
        let key_serializer = StringSerializer;

        // Create consumer based on serialization format
        let consumer = match &format {
            SerializationFormat::Json | SerializationFormat::Auto => {
                let value_serializer = JsonSerializer;
                let consumer = Self::create_and_subscribe_consumer(
                    brokers,
                    group_id,
                    &topic,
                    key_serializer,
                    value_serializer,
                )?;
                ConsumerWrapper::Json(consumer)
            }
            SerializationFormat::Avro => {
                if let Some(schema_json) = passed_schema_json {
                    let avro_serializer = create_avro_serializer(schema_json)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                    let consumer = Self::create_and_subscribe_consumer(
                        brokers,
                        group_id,
                        &topic,
                        key_serializer,
                        avro_serializer,
                    )?;
                    ConsumerWrapper::Avro(consumer)
                } else {
                    return Err("Avro format requires a schema to be provided".into());
                }
            }
            SerializationFormat::Protobuf => {
                if let Some(schema) = passed_schema_json {
                    let proto_serializer = create_protobuf_serializer(schema)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                    let consumer = Self::create_and_subscribe_consumer(
                        brokers,
                        group_id,
                        &topic,
                        key_serializer,
                        proto_serializer,
                    )?;
                    ConsumerWrapper::Protobuf(consumer)
                } else {
                    return Err(
                        "Protobuf format REQUIRES a schema (.proto definition) to be provided"
                            .into(),
                    );
                }
            }
        };

        Ok(Self {
            consumer,
            topic,
            format,
            batch_size: batch_size.unwrap_or(100),
        })
    }

    /// Helper to create consumer and subscribe to topic - eliminates duplication
    fn create_and_subscribe_consumer<K, V, KS, VS>(
        brokers: &str,
        group_id: &str,
        topic: &str,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<KafkaConsumer<K, V, KS, VS>, Box<dyn Error + Send + Sync>>
    where
        KS: crate::ferris::kafka::serialization::Serializer<K>,
        VS: crate::ferris::kafka::serialization::Serializer<V>,
    {
        let consumer = KafkaConsumer::new(brokers, group_id, key_serializer, value_serializer)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let topics = vec![topic];
        consumer
            .subscribe(&topics)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(consumer)
    }

    /// Convert message headers to HashMap - common logic for all message types
    fn extract_headers<K, V>(
        &self,
        message: &crate::ferris::kafka::Message<K, V>,
    ) -> HashMap<String, String> {
        message
            .headers()
            .iter()
            .filter_map(|(k, v_opt)| v_opt.as_ref().map(|v| (k.clone(), v.clone())))
            .collect::<HashMap<String, String>>()
    }

    /// Convert message to StreamRecord - eliminates duplication across all consumer types
    fn message_to_stream_record<K, V>(
        &self,
        fields: HashMap<String, FieldValue>,
        message: &crate::ferris::kafka::Message<K, V>,
    ) -> StreamRecord {
        StreamRecord {
            fields,
            timestamp: message
                .timestamp()
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
            offset: message.offset(),
            partition: message.partition(),
            headers: self.extract_headers(message),
        }
    }

    /// Add message key to fields map - common logic for all parsers
    fn add_message_key(&self, fields: &mut HashMap<String, FieldValue>, key: Option<&String>) {
        if let Some(key) = key {
            fields.insert("key".to_string(), FieldValue::String(key.clone()));
        } else {
            fields.insert("key".to_string(), FieldValue::Null);
        }
    }

    /// Parse message from ferris_streams KafkaConsumer (already decoded)
    fn parse_fields(
        &self,
        message: &crate::ferris::kafka::Message<String, HashMap<String, FieldValue>>,
    ) -> Result<HashMap<String, FieldValue>, Box<dyn Error + Send + Sync>> {
        let mut fields = message.value().clone();
        self.add_message_key(&mut fields, message.key());
        Ok(fields)
    }

    /// Parse JSON message from ferris_streams KafkaConsumer
    fn parse_json_message(
        &self,
        message: &crate::ferris::kafka::Message<String, String>,
    ) -> Result<HashMap<String, FieldValue>, Box<dyn Error + Send + Sync>> {
        let mut fields = HashMap::new();
        self.add_message_key(&mut fields, message.key());

        // Parse the value as JSON string
        let json_str = message.value();
        match serde_json::from_str::<Value>(json_str) {
            Ok(json_value) => {
                self.json_value_to_fields(&json_value, &mut fields)?;
            }
            Err(_) => {
                // If JSON parsing fails, store as string
                fields.insert(
                    "value".to_string(),
                    FieldValue::String(json_str.to_string()),
                );
            }
        }

        Ok(fields)
    }

    /// Parse bytes message from ferris_streams KafkaConsumer  
    fn parse_bytes_message(
        &self,
        message: &crate::ferris::kafka::Message<String, Vec<u8>>,
    ) -> Result<HashMap<String, FieldValue>, Box<dyn Error + Send + Sync>> {
        let mut fields = HashMap::new();
        self.add_message_key(&mut fields, message.key());

        let payload = message.value();
        match &self.format {
            SerializationFormat::Avro => {
                // No schema available, store as base64 encoded string with metadata
                use base64::Engine;
                let value_str = base64::engine::general_purpose::STANDARD.encode(payload);
                fields.insert("value".to_string(), FieldValue::String(value_str));
                fields.insert("_raw_avro".to_string(), FieldValue::Boolean(true));
            }
            SerializationFormat::Protobuf => {
                // Store protobuf as base64 encoded string
                use base64::Engine;
                let value_str = base64::engine::general_purpose::STANDARD.encode(payload);
                fields.insert("value".to_string(), FieldValue::String(value_str));
                fields.insert("_raw_protobuf".to_string(), FieldValue::Boolean(true));
            }
            _ => {
                // For other formats, try to detect or store as base64
                if payload.starts_with(b"{") || payload.starts_with(b"[") {
                    // Try to parse as JSON
                    let json_str = String::from_utf8_lossy(payload);
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            self.json_value_to_fields(&json_value, &mut fields)?;
                        }
                        Err(_) => {
                            fields.insert(
                                "value".to_string(),
                                FieldValue::String(json_str.to_string()),
                            );
                        }
                    }
                } else {
                    // Store as base64 encoded string
                    use base64::Engine;
                    let value_str = base64::engine::general_purpose::STANDARD.encode(payload);
                    fields.insert("value".to_string(), FieldValue::String(value_str));
                }
            }
        }

        Ok(fields)
    }

    /// Convert JSON Value to FieldValue entries using the standard serialization helper
    fn json_value_to_fields(
        &self,
        json_value: &Value,
        fields: &mut HashMap<String, FieldValue>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match json_value {
            Value::Object(obj) => {
                // If it's a JSON object, add each field
                for (key, value) in obj {
                    let field_value = json_to_field_value(value)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                    fields.insert(key.clone(), field_value);
                }
            }
            _ => {
                // If it's not an object, store as single "value" field
                let field_value = json_to_field_value(json_value)
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                fields.insert("value".to_string(), field_value);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DataReader for KafkaDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            let timeout = Duration::from_millis(1000);

            let result: Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> =
                match &self.consumer {
                    ConsumerWrapper::Avro(consumer) => {
                        match consumer.poll(timeout).await {
                            Ok(message) => {
                                let fields = self.parse_fields(&message)?;
                                let record = self.message_to_stream_record(fields, &message);
                                Ok(Some(record))
                            }
                            Err(_) => Ok(None), // Timeout or no message
                        }
                    }
                    ConsumerWrapper::Protobuf(consumer) => {
                        match consumer.poll(timeout).await {
                            Ok(message) => {
                                let fields = self.parse_fields(&message)?;
                                let record = self.message_to_stream_record(fields, &message);
                                Ok(Some(record))
                            }
                            Err(_) => Ok(None), // Timeout or no message
                        }
                    }
                    ConsumerWrapper::Json(consumer) => {
                        match consumer.poll(timeout).await {
                            Ok(message) => {
                                let fields = self.parse_json_message(&message)?;
                                let record = self.message_to_stream_record(fields, &message);
                                Ok(Some(record))
                            }
                            Err(_) => Ok(None), // Timeout or no message
                        }
                    }
                    ConsumerWrapper::Bytes(consumer) => {
                        match consumer.poll(timeout).await {
                            Ok(message) => {
                                let fields = self.parse_bytes_message(&message)?;
                                let record = self.message_to_stream_record(fields, &message);
                                Ok(Some(record))
                            }
                            Err(_) => Ok(None), // Timeout or no message
                        }
                    }
                };

            match result? {
                Some(record) => records.push(record),
                None => {
                    // Timeout - break if we have some records, otherwise continue
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
        // All consumer types have the same commit pattern - consolidate the logic
        let commit_result = match &self.consumer {
            ConsumerWrapper::Json(consumer) => consumer.commit(),
            ConsumerWrapper::Bytes(consumer) => consumer.commit(),
            ConsumerWrapper::Avro(consumer) => consumer.commit(),
            ConsumerWrapper::Protobuf(consumer) => consumer.commit(),
        };

        commit_result.map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
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
