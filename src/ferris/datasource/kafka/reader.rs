//! Unified Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::serialization::helpers::json_to_field_value;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::{Headers, Message},
    ClientConfig, TopicPartitionList,
};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

/// Supported serialization formats for Kafka messages
#[derive(Debug, Clone, PartialEq)]
pub enum SerializationFormat {
    Json,
    #[cfg(feature = "avro")]
    Avro,
    #[cfg(feature = "protobuf")]
    Protobuf,
    /// Auto-detect format from message headers or content
    Auto,
}

/// Unified Kafka DataReader that handles all serialization formats
pub struct KafkaDataReader {
    consumer: StreamConsumer,
    topic: String,
    format: SerializationFormat,
    batch_size: usize,
}

impl KafkaDataReader {
    /// Create a new Kafka data reader
    pub async fn new(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_size: Option<usize>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            // Set batch size via max.poll.records
            .set("max.poll.records", batch_size.unwrap_or(100).to_string());

        let consumer: StreamConsumer = config.create()?;
        
        // Subscribe to the topic
        let topics = vec![topic.as_str()];
        consumer.subscribe(&topics)?;

        Ok(Self {
            consumer,
            topic,
            format,
            batch_size: batch_size.unwrap_or(100),
        })
    }


    /// Parse message payload based on format
    fn parse_message_payload(
        &self,
        message: &rdkafka::message::BorrowedMessage,
        format: &SerializationFormat,
    ) -> Result<HashMap<String, FieldValue>, Box<dyn Error + Send + Sync>> {
        let mut fields = HashMap::new();

        // Add key if present
        if let Some(key) = message.key() {
            fields.insert("key".to_string(), FieldValue::String(String::from_utf8_lossy(key).to_string()));
        } else {
            fields.insert("key".to_string(), FieldValue::Null);
        }

        // Parse payload based on format
        if let Some(payload) = message.payload() {
            match format {
                SerializationFormat::Json => {
                    // Parse as JSON
                    let json_str = String::from_utf8_lossy(payload);
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_value) => {
                            self.json_value_to_fields(&json_value, &mut fields)?;
                        }
                        Err(_) => {
                            // If JSON parsing fails, store as string
                            fields.insert("value".to_string(), FieldValue::String(json_str.to_string()));
                        }
                    }
                }
                SerializationFormat::Avro => {
                    // For Avro, we would need the schema to deserialize properly
                    // For now, store as base64 encoded string with metadata
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
                SerializationFormat::Auto => {
                    // Try to detect from payload content for Auto format
                    if payload.starts_with(b"{") || payload.starts_with(b"[") {
                        // Parse as JSON
                        let json_str = String::from_utf8_lossy(payload);
                        match serde_json::from_str::<Value>(&json_str) {
                            Ok(json_value) => {
                                self.json_value_to_fields(&json_value, &mut fields)?;
                            }
                            Err(_) => {
                                // If JSON parsing fails, store as string
                                fields.insert("value".to_string(), FieldValue::String(json_str.to_string()));
                            }
                        }
                    } else {
                        // For binary data, store as base64 encoded string
                        use base64::Engine;
                        let value_str = base64::engine::general_purpose::STANDARD.encode(payload);
                        fields.insert("value".to_string(), FieldValue::String(value_str));
                    }
                }
            }
        } else {
            fields.insert("value".to_string(), FieldValue::Null);
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
            match tokio::time::timeout(Duration::from_millis(1000), self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    // Parse payload using configured format
                    let fields = self.parse_message_payload(&message, &self.format)?;

                    // Convert headers
                    let mut header_map = HashMap::new();
                    if let Some(headers) = message.headers() {
                        for i in 0..headers.count() {
                            let header = headers.get(i);
                            let key = header.key;
                            if let Some(value) = header.value {
                                if let Ok(value_str) = std::str::from_utf8(value) {
                                    header_map.insert(key.to_string(), value_str.to_string());
                                }
                            }
                        }
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .to_millis()
                            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: header_map,
                    };

                    records.push(record);
                }
                Ok(Err(e)) => {
                    return Err(Box::new(e));
                }
                Err(_) => {
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
        self.consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Async)?;
        Ok(())
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        match offset {
            SourceOffset::Kafka { partition, offset } => {
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset))?;
                self.consumer.assign(&tpl)?;
                Ok(())
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