//! Unified Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::kafka::{serialization::StringSerializer, KafkaConsumer};

// Removed unused imports - AvroSerializer and ProtoSerializer don't exist
// Using AvroCodec and ProtobufCodec directly instead
use crate::ferris::serialization::{helpers, json_codec::JsonCodec, SerializationCodec};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
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

/// Unified Kafka DataReader that handles all serialization formats
pub struct KafkaDataReader {
    consumer:
        KafkaConsumer<String, HashMap<String, FieldValue>, StringSerializer, SerializationCodec>,
    batch_size: usize,
}

impl KafkaDataReader {
    /// Create a new Kafka data reader with optional schema (Avro or Protobuf)
    pub async fn new_with_schema(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_size: Option<usize>,
        passed_schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create codec based on format
        let codec = match &format {
            SerializationFormat::Json | SerializationFormat::Auto => {
                SerializationCodec::Json(JsonCodec::new())
            }
            SerializationFormat::Avro => {
                if let Some(schema_json) = passed_schema_json {
                    let avro_codec = helpers::create_avro_codec(Some(schema_json))?;
                    SerializationCodec::Avro(avro_codec)
                } else {
                    return Err("Avro format requires a schema to be provided".into());
                }
            }
            SerializationFormat::Protobuf => {
                if let Some(proto_schema) = passed_schema_json {
                    let protobuf_codec = helpers::create_protobuf_codec(Some(proto_schema))?;
                    SerializationCodec::Protobuf(protobuf_codec)
                } else {
                    return Err(
                        "Protobuf format REQUIRES a schema (.proto definition) to be provided"
                            .into(),
                    );
                }
            }
        };

        let consumer = KafkaConsumer::new(brokers, group_id, StringSerializer, codec)?;
        consumer
            .subscribe(&vec![topic.as_str()])
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(Self {
            consumer,
            batch_size: batch_size.unwrap_or(100),
        })
    }
}

#[async_trait]
impl DataReader for KafkaDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            let timeout = Duration::from_millis(1000);

            match self.consumer.poll(timeout).await {
                Ok(message) => {
                    // The message already contains HashMap<String, FieldValue> as the value
                    let mut fields = message.value().clone();

                    // Add message key to fields map if present
                    if let Some(key) = message.key() {
                        fields.insert("key".to_string(), FieldValue::String(key.clone()));
                    } else {
                        fields.insert("key".to_string(), FieldValue::Null);
                    }

                    // Extract headers
                    let headers: HashMap<String, String> = message
                        .headers()
                        .iter()
                        .filter_map(|(k, v_opt)| v_opt.as_ref().map(|v| (k.clone(), v.clone())))
                        .collect();

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers,
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
