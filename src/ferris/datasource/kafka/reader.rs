//! Unified Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::kafka::{
    serialization::StringSerializer,
    KafkaConsumer,
};

// Removed unused imports - AvroSerializer and ProtoSerializer don't exist
// Using AvroCodec and ProtobufCodec directly instead
use crate::ferris::serialization::{json_codec::JsonCodec, helpers, UnifiedCodec};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use nom::Map;
use serde::Serialize;
use crate::Serializer;

/// Supported serialization formats for Kafka messages
#[derive(Debug, Clone, PartialEq)]
pub enum SerializationFormat {
    Json,
    Avro,
    Protobuf,
    /// Auto-detect format from message headers or content
    Auto,
}

/// Unified message structure for all formats
struct UnifiedMessage {
    key: Option<String>,
    fields: HashMap<String, FieldValue>,
    timestamp: i64,
    offset: i64,
    partition: i32,
    headers: HashMap<String, String>,
}

impl UnifiedMessage {
    pub(crate) fn to_stream_record(self) -> StreamRecord {
        let mut fields = self.fields;

        // Add message key to fields map
        if let Some(key) = self.key {
            fields.insert("key".to_string(), FieldValue::String(key));
        } else {
            fields.insert("key".to_string(), FieldValue::Null);
        }

        StreamRecord {
            fields,
            timestamp: self.timestamp,
            offset: self.offset,
            partition: self.partition,
            headers: self.headers,
        }
    }
}

/// Unified consumer that abstracts over different serialization formats using trait objects
/// This eliminates the need for ConsumerWrapper enum by using runtime polymorphism
pub struct UnifiedKafkaConsumer {
    // Use raw Kafka consumer with bytes and abstract over codec with trait object
    consumer: KafkaConsumer<String, Vec<u8>, StringSerializer, crate::ferris::kafka::serialization::BytesSerializer>,
    codec: Box<dyn UnifiedCodec>,
    format_name: String,
}

impl UnifiedKafkaConsumer {
    /// Create a new unified consumer with the specified codec
    pub fn new(
        brokers: &str,
        group_id: &str,
        topics: &[&str],
        codec: Box<dyn UnifiedCodec>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let key_serializer = StringSerializer;
        let value_serializer = crate::ferris::kafka::serialization::BytesSerializer;
        
        let consumer = KafkaConsumer::new(brokers, group_id, key_serializer, value_serializer)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        
        consumer.subscribe(topics)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
        
        let format_name = codec.format_name().to_string();
        
        Ok(UnifiedKafkaConsumer {
            consumer,
            codec,
            format_name,
        })
    }
    
    /// Poll for a message with unified deserialization
    pub async fn poll_unified(&self, timeout: Duration) -> Result<Option<UnifiedMessage>, Box<dyn Error + Send + Sync>> {
        match self.consumer.poll(timeout).await {
            Ok(message) => {
                // Get raw bytes from Kafka message
                let bytes = message.value();
                
                // Deserialize using the codec
                let fields = self.codec.deserialize_record(bytes)
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
                
                // Extract headers
                let headers: HashMap<String, String> = message.headers()
                    .iter()
                    .filter_map(|(k, v_opt)| v_opt.as_ref().map(|v| (k.clone(), v.clone())))
                    .collect();
                
                Ok(Some(UnifiedMessage {
                    key: message.key().cloned(),
                    fields,
                    timestamp: message.timestamp().unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                    offset: message.offset(),
                    partition: message.partition(),
                    headers,
                }))
            }
            Err(_) => Ok(None), // Timeout or no message
        }
    }
    
    /// Commit offsets
    pub fn commit_unified(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.consumer.commit().map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }
    
    /// Get format name for logging/debugging
    pub fn format_name(&self) -> &str {
        &self.format_name
    }
}

/// Unified Kafka DataReader that handles all serialization formats
pub struct KafkaDataReader {
    consumer: KafkaConsumer<String, HashMap<String, FieldValue>, StringSerializer, dyn Serializer<HashMap<String, FieldValue>>>,
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
        // Create codec based on format - all implement UnifiedCodec trait
        let codec: Box<dyn UnifiedCodec> = match &format {
            SerializationFormat::Json | SerializationFormat::Auto => {
                Box::new(JsonCodec::new())
            }
            SerializationFormat::Avro => {
                if let Some(schema_json) = passed_schema_json {
                    let avro_codec = helpers::create_avro_codec(Some(schema_json))?;
                    Box::new(avro_codec)
                } else {
                    return Err("Avro format requires a schema to be provided".into());
                }
            }
            SerializationFormat::Protobuf => {
                if let Some(proto_schema) = passed_schema_json {
                    let protobuf_codec = helpers::create_protobuf_codec(Some(proto_schema))?;
                    Box::new(protobuf_codec)
                } else {
                    return Err(
                        "Protobuf format REQUIRES a schema (.proto definition) to be provided"
                            .into(),
                    );
                }
            }
        };

        let consumer = KafkaConsumer::new(brokers, group_id, StringSerializer, codec)?;
        let _result = consumer.subscribe(&vec![topic.as_str()]).map_err(|e| {
            return Err("Consumer cannot subscribe: ".to_string() + &e.to_string());
        })?;

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

            match self.consumer.poll_unified(timeout).await? {
                Some(unified_msg) => {
                    records.push(unified_msg.to_stream_record());
                }
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
        // Simple unified commit - no more enum matching needed!
        self.consumer.commit_unified()
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
