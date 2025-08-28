//! Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::kafka::{
    kafka_error::ConsumerError, serialization::JsonSerializer, KafkaConsumer,
};
#[cfg(feature = "avro")]
use crate::ferris::serialization::avro_value_to_field_value;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

#[cfg(feature = "avro")]
use crate::ferris::kafka::serialization::AvroSerializer;
#[cfg(feature = "avro")]
use apache_avro::types::Value as AvroValue;

/// Generic Kafka DataReader implementation
pub struct KafkaDataReader<C> {
    pub consumer: C,
    pub topic: String,
}

#[async_trait]
impl DataReader for KafkaDataReader<KafkaConsumer<String, String, JsonSerializer, JsonSerializer>> {
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        // Use stream() instead of poll() for better async performance
        let mut stream = self.consumer.stream();
        match tokio::time::timeout(Duration::from_millis(1000), stream.next()).await {
            Ok(Some(result)) => match result {
                Ok(message) => {
                    let mut fields = HashMap::new();

                    // Add key if present
                    if let Some(key) = message.key() {
                        fields.insert("key".to_string(), FieldValue::String(key.clone()));
                    } else {
                        fields.insert("key".to_string(), FieldValue::Null);
                    }

                    // Add value
                    fields.insert(
                        "value".to_string(),
                        FieldValue::String(message.value().clone()),
                    );

                    // Convert headers
                    let mut header_map = HashMap::new();
                    for (key, value) in message.headers().iter() {
                        if let Some(v) = value {
                            header_map.insert(key.clone(), v.clone());
                        }
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .unwrap_or(chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: header_map,
                    };

                    Ok(Some(record))
                }
                Err(ConsumerError::Timeout) => Ok(None),
                Err(err) => Err(Box::new(err)),
            },
            Ok(None) => Ok(None), // Stream ended
            Err(_) => Ok(None),   // Timeout
        }
    }

    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(max_size);

        for _ in 0..max_size {
            match self.read().await? {
                Some(record) => records.push(record),
                None => break,
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
                // TODO: Implement seek functionality
                // This would require exposing seek methods from the KafkaConsumer
                Err("Seek not yet implemented for Kafka adapter".into())
            }
            _ => Err("Invalid offset type for Kafka source".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // For Kafka, we always potentially have more data unless the consumer is closed
        Ok(true)
    }
}

#[async_trait]
impl DataReader for KafkaDataReader<KafkaConsumer<String, Value, JsonSerializer, JsonSerializer>> {
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut stream = self.consumer.stream();
        match tokio::time::timeout(Duration::from_millis(1000), stream.next()).await {
            Ok(Some(result)) => match result {
                Ok(message) => {
                    let mut fields = HashMap::new();

                    // Add key if present
                    if let Some(key) = message.key() {
                        fields.insert("key".to_string(), FieldValue::String(key.clone()));
                    } else {
                        fields.insert("key".to_string(), FieldValue::Null);
                    }

                    // Add value as JSON
                    let value_str = serde_json::to_string(message.value())
                        .unwrap_or_else(|_| message.value().to_string());
                    fields.insert("value".to_string(), FieldValue::String(value_str));

                    // Convert headers
                    let mut header_map = HashMap::new();
                    for (key, value) in message.headers().iter() {
                        if let Some(v) = value {
                            header_map.insert(key.clone(), v.clone());
                        }
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .unwrap_or(chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: header_map,
                    };

                    Ok(Some(record))
                }
                Err(ConsumerError::Timeout) => Ok(None),
                Err(err) => Err(Box::new(err)),
            },
            Ok(None) => Ok(None), // Stream ended
            Err(_) => Ok(None),   // Timeout
        }
    }

    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(max_size);

        for _ in 0..max_size {
            match self.read().await? {
                Some(record) => records.push(record),
                None => break,
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
                // TODO: Implement seek functionality
                Err("Seek not yet implemented for Kafka adapter".into())
            }
            _ => Err("Invalid offset type for Kafka source".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Ok(true)
    }
}

#[cfg(feature = "protobuf")]
#[async_trait]
impl DataReader
    for KafkaDataReader<KafkaConsumer<String, Vec<u8>, JsonSerializer, JsonSerializer>>
{
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut stream = self.consumer.stream();
        match tokio::time::timeout(Duration::from_millis(1000), stream.next()).await {
            Ok(Some(result)) => match result {
                Ok(message) => {
                    let mut fields = HashMap::new();

                    // Add key if present
                    if let Some(key) = message.key() {
                        fields.insert("key".to_string(), FieldValue::String(key.clone()));
                    } else {
                        fields.insert("key".to_string(), FieldValue::Null);
                    }

                    // For protobuf, store the raw bytes as a base64 string
                    // The actual protobuf deserialization would happen at a higher level
                    use base64::Engine;
                    let value_str =
                        base64::engine::general_purpose::STANDARD.encode(message.value());
                    fields.insert("value".to_string(), FieldValue::String(value_str));
                    fields.insert("_raw_protobuf".to_string(), FieldValue::Boolean(true));

                    // Convert headers
                    let mut header_map = HashMap::new();
                    for (key, value) in message.headers().iter() {
                        if let Some(v) = value {
                            header_map.insert(key.clone(), v.clone());
                        }
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .unwrap_or(chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: header_map,
                    };

                    Ok(Some(record))
                }
                Err(ConsumerError::Timeout) => Ok(None),
                Err(err) => Err(Box::new(err)),
            },
            Ok(None) => Ok(None), // Stream ended
            Err(_) => Ok(None),   // Timeout
        }
    }

    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(max_size);

        for _ in 0..max_size {
            match self.read().await? {
                Some(record) => records.push(record),
                None => break,
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
                // TODO: Implement seek functionality
                Err("Seek not yet implemented for Kafka adapter".into())
            }
            _ => Err("Invalid offset type for Kafka source".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Ok(true)
    }
}

/// Dynamic Kafka reader that can handle different serialization formats
pub enum DynamicKafkaReader {
    JsonReader(KafkaDataReader<KafkaConsumer<String, Value, JsonSerializer, JsonSerializer>>),
    #[cfg(feature = "avro")]
    AvroReader(KafkaDataReader<KafkaConsumer<String, AvroValue, JsonSerializer, AvroSerializer>>),
    #[cfg(feature = "protobuf")]
    ProtobufReader(KafkaDataReader<KafkaConsumer<String, Vec<u8>, JsonSerializer, JsonSerializer>>),
}

#[async_trait]
impl DataReader for DynamicKafkaReader {
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaReader::JsonReader(reader) => reader.read().await,
            #[cfg(feature = "avro")]
            DynamicKafkaReader::AvroReader(reader) => reader.read().await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaReader::ProtobufReader(reader) => reader.read().await,
        }
    }

    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaReader::JsonReader(reader) => reader.read_batch(max_size).await,
            #[cfg(feature = "avro")]
            DynamicKafkaReader::AvroReader(reader) => reader.read_batch(max_size).await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaReader::ProtobufReader(reader) => reader.read_batch(max_size).await,
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaReader::JsonReader(reader) => reader.commit().await,
            #[cfg(feature = "avro")]
            DynamicKafkaReader::AvroReader(reader) => reader.commit().await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaReader::ProtobufReader(reader) => reader.commit().await,
        }
    }

    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaReader::JsonReader(reader) => reader.seek(offset).await,
            #[cfg(feature = "avro")]
            DynamicKafkaReader::AvroReader(reader) => reader.seek(offset).await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaReader::ProtobufReader(reader) => reader.seek(offset).await,
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaReader::JsonReader(reader) => reader.has_more().await,
            #[cfg(feature = "avro")]
            DynamicKafkaReader::AvroReader(reader) => reader.has_more().await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaReader::ProtobufReader(reader) => reader.has_more().await,
        }
    }
}

#[cfg(feature = "avro")]
#[async_trait]
impl DataReader
    for KafkaDataReader<KafkaConsumer<String, AvroValue, JsonSerializer, AvroSerializer>>
{
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut stream = self.consumer.stream();
        match tokio::time::timeout(Duration::from_millis(1000), stream.next()).await {
            Ok(Some(result)) => match result {
                Ok(message) => {
                    let mut fields = HashMap::new();

                    // Add key if present
                    if let Some(key) = message.key() {
                        fields.insert("key".to_string(), FieldValue::String(key.clone()));
                    } else {
                        fields.insert("key".to_string(), FieldValue::Null);
                    }

                    // Parse the Avro value and add all fields
                    let payload = message.value();
                    // Convert AvroValue to our field format
                    match payload {
                        AvroValue::Record(record_fields) => {
                            for (k, v) in record_fields {
                                if let Ok(field_value) = avro_value_to_field_value(v) {
                                    fields.insert(k.clone(), field_value);
                                }
                            }
                        }
                        _ => {
                            // If it's not a record, store as a single 'value' field
                            if let Ok(field_value) = avro_value_to_field_value(payload) {
                                fields.insert("value".to_string(), field_value);
                            }
                        }
                    }

                    // Convert headers
                    let mut header_map = HashMap::new();
                    for (key, value) in message.headers().iter() {
                        if let Some(v) = value {
                            header_map.insert(key.clone(), v.clone());
                        }
                    }

                    let record = StreamRecord {
                        fields,
                        timestamp: message
                            .timestamp()
                            .unwrap_or(chrono::Utc::now().timestamp_millis()),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: header_map,
                    };

                    Ok(Some(record))
                }
                Err(e) => Err(Box::new(e)),
            },
            Ok(None) => Ok(None), // No more messages
            Err(_) => Ok(None),   // Timeout
        }
    }

    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(max_size);

        for _ in 0..max_size {
            match self.read().await? {
                Some(record) => records.push(record),
                None => break,
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
                // TODO: Implement seek functionality
                // This would require exposing seek methods from the KafkaConsumer
                Err("Seek not yet implemented for Kafka adapter".into())
            }
            _ => Err("Invalid offset type for Kafka source".into()),
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // For Kafka, we always potentially have more data unless the consumer is closed
        Ok(true)
    }
}
