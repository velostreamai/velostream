//! Unified Kafka data writer implementation

use crate::ferris::datasource::DataWriter;
use crate::ferris::serialization::helpers::field_value_to_json;
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

// Import the codec implementations
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
    /// Create a new Kafka data writer
    pub async fn new(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        key_field: Option<String>, // Which field to use as Kafka message key
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schemas(brokers, topic, format, key_field, None, None, None).await
    }

    /// Create a new Kafka data writer with optional Avro schema
    pub async fn new_with_avro_schema(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        key_field: Option<String>,
        avro_schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schemas(
            brokers,
            topic,
            format,
            key_field,
            avro_schema_json,
            None,
            None,
        )
        .await
    }

    /// Create a new Kafka data writer with Protobuf schema
    pub async fn new_with_protobuf_schema(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        key_field: Option<String>,
        protobuf_schema: Option<&str>,
        message_type: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schemas(
            brokers,
            topic,
            format,
            key_field,
            None,
            protobuf_schema,
            message_type,
        )
        .await
    }

    /// Create a new Kafka data writer with optional schema configurations
    /// This is the unified method that handles all schema types
    async fn new_with_schemas(
        brokers: &str,
        topic: String,
        format: SerializationFormat,
        key_field: Option<String>,
        avro_schema_json: Option<&str>,
        protobuf_schema: Option<&str>,
        protobuf_message_type: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.ms", "100")
            .set("batch.num.messages", "1000");

        let producer: FutureProducer = config.create()?;

        // Initialize codecs based on format
        let avro_codec = if matches!(format, SerializationFormat::Avro) {
            if let Some(schema_json) = avro_schema_json {
                Some(AvroCodec::new(schema_json)?)
            } else {
                // Use a default schema for basic record structure
                let default_schema = r#"{
                    "type": "record",
                    "name": "StreamRecord",
                    "fields": [
                        {"name": "data", "type": ["null", "string"], "default": null}
                    ]
                }"#;
                Some(AvroCodec::new(default_schema)?)
            }
        } else {
            None
        };

        let protobuf_codec = if matches!(format, SerializationFormat::Protobuf) {
            if let Some(schema) = protobuf_schema {
                // Use provided schema
                let message_type = protobuf_message_type.unwrap_or("Record");
                match ProtobufCodec::new(schema, message_type) {
                    Ok(codec) => Some(codec),
                    Err(e) => {
                        return Err(Box::new(e) as Box<dyn Error + Send + Sync>);
                    }
                }
            } else {
                // Use the default schema for backward compatibility
                Some(ProtobufCodec::new_with_default_schema())
            }
        } else {
            None
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

    /// Convert StreamRecord to appropriate payload format
    fn serialize_payload(
        &self,
        record: &StreamRecord,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        match self.format {
            SerializationFormat::Json => {
                // Convert all fields to JSON object
                let mut json_obj = serde_json::Map::new();

                for (field_name, field_value) in &record.fields {
                    // Skip the key field if it's used as message key
                    if let Some(key_field) = &self.key_field {
                        if field_name == key_field {
                            continue;
                        }
                    }

                    let json_value = self.field_value_to_json(field_value)?;
                    json_obj.insert(field_name.clone(), json_value);
                }

                // Add metadata fields
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

                let json_str = serde_json::to_string(&Value::Object(json_obj))?;
                Ok(json_str.into_bytes())
            }
            SerializationFormat::Avro => {
                if let Some(codec) = &self.avro_codec {
                    // Use proper Avro serialization
                    codec
                        .serialize(&record.fields)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
                } else {
                    // Fallback to JSON if codec not available
                    let json_obj = self.convert_to_json_object(record)?;
                    let json_str = serde_json::to_string(&json_obj)?;
                    Ok(json_str.into_bytes())
                }
            }
            SerializationFormat::Protobuf => {
                if let Some(codec) = &self.protobuf_codec {
                    // Use proper Protobuf serialization
                    codec
                        .serialize(&record.fields)
                        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
                } else {
                    // Fallback to JSON if codec not available
                    let json_obj = self.convert_to_json_object(record)?;
                    let json_str = serde_json::to_string(&json_obj)?;
                    Ok(json_str.into_bytes())
                }
            }
            SerializationFormat::Auto => {
                // Default to JSON for Auto format
                let json_obj = self.convert_to_json_object(record)?;
                let json_str = serde_json::to_string(&json_obj)?;
                Ok(json_str.into_bytes())
            }
        }
    }

    /// Convert FieldValue to JSON Value using the standard serialization helper
    fn field_value_to_json(
        &self,
        field_value: &FieldValue,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        field_value_to_json(field_value).map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    /// Helper to convert entire record to JSON object
    fn convert_to_json_object(
        &self,
        record: &StreamRecord,
    ) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let mut json_obj = serde_json::Map::new();

        for (field_name, field_value) in &record.fields {
            // Skip the key field if it's used as message key
            if let Some(key_field) = &self.key_field {
                if field_name == key_field {
                    continue;
                }
            }

            let json_value = self.field_value_to_json(field_value)?;
            json_obj.insert(field_name.clone(), json_value);
        }

        // Add metadata fields
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

        Ok(Value::Object(json_obj))
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
