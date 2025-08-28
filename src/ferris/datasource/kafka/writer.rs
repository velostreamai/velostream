//! Kafka data writer implementation

use crate::ferris::datasource::DataWriter;
use crate::ferris::kafka::{
    serialization::{JsonSerializer, Serializer},
    Headers, KafkaProducer,
};
use crate::ferris::serialization::field_value_to_json;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;

#[cfg(feature = "avro")]
use crate::ferris::kafka::serialization::AvroSerializer;
#[cfg(feature = "avro")]
use crate::ferris::serialization::field_value_to_avro;
#[cfg(feature = "avro")]
use apache_avro::types::Value as AvroValue;

/// Generic Kafka DataWriter implementation
pub struct KafkaDataWriter<V, VS>
where
    VS: Serializer<V>,
{
    pub producer: KafkaProducer<String, V, JsonSerializer, VS>,
    pub topic: String,
}

/// Dynamic Kafka writer that can handle different serialization formats
pub enum DynamicKafkaWriter {
    JsonWriter(KafkaDataWriter<Value, JsonSerializer>),
    #[cfg(feature = "avro")]
    AvroWriter(KafkaDataWriter<AvroValue, AvroSerializer>),
    #[cfg(feature = "protobuf")]
    ProtobufWriter(KafkaDataWriter<Vec<u8>, JsonSerializer>),
}

// Helper function to extract key
fn extract_key(fields: &HashMap<String, FieldValue>) -> Option<String> {
    match fields.get("key") {
        Some(FieldValue::String(s)) => Some(s.clone()),
        Some(FieldValue::Null) => None,
        _ => None,
    }
}

// Helper function to convert headers
fn convert_headers(headers_map: &HashMap<String, String>) -> Headers {
    let mut headers = Headers::new();
    for (key, value) in headers_map {
        headers = headers.insert(key, value);
    }
    headers
}

#[async_trait]
impl DataWriter for KafkaDataWriter<Value, JsonSerializer> {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = extract_key(&record.fields);

        // Convert entire record to JSON value
        let mut json_map = serde_json::Map::new();
        for (k, field_value) in &record.fields {
            let json_value = field_value_to_json(field_value)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
            json_map.insert(k.clone(), json_value);
        }
        let value = Value::Object(json_map);

        let headers = convert_headers(&record.headers);

        self.producer
            .send(key.as_ref(), &value, headers, None)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        // Kafka doesn't support updates directly - treat as write
        self.write(record).await
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Kafka doesn't support deletes directly - would need tombstone messages
        Err("Delete not supported for Kafka sink - use tombstone messages".into())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // TODO: Implement flush if supported by underlying producer
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // TODO: Implement transaction commit if supported
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // TODO: Implement transaction rollback if supported
        Ok(())
    }
}

#[cfg(feature = "avro")]
#[async_trait]
impl DataWriter for KafkaDataWriter<AvroValue, AvroSerializer> {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = extract_key(&record.fields);

        // Convert record to Avro value
        let mut avro_fields = Vec::new();
        for (k, field_value) in &record.fields {
            let avro_value = match field_value {
                FieldValue::Null => {
                    // For nullable fields in unions, wrap null in union with index 1
                    AvroValue::Union(1, Box::new(AvroValue::Null))
                }
                _ => field_value_to_avro(field_value)
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?,
            };
            avro_fields.push((k.clone(), avro_value));
        }
        let value = AvroValue::Record(avro_fields);

        let headers = convert_headers(&record.headers);

        self.producer
            .send(key.as_ref(), &value, headers, None)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        self.write(record).await
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        Err("Delete not supported for Kafka sink - use tombstone messages".into())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}

#[cfg(feature = "protobuf")]
#[async_trait]
impl DataWriter for KafkaDataWriter<Vec<u8>, JsonSerializer> {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = extract_key(&record.fields);

        // For protobuf, convert to JSON first then to bytes (simplified approach)
        let mut json_map = serde_json::Map::new();
        for (k, field_value) in &record.fields {
            let json_value = field_value_to_json(field_value)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
            json_map.insert(k.clone(), json_value);
        }
        let json_object = Value::Object(json_map);
        let value = serde_json::to_vec(&json_object)
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let headers = convert_headers(&record.headers);

        self.producer
            .send(key.as_ref(), &value, headers, None)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        self.write(record).await
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        Err("Delete not supported for Kafka sink - use tombstone messages".into())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}

#[async_trait]
impl DataWriter for DynamicKafkaWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.write(record).await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.write(record).await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.write(record).await,
        }
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.write_batch(records).await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.write_batch(records).await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.write_batch(records).await,
        }
    }

    async fn update(
        &mut self,
        key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.update(key, record).await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.update(key, record).await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.update(key, record).await,
        }
    }

    async fn delete(&mut self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.delete(key).await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.delete(key).await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.delete(key).await,
        }
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.flush().await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.flush().await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.flush().await,
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.commit().await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.commit().await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.commit().await,
        }
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self {
            DynamicKafkaWriter::JsonWriter(writer) => writer.rollback().await,
            #[cfg(feature = "avro")]
            DynamicKafkaWriter::AvroWriter(writer) => writer.rollback().await,
            #[cfg(feature = "protobuf")]
            DynamicKafkaWriter::ProtobufWriter(writer) => writer.rollback().await,
        }
    }
}
