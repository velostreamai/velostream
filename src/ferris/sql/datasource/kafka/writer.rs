//! Kafka data writer implementation

use async_trait::async_trait;
use crate::ferris::kafka::{
    Headers, KafkaProducer,
    serialization::JsonSerializer,
};
use crate::ferris::sql::datasource::DataWriter;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::error::Error;

/// Kafka DataWriter implementation
pub struct KafkaDataWriter {
    pub producer: KafkaProducer<String, String, JsonSerializer, JsonSerializer>,
    pub topic: String,
}

#[async_trait]
impl DataWriter for KafkaDataWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Extract key from record fields
        let key = match record.fields.get("key") {
            Some(FieldValue::String(s)) => Some(s.clone()),
            Some(FieldValue::Null) => None,
            _ => None,
        };

        // Extract value from record fields
        let value = match record.fields.get("value") {
            Some(FieldValue::String(s)) => s.clone(),
            _ => format!("{:?}", record.fields), // Simple debug format instead of JSON
        };

        // Convert headers
        let mut headers = Headers::new();
        for (key, value) in &record.headers {
            headers = headers.insert(key, value);
        }

        // Send message
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