//! Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset};
use crate::ferris::kafka::{
    kafka_error::ConsumerError, serialization::JsonSerializer, KafkaConsumer,
};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

/// Kafka DataReader implementation
pub struct KafkaDataReader {
    pub consumer: KafkaConsumer<String, String, JsonSerializer, JsonSerializer>,
    pub topic: String,
}

#[async_trait]
impl DataReader for KafkaDataReader {
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
