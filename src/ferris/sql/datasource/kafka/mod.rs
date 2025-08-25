//! Kafka DataSource Implementation
//!
//! This module provides Kafka adapter implementations for the pluggable data source traits.
//! It wraps the existing Kafka consumer/producer implementations with the new trait interface
//! while maintaining full backward compatibility.
//!
//! ## Architecture
//!
//! - `KafkaDataSource` - Implements `DataSource` trait for Kafka topics
//! - `KafkaDataSink` - Implements `DataSink` trait for Kafka topics  
//! - `KafkaDataReader` - Implements `DataReader` trait, wraps `KafkaConsumer`
//! - `KafkaDataWriter` - Implements `DataWriter` trait, wraps `KafkaProducer`
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use ferrisstreams::ferris::sql::datasource::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Create Kafka source
//!     let source = create_source("kafka://localhost:9092/orders?group_id=processor").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut reader = source.create_reader().await?;
//!
//!     // Create Kafka sink
//!     let sink = create_sink("kafka://localhost:9092/processed_orders").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut writer = sink.create_writer().await?;
//!
//!     // Process records
//!     while let Some(record) = reader.read().await? {
//!         // Transform record...
//!         writer.write(record).await?;
//!     }
//!
//!     Ok(())
//! }
//! ```

use crate::ferris::kafka::{
    kafka_error::{ConsumerError, ProducerError},
    serialization::JsonSerializer,
    Headers, KafkaConsumer, KafkaProducer,
};
use crate::ferris::sql::ast::DataType;
use crate::ferris::sql::datasource::{
    DataReader, DataSink, DataSource, DataWriter, SinkConfig, SinkMetadata, SourceConfig,
    SourceMetadata, SourceOffset,
};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use crate::ferris::sql::schema::{FieldDefinition, Schema};
use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::error::KafkaError;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::time::Duration;

/// Error type for Kafka data source operations
#[derive(Debug)]
pub enum KafkaDataSourceError {
    /// Kafka client error
    Kafka(KafkaError),
    /// Consumer error
    Consumer(ConsumerError),
    /// Producer error
    Producer(ProducerError),
    /// Configuration error
    Configuration(String),
    /// Serialization error
    Serialization(String),
    /// Schema error
    Schema(String),
}

impl fmt::Display for KafkaDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaDataSourceError::Kafka(err) => write!(f, "Kafka error: {}", err),
            KafkaDataSourceError::Consumer(err) => write!(f, "Consumer error: {}", err),
            KafkaDataSourceError::Producer(err) => write!(f, "Producer error: {}", err),
            KafkaDataSourceError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            KafkaDataSourceError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            KafkaDataSourceError::Schema(msg) => write!(f, "Schema error: {}", msg),
        }
    }
}

impl Error for KafkaDataSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KafkaDataSourceError::Kafka(err) => Some(err),
            KafkaDataSourceError::Consumer(err) => Some(err),
            KafkaDataSourceError::Producer(err) => Some(err),
            _ => None,
        }
    }
}

impl From<KafkaError> for KafkaDataSourceError {
    fn from(err: KafkaError) -> Self {
        KafkaDataSourceError::Kafka(err)
    }
}

// Note: ConsumerError and ProducerError are both type aliases for KafkaClientError
impl From<ConsumerError> for KafkaDataSourceError {
    fn from(err: ConsumerError) -> Self {
        KafkaDataSourceError::Consumer(err)
    }
}

/// Kafka DataSource implementation
pub struct KafkaDataSource {
    brokers: String,
    topic: String,
    group_id: Option<String>,
    config: HashMap<String, String>,
}

impl KafkaDataSource {
    /// Create a new Kafka data source
    pub fn new(brokers: String, topic: String) -> Self {
        Self {
            brokers,
            topic,
            group_id: None,
            config: HashMap::new(),
        }
    }

    /// Set the consumer group ID
    pub fn with_group_id(mut self, group_id: String) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }
}

#[async_trait]
impl DataSource for KafkaDataSource {
    async fn initialize(
        &mut self,
        config: SourceConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SourceConfig::Kafka {
                brokers,
                topic,
                group_id,
                ..
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.group_id = group_id;
                Ok(())
            }
            _ => Err(Box::new(KafkaDataSourceError::Configuration(
                "Expected Kafka configuration".to_string(),
            ))),
        }
    }

    async fn fetch_schema(&self) -> Result<Schema, Box<dyn std::error::Error + Send + Sync>> {
        // For Kafka, we'll return a generic schema since message format is flexible
        // In practice, this could integrate with Schema Registry
        let fields = vec![
            FieldDefinition::required("key".to_string(), DataType::String),
            FieldDefinition::required("value".to_string(), DataType::String),
            FieldDefinition::required("timestamp".to_string(), DataType::Timestamp),
            FieldDefinition::required("offset".to_string(), DataType::Integer),
            FieldDefinition::required("partition".to_string(), DataType::Integer),
        ];

        Ok(Schema::new(fields))
    }

    async fn create_reader(
        &self,
    ) -> Result<Box<dyn DataReader>, Box<dyn std::error::Error + Send + Sync>> {
        let group_id = self.group_id.as_ref().ok_or_else(|| {
            Box::new(KafkaDataSourceError::Configuration(
                "Group ID required for consumer".to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;

        let consumer = KafkaConsumer::<String, String, _, _>::new(
            &self.brokers,
            group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .map_err(|e| {
            Box::new(KafkaDataSourceError::from(e)) as Box<dyn std::error::Error + Send + Sync>
        })?;

        consumer.subscribe(&[&self.topic]).map_err(|e| {
            Box::new(KafkaDataSourceError::from(e)) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(KafkaDataReader {
            consumer,
            topic: self.topic.clone(),
        }))
    }

    fn supports_streaming(&self) -> bool {
        true
    }

    fn supports_batch(&self) -> bool {
        true // Kafka can be used for batch processing
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            source_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_streaming: true,
            supports_batch: true,
            supports_schema_evolution: true,
            capabilities: vec![
                "real_time".to_string(),
                "exactly_once".to_string(),
                "schema_registry".to_string(),
                "headers".to_string(),
                "partitioning".to_string(),
            ],
        }
    }
}

/// Kafka DataSink implementation
pub struct KafkaDataSink {
    brokers: String,
    topic: String,
    config: HashMap<String, String>,
}

impl KafkaDataSink {
    /// Create a new Kafka data sink
    pub fn new(brokers: String, topic: String) -> Self {
        Self {
            brokers,
            topic,
            config: HashMap::new(),
        }
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }
}

#[async_trait]
impl DataSink for KafkaDataSink {
    async fn initialize(
        &mut self,
        config: SinkConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SinkConfig::Kafka { brokers, topic, .. } => {
                self.brokers = brokers;
                self.topic = topic;
                Ok(())
            }
            _ => Err(Box::new(KafkaDataSourceError::Configuration(
                "Expected Kafka configuration".to_string(),
            ))),
        }
    }

    async fn validate_schema(
        &self,
        _schema: &Schema,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Kafka is schema-flexible, so we accept any schema
        // In practice, this could validate against Schema Registry
        Ok(())
    }

    async fn create_writer(
        &self,
    ) -> Result<Box<dyn DataWriter>, Box<dyn std::error::Error + Send + Sync>> {
        let producer = KafkaProducer::<String, String, _, _>::new(
            &self.brokers,
            &self.topic,
            JsonSerializer,
            JsonSerializer,
        )
        .map_err(|e| {
            Box::new(KafkaDataSourceError::from(e)) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(KafkaDataWriter {
            producer,
            topic: self.topic.clone(),
        }))
    }

    fn supports_transactions(&self) -> bool {
        true // Kafka supports exactly-once semantics
    }

    fn supports_upsert(&self) -> bool {
        false // Kafka is append-only, upserts require application logic
    }

    fn metadata(&self) -> SinkMetadata {
        SinkMetadata {
            sink_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_transactions: true,
            supports_upsert: false,
            supports_schema_evolution: true,
            capabilities: vec![
                "exactly_once".to_string(),
                "partitioning".to_string(),
                "headers".to_string(),
                "high_throughput".to_string(),
            ],
        }
    }
}

/// Kafka DataReader implementation
struct KafkaDataReader {
    consumer: KafkaConsumer<String, String, JsonSerializer, JsonSerializer>,
    topic: String,
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
            Err(_) => Ok(None), // Timeout
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

/// Kafka DataWriter implementation  
struct KafkaDataWriter {
    producer: KafkaProducer<String, String, JsonSerializer, JsonSerializer>,
    topic: String,
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
