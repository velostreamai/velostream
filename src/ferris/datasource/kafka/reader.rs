//! Unified Kafka data reader implementation

use crate::ferris::datasource::{DataReader, SourceOffset, BatchConfig, BatchStrategy};
use crate::ferris::kafka::{serialization::StringSerializer, KafkaConsumer};

// Removed unused imports - AvroSerializer and ProtoSerializer don't exist
// Using AvroCodec and ProtobufCodec directly instead
use crate::ferris::serialization::{json_codec::JsonCodec, SerializationCodec};
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use async_trait::async_trait;
use chrono;
use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};
use log::info;

// Import the unified SerializationFormat from the main module
pub use crate::ferris::kafka::serialization_format::SerializationFormat;

/// Unified Kafka DataReader that handles all serialization formats
pub struct KafkaDataReader {
    consumer:
        KafkaConsumer<String, HashMap<String, FieldValue>, StringSerializer, SerializationCodec>,
    batch_config: BatchConfig,
    // State for adaptive batching
    current_batch_start: Option<Instant>,
    adaptive_state: AdaptiveBatchState,
}

/// State tracking for adaptive batch sizing
#[derive(Debug, Clone)]
struct AdaptiveBatchState {
    current_size: usize,
    recent_latencies: Vec<Duration>,
    last_adjustment: Instant,
}

impl AdaptiveBatchState {
    fn new(initial_size: usize) -> Self {
        Self {
            current_size: initial_size,
            recent_latencies: Vec::with_capacity(10),
            last_adjustment: Instant::now(),
        }
    }
    
    fn record_latency(&mut self, latency: Duration) {
        self.recent_latencies.push(latency);
        if self.recent_latencies.len() > 10 {
            self.recent_latencies.remove(0);
        }
    }
    
    fn average_latency(&self) -> Option<Duration> {
        if self.recent_latencies.is_empty() {
            None
        } else {
            let total: Duration = self.recent_latencies.iter().sum();
            Some(total / self.recent_latencies.len() as u32)
        }
    }
}

impl KafkaDataReader {
    /// Create a serialization codec based on format and schema, with robust error handling
    fn create_serialization_codec(
        format: &SerializationFormat,
        schema_json: Option<&str>,
    ) -> Result<SerializationCodec, Box<dyn Error + Send + Sync>> {
        use crate::ferris::serialization::helpers;

        match format {
            // JSON is always available - the default fallback
            SerializationFormat::Json => Ok(SerializationCodec::Json(JsonCodec::new())),

            // Avro requires schema
            SerializationFormat::Avro {
                schema_registry_url: _,
                subject: _,
            } => {
                if let Some(schema) = schema_json {
                    let avro_codec = helpers::create_avro_codec(Some(schema))?;
                    Ok(SerializationCodec::Avro(avro_codec))
                } else {
                    Err("Avro serialization requires a schema. Please provide schema JSON or use JSON format as fallback.".into())
                }
            }

            // Protobuf with optional schema
            SerializationFormat::Protobuf { message_type: _ } => {
                if schema_json.is_some() {
                    let protobuf_codec = helpers::create_protobuf_codec(schema_json)?;
                    Ok(SerializationCodec::Protobuf(protobuf_codec))
                } else {
                    // For protobuf, we can create a default schema codec
                    let protobuf_codec = helpers::create_protobuf_codec(None)?;
                    Ok(SerializationCodec::Protobuf(protobuf_codec))
                }
            }

            // Bytes and String formats are converted to JSON for structured data
            SerializationFormat::Bytes | SerializationFormat::String => {
                // For structured data processing, we use JSON as the internal format
                // The bytes/string will be converted to FieldValue::String during processing
                Ok(SerializationCodec::Json(JsonCodec::new()))
            }
        }
    }

    /// Create a new Kafka data reader with optional schema and batch configuration
    pub async fn new_with_schema(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_config: Option<BatchConfig>,
        passed_schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create codec using robust factory pattern
        let codec = Self::create_serialization_codec(&format, passed_schema_json)?;

        let batch_config = batch_config.unwrap_or_default();
        
        // Configure Kafka consumer based on BatchConfig
        let mut consumer_config = crate::ferris::kafka::consumer_config::ConsumerConfig::new(brokers, group_id);
        Self::apply_batch_config_to_consumer(&mut consumer_config, &batch_config);

        // Log the applied consumer configuration
        Self::log_consumer_config(&consumer_config, &batch_config);

        let consumer = crate::ferris::kafka::KafkaConsumer::with_config(consumer_config, StringSerializer, codec)?;

        consumer
            .subscribe(&[topic.as_str()])
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        let initial_size = match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => *size,
            BatchStrategy::AdaptiveSize { min_size, .. } => *min_size,
            BatchStrategy::LowLatency { max_batch_size, .. } => *max_batch_size,
            _ => 100,
        };

        Ok(Self {
            consumer,
            batch_config,
            current_batch_start: None,
            adaptive_state: AdaptiveBatchState::new(initial_size),
        })
    }
    
    /// Create a new Kafka data reader with BatchConfig (preferred method)
    pub async fn new_with_batch_config(
        brokers: &str,
        topic: String,
        group_id: &str,
        format: SerializationFormat,
        batch_config: BatchConfig,
        passed_schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new_with_schema(brokers, topic, group_id, format, Some(batch_config), passed_schema_json).await
    }

    /// Create a new Kafka data reader with JSON format (convenience method)
    pub async fn new_json(
        brokers: &str,
        topic: String,
        group_id: &str,
        batch_size: Option<usize>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let batch_config = batch_size.map(|size| BatchConfig {
            strategy: BatchStrategy::FixedSize(size),
            ..Default::default()
        });
        
        Self::new_with_schema(
            brokers,
            topic,
            group_id,
            SerializationFormat::Json,
            batch_config,
            None,
        )
        .await
    }

    /// Create a new Kafka data reader with format from string (with error handling)
    pub async fn new_from_format_string(
        brokers: &str,
        topic: String,
        group_id: &str,
        format_str: &str,
        batch_size: Option<usize>,
        schema_json: Option<&str>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        use std::str::FromStr;

        // Parse format string with proper error handling
        let format = SerializationFormat::from_str(format_str)
            .map_err(|e| format!("Invalid serialization format '{}': {}", format_str, e))?;

        let batch_config = batch_size.map(|size| BatchConfig {
            strategy: BatchStrategy::FixedSize(size),
            ..Default::default()
        });

        Self::new_with_schema(brokers, topic, group_id, format, batch_config, schema_json).await
    }
}

#[async_trait]
impl DataReader for KafkaDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        if !self.batch_config.enable_batching {
            return self.read_single().await;
        }

        match &self.batch_config.strategy {
            BatchStrategy::FixedSize(size) => self.read_fixed_size(*size).await,
            BatchStrategy::TimeWindow(duration) => self.read_time_window(*duration).await,
            BatchStrategy::AdaptiveSize { min_size, max_size, target_latency } => {
                self.read_adaptive(*min_size, *max_size, *target_latency).await
            }
            BatchStrategy::MemoryBased(max_bytes) => self.read_memory_based(*max_bytes).await,
            BatchStrategy::LowLatency { max_batch_size, max_wait_time, eager_processing } => {
                self.read_low_latency(*max_batch_size, *max_wait_time, *eager_processing).await
            }
        }
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

impl KafkaDataReader {
    /// Apply BatchConfig settings to Kafka consumer configuration
    fn apply_batch_config_to_consumer(
        consumer_config: &mut crate::ferris::kafka::consumer_config::ConsumerConfig,
        batch_config: &BatchConfig,
    ) {
        if !batch_config.enable_batching {
            // Disable batching - use minimal settings
            consumer_config.max_poll_records = 1;
            return;
        }

        // Configure consumer based on batch strategy
        match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => {
                // Set max_poll_records to batch size, capped by max_batch_size
                let poll_records = (*size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;
                
                // Use reasonable fetch settings for fixed size batching
                consumer_config.fetch_min_bytes = 1; // Don't wait for more data
                consumer_config.fetch_max_wait = std::time::Duration::from_millis(100); // Short wait
            }
            BatchStrategy::TimeWindow(duration) => {
                // For time-based batching, use a reasonable poll size and longer fetch wait
                consumer_config.max_poll_records = (batch_config.max_batch_size / 2) as u32;
                consumer_config.fetch_max_wait = *duration;
                consumer_config.fetch_min_bytes = 1; // Accept any amount of data
            }
            BatchStrategy::AdaptiveSize { min_size, max_size: _, target_latency } => {
                // Start conservatively with min_size
                let poll_records = (*min_size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;
                
                // Use target latency as fetch wait time
                consumer_config.fetch_max_wait = *target_latency;
                consumer_config.fetch_min_bytes = 1;
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                // For memory-based batching, use conservative polling
                // Estimate ~1KB per record as baseline
                let estimated_records = (*max_bytes / 1024).min(batch_config.max_batch_size);
                consumer_config.max_poll_records = estimated_records as u32;
                
                // Use memory target to set fetch limits
                consumer_config.max_partition_fetch_bytes = (*max_bytes as u32).min(50 * 1024 * 1024); // Cap at 50MB
                consumer_config.fetch_min_bytes = 1;
                consumer_config.fetch_max_wait = batch_config.batch_timeout;
            }

            // Ultra-low latency consumer settings
            //     fetch_min_bytes: 1           # Don't wait for batches
            //     fetch_max_wait_ms: 1         # 1ms maximum wait
            //     max_poll_records: 10         # Process small batches quickly
            // session_timeout_ms: 6000     # Fast failure detection
            //     heartbeat_interval_ms: 2000  # Frequent heartbeats
            // auto_offset_reset: "latest"  # Start from newest messages

            BatchStrategy::LowLatency { max_batch_size, max_wait_time, eager_processing: _ } => {
                // Optimize for minimal latency with very small batches
                let poll_records = (*max_batch_size).min(batch_config.max_batch_size) as u32;
                consumer_config.max_poll_records = poll_records;
                
                // Use aggressive timeout settings for low latency
                consumer_config.fetch_max_wait = *max_wait_time;
                consumer_config.fetch_min_bytes = 1; // Don't wait for data to accumulate
                consumer_config.max_partition_fetch_bytes = 64 * 1024; // Small fetch size (64KB)
                
                // Enable low-level optimizations for latency
                consumer_config.enable_auto_commit = true;
                consumer_config.auto_commit_interval = std::time::Duration::from_millis(50); // Frequent commits
            }
        }
        
        // Apply general batch timeout
        consumer_config.max_poll_interval = batch_config.batch_timeout.max(std::time::Duration::from_secs(30));
    }

    /// Log the consumer configuration for debugging and monitoring
    fn log_consumer_config(
        consumer_config: &crate::ferris::kafka::consumer_config::ConsumerConfig,
        batch_config: &BatchConfig,
    ) {
        info!("=== Kafka Consumer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);
        
        info!("Applied Consumer Settings:");
        info!("  - max_poll_records: {}", consumer_config.max_poll_records);
        info!("  - fetch_min_bytes: {}", consumer_config.fetch_min_bytes);
        info!("  - fetch_max_wait: {:?}", consumer_config.fetch_max_wait);
        info!("  - max_partition_fetch_bytes: {}", consumer_config.max_partition_fetch_bytes);
        info!("  - enable_auto_commit: {}", consumer_config.enable_auto_commit);
        info!("  - auto_commit_interval: {:?}", consumer_config.auto_commit_interval);
        info!("  - max_poll_interval: {:?}", consumer_config.max_poll_interval);
        info!("  - brokers: {}", consumer_config.brokers());
        info!("  - group_id: {}", consumer_config.group_id);
        info!("=====================================");
    }

    /// Read a single record (when batching is disabled)
    async fn read_single(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let timeout = self.batch_config.batch_timeout;
        
        match self.consumer.poll(timeout).await {
            Ok(mut message) => {
                let record = self.create_stream_record(message)?;
                Ok(vec![record])
            }
            Err(_) => Ok(vec![]), // Timeout or error - return empty batch
        }
    }

    /// Read fixed number of records
    async fn read_fixed_size(&mut self, size: usize) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(size.min(self.batch_config.max_batch_size));
        let timeout = Duration::from_millis(1000);

        for _ in 0..size.min(self.batch_config.max_batch_size) {
            match self.consumer.poll(timeout).await {
                Ok(mut message) => {
                    let record = self.create_stream_record(message)?;
                    records.push(record);
                }
                Err(_) => {
                    // Timeout or error - break if we have some records, otherwise continue
                    if !records.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(records)
    }

    /// Read records within a time window
    async fn read_time_window(&mut self, duration: Duration) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::new();
        let start_time = Instant::now();
        let timeout = Duration::from_millis(100); // Short poll timeout for time-based batching
        
        // Initialize batch start time if not set
        if self.current_batch_start.is_none() {
            self.current_batch_start = Some(start_time);
        }

        while start_time.elapsed() < duration && records.len() < self.batch_config.max_batch_size {
            match self.consumer.poll(timeout).await {
                Ok(mut message) => {
                    let record = self.create_stream_record(message)?;
                    records.push(record);
                }
                Err(_) => {
                    // Continue trying until time window expires
                    if records.is_empty() && start_time.elapsed() < duration {
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        // Reset batch start time for next batch
        self.current_batch_start = None;
        Ok(records)
    }

    /// Read records with adaptive batch sizing
    async fn read_adaptive(&mut self, min_size: usize, max_size: usize, target_latency: Duration) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let batch_start = Instant::now();
        let current_size = self.adaptive_state.current_size.clamp(min_size, max_size.min(self.batch_config.max_batch_size));
        
        // Read current adaptive batch size
        let records = self.read_fixed_size(current_size).await?;
        let batch_latency = batch_start.elapsed();
        
        // Record latency for adaptive adjustment
        self.adaptive_state.record_latency(batch_latency);
        
        // Adjust batch size every 10 seconds
        if self.adaptive_state.last_adjustment.elapsed() > Duration::from_secs(10) {
            if let Some(avg_latency) = self.adaptive_state.average_latency() {
                if avg_latency > target_latency && current_size > min_size {
                    // Too slow, reduce batch size
                    self.adaptive_state.current_size = (current_size as f64 * 0.8) as usize;
                    self.adaptive_state.current_size = self.adaptive_state.current_size.max(min_size);
                } else if avg_latency < target_latency / 2 && current_size < max_size {
                    // Too fast, increase batch size
                    self.adaptive_state.current_size = (current_size as f64 * 1.2) as usize;
                    self.adaptive_state.current_size = self.adaptive_state.current_size.min(max_size);
                }
            }
            self.adaptive_state.last_adjustment = Instant::now();
        }

        Ok(records)
    }

    /// Read records up to a memory limit (approximate)
    async fn read_memory_based(&mut self, max_bytes: usize) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::new();
        let mut estimated_size = 0usize;
        let timeout = Duration::from_millis(1000);

        while estimated_size < max_bytes && records.len() < self.batch_config.max_batch_size {
            match self.consumer.poll(timeout).await {
                Ok(mut message) => {
                    let record = self.create_stream_record(message)?;
                    
                    // Rough estimate: 24 bytes overhead + field data
                    let record_size = 24 + record.fields.iter()
                        .map(|(k, v)| k.len() + self.estimate_field_size(v))
                        .sum::<usize>();
                    
                    if estimated_size + record_size > max_bytes && !records.is_empty() {
                        // Would exceed memory limit, return current batch
                        break;
                    }
                    
                    estimated_size += record_size;
                    records.push(record);
                }
                Err(_) => {
                    // Timeout or error - return what we have
                    if !records.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(records)
    }

    /// Read records with low-latency optimization
    async fn read_low_latency(&mut self, max_batch_size: usize, max_wait_time: Duration, eager_processing: bool) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>> {
        let mut records = Vec::with_capacity(max_batch_size.min(self.batch_config.max_batch_size));
        let start_time = Instant::now();
        
        // Low-latency strategy: prioritize immediate processing over batch completeness
        while records.len() < max_batch_size && start_time.elapsed() < max_wait_time {
            let poll_result = if eager_processing {
                // Poll with immediate timeout for eager processing
                self.consumer.poll(Duration::from_millis(0))
            } else {
                // Poll with minimal timeout
                self.consumer.poll(std::cmp::min(max_wait_time, Duration::from_millis(1)))
            };
            
            match poll_result.await {
                Ok(message) => {
                    let record = self.create_stream_record(message)?;
                    records.push(record);
                    
                    // For eager processing, return immediately after first record
                    if eager_processing && !records.is_empty() {
                        break;
                    }
                }
                Err(e) => {
                    // In low-latency mode, we don't want to wait on errors
                    eprintln!("Kafka poll error in low-latency mode: {:?}", e);
                    break;
                }
            }
        }
        
        Ok(records)
    }

    /// Helper method to create a StreamRecord from a Kafka message
    fn create_stream_record(&self, mut message: crate::ferris::kafka::message::Message<String, HashMap<String, FieldValue>>) -> Result<StreamRecord, Box<dyn Error + Send + Sync>> {
        let mut fields = message.take_value();

        // Add message key to fields map if present
        if let Some(key) = message.take_key() {
            fields.insert("key".to_string(), FieldValue::String(key));
        } else {
            fields.insert("key".to_string(), FieldValue::Null);
        }

        Ok(StreamRecord {
            fields,
            timestamp: message.timestamp().unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
            offset: message.offset(),
            partition: message.partition(),
            headers: message.take_headers().into_map(),
        })
    }

    /// Estimate memory size of a FieldValue
    fn estimate_field_size(&self, field: &FieldValue) -> usize {
        match field {
            FieldValue::String(s) => s.len(),
            FieldValue::Integer(_) => 8,
            FieldValue::Float(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::ScaledInteger(_, _) => 16,
            FieldValue::Timestamp(_) => 16,
            FieldValue::Date(_) => 8,
            FieldValue::Decimal(_) => 16,
            FieldValue::Null => 0,
            FieldValue::Interval { .. } => 16,
            FieldValue::Array(arr) => 24 + arr.iter().map(|v| self.estimate_field_size(v)).sum::<usize>(),
            FieldValue::Map(map) => 24 + map.iter().map(|(k, v)| k.len() + self.estimate_field_size(v)).sum::<usize>(),
            FieldValue::Struct(s) => 24 + s.iter().map(|(k, v)| k.len() + self.estimate_field_size(v)).sum::<usize>(),
        }
    }
}
