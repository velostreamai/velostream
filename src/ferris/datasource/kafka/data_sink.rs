//! Kafka data sink implementation

use crate::ferris::datasource::{DataSink, DataWriter, SinkConfig, SinkMetadata, BatchConfig, BatchStrategy};
use crate::ferris::schema::Schema;
use async_trait::async_trait;
use std::collections::HashMap;

use super::error::KafkaDataSourceError;
use super::writer::KafkaDataWriter;

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

    /// Create a Kafka data sink from properties
    pub fn from_properties(props: &HashMap<String, String>, job_name: &str) -> Self {
        // Helper function to get property with sink. prefix fallback
        let get_sink_prop = |key: &str| {
            props
                .get(&format!("sink.{}", key))
                .or_else(|| props.get(key))
                .cloned()
        };

        let brokers = get_sink_prop("brokers")
            .or_else(|| get_sink_prop("bootstrap.servers"))
            .unwrap_or_else(|| "localhost:9092".to_string());
        let topic = get_sink_prop("topic").unwrap_or_else(|| format!("{}_output", job_name));

        // Create filtered config with sink. properties
        let mut sink_config = HashMap::new();
        for (key, value) in props.iter() {
            if key.starts_with("sink.") {
                // Remove sink. prefix for the config map
                let config_key = key.strip_prefix("sink.").unwrap().to_string();
                sink_config.insert(config_key, value.clone());
            } else if !key.starts_with("source.") && !props.contains_key(&format!("sink.{}", key)) {
                // Include unprefixed properties only if there's no prefixed version and it's not a source property
                sink_config.insert(key.clone(), value.clone());
            }
        }

        Self {
            brokers,
            topic,
            config: sink_config,
        }
    }

    /// Create a unified writer based on the serialization format in config
    async fn create_unified_writer(
        &self,
    ) -> Result<KafkaDataWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Get serialization format from config (default to JSON if not specified)
        let _value_format = self
            .config
            .get("value.serializer")
            .map(|s| s.as_str())
            .unwrap_or("json");

        // Map config value to SerializationFormat enum
        // Format is now handled by KafkaDataWriter::from_properties

        // Create writer with properties-based configuration
        KafkaDataWriter::from_properties(&self.brokers, self.topic.clone(), &self.config).await
    }

    /// Create a unified writer with batch configuration optimizations
    async fn create_unified_writer_with_batch_config(
        &self,
        batch_config: BatchConfig,
    ) -> Result<KafkaDataWriter, Box<dyn std::error::Error + Send + Sync>> {
        // Let the KafkaDataWriter handle batch configuration directly
        // This provides better integration and more accurate configuration
        KafkaDataWriter::from_properties_with_batch_config(
            &self.brokers, 
            self.topic.clone(), 
            &self.config,
            batch_config
        ).await
    }

    /// Apply BatchConfig settings to Kafka producer configuration
    /// NOTE: This method is now primarily used for logging. The actual batch configuration
    /// is handled directly by KafkaDataWriter::from_properties_with_batch_config()
    fn apply_batch_config_to_producer(
        &self,
        producer_config: &mut HashMap<String, String>,
        batch_config: &BatchConfig,
    ) {
        if !batch_config.enable_batching {
            // Disable batching - send immediately
            producer_config.insert("batch.size".to_string(), "0".to_string()); // No batching
            producer_config.insert("linger.ms".to_string(), "0".to_string()); // Send immediately
            return;
        }

        // Configure producer based on batch strategy
        match &batch_config.strategy {
            BatchStrategy::FixedSize(size) => {
                // Set batch.size to accommodate fixed batch size
                let batch_size = (*size * 1024).min(batch_config.max_batch_size * 1024); // KB estimate
                producer_config.insert("batch.size".to_string(), batch_size.to_string());
                
                // Use moderate linger time for fixed batching
                producer_config.insert("linger.ms".to_string(), "10".to_string());
                
                // Suggest compression only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "snappy".to_string()); // Efficient compression
                }
            }
            BatchStrategy::TimeWindow(duration) => {
                // Use time-based batching with linger.ms
                let linger_ms = duration.as_millis().min(30000) as u64; // Cap at 30s
                producer_config.insert("linger.ms".to_string(), linger_ms.to_string());
                producer_config.insert("batch.size".to_string(), "65536".to_string()); // 64KB batches
                
                // Suggest fast compression only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "lz4".to_string()); // Fast compression
                }
            }
            BatchStrategy::AdaptiveSize { target_latency, .. } => {
                // Use target latency as linger time
                let linger_ms = target_latency.as_millis().min(5000) as u64; // Cap at 5s
                producer_config.insert("linger.ms".to_string(), linger_ms.to_string());
                producer_config.insert("batch.size".to_string(), "32768".to_string()); // 32KB adaptive batches
                
                // Suggest compression only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "snappy".to_string());
                }
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                // Set batch.size based on memory target
                let batch_size = (*max_bytes / 2).min(1024 * 1024); // Half of memory target, max 1MB
                producer_config.insert("batch.size".to_string(), batch_size.to_string());
                producer_config.insert("linger.ms".to_string(), "100".to_string()); // Longer linger for large batches
                
                // Suggest better compression for large batches only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "gzip".to_string()); // Better compression for large batches
                }
                
                // Use memory target for buffer settings
                let buffer_memory = (*max_bytes * 4).min(64 * 1024 * 1024); // 4x batch size, max 64MB
                producer_config.insert("buffer.memory".to_string(), buffer_memory.to_string());
            }
            BatchStrategy::LowLatency { max_wait_time, .. } => {
                // Optimize for minimal latency
                producer_config.insert("batch.size".to_string(), "1024".to_string()); // Very small batches (1KB)
                let linger_ms = max_wait_time.as_millis().min(10) as u64; // Ultra-short linger time
                producer_config.insert("linger.ms".to_string(), linger_ms.to_string());
                
                // Suggest no compression for minimal latency only if not explicitly set
                if !producer_config.contains_key("compression.type") {
                    producer_config.insert("compression.type".to_string(), "none".to_string()); // No compression overhead
                }
                
                producer_config.insert("acks".to_string(), "1".to_string()); // Fast acknowledgment (leader only)
                producer_config.insert("retries".to_string(), "0".to_string()); // No retries for speed
                producer_config.insert("max.in.flight.requests.per.connection".to_string(), "5".to_string()); // Parallel requests
            }
        }

        // Apply general batch timeout as request timeout
        let request_timeout_ms = batch_config.batch_timeout.as_millis().min(300000) as u64; // Max 5 minutes
        producer_config.insert("request.timeout.ms".to_string(), request_timeout_ms.to_string());
        
        // Log the producer configuration
        self.log_producer_config(producer_config, batch_config);
    }

    /// Log the producer configuration for debugging and monitoring
    fn log_producer_config(
        &self,
        producer_config: &HashMap<String, String>,
        batch_config: &BatchConfig,
    ) {
        use log::info;
        
        info!("=== Kafka Producer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);
        
        info!("Applied Producer Settings:");
        info!("  - batch.size: {}", producer_config.get("batch.size").unwrap_or(&"default".to_string()));
        info!("  - linger.ms: {}", producer_config.get("linger.ms").unwrap_or(&"default".to_string()));
        info!("  - compression.type: {}", producer_config.get("compression.type").unwrap_or(&"none".to_string()));
        info!("  - acks: {}", producer_config.get("acks").unwrap_or(&"all".to_string()));
        info!("  - retries: {}", producer_config.get("retries").unwrap_or(&"default".to_string()));
        info!("  - request.timeout.ms: {}", producer_config.get("request.timeout.ms").unwrap_or(&"default".to_string()));
        if let Some(buffer_memory) = producer_config.get("buffer.memory") {
            info!("  - buffer.memory: {}", buffer_memory);
        }
        info!("  - topic: {}", self.topic);
        info!("  - brokers: {}", self.brokers);
        info!("=====================================");
    }

    /// Add a configuration parameter
    pub fn with_config(mut self, key: String, value: String) -> Self {
        self.config.insert(key, value);
        self
    }

    // Getter methods for testing
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn config(&self) -> &HashMap<String, String> {
        &self.config
    }
}

#[async_trait]
impl DataSink for KafkaDataSink {
    async fn initialize(
        &mut self,
        config: SinkConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match config {
            SinkConfig::Kafka {
                brokers,
                topic,
                properties,
            } => {
                self.brokers = brokers;
                self.topic = topic;
                self.config = properties;
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
        // Create the unified writer
        let writer = self.create_unified_writer().await.map_err(|e| {
            Box::new(KafkaDataSourceError::Configuration(format!(
                "Failed to create writer: {}",
                e
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(writer))
    }

    async fn create_writer_with_batch_config(
        &self,
        batch_config: BatchConfig,
    ) -> Result<Box<dyn DataWriter>, Box<dyn std::error::Error + Send + Sync>> {
        // Create a writer optimized for the specific batch strategy
        let writer = self.create_unified_writer_with_batch_config(batch_config).await.map_err(|e| {
            Box::new(KafkaDataSourceError::Configuration(format!(
                "Failed to create batch-optimized writer: {}",
                e
            ))) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(Box::new(writer))
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
