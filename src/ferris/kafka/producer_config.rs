use crate::ferris::kafka::common_config::{CommonKafkaConfig, HasCommonConfig};
use crate::ferris::kafka::performance_presets::{PerformancePresets, presets};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for Kafka producer with sensible defaults
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Common configuration shared with consumer
    pub common: CommonKafkaConfig,
    /// Default topic for messages
    pub default_topic: String,
    /// Message timeout
    pub message_timeout: Duration,
    /// Delivery timeout
    pub delivery_timeout: Duration,
    /// Enable idempotent producer
    pub enable_idempotence: bool,
    /// Max in-flight requests per connection
    pub max_in_flight_requests: u32,
    /// Number of retries
    pub retries: u32,
    /// Batch size in bytes
    pub batch_size: u32,
    /// Linger time before sending
    pub linger_ms: Duration,
    /// Compression type
    pub compression_type: CompressionType,
    /// Ack mode
    pub acks: AckMode,
    /// Buffer memory in bytes
    pub buffer_memory: u64,
}

#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip", 
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        }
    }
}

#[derive(Debug, Clone)]
pub enum AckMode {
    /// Don't wait for acknowledgment
    None,
    /// Wait for leader acknowledgment only
    Leader,
    /// Wait for all in-sync replicas
    All,
}

impl AckMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            AckMode::None => "0",
            AckMode::Leader => "1", 
            AckMode::All => "all",
        }
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            common: CommonKafkaConfig::default(),
            default_topic: "default-topic".to_string(),
            message_timeout: Duration::from_secs(30),
            delivery_timeout: Duration::from_secs(120),
            enable_idempotence: true,
            max_in_flight_requests: 5,
            retries: 10,
            batch_size: 16384, // 16KB
            linger_ms: Duration::from_millis(5),
            compression_type: CompressionType::Lz4,
            acks: AckMode::All,
            buffer_memory: 33554432, // 32MB
        }
    }
}

impl ProducerConfig {
    /// Create a new config with broker and topic
    pub fn new(brokers: impl Into<String>, default_topic: impl Into<String>) -> Self {
        Self {
            common: CommonKafkaConfig::new(brokers),
            default_topic: default_topic.into(),
            ..Default::default()
        }
    }

    /// Set client ID
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.common = self.common.client_id(client_id);
        self
    }

    /// Set message timeout
    pub fn message_timeout(mut self, timeout: Duration) -> Self {
        self.message_timeout = timeout;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.request_timeout(timeout);
        self
    }

    /// Set delivery timeout
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    /// Enable/disable idempotent producer
    pub fn idempotence(mut self, enable: bool) -> Self {
        self.enable_idempotence = enable;
        self
    }

    /// Set max in-flight requests
    pub fn max_in_flight_requests(mut self, max: u32) -> Self {
        self.max_in_flight_requests = max;
        self
    }

    /// Set retry configuration
    pub fn retries(mut self, retries: u32, backoff: Duration) -> Self {
        self.retries = retries;
        self.common = self.common.retry_backoff(backoff);
        self
    }

    /// Set batching configuration
    pub fn batching(mut self, batch_size: u32, linger_ms: Duration) -> Self {
        self.batch_size = batch_size;
        self.linger_ms = linger_ms;
        self
    }

    /// Set compression type
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression_type = compression;
        self
    }

    /// Set acknowledgment mode
    pub fn acks(mut self, acks: AckMode) -> Self {
        self.acks = acks;
        self
    }

    /// Set buffer memory
    pub fn buffer_memory(mut self, memory: u64) -> Self {
        self.buffer_memory = memory;
        self
    }

    /// Set message max bytes (consolidates custom property)
    pub fn message_max_bytes(mut self, max_bytes: u64) -> Self {
        self.common = self.common.custom_property("message.max.bytes", max_bytes.to_string());
        self
    }

    /// Set socket buffer sizes (consolidates custom properties)
    pub fn socket_buffers(mut self, send_buffer: u32, receive_buffer: u32) -> Self {
        self.common = self.common
            .custom_property("socket.send.buffer.bytes", send_buffer.to_string())
            .custom_property("socket.receive.buffer.bytes", receive_buffer.to_string());
        self
    }

    /// Set max in-flight requests per connection (consolidates custom property)
    pub fn max_in_flight_requests_per_connection(mut self, max_requests: u32) -> Self {
        self.max_in_flight_requests = max_requests;
        self.common = self.common.custom_property("max.in.flight.requests.per.connection", max_requests.to_string());
        self
    }

    /// Configure performance-related memory and network settings
    pub fn performance_tuning(mut self, message_max_mb: u32, socket_buffer_kb: u32) -> Self {
        self.message_max_bytes((message_max_mb * 1024 * 1024) as u64)
            .socket_buffers(socket_buffer_kb * 1024, socket_buffer_kb * 1024)
    }

    /// Add custom configuration property
    pub fn custom_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.common = self.common.custom_property(key, value);
        self
    }

    /// Convenience method to access brokers from common config
    pub fn brokers(&self) -> &str {
        &self.common.brokers
    }

    /// Convenience method to access client_id from common config
    pub fn client_id_ref(&self) -> Option<&str> {
        self.common.client_id.as_deref()
    }

    /// Convenience method to access request_timeout from common config
    pub fn request_timeout_duration(&self) -> Duration {
        self.common.request_timeout
    }

    /// Convenience method to access retry_backoff from common config
    pub fn retry_backoff_duration(&self) -> Duration {
        self.common.retry_backoff
    }

    /// Convenience method to access custom_config from common config
    pub fn custom_config_ref(&self) -> &HashMap<String, String> {
        &self.common.custom_config
    }
}

// Implement HasCommonConfig trait
impl HasCommonConfig for ProducerConfig {
    fn common_config(&self) -> &CommonKafkaConfig {
        &self.common
    }

    fn common_config_mut(&mut self) -> &mut CommonKafkaConfig {
        &mut self.common
    }
}

// Implement PerformancePresets trait
impl PerformancePresets for ProducerConfig {
    /// Performance preset for high throughput
    fn high_throughput(mut self) -> Self {
        presets::apply_high_throughput_common(&mut self.common);
        self.batch_size = 65536; // 64KB
        self.linger_ms = Duration::from_millis(10);
        self.compression_type = CompressionType::Lz4;
        self.enable_idempotence = false; // Disable idempotence for speed
        self.acks = AckMode::Leader; // Trade consistency for speed
        self.buffer_memory = 67108864; // 64MB
        
        // Apply consolidated performance tuning automatically
        self.message_max_bytes(67108864) // 64MB
            .max_in_flight_requests_per_connection(5)
            .socket_buffers(131072, 131072) // 128KB buffers
    }

    /// Performance preset for low latency
    fn low_latency(mut self) -> Self {
        presets::apply_low_latency_common(&mut self.common);
        self.batch_size = 1024; // 1KB
        self.linger_ms = Duration::from_millis(0);
        self.compression_type = CompressionType::None;
        self.max_in_flight_requests = 1;
        self
    }

    /// Reliability preset for maximum durability
    fn max_durability(mut self) -> Self {
        presets::apply_max_durability_common(&mut self.common);
        self.enable_idempotence = true;
        self.acks = AckMode::All;
        self.retries = 2147483647; // Max allowed value by Kafka (i32::MAX)
        self.max_in_flight_requests = 1;
        self.delivery_timeout = Duration::from_secs(300); // 5 minutes
        self
    }

    /// Development preset with reasonable defaults
    fn development(mut self) -> Self {
        presets::apply_development_common(&mut self.common);
        self.acks = AckMode::Leader;
        self.enable_idempotence = false; // Disable idempotence to allow Leader acks
        self.retries = 3;
        self.message_timeout = Duration::from_secs(10);
        self.compression_type = CompressionType::None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ProducerConfig::default();
        assert_eq!(config.brokers(), "localhost:9092");
        assert_eq!(config.enable_idempotence, true);
        assert_eq!(config.batch_size, 16384);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ProducerConfig::new("broker1:9092,broker2:9092", "my-topic")
            .client_id("test-producer")
            .message_timeout(Duration::from_secs(60))
            .compression(CompressionType::Gzip)
            .high_throughput();

        assert_eq!(config.brokers(), "broker1:9092,broker2:9092");
        assert_eq!(config.default_topic, "my-topic");
        assert_eq!(config.client_id_ref(), Some("test-producer"));
        assert_eq!(config.batch_size, 65536); // high_throughput preset
    }

    #[test]
    fn test_presets() {
        let high_throughput = ProducerConfig::default().high_throughput();
        assert_eq!(high_throughput.batch_size, 65536);
        assert_eq!(high_throughput.acks.as_str(), "1");

        let low_latency = ProducerConfig::default().low_latency();
        assert_eq!(low_latency.batch_size, 1024);
        assert_eq!(low_latency.linger_ms, Duration::from_millis(0));

        let max_durability = ProducerConfig::default().max_durability();
        assert_eq!(max_durability.acks.as_str(), "all");
        assert_eq!(max_durability.enable_idempotence, true);
    }
}