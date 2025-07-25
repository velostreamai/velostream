use crate::ferris::kafka::common_config::{CommonKafkaConfig, HasCommonConfig};
use crate::ferris::kafka::performance_presets::{PerformancePresets, presets};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration for Kafka consumer with sensible defaults
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Common configuration shared with producer
    pub common: CommonKafkaConfig,
    /// Consumer group ID
    pub group_id: String,
    /// Auto offset reset behavior
    pub auto_offset_reset: OffsetReset,
    /// Enable auto commit
    pub enable_auto_commit: bool,
    /// Auto commit interval
    pub auto_commit_interval: Duration,
    /// Session timeout
    pub session_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Max poll interval
    pub max_poll_interval: Duration,
    /// Max poll records
    pub max_poll_records: u32,
    /// Fetch min bytes
    pub fetch_min_bytes: u32,
    /// Fetch max bytes
    pub fetch_max_bytes: u32,
    /// Fetch max wait time
    pub fetch_max_wait: Duration,
    /// Max partition fetch bytes
    pub max_partition_fetch_bytes: u32,
}

#[derive(Debug, Clone)]
pub enum OffsetReset {
    /// Reset to earliest available offset
    Earliest,
    /// Reset to latest offset
    Latest,
    /// Throw error if no initial offset
    None,
}

impl OffsetReset {
    pub fn as_str(&self) -> &'static str {
        match self {
            OffsetReset::Earliest => "earliest",
            OffsetReset::Latest => "latest",
            OffsetReset::None => "none",
        }
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            common: CommonKafkaConfig::default(),
            group_id: "default-group".to_string(),
            auto_offset_reset: OffsetReset::Earliest,
            enable_auto_commit: false, // Manual commit for better control
            auto_commit_interval: Duration::from_secs(5),
            session_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            max_poll_interval: Duration::from_secs(300), // 5 minutes
            max_poll_records: 500,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52428800, // 50MB
            fetch_max_wait: Duration::from_millis(500),
            max_partition_fetch_bytes: 1048576, // 1MB
        }
    }
}

impl ConsumerConfig {
    /// Create a new config with brokers and group ID
    pub fn new(brokers: impl Into<String>, group_id: impl Into<String>) -> Self {
        Self {
            common: CommonKafkaConfig::new(brokers),
            group_id: group_id.into(),
            ..Default::default()
        }
    }

    /// Set client ID
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.common = self.common.client_id(client_id);
        self
    }

    /// Set auto offset reset behavior
    pub fn auto_offset_reset(mut self, reset: OffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Configure auto commit
    pub fn auto_commit(mut self, enable: bool, interval: Duration) -> Self {
        self.enable_auto_commit = enable;
        self.auto_commit_interval = interval;
        self
    }

    /// Set session and heartbeat timeouts
    pub fn session_config(mut self, session_timeout: Duration, heartbeat_interval: Duration) -> Self {
        self.session_timeout = session_timeout;
        self.heartbeat_interval = heartbeat_interval;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.request_timeout(timeout);
        self
    }

    /// Set max poll configuration
    pub fn poll_config(mut self, max_interval: Duration, max_records: u32) -> Self {
        self.max_poll_interval = max_interval;
        self.max_poll_records = max_records;
        self
    }

    /// Set fetch configuration
    pub fn fetch_config(
        mut self, 
        min_bytes: u32, 
        max_bytes: u32, 
        max_wait: Duration,
        max_partition_bytes: u32
    ) -> Self {
        self.fetch_min_bytes = min_bytes;
        self.fetch_max_bytes = max_bytes;
        self.fetch_max_wait = max_wait;
        self.max_partition_fetch_bytes = max_partition_bytes;
        self
    }

    /// Set retry backoff
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.common = self.common.retry_backoff(backoff);
        self
    }

    /// Set max poll records directly
    pub fn max_poll_records(mut self, max_records: u32) -> Self {
        self.max_poll_records = max_records;
        self
    }

    /// Set fetch min bytes directly
    pub fn fetch_min_bytes(mut self, min_bytes: u32) -> Self {
        self.fetch_min_bytes = min_bytes;
        self
    }

    /// Set fetch max bytes directly
    pub fn fetch_max_bytes(mut self, max_bytes: u32) -> Self {
        self.fetch_max_bytes = max_bytes;
        self
    }

    /// Set fetch max wait time directly
    pub fn fetch_max_wait(mut self, max_wait: Duration) -> Self {
        self.fetch_max_wait = max_wait;
        self
    }

    /// Set session timeout duration
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    /// Set max partition fetch bytes directly
    pub fn max_partition_fetch_bytes(mut self, max_bytes: u32) -> Self {
        self.max_partition_fetch_bytes = max_bytes;
        self
    }

    /// Set max poll interval directly
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.max_poll_interval = interval;
        self
    }

    /// Set fetch max bytes with custom property consolidation
    pub fn fetch_max_bytes_extended(mut self, max_bytes: u32) -> Self {
        self.fetch_max_bytes = max_bytes;
        self.common = self.common.custom_property("fetch.max.bytes", max_bytes.to_string());
        self
    }

    /// Set max partition fetch bytes with custom property consolidation
    pub fn max_partition_fetch_bytes_extended(mut self, max_bytes: u32) -> Self {
        self.max_partition_fetch_bytes = max_bytes;
        self.common = self.common.custom_property("max.partition.fetch.bytes", max_bytes.to_string());
        self
    }

    /// Set socket buffer sizes (consolidates custom properties)
    pub fn socket_buffers(mut self, send_buffer: u32, receive_buffer: u32) -> Self {
        self.common = self.common
            .custom_property("socket.send.buffer.bytes", send_buffer.to_string())
            .custom_property("socket.receive.buffer.bytes", receive_buffer.to_string());
        self
    }

    /// Configure performance-related fetch and buffer settings
    pub fn performance_tuning(mut self, fetch_max_mb: u32, partition_fetch_mb: u32, socket_buffer_kb: u32) -> Self {
        self.fetch_max_bytes_extended(fetch_max_mb * 1024 * 1024)
            .max_partition_fetch_bytes_extended(partition_fetch_mb * 1024 * 1024)
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
impl HasCommonConfig for ConsumerConfig {
    fn common_config(&self) -> &CommonKafkaConfig {
        &self.common
    }

    fn common_config_mut(&mut self) -> &mut CommonKafkaConfig {
        &mut self.common
    }
}

// Implement PerformancePresets trait
impl PerformancePresets for ConsumerConfig {
    /// Performance preset for high throughput
    fn high_throughput(mut self) -> Self {
        presets::apply_high_throughput_common(&mut self.common);
        self.fetch_min_bytes = 50000; // 50KB
        self.fetch_max_bytes = 104857600; // 100MB
        self.fetch_max_wait = Duration::from_millis(100);
        self.max_partition_fetch_bytes = 2097152; // 2MB
        self.max_poll_records = 1000;
        self.enable_auto_commit = true;
        self.auto_commit_interval = Duration::from_secs(1);

        // Apply consolidated performance tuning automatically
        self.fetch_max_bytes_extended(104857600) // 100MB
            .max_partition_fetch_bytes_extended(2097152) // 2MB
            .socket_buffers(131072, 131072) // 128KB buffers
    }

    /// Performance preset for low latency
    fn low_latency(mut self) -> Self {
        presets::apply_low_latency_common(&mut self.common);
        self.fetch_min_bytes = 1;
        self.fetch_max_wait = Duration::from_millis(1);
        self.max_poll_records = 1;
        self.heartbeat_interval = Duration::from_secs(1);
        self.session_timeout = Duration::from_secs(6);
        self
    }

    /// Reliability preset for maximum durability
    fn max_durability(mut self) -> Self {
        presets::apply_max_durability_common(&mut self.common);
        self.enable_auto_commit = false; // Manual commit only
        self.session_timeout = Duration::from_secs(60);
        self.heartbeat_interval = Duration::from_secs(10);
        self.max_poll_interval = Duration::from_secs(600); // 10 minutes
        self
    }

    /// Development preset with reasonable defaults
    fn development(mut self) -> Self {
        presets::apply_development_common(&mut self.common);
        self.auto_offset_reset = OffsetReset::Latest;
        self.enable_auto_commit = true;
        self.auto_commit_interval = Duration::from_secs(1);
        self.session_timeout = Duration::from_secs(10);
        self.heartbeat_interval = Duration::from_secs(1);
        self
    }

    /// Streaming preset optimized for continuous processing
    fn streaming(mut self) -> Self {
        presets::apply_streaming_common(&mut self.common);
        self.fetch_min_bytes = 1;
        self.fetch_max_wait = Duration::from_millis(50);
        self.max_poll_records = 100;
        self.enable_auto_commit = false; // Better control for streaming
        self.max_poll_interval = Duration::from_secs(120); // 2 minutes
        self
    }

    /// Batch processing preset
    fn batch_processing(mut self) -> Self {
        presets::apply_batch_processing_common(&mut self.common);
        self.fetch_min_bytes = 100000; // 100KB
        self.fetch_max_bytes = 52428800; // 50MB
        self.max_poll_records = 2000;
        self.fetch_max_wait = Duration::from_millis(500);
        self.max_poll_interval = Duration::from_secs(1800); // 30 minutes
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConsumerConfig::default();
        assert_eq!(config.common.brokers, "localhost:9092");
        assert_eq!(config.group_id, "default-group");
        assert_eq!(config.enable_auto_commit, false);
        assert_eq!(config.auto_offset_reset.as_str(), "earliest");
    }

    #[test]
    fn test_builder_pattern() {
        let config = ConsumerConfig::new("broker1:9092,broker2:9092", "my-group")
            .client_id("test-consumer")
            .auto_offset_reset(OffsetReset::Latest)
            .auto_commit(true, Duration::from_secs(10))
            .high_throughput();

        assert_eq!(config.common.brokers, "broker1:9092,broker2:9092");
        assert_eq!(config.group_id, "my-group");
        assert_eq!(config.common.client_id, Some("test-consumer".to_string()));
        assert_eq!(config.auto_offset_reset.as_str(), "latest");
        assert_eq!(config.max_poll_records, 1000); // high_throughput preset
    }

    #[test]
    fn test_presets() {
        let high_throughput = ConsumerConfig::default().high_throughput();
        assert_eq!(high_throughput.fetch_min_bytes, 50000);
        assert_eq!(high_throughput.enable_auto_commit, true);

        let low_latency = ConsumerConfig::default().low_latency();
        assert_eq!(low_latency.fetch_min_bytes, 1);
        assert_eq!(low_latency.max_poll_records, 1);

        let max_durability = ConsumerConfig::default().max_durability();
        assert_eq!(max_durability.enable_auto_commit, false);
        assert_eq!(max_durability.session_timeout, Duration::from_secs(60));

        let streaming = ConsumerConfig::default().streaming();
        assert_eq!(streaming.max_poll_records, 100);
        assert_eq!(streaming.enable_auto_commit, false);
    }
}