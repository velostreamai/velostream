use super::{BatchConfig, BatchStrategy};
/// Unified configuration management system for VeloStream
///
/// This module provides a consistent, simplified approach to applying batch configuration
/// optimizations while respecting explicit user settings across all datasource types.
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Central configuration constants to eliminate magic numbers
pub mod defaults {
    // File configuration defaults
    pub const FILE_BUFFER_SIZE_BYTES: u64 = 65536; // 64KB

    // Kafka producer defaults
    pub const KAFKA_BATCH_SIZE: &str = "16384"; // 16KB
    pub const KAFKA_LINGER_MS: &str = "0"; // No linger by default
    pub const KAFKA_COMPRESSION_TYPE: &str = "none";
    pub const KAFKA_ACKS: &str = "all";
    pub const KAFKA_RETRIES: &str = "2147483647"; // Integer.MAX_VALUE (default)
    pub const KAFKA_REQUEST_TIMEOUT_MS: &str = "30000"; // 30 seconds

    // Kafka consumer defaults
    pub const KAFKA_MAX_POLL_RECORDS: u32 = 500;
    pub const KAFKA_FETCH_MIN_BYTES: u32 = 1;
    pub const KAFKA_FETCH_MAX_WAIT_MS: u32 = 500;
}

/// Trait for applying property suggestions without overriding explicit user settings
pub trait PropertySuggestor {
    /// Suggest a property value only if not explicitly set by user
    fn suggest<T: ToString>(&mut self, key: &str, value: T);

    /// Check if a property was explicitly set (for logging/debugging)
    fn is_user_set(&self, key: &str) -> bool;
}

/// Implementation for HashMap-based configurations (Kafka)
impl PropertySuggestor for HashMap<String, String> {
    fn suggest<T: ToString>(&mut self, key: &str, value: T) {
        if !self.contains_key(key) {
            self.insert(key.to_string(), value.to_string());
        }
    }

    fn is_user_set(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

/// Wrapper for struct-based configurations to enable suggestion pattern
pub struct StructConfigSuggestor<T> {
    pub config: T,
    user_set_fields: HashSet<String>,
}

impl<T> StructConfigSuggestor<T> {
    /// Create a new config suggestor, tracking which fields were explicitly set
    pub fn new(config: T, user_properties: &HashMap<String, String>) -> Self {
        let user_set_fields = user_properties.keys().cloned().collect();
        Self {
            config,
            user_set_fields,
        }
    }

    /// Check if a field was explicitly set by user
    pub fn is_user_set(&self, field: &str) -> bool {
        self.user_set_fields.contains(field)
    }

    /// Extract the final configuration
    pub fn into_config(self) -> T {
        self.config
    }
}

/// File sink configuration wrapper with suggestion capabilities
impl StructConfigSuggestor<super::super::file::config::FileSinkConfig> {
    /// Suggest buffer size only if not explicitly set by user
    pub fn suggest_buffer_size(&mut self, size: u64) {
        if !self.is_user_set("buffer_size_bytes") {
            self.config.buffer_size_bytes = size;
        }
    }

    /// Suggest compression only if not explicitly set by user  
    pub fn suggest_compression(
        &mut self,
        compression: Option<super::super::file::config::CompressionType>,
    ) {
        if !self.is_user_set("compression") && self.config.compression.is_none() {
            self.config.compression = compression;
        }
    }
}

/// Centralized batch strategy application logic
pub struct BatchConfigApplicator;

impl BatchConfigApplicator {
    /// Apply batch strategy optimizations to Kafka producer configuration
    pub fn apply_kafka_producer_strategy(
        config: &mut HashMap<String, String>,
        strategy: &BatchStrategy,
        max_batch_size: usize,
        batch_timeout: Duration,
    ) {
        match strategy {
            BatchStrategy::FixedSize(size) => {
                Self::apply_fixed_size_kafka(config, *size, max_batch_size);
            }
            BatchStrategy::TimeWindow(duration) => {
                Self::apply_time_window_kafka(config, *duration);
            }
            BatchStrategy::AdaptiveSize { target_latency, .. } => {
                Self::apply_adaptive_size_kafka(config, *target_latency);
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                Self::apply_memory_based_kafka(config, *max_bytes);
            }
            BatchStrategy::LowLatency { max_wait_time, .. } => {
                Self::apply_low_latency_kafka(config, *max_wait_time);
            }
        }

        // Apply general timeout setting
        let timeout_ms = batch_timeout.as_millis().min(300000) as u64; // Max 5 minutes
        config.suggest("request.timeout.ms", timeout_ms.to_string());
    }

    /// Apply batch strategy optimizations to file sink configuration
    pub fn apply_file_sink_strategy(
        config: &mut StructConfigSuggestor<super::super::file::config::FileSinkConfig>,
        strategy: &BatchStrategy,
        max_batch_size: usize,
    ) {
        match strategy {
            BatchStrategy::FixedSize(size) => {
                let buffer_size = (*size * 4096).min(max_batch_size * 4096); // 4KB per record estimate
                config.suggest_buffer_size(buffer_size as u64);
            }
            BatchStrategy::TimeWindow(_) => {
                config.suggest_buffer_size(1024 * 1024); // 1MB buffer for time-based batching
            }
            BatchStrategy::AdaptiveSize { .. } => {
                config.suggest_buffer_size(512 * 1024); // 512KB adaptive buffer
            }
            BatchStrategy::MemoryBased(max_bytes) => {
                let buffer_size = (*max_bytes).min(16 * 1024 * 1024); // Max 16MB buffer
                config.suggest_buffer_size(buffer_size as u64);

                // Suggest compression for large batches
                if buffer_size > 1024 * 1024 {
                    use super::super::file::config::CompressionType;
                    config.suggest_compression(Some(CompressionType::Gzip));
                }
            }
            BatchStrategy::LowLatency {
                eager_processing, ..
            } => {
                if *eager_processing {
                    config.suggest_buffer_size(0); // Immediate write mode
                } else {
                    config.suggest_buffer_size(4096); // 4KB minimal buffer
                }
            }
        }
    }

    // Private helper methods for Kafka strategy application
    fn apply_fixed_size_kafka(
        config: &mut HashMap<String, String>,
        size: usize,
        max_batch_size: usize,
    ) {
        let batch_size = (size * 1024).min(max_batch_size * 1024); // KB estimate
        config.suggest("batch.size", batch_size.to_string());
        config.suggest("linger.ms", "10");
        config.suggest("compression.type", "snappy");
    }

    fn apply_time_window_kafka(config: &mut HashMap<String, String>, duration: Duration) {
        let linger_ms = duration.as_millis().min(30000) as u64; // Cap at 30s
        config.suggest("linger.ms", linger_ms.to_string());
        config.suggest("batch.size", "65536"); // 64KB batches
        config.suggest("compression.type", "lz4");
    }

    fn apply_adaptive_size_kafka(config: &mut HashMap<String, String>, target_latency: Duration) {
        let linger_ms = target_latency.as_millis().min(5000) as u64; // Cap at 5s
        config.suggest("linger.ms", linger_ms.to_string());
        config.suggest("batch.size", "32768"); // 32KB adaptive batches
        config.suggest("compression.type", "snappy");
    }

    fn apply_memory_based_kafka(config: &mut HashMap<String, String>, max_bytes: usize) {
        let batch_size = (max_bytes / 2).min(1024 * 1024); // Half of memory target, max 1MB
        config.suggest("batch.size", batch_size.to_string());
        config.suggest("linger.ms", "100"); // Longer linger for large batches
        config.suggest("compression.type", "gzip"); // Better compression for large batches

        // Use memory target for buffer settings
        let buffer_memory_kb = ((max_bytes * 4).min(64 * 1024 * 1024)) / 1024; // Convert to KB
        config.suggest("queue.buffering.max.kbytes", buffer_memory_kb.to_string());
    }

    fn apply_low_latency_kafka(config: &mut HashMap<String, String>, max_wait_time: Duration) {
        config.suggest("batch.size", "1024"); // Very small batches (1KB)
        let linger_ms = max_wait_time.as_millis().min(10) as u64; // Ultra-short linger time
        config.suggest("linger.ms", linger_ms.to_string());
        config.suggest("compression.type", "none"); // No compression overhead
        config.suggest("acks", "1"); // Fast acknowledgment (leader only)
        config.suggest("retries", "0"); // No retries for speed
        config.suggest("max.in.flight.requests.per.connection", "5"); // Parallel requests
    }
}

/// Utility for logging applied configuration (replaces duplicate logging code)
pub struct ConfigLogger;

impl ConfigLogger {
    /// Log Kafka producer configuration with batch strategy details
    pub fn log_kafka_producer_config(
        config: &HashMap<String, String>,
        batch_config: &BatchConfig,
        topic: &str,
        brokers: &str,
    ) {
        use log::info;

        info!("=== Unified Kafka Producer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);

        info!("Applied Producer Settings:");
        info!(
            "  - batch.size: {} {}",
            config
                .get("batch.size")
                .unwrap_or(&defaults::KAFKA_BATCH_SIZE.to_string()),
            if config.is_user_set("batch.size") {
                "(user)"
            } else {
                "(suggested)"
            }
        );
        info!(
            "  - linger.ms: {} {}",
            config
                .get("linger.ms")
                .unwrap_or(&defaults::KAFKA_LINGER_MS.to_string()),
            if config.is_user_set("linger.ms") {
                "(user)"
            } else {
                "(suggested)"
            }
        );
        info!(
            "  - compression.type: {} {}",
            config
                .get("compression.type")
                .unwrap_or(&defaults::KAFKA_COMPRESSION_TYPE.to_string()),
            if config.is_user_set("compression.type") {
                "(user)"
            } else {
                "(suggested)"
            }
        );
        info!(
            "  - acks: {} {}",
            config
                .get("acks")
                .unwrap_or(&defaults::KAFKA_ACKS.to_string()),
            if config.is_user_set("acks") {
                "(user)"
            } else {
                "(suggested)"
            }
        );
        info!(
            "  - retries: {} {}",
            config
                .get("retries")
                .unwrap_or(&defaults::KAFKA_RETRIES.to_string()),
            if config.is_user_set("retries") {
                "(user)"
            } else {
                "(suggested)"
            }
        );

        if let Some(buffer_memory) = config.get("queue.buffering.max.kbytes") {
            info!(
                "  - queue.buffering.max.kbytes: {}KB {}",
                buffer_memory,
                if config.is_user_set("queue.buffering.max.kbytes") {
                    "(user)"
                } else {
                    "(suggested)"
                }
            );
        }
        info!("  - topic: {}", topic);
        info!("  - brokers: {}", brokers);
        info!("=============================================");
    }

    /// Log file sink configuration with batch strategy details
    pub fn log_file_sink_config(
        config: &super::super::file::config::FileSinkConfig,
        config_wrapper: &StructConfigSuggestor<super::super::file::config::FileSinkConfig>,
        batch_config: &BatchConfig,
    ) {
        use log::info;

        info!("=== Unified File Writer Configuration ===");
        info!("Batch Strategy: {:?}", batch_config.strategy);
        info!("Batch Configuration:");
        info!("  - Enable Batching: {}", batch_config.enable_batching);
        info!("  - Max Batch Size: {}", batch_config.max_batch_size);
        info!("  - Batch Timeout: {:?}", batch_config.batch_timeout);

        info!("Applied File Writer Settings:");
        info!(
            "  - buffer_size_bytes: {} {}",
            config.buffer_size_bytes,
            if config_wrapper.is_user_set("buffer_size_bytes") {
                "(user)"
            } else {
                "(suggested)"
            }
        );
        info!(
            "  - compression: {:?} {}",
            config.compression,
            if config_wrapper.is_user_set("compression") {
                "(user)"
            } else {
                "(suggested)"
            }
        );
        info!("  - format: {:?}", config.format);
        info!("  - path: {}", config.path);
        info!("  - append_if_exists: {}", config.append_if_exists);
        info!("========================================");
    }
}

/// Simplified factory methods for common configuration patterns
pub struct ConfigFactory;

impl ConfigFactory {
    /// Create optimized Kafka producer configuration with batch strategy
    pub fn create_kafka_producer_config(
        brokers: &str,
        user_properties: &HashMap<String, String>,
        batch_config: &BatchConfig,
    ) -> HashMap<String, String> {
        let mut config = HashMap::new();
        config.insert("bootstrap.servers".to_string(), brokers.to_string());

        // Apply base defaults
        config.suggest("message.timeout.ms", "5000");
        config.suggest("queue.buffering.max.messages", "100000");
        config.suggest("queue.buffering.max.ms", "100");
        config.suggest("batch.num.messages", "1000");

        // Apply user properties (these will be preserved)
        for (key, value) in user_properties {
            config.insert(key.clone(), value.clone());
        }

        // Apply batch optimization suggestions (won't override user settings)
        if batch_config.enable_batching {
            BatchConfigApplicator::apply_kafka_producer_strategy(
                &mut config,
                &batch_config.strategy,
                batch_config.max_batch_size,
                batch_config.batch_timeout,
            );
        } else {
            // Suggest immediate processing
            config.suggest("batch.size", "0");
            config.suggest("linger.ms", "0");
        }

        config
    }

    /// Create optimized file sink configuration with batch strategy
    pub fn create_file_sink_config(
        base_config: super::super::file::config::FileSinkConfig,
        user_properties: &HashMap<String, String>,
        batch_config: &BatchConfig,
    ) -> super::super::file::config::FileSinkConfig {
        let mut config_wrapper = StructConfigSuggestor::new(base_config, user_properties);

        // Apply batch optimization suggestions (won't override user settings)
        if batch_config.enable_batching {
            BatchConfigApplicator::apply_file_sink_strategy(
                &mut config_wrapper,
                &batch_config.strategy,
                batch_config.max_batch_size,
            );
        } else {
            // Suggest immediate processing
            config_wrapper.suggest_buffer_size(0);
        }

        config_wrapper.into_config()
    }
}
