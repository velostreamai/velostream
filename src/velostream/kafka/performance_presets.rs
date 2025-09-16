use crate::velostream::kafka::common_config::CommonKafkaConfig;
use std::time::Duration;

/// Shared performance preset utilities for both producer and consumer configurations
///
/// This module provides common preset application functions that can be used
/// by both ProducerConfig and ConsumerConfig, eliminating code duplication
/// and ensuring consistency across configurations.
pub mod presets {
    use super::*;

    /// Apply high throughput optimizations to common configuration
    ///
    /// This preset optimizes for maximum message throughput by:
    /// - Increasing timeout values to allow for larger batches
    /// - Setting more aggressive retry policies
    /// - Optimizing for bulk operations
    pub fn apply_high_throughput_common(config: &mut CommonKafkaConfig) {
        config.request_timeout = Duration::from_secs(60);
        config.retry_backoff = Duration::from_millis(200);

        // Add common high-throughput properties (only valid rdkafka properties)
        config
            .custom_config
            .insert("socket.send.buffer.bytes".to_string(), "131072".to_string()); // 128KB
        config.custom_config.insert(
            "socket.receive.buffer.bytes".to_string(),
            "131072".to_string(),
        ); // 128KB
    }

    /// Apply low latency optimizations to common configuration
    ///
    /// This preset optimizes for minimal latency by:
    /// - Reducing timeout values for faster failure detection
    /// - Setting aggressive retry policies with shorter backoffs
    /// - Optimizing for immediate processing
    pub fn apply_low_latency_common(config: &mut CommonKafkaConfig) {
        config.request_timeout = Duration::from_secs(10);
        config.retry_backoff = Duration::from_millis(50);

        // Add common low-latency properties (only valid rdkafka properties)
        config
            .custom_config
            .insert("socket.send.buffer.bytes".to_string(), "32768".to_string()); // 32KB
        config.custom_config.insert(
            "socket.receive.buffer.bytes".to_string(),
            "32768".to_string(),
        ); // 32KB
    }

    /// Apply maximum durability optimizations to common configuration
    ///
    /// This preset optimizes for data safety and reliability by:
    /// - Increasing timeout values to handle network issues
    /// - Setting conservative retry policies
    /// - Adding reliability-focused properties
    pub fn apply_max_durability_common(config: &mut CommonKafkaConfig) {
        config.request_timeout = Duration::from_secs(120);
        config.retry_backoff = Duration::from_millis(1000);

        // Add common durability properties (only valid rdkafka properties)
        config
            .custom_config
            .insert("socket.send.buffer.bytes".to_string(), "262144".to_string()); // 256KB
        config.custom_config.insert(
            "socket.receive.buffer.bytes".to_string(),
            "262144".to_string(),
        ); // 256KB
    }

    /// Apply development-friendly optimizations to common configuration
    ///
    /// This preset optimizes for development and debugging by:
    /// - Setting reasonable timeout values for local development
    /// - Adding debug-friendly properties
    /// - Optimizing for ease of use over performance
    pub fn apply_development_common(config: &mut CommonKafkaConfig) {
        config.request_timeout = Duration::from_secs(15);
        config.retry_backoff = Duration::from_millis(100);

        // Add common development properties (only valid rdkafka properties)
        config
            .custom_config
            .insert("api.version.request".to_string(), "true".to_string());
        config
            .custom_config
            .insert("log.connection.close".to_string(), "false".to_string());
    }

    /// Apply streaming optimizations to common configuration
    ///
    /// This preset optimizes for continuous stream processing by:
    /// - Balancing latency and throughput
    /// - Setting appropriate buffer sizes for streaming
    /// - Optimizing for steady-state operation
    pub fn apply_streaming_common(config: &mut CommonKafkaConfig) {
        config.request_timeout = Duration::from_secs(30);
        config.retry_backoff = Duration::from_millis(150);

        // Add common streaming properties (only valid rdkafka properties)
        config
            .custom_config
            .insert("socket.send.buffer.bytes".to_string(), "65536".to_string()); // 64KB
        config.custom_config.insert(
            "socket.receive.buffer.bytes".to_string(),
            "65536".to_string(),
        ); // 64KB
    }

    /// Apply batch processing optimizations to common configuration
    ///
    /// This preset optimizes for batch processing workloads by:
    /// - Setting longer timeouts to handle large batches
    /// - Increasing buffer sizes for bulk operations
    /// - Optimizing for periodic processing patterns
    pub fn apply_batch_processing_common(config: &mut CommonKafkaConfig) {
        config.request_timeout = Duration::from_secs(180); // 3 minutes
        config.retry_backoff = Duration::from_millis(500);

        // Add common batch processing properties (only valid rdkafka properties)
        config
            .custom_config
            .insert("socket.send.buffer.bytes".to_string(), "524288".to_string()); // 512KB
        config.custom_config.insert(
            "socket.receive.buffer.bytes".to_string(),
            "524288".to_string(),
        ); // 512KB
    }
}

/// Trait for configuration types that support performance presets
pub trait PerformancePresets {
    /// Apply high throughput preset
    fn high_throughput(self) -> Self;

    /// Apply low latency preset
    fn low_latency(self) -> Self;

    /// Apply maximum durability preset
    fn max_durability(self) -> Self;

    /// Apply development preset
    fn development(self) -> Self;

    /// Apply streaming preset (if applicable)
    fn streaming(self) -> Self
    where
        Self: Sized,
    {
        self // Default implementation does nothing
    }

    /// Apply batch processing preset (if applicable)
    fn batch_processing(self) -> Self
    where
        Self: Sized,
    {
        self // Default implementation does nothing
    }
}

/// Utility functions for preset validation and information
pub mod utils {
    /// Get description of a preset's purpose and optimizations
    pub fn get_preset_description(preset_name: &str) -> Option<&'static str> {
        match preset_name {
            "high_throughput" => {
                Some("Optimized for maximum message throughput with larger batches and buffers")
            }
            "low_latency" => {
                Some("Optimized for minimal latency with immediate processing and small buffers")
            }
            "max_durability" => Some(
                "Optimized for data safety with conservative timeouts and reliability features",
            ),
            "development" => {
                Some("Development-friendly settings with reasonable timeouts and debug features")
            }
            "streaming" => Some("Balanced settings for continuous stream processing workloads"),
            "batch_processing" => Some(
                "Optimized for periodic batch processing with large buffers and extended timeouts",
            ),
            _ => None,
        }
    }

    /// Get recommended use cases for a preset
    pub fn get_preset_use_cases(preset_name: &str) -> Option<Vec<&'static str>> {
        match preset_name {
            "high_throughput" => Some(vec![
                "Log aggregation systems",
                "Event streaming with high volume",
                "ETL pipelines with large datasets",
                "Metrics collection systems",
            ]),
            "low_latency" => Some(vec![
                "Real-time analytics",
                "Trading systems",
                "Live dashboards",
                "Interactive applications",
            ]),
            "max_durability" => Some(vec![
                "Financial transactions",
                "Audit logging",
                "Critical business events",
                "Compliance systems",
            ]),
            "development" => Some(vec![
                "Local development",
                "Testing environments",
                "Debugging applications",
                "Prototyping",
            ]),
            "streaming" => Some(vec![
                "Stream processing applications",
                "Real-time ETL",
                "Event-driven architectures",
                "Microservice communication",
            ]),
            "batch_processing" => Some(vec![
                "Scheduled data processing",
                "Bulk data migration",
                "Periodic reporting",
                "Large dataset analysis",
            ]),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_high_throughput_preset() {
        let mut config = CommonKafkaConfig::default();
        presets::apply_high_throughput_common(&mut config);

        assert_eq!(config.request_timeout, Duration::from_secs(60));
        assert_eq!(config.retry_backoff, Duration::from_millis(200));
        assert!(config
            .custom_config
            .contains_key("socket.send.buffer.bytes"));
        assert_eq!(
            config.custom_config.get("socket.send.buffer.bytes"),
            Some(&"131072".to_string())
        );
    }

    #[test]
    fn test_low_latency_preset() {
        let mut config = CommonKafkaConfig::default();
        presets::apply_low_latency_common(&mut config);

        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.retry_backoff, Duration::from_millis(50));
        assert!(config
            .custom_config
            .contains_key("socket.send.buffer.bytes"));
        assert_eq!(
            config.custom_config.get("socket.send.buffer.bytes"),
            Some(&"32768".to_string())
        );
    }

    #[test]
    fn test_max_durability_preset() {
        let mut config = CommonKafkaConfig::default();
        presets::apply_max_durability_common(&mut config);

        assert_eq!(config.request_timeout, Duration::from_secs(120));
        assert_eq!(config.retry_backoff, Duration::from_millis(1000));
        assert!(config
            .custom_config
            .contains_key("socket.send.buffer.bytes"));
        assert_eq!(
            config.custom_config.get("socket.send.buffer.bytes"),
            Some(&"262144".to_string())
        );
    }

    #[test]
    fn test_development_preset() {
        let mut config = CommonKafkaConfig::default();
        presets::apply_development_common(&mut config);

        assert_eq!(config.request_timeout, Duration::from_secs(15));
        assert_eq!(config.retry_backoff, Duration::from_millis(100));
        assert!(config.custom_config.contains_key("api.version.request"));
    }

    #[test]
    fn test_preset_descriptions() {
        assert!(utils::get_preset_description("high_throughput").is_some());
        assert!(utils::get_preset_description("low_latency").is_some());
        assert!(utils::get_preset_description("max_durability").is_some());
        assert!(utils::get_preset_description("development").is_some());
        assert!(utils::get_preset_description("streaming").is_some());
        assert!(utils::get_preset_description("batch_processing").is_some());
        assert!(utils::get_preset_description("unknown_preset").is_none());
    }

    #[test]
    fn test_preset_use_cases() {
        let use_cases = utils::get_preset_use_cases("high_throughput").unwrap();
        assert!(use_cases.contains(&"Log aggregation systems"));

        let use_cases = utils::get_preset_use_cases("low_latency").unwrap();
        assert!(use_cases.contains(&"Real-time analytics"));

        assert!(utils::get_preset_use_cases("unknown_preset").is_none());
    }
}
