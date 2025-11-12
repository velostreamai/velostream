//! StreamJobServer configuration
//!
//! Configuration structures and utilities for the StreamJobServer.
//!
//! Supports layered configuration:
//! 1. Defaults (localhost:9092 for development)
//! 2. Constructor parameters (explicit override)
//! 3. Environment variables (for deployment)
//! 4. Builder pattern (for runtime customization)
//!
//! # Configuration Patterns
//!
//! Three main patterns for configuring StreamJobServer:
//!
//! **Pattern 1: Direct Parameters** (Backward Compatible)
//! - Pass broker and group directly
//! - Uses defaults for other settings
//! - Best for: Development, simple tests
//!
//! **Pattern 2: Configuration Objects** (Recommended for Tests)
//! - Use StreamJobServerConfig struct with builder pattern
//! - More flexible and readable for complex scenarios
//! - Best for: Tests with custom brokers, monitoring, timeouts
//!
//! **Pattern 3: Environment Variables** (Production)
//! - Load configuration from environment variables
//! - Perfect for Docker, Kubernetes, cloud deployments
//! - Best for: Production, CI/CD pipelines
//!
//! # Examples
//!
//! ```rust,ignore
//! use velostream::velostream::server::config::StreamJobServerConfig;
//!
//! // Pattern 1: Direct parameters (development)
//! let server = StreamJobServer::new("localhost:9092".to_string(), "my-group", 10);
//!
//! // Pattern 2: Configuration objects (testing - RECOMMENDED)
//! let config = StreamJobServerConfig::new("test-broker:9092", "test-group")
//!     .with_max_jobs(50)
//!     .with_monitoring(true);
//! let server = StreamJobServer::with_config(config);
//!
//! // Pattern 3: Environment variables (production)
//! // export VELOSTREAM_KAFKA_BROKERS="broker1:9092,broker2:9092"
//! // export VELOSTREAM_MAX_JOBS=500
//! let config = StreamJobServerConfig::from_env("my-group");
//! let server = StreamJobServer::with_config(config);
//! ```
//!
//! # Documentation
//!
//! For complete usage guide with examples, see:
//! - `docs/configuration-usage-guide.md` - Comprehensive usage guide
//! - `tests/integration/stream_job_server_processor_config_test.rs` - Live test examples
//!   - Pattern 1 example: `test_configuration_pattern_1_direct_parameters`
//!   - Pattern 2 example: `test_configuration_pattern_2_config_objects`
//!   - Pattern 3 example: `test_configuration_pattern_3_environment_variables`
//!   - Custom broker examples: `test_custom_kafka_broker_pattern_1` and `test_custom_kafka_broker_pattern_2`

use std::env;
use std::time::Duration;

/// Configuration for StreamJobServer
///
/// This struct centralizes all configuration parameters and supports
/// multiple configuration sources through environment variables and
/// builder pattern methods.
///
/// ## Configuration Precedence
/// 1. Environment variables (highest priority)
/// 2. Constructor/builder parameters
/// 3. Default values (lowest priority)
///
/// ## Environment Variables
/// - `VELOSTREAM_KAFKA_BROKERS`: Kafka broker endpoints (default: localhost:9092)
/// - `VELOSTREAM_MAX_JOBS`: Maximum concurrent jobs (default: 100)
/// - `VELOSTREAM_ENABLE_MONITORING`: Enable performance monitoring (default: false)
/// - `VELOSTREAM_JOB_TIMEOUT_SECS`: Job timeout in seconds (default: 86400)
/// - `VELOSTREAM_TABLE_CACHE_SIZE`: Table registry cache size (default: 100)
#[derive(Debug, Clone)]
pub struct StreamJobServerConfig {
    /// Kafka broker endpoints (comma-separated)
    /// Format: "localhost:9092" or "broker1:9092,broker2:9092,broker3:9092"
    pub kafka_brokers: String,

    /// Consumer group ID base for Kafka operations
    pub base_group_id: String,

    /// Maximum number of concurrent jobs
    pub max_jobs: usize,

    /// Enable performance monitoring and metrics collection
    pub enable_monitoring: bool,

    /// Job timeout duration (after which jobs are terminated)
    pub job_timeout: Duration,

    /// Table registry cache size (number of cached table definitions)
    pub table_cache_size: usize,
}

impl StreamJobServerConfig {
    /// Create a new configuration with the given brokers and group ID
    ///
    /// Other fields use default values. Use builder methods to customize.
    ///
    /// # Arguments
    /// * `kafka_brokers` - Kafka broker endpoints (e.g., "localhost:9092")
    /// * `base_group_id` - Base consumer group ID for Kafka
    ///
    /// # Example
    /// ```rust,ignore
    /// let config = StreamJobServerConfig::new("localhost:9092", "my-group");
    /// ```
    pub fn new(kafka_brokers: impl Into<String>, base_group_id: impl Into<String>) -> Self {
        Self {
            kafka_brokers: kafka_brokers.into(),
            base_group_id: base_group_id.into(),
            max_jobs: 100,
            enable_monitoring: false,
            job_timeout: Duration::from_secs(86400), // 24 hours
            table_cache_size: 100,
        }
    }

    /// Load configuration from environment variables with fallback to defaults
    ///
    /// Only `base_group_id` is required and must be provided as parameter.
    /// All other values are loaded from environment variables or defaults.
    ///
    /// # Arguments
    /// * `base_group_id` - Base consumer group ID for Kafka
    ///
    /// # Environment Variables
    /// - `VELOSTREAM_KAFKA_BROKERS` - Kafka brokers (default: localhost:9092)
    /// - `VELOSTREAM_MAX_JOBS` - Max jobs (default: 100)
    /// - `VELOSTREAM_ENABLE_MONITORING` - Enable monitoring, true/false (default: false)
    /// - `VELOSTREAM_JOB_TIMEOUT_SECS` - Job timeout in seconds (default: 86400)
    /// - `VELOSTREAM_TABLE_CACHE_SIZE` - Table cache size (default: 100)
    ///
    /// # Example
    /// ```rust,ignore
    /// // With environment variables set:
    /// // VELOSTREAM_KAFKA_BROKERS=prod1:9092,prod2:9092
    /// // VELOSTREAM_MAX_JOBS=500
    /// let config = StreamJobServerConfig::from_env("prod-group");
    /// ```
    pub fn from_env(base_group_id: impl Into<String>) -> Self {
        let brokers =
            env::var("VELOSTREAM_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());

        let max_jobs = env::var("VELOSTREAM_MAX_JOBS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        let enable_monitoring = env::var("VELOSTREAM_ENABLE_MONITORING")
            .ok()
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        let job_timeout_secs = env::var("VELOSTREAM_JOB_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(86400);

        let table_cache_size = env::var("VELOSTREAM_TABLE_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);

        Self {
            kafka_brokers: brokers,
            base_group_id: base_group_id.into(),
            max_jobs,
            enable_monitoring,
            job_timeout: Duration::from_secs(job_timeout_secs),
            table_cache_size,
        }
    }

    /// Set maximum number of concurrent jobs
    pub fn with_max_jobs(mut self, max_jobs: usize) -> Self {
        self.max_jobs = max_jobs;
        self
    }

    /// Enable or disable performance monitoring
    pub fn with_monitoring(mut self, enable_monitoring: bool) -> Self {
        self.enable_monitoring = enable_monitoring;
        self
    }

    /// Set job timeout duration
    pub fn with_job_timeout(mut self, duration: Duration) -> Self {
        self.job_timeout = duration;
        self
    }

    /// Set table registry cache size
    pub fn with_table_cache_size(mut self, size: usize) -> Self {
        self.table_cache_size = size;
        self
    }

    /// Set Kafka broker endpoints
    pub fn with_kafka_brokers(mut self, brokers: impl Into<String>) -> Self {
        self.kafka_brokers = brokers.into();
        self
    }

    /// Apply development presets (localhost, larger cache, no monitoring)
    pub fn with_dev_preset(mut self) -> Self {
        self.kafka_brokers = "localhost:9092".to_string();
        self.enable_monitoring = false;
        self.table_cache_size = 100;
        self.max_jobs = 10;
        self
    }

    /// Apply production presets (monitoring enabled, optimized timeouts)
    pub fn with_production_preset(mut self) -> Self {
        self.enable_monitoring = true;
        self.job_timeout = Duration::from_secs(172800); // 48 hours
        self.table_cache_size = 500;
        self.max_jobs = 500;
        self
    }

    /// Get a summary of the configuration for logging
    pub fn summary(&self) -> String {
        format!(
            "StreamJobServer Configuration: brokers={}, group_id={}, max_jobs={}, monitoring={}, timeout={}s, cache_size={}",
            self.kafka_brokers,
            self.base_group_id,
            self.max_jobs,
            self.enable_monitoring,
            self.job_timeout.as_secs(),
            self.table_cache_size,
        )
    }
}

impl Default for StreamJobServerConfig {
    /// Default configuration suitable for local development
    ///
    /// - Kafka brokers: localhost:9092
    /// - Group ID: default-group
    /// - Max jobs: 100
    /// - Monitoring: disabled
    /// - Job timeout: 24 hours
    /// - Cache size: 100
    fn default() -> Self {
        Self::new("localhost:9092", "default-group")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StreamJobServerConfig::default();
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert_eq!(config.base_group_id, "default-group");
        assert_eq!(config.max_jobs, 100);
        assert!(!config.enable_monitoring);
        assert_eq!(config.job_timeout, Duration::from_secs(86400));
        assert_eq!(config.table_cache_size, 100);
    }

    #[test]
    fn test_new_with_custom_values() {
        let config = StreamJobServerConfig::new("broker1:9092", "custom-group");
        assert_eq!(config.kafka_brokers, "broker1:9092");
        assert_eq!(config.base_group_id, "custom-group");
        assert_eq!(config.max_jobs, 100); // Default
    }

    #[test]
    fn test_builder_with_max_jobs() {
        let config = StreamJobServerConfig::new("localhost:9092", "group").with_max_jobs(50);
        assert_eq!(config.max_jobs, 50);
    }

    #[test]
    fn test_builder_with_monitoring() {
        let config = StreamJobServerConfig::new("localhost:9092", "group").with_monitoring(true);
        assert!(config.enable_monitoring);
    }

    #[test]
    fn test_builder_with_timeout() {
        let timeout = Duration::from_secs(3600);
        let config =
            StreamJobServerConfig::new("localhost:9092", "group").with_job_timeout(timeout);
        assert_eq!(config.job_timeout, timeout);
    }

    #[test]
    fn test_builder_with_cache_size() {
        let config =
            StreamJobServerConfig::new("localhost:9092", "group").with_table_cache_size(250);
        assert_eq!(config.table_cache_size, 250);
    }

    #[test]
    fn test_builder_with_kafka_brokers() {
        let config = StreamJobServerConfig::new("localhost:9092", "group")
            .with_kafka_brokers("prod1:9092,prod2:9092");
        assert_eq!(config.kafka_brokers, "prod1:9092,prod2:9092");
    }

    #[test]
    fn test_dev_preset() {
        let config = StreamJobServerConfig::default().with_dev_preset();
        assert_eq!(config.kafka_brokers, "localhost:9092");
        assert!(!config.enable_monitoring);
        assert_eq!(config.max_jobs, 10);
        assert_eq!(config.table_cache_size, 100);
    }

    #[test]
    fn test_production_preset() {
        let config = StreamJobServerConfig::default().with_production_preset();
        assert!(config.enable_monitoring);
        assert_eq!(config.job_timeout, Duration::from_secs(172800));
        assert_eq!(config.max_jobs, 500);
        assert_eq!(config.table_cache_size, 500);
    }

    #[test]
    fn test_chained_builders() {
        let config = StreamJobServerConfig::new("broker1:9092", "group1")
            .with_max_jobs(75)
            .with_monitoring(true)
            .with_table_cache_size(200)
            .with_job_timeout(Duration::from_secs(7200));

        assert_eq!(config.kafka_brokers, "broker1:9092");
        assert_eq!(config.max_jobs, 75);
        assert!(config.enable_monitoring);
        assert_eq!(config.table_cache_size, 200);
        assert_eq!(config.job_timeout, Duration::from_secs(7200));
    }

    #[test]
    fn test_summary() {
        let config = StreamJobServerConfig::new("localhost:9092", "test-group");
        let summary = config.summary();
        assert!(summary.contains("localhost:9092"));
        assert!(summary.contains("test-group"));
        assert!(summary.contains("max_jobs=100"));
    }

    #[test]
    fn test_from_env_with_defaults() {
        // Clear any existing env vars before and after the test
        // to ensure clean state and avoid interference from other tests
        unsafe {
            std::env::remove_var("VELOSTREAM_KAFKA_BROKERS");
            std::env::remove_var("VELOSTREAM_MAX_JOBS");
            std::env::remove_var("VELOSTREAM_ENABLE_MONITORING");
            std::env::remove_var("VELOSTREAM_JOB_TIMEOUT_SECS");
            std::env::remove_var("VELOSTREAM_TABLE_CACHE_SIZE");
        }

        let config = StreamJobServerConfig::from_env("env-group");
        assert_eq!(config.kafka_brokers, "localhost:9092"); // Default
        assert_eq!(config.base_group_id, "env-group");
        assert_eq!(config.max_jobs, 100); // Default
        assert!(!config.enable_monitoring); // Default

        // Clean up after test
        unsafe {
            std::env::remove_var("VELOSTREAM_KAFKA_BROKERS");
            std::env::remove_var("VELOSTREAM_MAX_JOBS");
            std::env::remove_var("VELOSTREAM_ENABLE_MONITORING");
            std::env::remove_var("VELOSTREAM_JOB_TIMEOUT_SECS");
            std::env::remove_var("VELOSTREAM_TABLE_CACHE_SIZE");
        }
    }

    #[test]
    fn test_from_env_with_broker_override() {
        unsafe {
            std::env::set_var("VELOSTREAM_KAFKA_BROKERS", "test-broker:9092");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert_eq!(config.kafka_brokers, "test-broker:9092");
        unsafe {
            std::env::remove_var("VELOSTREAM_KAFKA_BROKERS");
        }
    }

    #[test]
    fn test_from_env_with_max_jobs_override() {
        unsafe {
            std::env::set_var("VELOSTREAM_MAX_JOBS", "250");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert_eq!(config.max_jobs, 250);
        unsafe {
            std::env::remove_var("VELOSTREAM_MAX_JOBS");
        }
    }

    #[test]
    fn test_from_env_with_monitoring_override() {
        unsafe {
            std::env::set_var("VELOSTREAM_ENABLE_MONITORING", "true");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert!(config.enable_monitoring);
        unsafe {
            std::env::remove_var("VELOSTREAM_ENABLE_MONITORING");
        }
    }

    #[test]
    fn test_from_env_with_timeout_override() {
        unsafe {
            std::env::set_var("VELOSTREAM_JOB_TIMEOUT_SECS", "3600");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert_eq!(config.job_timeout, Duration::from_secs(3600));
        unsafe {
            std::env::remove_var("VELOSTREAM_JOB_TIMEOUT_SECS");
        }
    }

    #[test]
    fn test_from_env_with_cache_override() {
        unsafe {
            std::env::set_var("VELOSTREAM_TABLE_CACHE_SIZE", "500");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert_eq!(config.table_cache_size, 500);
        unsafe {
            std::env::remove_var("VELOSTREAM_TABLE_CACHE_SIZE");
        }
    }

    #[test]
    fn test_from_env_with_monitoring_1() {
        unsafe {
            std::env::set_var("VELOSTREAM_ENABLE_MONITORING", "1");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert!(config.enable_monitoring);
        unsafe {
            std::env::remove_var("VELOSTREAM_ENABLE_MONITORING");
        }
    }

    #[test]
    fn test_from_env_with_monitoring_false() {
        unsafe {
            std::env::set_var("VELOSTREAM_ENABLE_MONITORING", "false");
        }
        let config = StreamJobServerConfig::from_env("env-group");
        assert!(!config.enable_monitoring);
        unsafe {
            std::env::remove_var("VELOSTREAM_ENABLE_MONITORING");
        }
    }
}
