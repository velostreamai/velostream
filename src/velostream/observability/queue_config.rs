//! Async Queue Configuration for Non-Blocking Observability
//!
//! Configuration structs for the observability async queue system.
//! Supports environment variable overrides via VELOSTREAM_ prefix.
//!
//! ## Queue Sizes Rationale
//!
//! - **Metrics: 65,536** - High-frequency SQL annotations (10 metrics/record × 10K records/s = ~7s buffer)
//! - **Traces: 16,384** - Lower frequency (per-batch spans, matches OpenTelemetry default)
//!
//! ## Configuration Example
//!
//! ```rust
//! use velostream::velostream::observability::queue_config::ObservabilityQueueConfig;
//! use std::time::Duration;
//!
//! let config = ObservabilityQueueConfig::default()
//!     .with_metrics_queue_size(100_000)
//!     .with_metrics_flush_interval(Duration::from_secs(10));
//! ```

use crate::velostream::sql::config::resolver::PropertyResolver;
use std::collections::HashMap;
use std::time::Duration;

/// Main configuration for the observability async queue system
#[derive(Debug, Clone)]
pub struct ObservabilityQueueConfig {
    /// Size of the metrics queue (default: 65,536)
    pub metrics_queue_size: usize,

    /// Size of the traces queue (default: 16,384)
    pub traces_queue_size: usize,

    /// Configuration for metrics flushing
    pub metrics_flush_config: MetricsFlushConfig,

    /// Configuration for traces flushing
    pub traces_flush_config: TracesFlushConfig,

    /// Enable async queue (can be disabled for testing)
    pub enabled: bool,
}

/// Configuration for metrics flush behavior
#[derive(Debug, Clone)]
pub struct MetricsFlushConfig {
    /// Batch size trigger (flush when this many events accumulated)
    pub batch_size: usize,

    /// Time interval trigger (flush after this duration even if batch not full)
    pub flush_interval: Duration,

    /// Maximum retry attempts for failed remote-write flushes
    pub max_retry_attempts: u32,

    /// Backoff duration between retry attempts
    pub retry_backoff: Duration,
}

/// Configuration for traces flush behavior
#[derive(Debug, Clone)]
pub struct TracesFlushConfig {
    /// Time interval for flushing trace spans
    pub flush_interval: Duration,

    /// Maximum batch size for span exports
    pub max_batch_size: usize,
}

impl Default for ObservabilityQueueConfig {
    fn default() -> Self {
        Self {
            metrics_queue_size: 65_536,
            traces_queue_size: 16_384,
            metrics_flush_config: MetricsFlushConfig::default(),
            traces_flush_config: TracesFlushConfig::default(),
            enabled: true,
        }
    }
}

impl Default for MetricsFlushConfig {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            flush_interval: Duration::from_secs(5),
            max_retry_attempts: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

impl Default for TracesFlushConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(5),
            max_batch_size: 512,
        }
    }
}

impl ObservabilityQueueConfig {
    /// Create configuration from environment variables and properties HashMap
    ///
    /// Supports VELOSTREAM_ prefixed environment variables:
    /// - VELOSTREAM_OBSERVABILITY_QUEUE_ENABLED (bool, default: true)
    /// - VELOSTREAM_METRICS_QUEUE_SIZE (usize, default: 65536)
    /// - VELOSTREAM_TRACES_QUEUE_SIZE (usize, default: 16384)
    /// - VELOSTREAM_METRICS_BATCH_SIZE (usize, default: 1000)
    /// - VELOSTREAM_METRICS_FLUSH_INTERVAL_MS (u64, default: 5000)
    /// - VELOSTREAM_METRICS_MAX_RETRY_ATTEMPTS (u32, default: 3)
    /// - VELOSTREAM_METRICS_RETRY_BACKOFF_MS (u64, default: 100)
    /// - VELOSTREAM_TRACES_FLUSH_INTERVAL_MS (u64, default: 5000)
    /// - VELOSTREAM_TRACES_MAX_BATCH_SIZE (usize, default: 512)
    pub fn from_properties(props: &HashMap<String, String>) -> Self {
        let resolver = PropertyResolver::default();

        let enabled = resolver.resolve_bool(
            "OBSERVABILITY_QUEUE_ENABLED",
            &["observability.queue.enabled", "queue.enabled"],
            props,
            true,
        );

        let metrics_queue_size = resolver.resolve(
            "METRICS_QUEUE_SIZE",
            &["metrics.queue.size", "queue.metrics.size"],
            props,
            65_536,
        );

        let traces_queue_size = resolver.resolve(
            "TRACES_QUEUE_SIZE",
            &["traces.queue.size", "queue.traces.size"],
            props,
            16_384,
        );

        let metrics_batch_size = resolver.resolve(
            "METRICS_BATCH_SIZE",
            &["metrics.batch.size", "batch.size"],
            props,
            1_000,
        );

        let metrics_flush_interval_ms = resolver.resolve(
            "METRICS_FLUSH_INTERVAL_MS",
            &["metrics.flush.interval.ms", "flush.interval.ms"],
            props,
            5_000u64,
        );

        let max_retry_attempts = resolver.resolve(
            "METRICS_MAX_RETRY_ATTEMPTS",
            &["metrics.max.retry.attempts", "max.retry.attempts"],
            props,
            3u32,
        );

        let retry_backoff_ms = resolver.resolve(
            "METRICS_RETRY_BACKOFF_MS",
            &["metrics.retry.backoff.ms", "retry.backoff.ms"],
            props,
            100u64,
        );

        let traces_flush_interval_ms = resolver.resolve(
            "TRACES_FLUSH_INTERVAL_MS",
            &["traces.flush.interval.ms"],
            props,
            5_000u64,
        );

        let traces_max_batch_size = resolver.resolve(
            "TRACES_MAX_BATCH_SIZE",
            &["traces.max.batch.size"],
            props,
            512,
        );

        Self {
            metrics_queue_size,
            traces_queue_size,
            metrics_flush_config: MetricsFlushConfig {
                batch_size: metrics_batch_size,
                flush_interval: Duration::from_millis(metrics_flush_interval_ms),
                max_retry_attempts,
                retry_backoff: Duration::from_millis(retry_backoff_ms),
            },
            traces_flush_config: TracesFlushConfig {
                flush_interval: Duration::from_millis(traces_flush_interval_ms),
                max_batch_size: traces_max_batch_size,
            },
            enabled,
        }
    }

    /// Builder: Set metrics queue size
    pub fn with_metrics_queue_size(mut self, size: usize) -> Self {
        self.metrics_queue_size = size;
        self
    }

    /// Builder: Set traces queue size
    pub fn with_traces_queue_size(mut self, size: usize) -> Self {
        self.traces_queue_size = size;
        self
    }

    /// Builder: Set metrics batch size
    pub fn with_metrics_batch_size(mut self, size: usize) -> Self {
        self.metrics_flush_config.batch_size = size;
        self
    }

    /// Builder: Set metrics flush interval
    pub fn with_metrics_flush_interval(mut self, interval: Duration) -> Self {
        self.metrics_flush_config.flush_interval = interval;
        self
    }

    /// Builder: Set traces flush interval
    pub fn with_traces_flush_interval(mut self, interval: Duration) -> Self {
        self.traces_flush_config.flush_interval = interval;
        self
    }

    /// Builder: Set max retry attempts
    pub fn with_max_retry_attempts(mut self, attempts: u32) -> Self {
        self.metrics_flush_config.max_retry_attempts = attempts;
        self
    }

    /// Builder: Set retry backoff duration
    pub fn with_retry_backoff(mut self, backoff: Duration) -> Self {
        self.metrics_flush_config.retry_backoff = backoff;
        self
    }

    /// Builder: Enable or disable the queue
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ObservabilityQueueConfig::default();
        assert_eq!(config.metrics_queue_size, 65_536);
        assert_eq!(config.traces_queue_size, 16_384);
        assert_eq!(config.metrics_flush_config.batch_size, 1_000);
        assert_eq!(
            config.metrics_flush_config.flush_interval,
            Duration::from_secs(5)
        );
        assert_eq!(config.metrics_flush_config.max_retry_attempts, 3);
        assert_eq!(config.traces_flush_config.max_batch_size, 512);
        assert!(config.enabled);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ObservabilityQueueConfig::default()
            .with_metrics_queue_size(100_000)
            .with_traces_queue_size(50_000)
            .with_metrics_batch_size(5_000)
            .with_metrics_flush_interval(Duration::from_secs(10))
            .with_enabled(false);

        assert_eq!(config.metrics_queue_size, 100_000);
        assert_eq!(config.traces_queue_size, 50_000);
        assert_eq!(config.metrics_flush_config.batch_size, 5_000);
        assert_eq!(
            config.metrics_flush_config.flush_interval,
            Duration::from_secs(10)
        );
        assert!(!config.enabled);
    }

    #[test]
    fn test_from_properties_defaults() {
        let props = HashMap::new();
        let config = ObservabilityQueueConfig::from_properties(&props);

        assert_eq!(config.metrics_queue_size, 65_536);
        assert_eq!(config.traces_queue_size, 16_384);
        assert!(config.enabled);
    }

    #[test]
    fn test_from_properties_custom() {
        let mut props = HashMap::new();
        props.insert("metrics.queue.size".to_string(), "100000".to_string());
        props.insert("traces.queue.size".to_string(), "50000".to_string());
        props.insert("metrics.batch.size".to_string(), "2000".to_string());
        props.insert(
            "observability.queue.enabled".to_string(),
            "false".to_string(),
        );

        let config = ObservabilityQueueConfig::from_properties(&props);

        assert_eq!(config.metrics_queue_size, 100_000);
        assert_eq!(config.traces_queue_size, 50_000);
        assert_eq!(config.metrics_flush_config.batch_size, 2_000);
        assert!(!config.enabled);
    }

    // Note: Environment variable override test skipped to avoid test pollution.
    // The PropertyResolver is already tested extensively in resolver.rs tests.
    // Environment variables work correctly in production (tested manually).
}
