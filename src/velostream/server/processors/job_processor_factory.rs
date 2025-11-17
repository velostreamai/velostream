//! JobProcessorFactory - Creates processors based on configuration
//!
//! This factory provides a simple interface for creating the appropriate
//! job processor based on JobProcessorConfig.

use crate::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessor, JobProcessorConfig, MockJobProcessor,
    SimpleJobProcessor, TransactionalJobProcessor,
};
use crate::velostream::server::v2::AdaptiveJobProcessor;
use log::info;
use std::sync::Arc;
use std::time::Duration;

/// Factory for creating JobProcessor implementations
pub struct JobProcessorFactory;

impl JobProcessorFactory {
    /// Create a job processor based on the provided configuration
    ///
    /// This factory method creates the appropriate processor:
    /// - Simple: Single-threaded, best-effort delivery
    /// - Transactional: Single-threaded, at-least-once delivery
    /// - Adaptive: Multi-partition parallel execution
    ///
    /// The processor is returned as a trait object for flexible usage.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use velostream::velostream::server::processors::{JobProcessorFactory, JobProcessorConfig};
    ///
    /// // Create Adaptive processor with 8 partitions
    /// let adaptive_config = JobProcessorConfig::Adaptive {
    ///     num_partitions: Some(8),
    ///     enable_core_affinity: false,
    /// };
    /// let adaptive = JobProcessorFactory::create(adaptive_config);
    /// assert_eq!(adaptive.processor_version(), "V2");
    /// assert_eq!(adaptive.num_partitions(), 8);
    /// ```
    pub fn create(config: JobProcessorConfig) -> Arc<dyn JobProcessor> {
        Self::create_with_config(config, None)
    }

    /// Create a job processor with explicit JobProcessingConfig
    ///
    /// This allows fine-grained control over processor behavior including:
    /// - Batch size and timeout
    /// - Failure strategy and retry behavior
    /// - DLQ configuration
    /// - Empty batch handling
    ///
    /// If job_processing_config is None, defaults based on processor type are used.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use velostream::velostream::server::processors::{
    ///     JobProcessorFactory, JobProcessorConfig, JobProcessingConfig, FailureStrategy
    /// };
    /// use std::time::Duration;
    ///
    /// let config = JobProcessingConfig {
    ///     use_transactions: false,
    ///     failure_strategy: FailureStrategy::LogAndContinue,
    ///     max_batch_size: 100,
    ///     batch_timeout: Duration::from_millis(100),
    ///     max_retries: 2,
    ///     retry_backoff: Duration::from_millis(50),
    ///     progress_interval: 100,
    ///     log_progress: false,
    ///     empty_batch_count: 0,
    ///     wait_on_empty_batch_ms: 100,
    ///     enable_dlq: true,
    ///     dlq_max_size: Some(100),
    /// };
    ///
    /// let processor = JobProcessorFactory::create_with_config(
    ///     JobProcessorConfig::Simple,
    ///     Some(config)
    /// );
    /// ```
    pub fn create_with_config(
        config: JobProcessorConfig,
        job_processing_config: Option<JobProcessingConfig>,
    ) -> Arc<dyn JobProcessor> {
        match config {
            JobProcessorConfig::Simple => {
                info!("Creating Simple processor (Single-threaded, Best-Effort delivery)");
                let processing_config =
                    job_processing_config.unwrap_or_else(|| JobProcessingConfig {
                        use_transactions: false,
                        failure_strategy: FailureStrategy::LogAndContinue,
                        ..Default::default()
                    });
                Arc::new(SimpleJobProcessor::new(processing_config))
            }
            JobProcessorConfig::Transactional => {
                info!("Creating Transactional processor (Single-threaded, At-Least-Once delivery)");
                let processing_config =
                    job_processing_config.unwrap_or_else(|| JobProcessingConfig {
                        use_transactions: true,
                        failure_strategy: FailureStrategy::FailBatch,
                        ..Default::default()
                    });
                Arc::new(TransactionalJobProcessor::new(processing_config))
            }
            JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            } => {
                let partitioned_config = crate::velostream::server::v2::PartitionedJobConfig {
                    num_partitions,
                    enable_core_affinity,
                    ..Default::default()
                };

                let actual_partitions = num_partitions.unwrap_or_else(|| {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(8)
                });

                info!(
                    "Creating Adaptive processor (AdaptiveJobProcessor): {} partitions, affinity: {}",
                    actual_partitions, enable_core_affinity
                );
                // Note: Adaptive processor doesn't use JobProcessingConfig yet
                // In the future, this can be extended to pass job_processing_config
                Arc::new(AdaptiveJobProcessor::new(partitioned_config))
            }
        }
    }

    /// Create a Simple mode processor (single-threaded, best-effort)
    pub fn create_simple() -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::Simple)
    }

    /// Create a Simple mode processor with explicit configuration
    pub fn create_simple_with_config(config: JobProcessingConfig) -> Arc<dyn JobProcessor> {
        Self::create_with_config(JobProcessorConfig::Simple, Some(config))
    }

    /// Create a Transactional mode processor (single-threaded, at-least-once)
    pub fn create_transactional() -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::Transactional)
    }

    /// Create a Transactional mode processor with explicit configuration
    pub fn create_transactional_with_config(config: JobProcessingConfig) -> Arc<dyn JobProcessor> {
        Self::create_with_config(JobProcessorConfig::Transactional, Some(config))
    }

    /// Create an Adaptive mode processor with default configuration
    pub fn create_adaptive_default() -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::Adaptive {
            num_partitions: None,
            enable_core_affinity: false,
        })
    }

    /// Create an Adaptive mode processor with specific partition count
    pub fn create_adaptive_with_partitions(num_partitions: usize) -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::Adaptive {
            num_partitions: Some(num_partitions),
            enable_core_affinity: false,
        })
    }

    /// Legacy: Create a V1 Simple processor (single-threaded, best-effort)
    #[deprecated(since = "0.2.0", note = "use create_simple() instead")]
    pub fn create_v1_simple() -> Arc<dyn JobProcessor> {
        Self::create_simple()
    }

    /// Legacy: Create a V1 Transactional processor (single-threaded, at-least-once)
    #[deprecated(since = "0.2.0", note = "use create_transactional() instead")]
    pub fn create_v1_transactional() -> Arc<dyn JobProcessor> {
        Self::create_transactional()
    }

    /// Legacy: Create a V2 processor with default configuration
    #[deprecated(since = "0.2.0", note = "use create_adaptive_default() instead")]
    pub fn create_v2_default() -> Arc<dyn JobProcessor> {
        Self::create_adaptive_default()
    }

    /// Legacy: Create a V2 processor with specific partition count
    #[deprecated(
        since = "0.2.0",
        note = "use create_adaptive_with_partitions() instead"
    )]
    pub fn create_v2_with_partitions(num_partitions: usize) -> Arc<dyn JobProcessor> {
        Self::create_adaptive_with_partitions(num_partitions)
    }

    /// Create a processor from a configuration string
    ///
    /// Parses configuration string and creates the appropriate processor.
    /// This is useful for config file parsing (YAML, TOML, etc.)
    ///
    /// Supported formats:
    /// - "simple", "v1:simple" → Simple mode
    /// - "transactional", "v1:transactional" → Transactional mode
    /// - "adaptive", "v2" → Adaptive with default partitions
    /// - "adaptive:8", "v2:8" → Adaptive with 8 partitions
    /// - "adaptive:8:affinity", "v2:8:affinity" → Adaptive with affinity
    pub fn create_from_str(config_str: &str) -> Result<Arc<dyn JobProcessor>, String> {
        let config: JobProcessorConfig = config_str.parse()?;
        Ok(Self::create(config))
    }

    /// Create a mock processor for testing
    ///
    /// This creates a MockJobProcessor that simulates the JobProcessor trait
    /// without requiring real Kafka connections or data processing.
    /// Useful for testing StreamJobServer logic in isolation.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use velostream::velostream::server::processors::JobProcessorFactory;
    ///
    /// let mock = JobProcessorFactory::create_mock();
    /// assert_eq!(mock.processor_version(), "Mock");
    /// ```
    pub fn create_mock() -> Arc<dyn JobProcessor> {
        info!("Creating Mock processor for testing");
        Arc::new(MockJobProcessor::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_create_simple() {
        let processor = JobProcessorFactory::create_simple();
        assert_eq!(processor.processor_version(), "V1");
        assert_eq!(processor.num_partitions(), 1);
        assert_eq!(processor.processor_name(), "SimpleJobProcessor");
    }

    #[test]
    fn test_factory_create_transactional() {
        let processor = JobProcessorFactory::create_transactional();
        assert_eq!(processor.processor_version(), "V1-Transactional");
        assert_eq!(processor.num_partitions(), 1);
        assert_eq!(processor.processor_name(), "TransactionalJobProcessor");
    }

    #[test]
    fn test_factory_create_from_string_simple() {
        let processor = JobProcessorFactory::create_from_str("simple").unwrap();
        assert_eq!(processor.processor_version(), "V1");
        assert_eq!(processor.num_partitions(), 1);
    }

    #[test]
    fn test_factory_create_from_string_transactional() {
        let processor = JobProcessorFactory::create_from_str("transactional").unwrap();
        assert_eq!(processor.processor_version(), "V1-Transactional");
        assert_eq!(processor.num_partitions(), 1);
    }

    #[test]
    fn test_factory_create_adaptive_default() {
        let processor = JobProcessorFactory::create_adaptive_default();
        assert_eq!(processor.processor_version(), "V2");
        assert!(processor.num_partitions() > 0);
    }

    #[test]
    fn test_factory_create_adaptive_with_partitions() {
        let processor = JobProcessorFactory::create_adaptive_with_partitions(8);
        assert_eq!(processor.processor_version(), "V2");
        assert_eq!(processor.num_partitions(), 8);
    }

    #[test]
    fn test_factory_create_from_string_adaptive() {
        let processor = JobProcessorFactory::create_from_str("adaptive").unwrap();
        assert_eq!(processor.processor_version(), "V2");
    }

    #[test]
    fn test_factory_create_from_string_adaptive_8() {
        let processor = JobProcessorFactory::create_from_str("adaptive:8").unwrap();
        assert_eq!(processor.processor_version(), "V2");
        assert_eq!(processor.num_partitions(), 8);
    }

    #[test]
    fn test_factory_invalid_config() {
        let result = JobProcessorFactory::create_from_str("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_factory_create_mock() {
        let mock = JobProcessorFactory::create_mock();
        assert_eq!(mock.processor_version(), "Mock");
        assert_eq!(mock.processor_name(), "Mock Test Processor");
        assert_eq!(mock.num_partitions(), 1);
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_v1_simple() {
        let processor = JobProcessorFactory::create_v1_simple();
        assert_eq!(processor.processor_version(), "V1");
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_v1_transactional() {
        let processor = JobProcessorFactory::create_v1_transactional();
        assert_eq!(processor.processor_version(), "V1-Transactional");
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_v2_default() {
        let processor = JobProcessorFactory::create_v2_default();
        assert_eq!(processor.processor_version(), "V2");
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_v2_with_partitions() {
        let processor = JobProcessorFactory::create_v2_with_partitions(8);
        assert_eq!(processor.processor_version(), "V2");
        assert_eq!(processor.num_partitions(), 8);
    }
}
