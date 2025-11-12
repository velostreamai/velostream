//! JobProcessorFactory - Creates processors based on configuration
//!
//! This factory provides a simple interface for creating the appropriate
//! job processor based on JobProcessorConfig.

use crate::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessor, JobProcessorConfig, MockJobProcessor,
    SimpleJobProcessor, TransactionalJobProcessor,
};
use crate::velostream::server::v2::PartitionedJobCoordinator;
use log::info;
use std::sync::Arc;
use std::time::Duration;

/// Factory for creating JobProcessor implementations
pub struct JobProcessorFactory;

impl JobProcessorFactory {
    /// Create a job processor based on the provided configuration
    ///
    /// This factory method creates the appropriate processor type:
    /// - V2: Returns a PartitionedJobCoordinator (multi-partition parallel)
    ///
    /// The processor is returned as a trait object for flexible usage.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// use velostream::velostream::server::processors::{JobProcessorFactory, JobProcessorConfig};
    ///
    /// // Create V2 processor with 8 partitions
    /// let v2_config = JobProcessorConfig::V2 {
    ///     num_partitions: Some(8),
    ///     enable_core_affinity: false,
    /// };
    /// let v2 = JobProcessorFactory::create(v2_config);
    /// assert_eq!(v2.processor_version(), "V2");
    /// assert_eq!(v2.num_partitions(), 8);
    /// ```
    pub fn create(config: JobProcessorConfig) -> Arc<dyn JobProcessor> {
        match config {
            JobProcessorConfig::V1Simple => {
                info!("Creating V1 Simple JobProcessor (SingleThreaded, Best-Effort)");
                Arc::new(SimpleJobProcessor::new(JobProcessingConfig {
                    use_transactions: false,
                    failure_strategy: FailureStrategy::LogAndContinue,
                    ..Default::default()
                }))
            }
            JobProcessorConfig::V1Transactional => {
                info!("Creating V1 Transactional JobProcessor (SingleThreaded, At-Least-Once)");
                Arc::new(TransactionalJobProcessor::new(JobProcessingConfig {
                    use_transactions: true,
                    failure_strategy: FailureStrategy::FailBatch,
                    ..Default::default()
                }))
            }
            JobProcessorConfig::V2 {
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
                    "Creating V2 JobProcessor (PartitionedJobCoordinator): {} partitions, affinity: {}",
                    actual_partitions, enable_core_affinity
                );
                Arc::new(PartitionedJobCoordinator::new(partitioned_config))
            }
        }
    }

    /// Create a V1 Simple processor (single-threaded, best-effort)
    pub fn create_v1_simple() -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::V1Simple)
    }

    /// Create a V1 Transactional processor (single-threaded, at-least-once)
    pub fn create_v1_transactional() -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::V1Transactional)
    }

    /// Create a V2 processor with default configuration
    pub fn create_v2_default() -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::V2 {
            num_partitions: None,
            enable_core_affinity: false,
        })
    }

    /// Create a V2 processor with specific partition count
    pub fn create_v2_with_partitions(num_partitions: usize) -> Arc<dyn JobProcessor> {
        Self::create(JobProcessorConfig::V2 {
            num_partitions: Some(num_partitions),
            enable_core_affinity: false,
        })
    }

    /// Create a processor from a configuration string
    ///
    /// Parses configuration string and creates the appropriate processor.
    /// This is useful for config file parsing (YAML, TOML, etc.)
    ///
    /// Supported formats:
    /// - "v1" → V1 processor
    /// - "v2" → V2 processor (default partitions)
    /// - "v2:8" → V2 processor with 8 partitions
    /// - "v2:8:affinity" → V2 processor with 8 partitions and core affinity
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
        info!("Creating Mock JobProcessor for testing");
        Arc::new(MockJobProcessor::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_create_v1_simple() {
        let processor = JobProcessorFactory::create_v1_simple();
        assert_eq!(processor.processor_version(), "V1");
        assert_eq!(processor.num_partitions(), 1);
        assert_eq!(processor.processor_name(), "SimpleJobProcessor");
    }

    #[test]
    fn test_factory_create_v1_transactional() {
        let processor = JobProcessorFactory::create_v1_transactional();
        assert_eq!(processor.processor_version(), "V1-Transactional");
        assert_eq!(processor.num_partitions(), 1);
        assert_eq!(processor.processor_name(), "TransactionalJobProcessor");
    }

    #[test]
    fn test_factory_create_from_string_v1_simple() {
        let processor = JobProcessorFactory::create_from_str("v1:simple").unwrap();
        assert_eq!(processor.processor_version(), "V1");
        assert_eq!(processor.num_partitions(), 1);
    }

    #[test]
    fn test_factory_create_from_string_v1_transactional() {
        let processor = JobProcessorFactory::create_from_str("v1:transactional").unwrap();
        assert_eq!(processor.processor_version(), "V1-Transactional");
        assert_eq!(processor.num_partitions(), 1);
    }

    #[test]
    fn test_factory_create_v2_default() {
        let processor = JobProcessorFactory::create_v2_default();
        assert_eq!(processor.processor_version(), "V2");
        assert!(processor.num_partitions() > 0);
    }

    #[test]
    fn test_factory_create_v2_with_partitions() {
        let processor = JobProcessorFactory::create_v2_with_partitions(8);
        assert_eq!(processor.processor_version(), "V2");
        assert_eq!(processor.num_partitions(), 8);
    }

    #[test]
    fn test_factory_create_from_string_v2() {
        let processor = JobProcessorFactory::create_from_str("v2").unwrap();
        assert_eq!(processor.processor_version(), "V2");
    }

    #[test]
    fn test_factory_create_from_string_v2_8() {
        let processor = JobProcessorFactory::create_from_str("v2:8").unwrap();
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
}
