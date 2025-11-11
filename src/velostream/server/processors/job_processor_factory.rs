//! JobProcessorFactory - Creates processors based on configuration
//!
//! This factory provides a simple interface for creating the appropriate
//! job processor based on JobProcessorConfig.

use crate::velostream::server::processors::{JobProcessor, JobProcessorConfig};
use crate::velostream::server::v2::PartitionedJobCoordinator;
use log::info;
use std::sync::Arc;

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
