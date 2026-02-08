//! JobProcessorFactory - Creates processors based on configuration
//!
//! This factory provides a simple interface for creating the appropriate
//! job processor based on JobProcessorConfig.

use crate::velostream::observability::SharedObservabilityManager;
use crate::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessor, JobProcessorConfig, MockJobProcessor,
    SimpleJobProcessor, TransactionalJobProcessor,
};
use crate::velostream::server::v2::{AdaptiveJobProcessor, PartitionedJobConfig};
use crate::velostream::table::UnifiedTable;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

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
        Self::create_with_config_and_tables(config, job_processing_config, None)
    }

    /// Create a job processor with explicit JobProcessingConfig and optional table registry
    ///
    /// This is the main factory method that supports table injection for SQL queries.
    /// Tables will be available to all SQL queries executed by the processor.
    ///
    /// # Arguments
    /// * `config` - JobProcessorConfig specifying the processor type
    /// * `job_processing_config` - Optional JobProcessingConfig for fine-grained control
    /// * `table_registry` - Optional HashMap of tables to inject into query execution context
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    /// use velostream::velostream::server::processors::{JobProcessorFactory, JobProcessorConfig};
    /// use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};
    ///
    /// fn main() {
    ///     // Create table
    ///     let table = OptimizedTableImpl::new();
    ///     // Populate table...
    ///
    ///     // Create registry
    ///     let mut tables: HashMap<String, Arc<dyn UnifiedTable>> = HashMap::new();
    ///     tables.insert("employees".to_string(), Arc::new(table));
    ///
    ///     let processor = JobProcessorFactory::create_with_config_and_tables(
    ///         JobProcessorConfig::Adaptive { num_partitions: Some(4), enable_core_affinity: false },
    ///         None,
    ///         Some(tables),
    ///     );
    /// }
    /// ```
    pub fn create_with_config_and_tables(
        config: JobProcessorConfig,
        job_processing_config: Option<JobProcessingConfig>,
        table_registry: Option<HashMap<String, Arc<dyn UnifiedTable>>>,
    ) -> Arc<dyn JobProcessor> {
        match config {
            JobProcessorConfig::Simple => {
                info!("Creating Simple processor (Single-threaded, Best-Effort delivery)");
                let mut processor =
                    SimpleJobProcessor::new(job_processing_config.unwrap_or_else(|| {
                        JobProcessingConfig {
                            use_transactions: false,
                            failure_strategy: FailureStrategy::LogAndContinue,
                            ..Default::default()
                        }
                    }));
                if let Some(tables) = table_registry {
                    processor.set_table_registry(tables);
                }
                Arc::new(processor)
            }
            JobProcessorConfig::Transactional => {
                info!("Creating Transactional processor (Single-threaded, At-Least-Once delivery)");
                let mut processor =
                    TransactionalJobProcessor::new(job_processing_config.unwrap_or_else(|| {
                        JobProcessingConfig {
                            use_transactions: true,
                            failure_strategy: FailureStrategy::FailBatch,
                            ..Default::default()
                        }
                    }));
                if let Some(tables) = table_registry {
                    processor.set_table_registry(tables);
                }
                Arc::new(processor)
            }
            JobProcessorConfig::Adaptive {
                num_partitions,
                enable_core_affinity,
            } => {
                let mut partitioned_config = PartitionedJobConfig {
                    num_partitions,
                    enable_core_affinity,
                    ..Default::default()
                };

                // Inject table registry if provided
                if let Some(tables) = table_registry {
                    partitioned_config.table_registry = Some(tables);
                }

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

    /// Create an Adaptive mode processor optimized for testing/benchmarking
    ///
    /// Uses immediate EOF detection (empty_batch_count=0) instead of the production default,
    /// eliminating 200-300ms empty batch polling overhead that affects test measurements.
    /// This configuration is optimal for finite test datasets where EOF is deterministic.
    ///
    /// # Performance Impact
    /// - Production config (empty_batch_count=3): 200-300ms overhead for EOF detection
    /// - Test config (empty_batch_count=0): 0ms overhead, immediate EOF detection
    ///
    /// See docs/developer/adaptive_processor_performance_analysis.md for details.
    pub fn create_adaptive_test_optimized(num_partitions: Option<usize>) -> Arc<dyn JobProcessor> {
        let partitioned_config = PartitionedJobConfig {
            num_partitions,
            enable_core_affinity: false,
            empty_batch_count: 0, // Immediate EOF detection for test datasets
            wait_on_empty_batch_ms: 0, // No wait needed
            ..Default::default()
        };

        let actual_partitions = num_partitions.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        });

        info!(
            "Creating Adaptive processor (Test-optimized): {} partitions, immediate EOF detection",
            actual_partitions
        );
        Arc::new(AdaptiveJobProcessor::new(partitioned_config))
    }

    /// Create an Adaptive mode processor with query-based strategy auto-selection
    ///
    /// Enables automatic partitioning strategy selection based on query analysis.
    /// Used in baselines/benchmarks to measure with optimal strategy selection.
    pub fn create_adaptive_test_optimized_with_auto_select(
        num_partitions: Option<usize>,
        query: Arc<crate::velostream::sql::ast::StreamingQuery>,
    ) -> Arc<dyn JobProcessor> {
        let partitioned_config = PartitionedJobConfig {
            num_partitions,
            enable_core_affinity: false,
            empty_batch_count: 0, // Immediate EOF detection for test datasets
            wait_on_empty_batch_ms: 0, // No wait needed
            auto_select_from_query: Some(query),
            ..Default::default()
        };

        let actual_partitions = num_partitions.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        });

        info!(
            "Creating Adaptive processor (Test-optimized with auto-selection): {} partitions, immediate EOF detection",
            actual_partitions
        );
        Arc::new(AdaptiveJobProcessor::new(partitioned_config))
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

    /// Create an Adaptive mode processor with full configuration options.
    ///
    /// This is the most complete factory method for Adaptive processors, supporting:
    /// - Custom partition count and core affinity
    /// - Partitioning strategy selection (by name: "always_hash", "smart_repartition", etc.)
    /// - Query-based auto-selection
    /// - Table registry injection for subqueries
    /// - Production-ready timeout configuration
    /// - Observability for @metric annotation support
    ///
    /// Used by StreamJobServer for deploying Adaptive jobs.
    #[allow(clippy::too_many_arguments)]
    pub fn create_adaptive_full(
        num_partitions: Option<usize>,
        enable_core_affinity: bool,
        partitioning_strategy: Option<String>,
        auto_select_from_query: Option<Arc<crate::velostream::sql::ast::StreamingQuery>>,
        table_registry: Option<HashMap<String, Arc<dyn UnifiedTable>>>,
        empty_batch_count: u32,
        wait_on_empty_batch_ms: u64,
    ) -> Arc<dyn JobProcessor> {
        Self::create_adaptive_full_with_observability(
            num_partitions,
            enable_core_affinity,
            partitioning_strategy,
            auto_select_from_query,
            table_registry,
            empty_batch_count,
            wait_on_empty_batch_ms,
            None,
            None,
        )
    }

    /// Create an Adaptive mode processor with full configuration options and observability.
    ///
    /// This is the most complete factory method for Adaptive processors, supporting:
    /// - Custom partition count and core affinity
    /// - Partitioning strategy selection (by name: "always_hash", "smart_repartition", etc.)
    /// - Query-based auto-selection
    /// - Table registry injection for subqueries
    /// - Production-ready timeout configuration
    /// - Observability for @metric annotation support
    ///
    /// Used by StreamJobServer for deploying Adaptive jobs with metrics support.
    #[allow(clippy::too_many_arguments)]
    pub fn create_adaptive_full_with_observability(
        num_partitions: Option<usize>,
        enable_core_affinity: bool,
        partitioning_strategy: Option<String>,
        auto_select_from_query: Option<Arc<crate::velostream::sql::ast::StreamingQuery>>,
        table_registry: Option<HashMap<String, Arc<dyn UnifiedTable>>>,
        empty_batch_count: u32,
        wait_on_empty_batch_ms: u64,
        observability: Option<SharedObservabilityManager>,
        app_name: Option<String>,
    ) -> Arc<dyn JobProcessor> {
        let partitioned_config = PartitionedJobConfig {
            num_partitions,
            enable_core_affinity,
            partitioning_strategy: partitioning_strategy.clone(),
            auto_select_from_query,
            table_registry,
            empty_batch_count,
            wait_on_empty_batch_ms,
            observability,
            app_name,
            ..Default::default()
        };

        let actual_partitions = num_partitions.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
        });

        info!(
            "Creating Adaptive processor (Full config): {} partitions, affinity: {}, strategy: {:?}",
            actual_partitions,
            enable_core_affinity,
            partitioning_strategy.as_deref().unwrap_or("auto")
        );

        Arc::new(AdaptiveJobProcessor::new(partitioned_config))
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
}
