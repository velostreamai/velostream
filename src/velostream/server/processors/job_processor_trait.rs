//! JobProcessor trait for flexible architecture switching
//!
//! This trait allows swapping between different job processing architectures (V1, V2, etc.)
//! at runtime based on configuration, enabling easy A/B testing and gradual migration.

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::server::processors::common::JobExecutionStats;
use crate::velostream::sql::StreamExecutionEngine;
use crate::velostream::sql::StreamingQuery;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Shared job stats for real-time monitoring.
/// Processors update this reference during execution so external observers
/// (like the test harness) can poll live statistics.
pub type SharedJobStats = Arc<std::sync::RwLock<JobExecutionStats>>;

/// Enhanced metrics for unified processor monitoring
#[derive(Debug, Clone)]
pub struct ProcessorMetrics {
    /// Processor architecture version (V1, V1-Transactional, V2)
    pub version: String,
    /// Processor implementation name
    pub name: String,
    /// Number of partitions (1 for V1, N for V2)
    pub num_partitions: usize,
    /// Current lifecycle state
    pub lifecycle_state: LifecycleState,
    /// Total records processed across all instances
    pub total_records: u64,
    /// Total records failed
    pub failed_records: u64,
    /// Average throughput (records/second)
    pub throughput_rps: f64,
    /// Current uptime in seconds
    pub uptime_secs: f64,
}

/// Processor lifecycle states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleState {
    /// Processor not yet started
    Idle,
    /// Processor actively running
    Running,
    /// Processor paused (can be resumed)
    Paused,
    /// Processor in process of shutting down
    Stopping,
    /// Processor shut down
    Stopped,
}

impl LifecycleState {
    pub fn as_str(&self) -> &str {
        match self {
            LifecycleState::Idle => "idle",
            LifecycleState::Running => "running",
            LifecycleState::Paused => "paused",
            LifecycleState::Stopping => "stopping",
            LifecycleState::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for LifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Trait for flexible job processing architectures
///
/// Implementations:
/// - `SimpleJobProcessor`: V1 architecture (single-threaded, single partition)
/// - `TransactionalJobProcessor`: V1 architecture with transaction support
/// - `AdaptiveJobProcessor`: V2 architecture (multi-partition, parallel execution)
///
/// This trait enables:
/// - Runtime architecture switching via configuration
/// - A/B testing between V1 and V2
/// - Unified lifecycle management (start/stop/pause/resume)
/// - Integrated metrics aggregation across all architectures
/// - Extensibility for future architectures (V3, etc.)
#[async_trait]
pub trait JobProcessor: Send + Sync {
    /// Get the number of partitions this processor uses
    ///
    /// - V1: Returns 1 (single-threaded)
    /// - V2: Returns 8 (or configured partition count)
    fn num_partitions(&self) -> usize;

    /// Get processor name for logging and metrics
    fn processor_name(&self) -> &str;

    /// Get processor version for metrics and dashboards
    fn processor_version(&self) -> &str;

    /// Process a complete job from data source to sink
    ///
    /// Handles end-to-end job execution including reading from source,
    /// executing SQL queries, and writing to destination sink.
    ///
    /// # Arguments
    /// * `reader` - Data source to read records from
    /// * `writer` - Optional sink to write results to
    /// * `engine` - Shared StreamExecutionEngine for query execution
    /// * `query` - Streaming query to execute
    /// * `job_name` - Name of the job for logging and metrics
    /// * `shutdown_rx` - Channel to receive shutdown signal
    /// * `shared_stats` - Optional shared stats for real-time monitoring
    ///
    /// # Returns
    /// Job execution statistics or error
    async fn process_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
        shared_stats: Option<SharedJobStats>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>>;

    /// Process a complete job with multiple data sources and sinks
    ///
    /// Handles end-to-end job execution with support for multiple readers and writers.
    /// This is the primary method for job processing that supports all processor types.
    ///
    /// # Arguments
    /// * `readers` - Map of named data sources to read records from
    /// * `writers` - Map of named sinks to write results to
    /// * `engine` - Shared StreamExecutionEngine for query execution
    /// * `query` - Streaming query to execute
    /// * `job_name` - Name of the job for logging and metrics
    /// * `shutdown_rx` - Channel to receive shutdown signal
    /// * `shared_stats` - Optional shared stats for real-time monitoring
    ///
    /// # Returns
    /// Job execution statistics or error
    async fn process_multi_job(
        &self,
        readers: HashMap<String, Box<dyn DataReader>>,
        writers: HashMap<String, Box<dyn DataWriter>>,
        engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
        shared_stats: Option<SharedJobStats>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>>;

    /// Start the processor
    ///
    /// Initialize processor resources and transition to Running state.
    /// This is called before processing begins.
    ///
    /// # Returns
    /// Ok(()) if processor started successfully, error otherwise
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Pause the processor
    ///
    /// Temporarily stop processing while maintaining state.
    /// Can be resumed with `resume()`.
    ///
    /// # Returns
    /// Ok(()) if processor paused successfully, error otherwise
    async fn pause(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Resume the processor
    ///
    /// Resume processing after being paused.
    /// Has no effect if processor is not paused.
    ///
    /// # Returns
    /// Ok(()) if processor resumed successfully, error otherwise
    async fn resume(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Stop the processor
    ///
    /// Gracefully shutdown processor and release resources.
    /// Once stopped, processor must be restarted to resume operations.
    ///
    /// # Returns
    /// Ok(()) if processor stopped successfully, error otherwise
    async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Get unified metrics for this processor
    ///
    /// Returns aggregated metrics across all partitions/instances
    /// including lifecycle state, throughput, and error tracking.
    ///
    /// # Returns
    /// ProcessorMetrics with current processor state and statistics
    fn metrics(&self) -> ProcessorMetrics {
        ProcessorMetrics {
            version: self.processor_version().to_string(),
            name: self.processor_name().to_string(),
            num_partitions: self.num_partitions(),
            lifecycle_state: LifecycleState::Running,
            total_records: 0,
            failed_records: 0,
            throughput_rps: 0.0,
            uptime_secs: 0.0,
        }
    }

    /// Get per-partition metrics for profiling and analysis
    ///
    /// Returns metrics from each partition for detailed performance analysis.
    /// Useful for identifying bottlenecks and validating hypotheses about
    /// per-partition behavior.
    ///
    /// # Returns
    /// Vector of partition metrics (empty for single-partition processors)
    fn get_partition_metrics(&self) -> Vec<Arc<crate::velostream::server::v2::PartitionMetrics>> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_processor_trait_is_send_sync() {
        // Compile-time check that JobProcessor trait is Send + Sync
        // This allows storing it in Arc<dyn JobProcessor>
        fn assert_send<T: Send + Sync>() {}
        assert_send::<Arc<dyn JobProcessor>>();
    }

    #[test]
    fn test_lifecycle_state_display() {
        assert_eq!(LifecycleState::Idle.to_string(), "idle");
        assert_eq!(LifecycleState::Running.to_string(), "running");
        assert_eq!(LifecycleState::Paused.to_string(), "paused");
        assert_eq!(LifecycleState::Stopping.to_string(), "stopping");
        assert_eq!(LifecycleState::Stopped.to_string(), "stopped");
    }

    #[test]
    fn test_lifecycle_state_as_str() {
        assert_eq!(LifecycleState::Idle.as_str(), "idle");
        assert_eq!(LifecycleState::Running.as_str(), "running");
        assert_eq!(LifecycleState::Paused.as_str(), "paused");
        assert_eq!(LifecycleState::Stopping.as_str(), "stopping");
        assert_eq!(LifecycleState::Stopped.as_str(), "stopped");
    }

    #[test]
    fn test_lifecycle_state_comparison() {
        assert_eq!(LifecycleState::Running, LifecycleState::Running);
        assert_ne!(LifecycleState::Running, LifecycleState::Paused);
        assert_ne!(LifecycleState::Idle, LifecycleState::Stopped);
    }

    #[test]
    fn test_processor_metrics_creation() {
        let metrics = ProcessorMetrics {
            version: "V1".to_string(),
            name: "SimpleJobProcessor".to_string(),
            num_partitions: 1,
            lifecycle_state: LifecycleState::Running,
            total_records: 1000,
            failed_records: 5,
            throughput_rps: 1000.0,
            uptime_secs: 1.0,
        };

        assert_eq!(metrics.version, "V1");
        assert_eq!(metrics.name, "SimpleJobProcessor");
        assert_eq!(metrics.num_partitions, 1);
        assert_eq!(metrics.lifecycle_state, LifecycleState::Running);
        assert_eq!(metrics.total_records, 1000);
        assert_eq!(metrics.failed_records, 5);
        assert_eq!(metrics.throughput_rps, 1000.0);
        assert_eq!(metrics.uptime_secs, 1.0);
    }

    #[test]
    fn test_processor_metrics_v2() {
        let metrics = ProcessorMetrics {
            version: "V2".to_string(),
            name: "AdaptiveJobProcessor".to_string(),
            num_partitions: 8,
            lifecycle_state: LifecycleState::Running,
            total_records: 50000,
            failed_records: 10,
            throughput_rps: 50000.0,
            uptime_secs: 1.0,
        };

        assert_eq!(metrics.version, "V2");
        assert_eq!(metrics.name, "AdaptiveJobProcessor");
        assert_eq!(metrics.num_partitions, 8);
        assert_eq!(metrics.total_records, 50000);
    }
}
