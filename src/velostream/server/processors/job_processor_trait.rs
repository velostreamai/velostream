//! JobProcessor trait for flexible architecture switching
//!
//! This trait allows swapping between different job processing architectures (V1, V2, etc.)
//! at runtime based on configuration, enabling easy A/B testing and gradual migration.

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::server::processors::common::JobExecutionStats;
use crate::velostream::sql::StreamExecutionEngine;
use crate::velostream::sql::StreamingQuery;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Trait for flexible job processing architectures
///
/// Implementations:
/// - `SimpleJobProcessor`: V1 architecture (single-threaded, single partition)
/// - `PartitionedJobCoordinator`: V2 architecture (multi-partition, parallel execution)
///
/// This trait enables:
/// - Runtime architecture switching via configuration
/// - A/B testing between V1 and V2
/// - Extensibility for future architectures (V3, etc.)
#[async_trait]
pub trait JobProcessor: Send + Sync {
    /// Process a batch of records through the job processing pipeline
    ///
    /// # Arguments
    /// * `records` - Input records to process
    /// * `engine` - Shared StreamExecutionEngine for query execution
    ///
    /// # Returns
    /// Processed records (may be amplified for EMIT CHANGES queries)
    async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        engine: Arc<StreamExecutionEngine>,
    ) -> Result<Vec<StreamRecord>, SqlError>;

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
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>>;
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
}
