//! JobProcessor trait for flexible architecture switching
//!
//! This trait allows swapping between different job processing architectures (V1, V2, etc.)
//! at runtime based on configuration, enabling easy A/B testing and gradual migration.

use async_trait::async_trait;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;
use crate::velostream::sql::StreamExecutionEngine;
use std::sync::Arc;

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
