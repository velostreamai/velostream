//! Partition state manager for lock-free per-partition query execution
//!
//! This module implements `PartitionStateManager` to manage query state
//! for a single partition without Arc<Mutex> contention.

use crate::velostream::server::v2::metrics::PartitionMetrics;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;
use std::sync::Arc;
use std::time::Instant;

/// Manages query state for a single partition (lock-free design)
///
/// ## Phase 1 Implementation
///
/// Provides the foundation for per-partition query execution:
/// - Integrated metrics tracking
/// - Record processing hooks
/// - State isolation (no cross-partition locks)
///
/// ## Usage
///
/// ```rust
/// use velostream::velostream::server::v2::PartitionStateManager;
///
/// let manager = PartitionStateManager::new(0);
///
/// // Process records through partition
/// // (Phase 2+ will add full query execution integration)
/// ```
///
/// ## Architecture
///
/// Each partition runs independently with:
/// - **Dedicated State**: No shared locks with other partitions
/// - **Metrics Integration**: Real-time throughput and latency tracking
/// - **Backpressure Detection**: Queue depth and latency monitoring
/// - **Future CPU Affinity**: Pin to dedicated core (Linux only, Phase 3+)
///
/// ## Future Enhancements (Phase 2+)
///
/// - **Query State**: WindowManager, GroupByStateManager integration
/// - **Output Emission**: Channel for emitting processed records
/// - **Watermark Tracking**: Per-partition watermark management
/// - **State TTL**: Automatic cleanup of expired state
pub struct PartitionStateManager {
    partition_id: usize,
    metrics: Arc<PartitionMetrics>,
    // TODO Phase 2: Add query state fields
    // - window_manager: Option<WindowManager>
    // - group_by_state: Option<GroupByStateManager>
    // - output_sender: mpsc::UnboundedSender<StreamRecord>
}

impl PartitionStateManager {
    /// Create new partition state manager with integrated metrics
    pub fn new(partition_id: usize) -> Self {
        let metrics = Arc::new(PartitionMetrics::new(partition_id));
        Self {
            partition_id,
            metrics,
        }
    }

    /// Create with existing metrics (useful for testing)
    pub fn with_metrics(partition_id: usize, metrics: Arc<PartitionMetrics>) -> Self {
        Self {
            partition_id,
            metrics,
        }
    }

    /// Get partition ID
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    /// Get metrics reference
    pub fn metrics(&self) -> Arc<PartitionMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Process a single record through partition
    ///
    /// ## Phase 1 Implementation
    ///
    /// Currently tracks metrics only. Phase 2+ will add full query execution.
    ///
    /// ## Returns
    ///
    /// - `Ok(())` if record processed successfully
    /// - `Err(SqlError)` if processing failed
    pub fn process_record(&self, _record: &StreamRecord) -> Result<(), SqlError> {
        let start = Instant::now();

        // TODO Phase 2: Execute query processing
        // - Extract group key
        // - Update aggregation state
        // - Check window emission conditions
        // - Emit results to output channel

        // Track metrics
        self.metrics.record_batch_processed(1);
        self.metrics.record_latency(start.elapsed());

        Ok(())
    }

    /// Process a batch of records (optimized path)
    ///
    /// ## Phase 1 Implementation
    ///
    /// Batch processing reduces per-record overhead. Phase 2+ will add
    /// vectorized query execution.
    ///
    /// ## Returns
    ///
    /// - Number of records successfully processed
    pub fn process_batch(&self, records: &[StreamRecord]) -> Result<usize, SqlError> {
        let start = Instant::now();
        let batch_size = records.len();

        // TODO Phase 2: Vectorized batch processing
        // - Extract all group keys in batch
        // - Update aggregation states in batch
        // - Check window emission conditions
        // - Emit results in batch

        // Track metrics
        self.metrics.record_batch_processed(batch_size as u64);
        self.metrics.record_latency(start.elapsed());

        Ok(batch_size)
    }

    /// Check if partition is experiencing backpressure
    ///
    /// Uses queue depth and latency thresholds to detect overload
    pub fn has_backpressure(
        &self,
        queue_threshold: usize,
        latency_threshold: std::time::Duration,
    ) -> bool {
        self.metrics
            .has_backpressure(queue_threshold, latency_threshold)
    }

    /// Set CPU core affinity (Linux only)
    ///
    /// ## Phase 3+ Implementation
    ///
    /// Pins partition processing thread to dedicated CPU core for:
    /// - Reduced context switching
    /// - Better CPU cache locality
    /// - More predictable latency
    ///
    /// ## Platform Support
    ///
    /// - **Linux**: Uses `core_affinity` crate
    /// - **macOS/Windows**: No-op (not supported by OS)
    #[cfg(target_os = "linux")]
    pub fn set_cpu_affinity(&self, core_id: usize) -> Result<(), String> {
        // TODO Phase 3: Implement CPU affinity
        // use core_affinity::CoreId;
        // let core = CoreId { id: core_id };
        // core_affinity::set_for_current(core)
        //     .map_err(|e| format!("Failed to set CPU affinity: {}", e))
        let _ = core_id; // Suppress unused warning
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn set_cpu_affinity(&self, _core_id: usize) -> Result<(), String> {
        // CPU affinity not supported on this platform
        Ok(())
    }

    /// Get current throughput (records per second)
    pub fn throughput_per_sec(&self) -> u64 {
        self.metrics.throughput_per_sec()
    }

    /// Get total records processed
    pub fn total_records_processed(&self) -> u64 {
        self.metrics.total_records_processed()
    }

    /// Reset metrics (useful for benchmarks)
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }
}
