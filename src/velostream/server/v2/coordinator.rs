//! Partitioned Job Coordinator for multi-partition orchestration
//!
//! Coordinates execution across N partitions for linear scaling performance.

use crate::velostream::server::v2::{
    HashRouter, PartitionMetrics, PartitionStateManager, PartitionStrategy,
};
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Configuration for partitioned job execution
#[derive(Debug, Clone)]
pub struct PartitionedJobConfig {
    /// Number of partitions (defaults to CPU count if None)
    pub num_partitions: Option<usize>,

    /// Processing mode: Individual or Batch
    pub processing_mode: ProcessingMode,

    /// Channel buffer size per partition (default: 1000)
    pub partition_buffer_size: usize,

    /// Enable CPU core affinity pinning (Linux only, Phase 3+)
    pub enable_core_affinity: bool,

    /// Backpressure configuration
    pub backpressure_config: BackpressureConfig,
}

impl Default for PartitionedJobConfig {
    fn default() -> Self {
        Self {
            num_partitions: None, // Will default to num_cpus::get()
            processing_mode: ProcessingMode::Individual,
            partition_buffer_size: 1000,
            enable_core_affinity: false,
            backpressure_config: BackpressureConfig::default(),
        }
    }
}

/// Processing mode for records
#[derive(Debug, Clone, Copy)]
pub enum ProcessingMode {
    /// Process records individually (ultra-low-latency: p95 <1ms)
    Individual,

    /// Process records in batches (higher throughput)
    Batch { size: usize },
}

/// Backpressure detection and handling configuration
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Queue depth threshold for backpressure detection
    pub queue_threshold: usize,

    /// Latency threshold for backpressure detection
    pub latency_threshold: Duration,

    /// Enable automatic backpressure handling
    pub enabled: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            queue_threshold: 1000,
            latency_threshold: Duration::from_millis(100),
            enabled: true,
        }
    }
}

/// Coordinates multi-partition job execution
///
/// ## Phase 2 Implementation
///
/// Orchestrates N partitions running in parallel with:
/// - Hash-based record routing to partitions
/// - Independent partition execution (no cross-partition locks)
/// - Per-partition metrics and monitoring
/// - Backpressure detection (Phase 3)
///
/// ## Usage
///
/// ```rust,no_run
/// use velostream::velostream::server::v2::{PartitionedJobCoordinator, PartitionedJobConfig};
///
/// let config = PartitionedJobConfig::default();
/// let coordinator = PartitionedJobCoordinator::new(config);
///
/// // Phase 2: Basic partition orchestration
/// // Phase 3+: Full SQL execution integration
/// ```
///
/// ## Architecture
///
/// ```text
///                 ┌─────────────────┐
/// Records ───► Router (Hash-Based) │
///                 └─────────────────┘
///                         │
///           ┌─────────────┼─────────────┐
///           ▼             ▼             ▼
///     Partition 0    Partition 1   Partition N
///     [200K r/s]     [200K r/s]    [200K r/s]
///           │             │             │
///           └─────────────┼─────────────┘
///                         ▼
///                   Output Merger
///                 (N × 200K rec/sec)
/// ```
pub struct PartitionedJobCoordinator {
    config: PartitionedJobConfig,
    num_partitions: usize,
}

impl PartitionedJobCoordinator {
    /// Create new partitioned job coordinator with configuration
    pub fn new(config: PartitionedJobConfig) -> Self {
        let num_partitions = config
            .num_partitions
            .unwrap_or_else(|| num_cpus::get().max(1));

        Self {
            config,
            num_partitions,
        }
    }

    /// Get number of partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Get configuration
    pub fn config(&self) -> &PartitionedJobConfig {
        &self.config
    }

    /// Initialize partitions with managers and channels
    ///
    /// Returns partition managers and input channel senders
    fn initialize_partitions(
        &self,
    ) -> (
        Vec<Arc<PartitionStateManager>>,
        Vec<mpsc::Sender<StreamRecord>>,
    ) {
        let mut managers = Vec::with_capacity(self.num_partitions);
        let mut senders = Vec::with_capacity(self.num_partitions);

        for partition_id in 0..self.num_partitions {
            let manager = Arc::new(PartitionStateManager::new(partition_id));
            let (tx, _rx) = mpsc::channel(self.config.partition_buffer_size);

            managers.push(manager);
            senders.push(tx);
        }

        (managers, senders)
    }

    /// Route and process a batch of records across partitions
    ///
    /// ## Phase 2 Implementation
    ///
    /// Routes each record to its target partition and processes them in parallel.
    /// Phase 3+ will add full SQL execution integration.
    pub async fn process_batch(
        &self,
        records: Vec<StreamRecord>,
        router: &HashRouter,
        partition_senders: &[mpsc::Sender<StreamRecord>],
    ) -> Result<usize, SqlError> {
        let mut processed = 0;

        for record in records {
            let partition_id = router.route_record(&record)?;
            let sender = &partition_senders[partition_id];

            // Send to partition (non-blocking in Phase 2)
            if sender.send(record).await.is_ok() {
                processed += 1;
            }
        }

        Ok(processed)
    }

    /// Monitor partition metrics and detect backpressure
    ///
    /// ## Phase 3 Implementation
    ///
    /// Currently placeholder for Phase 2. Will be implemented in Phase 3 with:
    /// - Real-time throughput monitoring
    /// - Automatic throttling on backpressure
    /// - Hot partition detection
    pub fn check_backpressure(&self, _partition_metrics: &[Arc<PartitionMetrics>]) -> bool {
        // Phase 2: No backpressure handling yet
        // Phase 3: Implement real backpressure detection
        false
    }

    /// Collect aggregated metrics from all partitions
    pub fn collect_metrics(
        &self,
        partition_managers: &[Arc<PartitionStateManager>],
    ) -> CoordinatorMetrics {
        let mut total_records = 0;
        let mut total_throughput = 0;
        let mut max_queue_depth = 0;
        let mut max_latency_micros = 0;

        for manager in partition_managers {
            let metrics = manager.metrics();
            total_records += metrics.total_records_processed();
            total_throughput += metrics.throughput_per_sec();

            let snapshot = metrics.snapshot();
            max_queue_depth = max_queue_depth.max(snapshot.queue_depth);
            max_latency_micros = max_latency_micros.max(snapshot.avg_latency_micros);
        }

        CoordinatorMetrics {
            total_records_processed: total_records,
            aggregate_throughput: total_throughput,
            num_partitions: self.num_partitions,
            max_queue_depth,
            max_latency_micros,
        }
    }
}

/// Aggregated metrics across all partitions
#[derive(Debug, Clone)]
pub struct CoordinatorMetrics {
    pub total_records_processed: u64,
    pub aggregate_throughput: u64,
    pub num_partitions: usize,
    pub max_queue_depth: usize,
    pub max_latency_micros: u64,
}

impl CoordinatorMetrics {
    /// Format metrics for logging
    pub fn format_summary(&self) -> String {
        format!(
            "Coordinator: {} partitions, {} records, {} rec/sec aggregate, max queue: {}, max latency: {}μs",
            self.num_partitions,
            self.total_records_processed,
            self.aggregate_throughput,
            self.max_queue_depth,
            self.max_latency_micros
        )
    }
}
