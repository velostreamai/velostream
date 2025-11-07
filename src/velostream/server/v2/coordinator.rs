//! Partitioned Job Coordinator for multi-partition orchestration
//!
//! Coordinates execution across N partitions for linear scaling performance.

use crate::velostream::server::v2::{
    AlwaysHashStrategy, HashRouter, PartitionMetrics, PartitionStateManager, PartitionStrategy,
    PartitioningStrategy, QueryMetadata, RoutingContext,
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

    /// Throttling configuration for adaptive backpressure response
    pub throttle_config: ThrottleConfig,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            queue_threshold: 1000,
            latency_threshold: Duration::from_millis(100),
            enabled: true,
            throttle_config: ThrottleConfig::default(),
        }
    }
}

/// Throttle configuration for adaptive backpressure response
///
/// ## Phase 3 Implementation
///
/// Controls how aggressively the system throttles when backpressure is detected:
/// - **Warning**: Light throttling (min_delay)
/// - **Critical**: Moderate throttling (min_delay * 2)
/// - **Saturated**: Aggressive throttling (max_delay)
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    /// Minimum throttle delay for Warning state
    pub min_delay: Duration,

    /// Maximum throttle delay for Saturated state
    pub max_delay: Duration,

    /// Exponential backoff multiplier for Critical state
    pub backoff_multiplier: f64,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_micros(100), // 0.1ms for Warning
            max_delay: Duration::from_millis(10),  // 10ms for Saturated
            backoff_multiplier: 2.0,               // 2x for Critical
        }
    }
}

/// Coordinates multi-partition job execution with pluggable routing strategies
///
/// ## Phase 2+ Implementation
///
/// Orchestrates N partitions running in parallel with:
/// - Pluggable partitioning strategies (AlwaysHash, SmartRepartition, RoundRobin)
/// - Independent partition execution (no cross-partition locks)
/// - Per-partition metrics and monitoring
/// - Backpressure detection (Phase 3)
/// - State consistency guarantees (same GROUP BY key → same partition)
///
/// ## Usage
///
/// ```rust,no_run
/// use velostream::velostream::server::v2::{PartitionedJobCoordinator, PartitionedJobConfig, AlwaysHashStrategy};
///
/// let config = PartitionedJobConfig::default();
/// let coordinator = PartitionedJobCoordinator::new(config)
///     .with_group_by_columns(vec!["trader_id".to_string()])
///     .with_strategy(std::sync::Arc::new(AlwaysHashStrategy::new()));
///
/// // Phase 2: Basic partition orchestration with strategies
/// // Phase 3+: Full SQL execution integration
/// ```
///
/// ## Architecture
///
/// ```text
///                 ┌──────────────────────────┐
/// Records ───► Router (Strategy-Based)      │
///                 └──────────────────────────┘
///                         │
///           ┌─────────────┼─────────────┐
///           ▼             ▼             ▼
///     Partition 0    Partition 1   Partition N
///     [State Mgr]    [State Mgr]    [State Mgr]
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
    /// Pluggable routing strategy for record distribution
    strategy: Arc<dyn PartitioningStrategy>,
    /// GROUP BY columns for state consistency
    group_by_columns: Vec<String>,
    /// Number of available CPU slots
    num_cpu_slots: usize,
}

impl PartitionedJobCoordinator {
    /// Create new partitioned job coordinator with configuration
    pub fn new(config: PartitionedJobConfig) -> Self {
        let num_partitions = config
            .num_partitions
            .unwrap_or_else(|| num_cpus::get().max(1));

        let num_cpu_slots = num_cpus::get().max(1);

        Self {
            config,
            num_partitions,
            // Default to AlwaysHashStrategy for safety
            strategy: Arc::new(AlwaysHashStrategy::new()),
            group_by_columns: Vec::new(),
            num_cpu_slots,
        }
    }

    /// Configure GROUP BY columns for routing decisions
    ///
    /// Required for strategies that depend on GROUP BY key routing.
    /// Enables state consistency: records with same GROUP BY key route to same partition.
    pub fn with_group_by_columns(mut self, columns: Vec<String>) -> Self {
        self.group_by_columns = columns;
        self
    }

    /// Set custom partitioning strategy
    ///
    /// Allows switching between strategies (AlwaysHash, SmartRepartition, RoundRobin)
    /// based on workload characteristics.
    pub fn with_strategy(mut self, strategy: Arc<dyn PartitioningStrategy>) -> Self {
        self.strategy = strategy;
        self
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
    ///
    /// ## Phase 3 Implementation
    ///
    /// Made public for testing purposes
    pub fn initialize_partitions(
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

    /// Route and process a batch of records across partitions using pluggable strategy
    ///
    /// ## Phase 2+ Implementation (Strategy-Based)
    ///
    /// Routes each record to its target partition using the configured partitioning strategy.
    /// This enables state consistency: records with same GROUP BY key always go to same partition.
    ///
    /// ## Strategy Validation
    ///
    /// Validates that the configured strategy is compatible with the query metadata
    /// (GROUP BY columns, window configuration, etc.) before routing any records.
    pub async fn process_batch_with_strategy(
        &self,
        records: Vec<StreamRecord>,
        partition_senders: &[mpsc::Sender<StreamRecord>],
    ) -> Result<usize, SqlError> {
        // Validate strategy compatibility with query
        let query_metadata = QueryMetadata {
            group_by_columns: self.group_by_columns.clone(),
            has_window: false,
            num_partitions: self.num_partitions,
            num_cpu_slots: self.num_cpu_slots,
        };

        self.strategy
            .validate(&query_metadata)
            .map_err(|err| SqlError::ExecutionError {
                message: format!("Strategy validation failed: {}", err),
                query: None,
            })?;

        let mut processed = 0;

        for record in records {
            // Create routing context for this record
            let routing_context = RoutingContext {
                source_partition: None, // Will be enhanced in Phase 2b with source metadata
                source_partition_key: None,
                group_by_columns: self.group_by_columns.clone(),
                num_partitions: self.num_partitions,
                num_cpu_slots: self.num_cpu_slots,
            };

            // Route record using strategy
            let partition_id = self
                .strategy
                .route_record(&record, &routing_context)
                .await?;
            let sender = &partition_senders[partition_id];

            // Send to partition (non-blocking)
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
    /// Real-time backpressure detection:
    /// - Checks all partition channel utilization
    /// - Classifies each partition: Healthy/Warning/Critical/Saturated
    /// - Returns true if ANY partition requires throttling
    /// - Logs backpressure events for monitoring
    ///
    /// ## Usage
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use velostream::velostream::server::v2::{PartitionedJobCoordinator, PartitionedJobConfig, PartitionMetrics};
    ///
    /// let config = PartitionedJobConfig::default();
    /// let coordinator = PartitionedJobCoordinator::new(config);
    ///
    /// let metrics: Vec<Arc<PartitionMetrics>> = vec![
    ///     Arc::new(PartitionMetrics::new(0)),
    ///     Arc::new(PartitionMetrics::new(1)),
    /// ];
    ///
    /// if coordinator.check_backpressure(&metrics) {
    ///     println!("Backpressure detected - throttling required");
    /// }
    /// ```
    pub fn check_backpressure(&self, partition_metrics: &[Arc<PartitionMetrics>]) -> bool {
        use crate::velostream::server::v2::BackpressureState;

        let buffer_size = self.config.partition_buffer_size;
        let mut has_backpressure = false;

        for metrics in partition_metrics {
            let state = metrics.backpressure_state(buffer_size);

            match state {
                BackpressureState::Healthy => {
                    // Normal operation - no action needed
                }
                BackpressureState::Warning {
                    severity,
                    partition,
                } => {
                    log::warn!(
                        "Partition {} experiencing backpressure ({}% utilization)",
                        partition,
                        (severity * 100.0) as u32
                    );
                }
                BackpressureState::Critical {
                    severity,
                    partition,
                } => {
                    log::error!(
                        "Partition {} CRITICAL backpressure ({}% utilization) - throttling required",
                        partition,
                        (severity * 100.0) as u32
                    );
                    has_backpressure = true;
                }
                BackpressureState::Saturated { partition } => {
                    log::error!(
                        "Partition {} SATURATED (>95% utilization) - immediate throttling required",
                        partition
                    );
                    has_backpressure = true;
                }
            }
        }

        has_backpressure
    }

    /// Detect hot partitions (load imbalance)
    ///
    /// ## Phase 3 Implementation
    ///
    /// Identifies partitions processing significantly more records than average:
    /// - Calculates average throughput across all partitions
    /// - Flags partitions exceeding threshold (e.g., 2x average)
    /// - Useful for detecting skewed GROUP BY keys
    ///
    /// ## Returns
    ///
    /// Vec of (partition_id, throughput) for hot partitions
    pub fn detect_hot_partitions(
        &self,
        partition_metrics: &[Arc<PartitionMetrics>],
        threshold_multiplier: f64,
    ) -> Vec<(usize, u64)> {
        if partition_metrics.is_empty() {
            return Vec::new();
        }

        // Calculate average throughput
        let total_throughput: u64 = partition_metrics
            .iter()
            .map(|m| m.throughput_per_sec())
            .sum();

        let avg_throughput = total_throughput / partition_metrics.len() as u64;

        if avg_throughput == 0 {
            return Vec::new(); // No throughput yet
        }

        // Identify hot partitions
        let threshold = (avg_throughput as f64 * threshold_multiplier) as u64;

        partition_metrics
            .iter()
            .filter_map(|metrics| {
                let throughput = metrics.throughput_per_sec();
                if throughput > threshold {
                    Some((metrics.partition_id(), throughput))
                } else {
                    None
                }
            })
            .collect()
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

    /// Calculate appropriate throttle delay based on backpressure severity
    ///
    /// ## Phase 3 Implementation
    ///
    /// Adaptive throttling strategy:
    /// - **Healthy**: No delay (0ms)
    /// - **Warning**: Light throttling (min_delay, typically 0.1ms)
    /// - **Critical**: Moderate throttling (min_delay * backoff_multiplier, typically 0.2ms)
    /// - **Saturated**: Aggressive throttling (max_delay, typically 10ms)
    ///
    /// ## Usage
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use velostream::velostream::server::v2::{PartitionedJobCoordinator, PartitionedJobConfig, PartitionMetrics};
    ///
    /// let config = PartitionedJobConfig::default();
    /// let coordinator = PartitionedJobCoordinator::new(config);
    ///
    /// let metrics: Vec<Arc<PartitionMetrics>> = vec![
    ///     Arc::new(PartitionMetrics::new(0)),
    /// ];
    ///
    /// let delay = coordinator.calculate_throttle_delay(&metrics);
    /// // Returns Duration based on worst partition state
    /// ```
    pub fn calculate_throttle_delay(
        &self,
        partition_metrics: &[Arc<PartitionMetrics>],
    ) -> Duration {
        use crate::velostream::server::v2::BackpressureState;

        if !self.config.backpressure_config.enabled {
            return Duration::from_secs(0);
        }

        let buffer_size = self.config.partition_buffer_size;
        let throttle_config = &self.config.backpressure_config.throttle_config;

        // Find worst backpressure state across all partitions
        let mut max_severity = 0.0;
        let mut worst_state = BackpressureState::Healthy;

        for metrics in partition_metrics {
            let state = metrics.backpressure_state(buffer_size);
            let severity = state.severity();

            if severity > max_severity {
                max_severity = severity;
                worst_state = state;
            }
        }

        // Calculate delay based on worst state
        match worst_state {
            BackpressureState::Healthy => Duration::from_secs(0),
            BackpressureState::Warning { .. } => throttle_config.min_delay,
            BackpressureState::Critical { .. } => {
                // Exponential backoff for critical state
                let delay_micros = throttle_config.min_delay.as_micros() as f64
                    * throttle_config.backoff_multiplier;
                Duration::from_micros(delay_micros as u64)
            }
            BackpressureState::Saturated { .. } => throttle_config.max_delay,
        }
    }

    /// Process batch with strategy-based routing and automatic throttling
    ///
    /// ## Phase 3 Implementation (Strategy-Enhanced)
    ///
    /// Enhanced version of process_batch_with_strategy() that applies adaptive throttling:
    /// - Uses pluggable partitioning strategy for routing
    /// - Monitors partition metrics continuously
    /// - Calculates appropriate throttle delay
    /// - Applies delay between record sends when backpressure detected
    /// - Logs throttling events for observability
    ///
    /// ## Usage
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use velostream::velostream::server::v2::{PartitionedJobCoordinator, PartitionedJobConfig, AlwaysHashStrategy};
    /// use velostream::velostream::sql::execution::types::StreamRecord;
    /// use std::collections::HashMap;
    ///
    /// # async fn example() {
    /// let config = PartitionedJobConfig::default();
    /// let coordinator = PartitionedJobCoordinator::new(config)
    ///     .with_group_by_columns(vec!["key".to_string()])
    ///     .with_strategy(Arc::new(AlwaysHashStrategy::new()));
    ///
    /// let (managers, senders) = coordinator.initialize_partitions();
    ///
    /// let records = vec![StreamRecord::new(HashMap::new())];
    /// let partition_metrics: Vec<Arc<_>> = managers.iter().map(|m| m.metrics()).collect();
    ///
    /// let processed = coordinator.process_batch_with_strategy_and_throttling(
    ///     records,
    ///     &senders,
    ///     &partition_metrics,
    /// ).await;
    /// # }
    /// ```
    pub async fn process_batch_with_strategy_and_throttling(
        &self,
        records: Vec<StreamRecord>,
        partition_senders: &[mpsc::Sender<StreamRecord>],
        partition_metrics: &[Arc<PartitionMetrics>],
    ) -> Result<usize, SqlError> {
        // Validate strategy compatibility with query
        let query_metadata = QueryMetadata {
            group_by_columns: self.group_by_columns.clone(),
            has_window: false,
            num_partitions: self.num_partitions,
            num_cpu_slots: self.num_cpu_slots,
        };

        self.strategy
            .validate(&query_metadata)
            .map_err(|err| SqlError::ExecutionError {
                message: format!("Strategy validation failed: {}", err),
                query: None,
            })?;

        let mut processed = 0;
        let mut last_throttle_log = std::time::Instant::now();

        for record in records {
            // Create routing context for this record
            let routing_context = RoutingContext {
                source_partition: None,
                source_partition_key: None,
                group_by_columns: self.group_by_columns.clone(),
                num_partitions: self.num_partitions,
                num_cpu_slots: self.num_cpu_slots,
            };

            // Route record using strategy
            let partition_id = self
                .strategy
                .route_record(&record, &routing_context)
                .await?;
            let sender = &partition_senders[partition_id];

            // Calculate throttle delay based on current backpressure
            let throttle_delay = self.calculate_throttle_delay(partition_metrics);

            // Apply throttling if needed
            if throttle_delay > Duration::from_secs(0) {
                // Log throttling events (rate-limited to once per second)
                if last_throttle_log.elapsed() >= Duration::from_secs(1) {
                    log::warn!(
                        "Applying throttle delay: {:?} due to backpressure",
                        throttle_delay
                    );
                    last_throttle_log = std::time::Instant::now();
                }

                tokio::time::sleep(throttle_delay).await;
            }

            // Send to partition
            if sender.send(record).await.is_ok() {
                processed += 1;
            }
        }

        Ok(processed)
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
