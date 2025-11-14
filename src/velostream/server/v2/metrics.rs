//! Per-partition metrics for monitoring and backpressure detection
//!
//! Tracks throughput, queue depth, and latency for each partition to enable:
//! - Real-time performance monitoring
//! - Backpressure signal generation
//! - Hot partition detection
//! - Adaptive scaling decisions

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Per-partition performance metrics (thread-safe)
///
/// ## Phase 1 Implementation
///
/// Tracks core metrics for monitoring and backpressure:
/// - **Throughput**: Records per second
/// - **Queue Depth**: Pending records in partition queue
/// - **Latency**: p99 processing time per record
///
/// ## Usage
///
/// ```rust
/// use velostream::velostream::server::v2::PartitionMetrics;
/// use std::time::Duration;
///
/// let metrics = PartitionMetrics::new(0);
///
/// // Update metrics during processing
/// metrics.record_batch_processed(1000);
/// metrics.update_queue_depth(500);
/// metrics.record_latency(Duration::from_micros(50));
///
/// // Query current state
/// let throughput = metrics.throughput_per_sec();
/// let depth = metrics.queue_depth();
/// let backpressure = metrics.has_backpressure(1000, Duration::from_millis(100));
/// ```
///
/// ## Future Enhancements (Phase 3+)
///
/// - CPU utilization per partition
/// - Memory usage tracking
/// - Network bandwidth consumption
/// - State size growth rate
#[derive(Debug)]
pub struct PartitionMetrics {
    partition_id: usize,

    // Throughput tracking
    records_processed: AtomicU64,
    last_snapshot_time: std::sync::Mutex<Instant>,
    last_snapshot_count: AtomicU64,

    // Queue backpressure detection
    queue_depth: AtomicUsize,

    // Latency tracking (microseconds)
    total_latency_micros: AtomicU64,
    latency_sample_count: AtomicU64,
}

impl PartitionMetrics {
    /// Create new metrics tracker for partition
    pub fn new(partition_id: usize) -> Self {
        Self {
            partition_id,
            records_processed: AtomicU64::new(0),
            last_snapshot_time: std::sync::Mutex::new(Instant::now()),
            last_snapshot_count: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            total_latency_micros: AtomicU64::new(0),
            latency_sample_count: AtomicU64::new(0),
        }
    }

    /// Get partition ID
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    /// Record batch of records processed
    pub fn record_batch_processed(&self, count: u64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);
    }

    /// Update current queue depth
    pub fn update_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Record processing latency for a record
    pub fn record_latency(&self, latency: Duration) {
        let micros = latency.as_micros() as u64;
        self.total_latency_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.latency_sample_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current throughput (records per second)
    ///
    /// Calculates throughput since last snapshot
    pub fn throughput_per_sec(&self) -> u64 {
        let mut last_time = self.last_snapshot_time.lock().unwrap();
        let elapsed = last_time.elapsed();

        if elapsed.as_secs() == 0 {
            return 0; // Avoid division by zero
        }

        let current_count = self.records_processed.load(Ordering::Relaxed);
        let last_count = self.last_snapshot_count.load(Ordering::Relaxed);
        let delta = current_count.saturating_sub(last_count);

        // Update snapshot
        *last_time = Instant::now();
        self.last_snapshot_count
            .store(current_count, Ordering::Relaxed);

        delta / elapsed.as_secs()
    }

    /// Get current queue depth
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Get total records processed since creation
    pub fn total_records_processed(&self) -> u64 {
        self.records_processed.load(Ordering::Relaxed)
    }

    /// Get average processing latency (microseconds)
    pub fn avg_latency_micros(&self) -> u64 {
        let total = self.total_latency_micros.load(Ordering::Relaxed);
        let count = self.latency_sample_count.load(Ordering::Relaxed);

        if count == 0 {
            return 0;
        }

        total / count
    }

    /// Check if partition is experiencing backpressure
    ///
    /// Returns true if queue depth exceeds threshold OR latency exceeds threshold
    pub fn has_backpressure(&self, queue_threshold: usize, latency_threshold: Duration) -> bool {
        let depth = self.queue_depth();
        let avg_latency = Duration::from_micros(self.avg_latency_micros());

        depth > queue_threshold || avg_latency > latency_threshold
    }

    /// Get metrics snapshot for logging/monitoring
    pub fn snapshot(&self) -> PartitionMetricsSnapshot {
        PartitionMetricsSnapshot {
            partition_id: self.partition_id,
            records_processed: self.total_records_processed(),
            throughput_per_sec: self.throughput_per_sec(),
            queue_depth: self.queue_depth(),
            avg_latency_micros: self.avg_latency_micros(),
        }
    }

    /// Reset metrics (useful for benchmarks and tests)
    pub fn reset(&self) {
        self.records_processed.store(0, Ordering::Relaxed);
        *self.last_snapshot_time.lock().unwrap() = Instant::now();
        self.last_snapshot_count.store(0, Ordering::Relaxed);
        self.queue_depth.store(0, Ordering::Relaxed);
        self.total_latency_micros.store(0, Ordering::Relaxed);
        self.latency_sample_count.store(0, Ordering::Relaxed);
    }
}

/// Immutable snapshot of partition metrics
#[derive(Debug, Clone)]
pub struct PartitionMetricsSnapshot {
    pub partition_id: usize,
    pub records_processed: u64,
    pub throughput_per_sec: u64,
    pub queue_depth: usize,
    pub avg_latency_micros: u64,
}

impl PartitionMetricsSnapshot {
    /// Format metrics for human-readable logging
    pub fn format_summary(&self) -> String {
        format!(
            "Partition {}: {} rec/sec, {} records, queue depth: {}, avg latency: {}μs",
            self.partition_id,
            self.throughput_per_sec,
            self.records_processed,
            self.queue_depth,
            self.avg_latency_micros
        )
    }
}

/// Backpressure state classification based on metrics
///
/// ## Phase 3 Implementation
///
/// Classifies partition health based on channel utilization:
/// - **Healthy**: <70% utilization, normal operation
/// - **Warning**: 70-85% utilization, approaching capacity
/// - **Critical**: 85-95% utilization, needs throttling
/// - **Saturated**: >95% utilization, requires immediate action
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackpressureState {
    /// Normal operation (<70% channel utilization)
    Healthy,

    /// Approaching capacity (70-85% utilization)
    Warning { severity: f64, partition: usize },

    /// Critical load (85-95% utilization)
    Critical { severity: f64, partition: usize },

    /// Saturated (>95% utilization)
    Saturated { partition: usize },
}

impl BackpressureState {
    /// Check if backpressure requires action
    pub fn requires_throttling(&self) -> bool {
        matches!(
            self,
            BackpressureState::Critical { .. } | BackpressureState::Saturated { .. }
        )
    }

    /// Get severity level (0.0 - 1.0)
    pub fn severity(&self) -> f64 {
        match self {
            BackpressureState::Healthy => 0.0,
            BackpressureState::Warning { severity, .. } => *severity,
            BackpressureState::Critical { severity, .. } => *severity,
            BackpressureState::Saturated { .. } => 1.0,
        }
    }

    /// Format for logging
    pub fn format_state(&self) -> String {
        match self {
            BackpressureState::Healthy => "HEALTHY".to_string(),
            BackpressureState::Warning {
                severity,
                partition,
            } => {
                format!(
                    "WARNING (partition {}, {}% util)",
                    partition,
                    (severity * 100.0) as u32
                )
            }
            BackpressureState::Critical {
                severity,
                partition,
            } => {
                format!(
                    "CRITICAL (partition {}, {}% util)",
                    partition,
                    (severity * 100.0) as u32
                )
            }
            BackpressureState::Saturated { partition } => {
                format!("SATURATED (partition {})", partition)
            }
        }
    }
}

impl PartitionMetrics {
    /// Calculate channel utilization (0.0 - 1.0)
    ///
    /// ## Phase 3 Implementation
    ///
    /// Channel utilization is the primary backpressure indicator:
    /// - Uses queue_depth vs configured buffer size
    /// - Returns fraction: queue_depth / buffer_size
    ///
    /// ## Usage
    ///
    /// ```rust
    /// use velostream::velostream::server::v2::PartitionMetrics;
    ///
    /// let metrics = PartitionMetrics::new(0);
    /// metrics.update_queue_depth(700);
    ///
    /// let utilization = metrics.channel_utilization(1000);
    /// assert_eq!(utilization, 0.7); // 70% utilization
    /// ```
    pub fn channel_utilization(&self, buffer_size: usize) -> f64 {
        if buffer_size == 0 {
            return 0.0;
        }

        let depth = self.queue_depth() as f64;
        let capacity = buffer_size as f64;

        (depth / capacity).min(1.0) // Cap at 1.0
    }

    /// Get backpressure state based on channel utilization
    ///
    /// ## Phase 3 Implementation
    ///
    /// Classifies partition health into 4 states:
    /// - **Healthy**: <70% utilization
    /// - **Warning**: 70-85% utilization
    /// - **Critical**: 85-95% utilization
    /// - **Saturated**: >95% utilization
    ///
    /// ## Usage
    ///
    /// ```rust
    /// use velostream::velostream::server::v2::{PartitionMetrics, BackpressureState};
    ///
    /// let metrics = PartitionMetrics::new(0);
    /// metrics.update_queue_depth(850);
    ///
    /// let state = metrics.backpressure_state(1000);
    /// assert!(matches!(state, BackpressureState::Critical { .. }));
    /// ```
    pub fn backpressure_state(&self, buffer_size: usize) -> BackpressureState {
        let utilization = self.channel_utilization(buffer_size);

        match utilization {
            u if u < 0.7 => BackpressureState::Healthy,
            u if u < 0.85 => BackpressureState::Warning {
                severity: u,
                partition: self.partition_id,
            },
            u if u < 0.95 => BackpressureState::Critical {
                severity: u,
                partition: self.partition_id,
            },
            _ => BackpressureState::Saturated {
                partition: self.partition_id,
            },
        }
    }
}
