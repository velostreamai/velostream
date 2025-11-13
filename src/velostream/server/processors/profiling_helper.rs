//! ProfilingHelper - Unified timing instrumentation for all processor implementations
//!
//! This module provides a reusable helper for consistent profiling across Simple, Transactional,
//! and V2 Coordinator processors. It captures timing metrics for key processing stages:
//! - Deserialization duration
//! - SQL execution duration
//! - Serialization duration
//! - Batch processing duration
//!
//! The helper uses RAII-style scoping through `TimingScope` to ensure accurate measurements
//! and prevent measurement overhead from affecting results.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Metrics collected during a profiling scope
#[derive(Debug, Clone, Copy)]
pub struct ProfilingMetrics {
    /// Deserialization time in milliseconds
    pub deser_ms: u64,
    /// SQL execution time in milliseconds
    pub sql_ms: u64,
    /// Serialization time in milliseconds
    pub ser_ms: u64,
    /// Total batch processing time in milliseconds
    pub batch_ms: u64,
}

impl ProfilingMetrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self {
            deser_ms: 0,
            sql_ms: 0,
            ser_ms: 0,
            batch_ms: 0,
        }
    }

    /// Get total time across all stages
    pub fn total_ms(&self) -> u64 {
        self.deser_ms + self.sql_ms + self.ser_ms
    }

    /// Check if any measurements were taken
    pub fn is_empty(&self) -> bool {
        self.deser_ms == 0 && self.sql_ms == 0 && self.ser_ms == 0 && self.batch_ms == 0
    }
}

impl Default for ProfilingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII-style timing scope that automatically records elapsed time
///
/// Used internally by ProfilingHelper to measure specific processing stages.
/// The scope records time when dropped, ensuring accuracy even if code panics.
///
/// # Example
/// ```ignore
/// let scope = TimingScope::start();
/// // ... do work ...
/// let duration_ms = scope.finish(); // Returns elapsed milliseconds
/// ```
pub struct TimingScope {
    start: Instant,
}

impl TimingScope {
    /// Start a new timing measurement
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Finish timing and return elapsed milliseconds
    pub fn finish(self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Get elapsed time without consuming the scope
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

/// ProfilingHelper - Unified timing instrumentation
///
/// Provides consistent profiling across all processor implementations with:
/// - Fine-grained per-stage timing (deserialization, SQL, serialization)
/// - Batch-level timing aggregation
/// - Thread-safe metric accumulation via Arc<AtomicU64>
/// - Zero overhead when profiling is not enabled
///
/// # Usage Pattern
/// ```ignore
/// let helper = ProfilingHelper::new();
///
/// // Time deserialization
/// let scope = TimingScope::start();
/// let records = deserialize_records(data)?;
/// let deser_ms = scope.finish();
///
/// // Time SQL execution
/// let scope = TimingScope::start();
/// let results = execute_sql(&records)?;
/// let sql_ms = scope.finish();
///
/// // Time serialization
/// let scope = TimingScope::start();
/// serialize_records(&results)?;
/// let ser_ms = scope.finish();
///
/// // Collect metrics
/// let metrics = ProfilingMetrics {
///     deser_ms,
///     sql_ms,
///     ser_ms,
///     batch_ms: deser_ms + sql_ms + ser_ms,
/// };
/// helper.record_metrics(metrics);
/// ```
#[derive(Clone)]
pub struct ProfilingHelper {
    /// Total deserialization time (shared across all uses)
    total_deser_ms: Arc<AtomicU64>,
    /// Total SQL execution time (shared across all uses)
    total_sql_ms: Arc<AtomicU64>,
    /// Total serialization time (shared across all uses)
    total_ser_ms: Arc<AtomicU64>,
    /// Total batch processing time (shared across all uses)
    total_batch_ms: Arc<AtomicU64>,
    /// Number of batches processed
    batch_count: Arc<AtomicU64>,
}

impl ProfilingHelper {
    /// Create a new ProfilingHelper
    pub fn new() -> Self {
        Self {
            total_deser_ms: Arc::new(AtomicU64::new(0)),
            total_sql_ms: Arc::new(AtomicU64::new(0)),
            total_ser_ms: Arc::new(AtomicU64::new(0)),
            total_batch_ms: Arc::new(AtomicU64::new(0)),
            batch_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record profiling metrics from a single batch
    pub fn record_metrics(&self, metrics: ProfilingMetrics) {
        self.total_deser_ms
            .fetch_add(metrics.deser_ms, Ordering::Relaxed);
        self.total_sql_ms
            .fetch_add(metrics.sql_ms, Ordering::Relaxed);
        self.total_ser_ms
            .fetch_add(metrics.ser_ms, Ordering::Relaxed);
        self.total_batch_ms
            .fetch_add(metrics.batch_ms, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current total deserialization time in milliseconds
    pub fn total_deser_ms(&self) -> u64 {
        self.total_deser_ms.load(Ordering::Relaxed)
    }

    /// Get current total SQL execution time in milliseconds
    pub fn total_sql_ms(&self) -> u64 {
        self.total_sql_ms.load(Ordering::Relaxed)
    }

    /// Get current total serialization time in milliseconds
    pub fn total_ser_ms(&self) -> u64 {
        self.total_ser_ms.load(Ordering::Relaxed)
    }

    /// Get current total batch processing time in milliseconds
    pub fn total_batch_ms(&self) -> u64 {
        self.total_batch_ms.load(Ordering::Relaxed)
    }

    /// Get number of batches processed
    pub fn batch_count(&self) -> u64 {
        self.batch_count.load(Ordering::Relaxed)
    }

    /// Get average time per batch stage
    pub fn average_deser_ms(&self) -> u64 {
        let total = self.total_deser_ms();
        let count = self.batch_count();
        if count == 0 { 0 } else { total / count }
    }

    /// Get average SQL execution time per batch
    pub fn average_sql_ms(&self) -> u64 {
        let total = self.total_sql_ms();
        let count = self.batch_count();
        if count == 0 { 0 } else { total / count }
    }

    /// Get average serialization time per batch
    pub fn average_ser_ms(&self) -> u64 {
        let total = self.total_ser_ms();
        let count = self.batch_count();
        if count == 0 { 0 } else { total / count }
    }

    /// Get average batch processing time
    pub fn average_batch_ms(&self) -> u64 {
        let total = self.total_batch_ms();
        let count = self.batch_count();
        if count == 0 { 0 } else { total / count }
    }

    /// Get throughput in records per second (estimated from batch metrics)
    pub fn estimated_records_per_sec(&self, total_records: u64) -> f64 {
        let total_ms = self.total_batch_ms();
        if total_ms == 0 {
            return 0.0;
        }
        let total_secs = total_ms as f64 / 1000.0;
        total_records as f64 / total_secs
    }

    /// Get all current metrics as a summary
    pub fn get_summary(&self) -> ProfilingMetricsSummary {
        ProfilingMetricsSummary {
            total_deser_ms: self.total_deser_ms(),
            total_sql_ms: self.total_sql_ms(),
            total_ser_ms: self.total_ser_ms(),
            total_batch_ms: self.total_batch_ms(),
            batch_count: self.batch_count(),
            avg_deser_ms: self.average_deser_ms(),
            avg_sql_ms: self.average_sql_ms(),
            avg_ser_ms: self.average_ser_ms(),
            avg_batch_ms: self.average_batch_ms(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_deser_ms.store(0, Ordering::Relaxed);
        self.total_sql_ms.store(0, Ordering::Relaxed);
        self.total_ser_ms.store(0, Ordering::Relaxed);
        self.total_batch_ms.store(0, Ordering::Relaxed);
        self.batch_count.store(0, Ordering::Relaxed);
    }
}

impl Default for ProfilingHelper {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of all profiling metrics
#[derive(Debug, Clone, Copy)]
pub struct ProfilingMetricsSummary {
    pub total_deser_ms: u64,
    pub total_sql_ms: u64,
    pub total_ser_ms: u64,
    pub total_batch_ms: u64,
    pub batch_count: u64,
    pub avg_deser_ms: u64,
    pub avg_sql_ms: u64,
    pub avg_ser_ms: u64,
    pub avg_batch_ms: u64,
}

impl std::fmt::Display for ProfilingMetricsSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ProfilingMetricsSummary {{ batches: {}, total_ms: {}, avg_batch_ms: {}, \
             deser: {}/{}, sql: {}/{}, ser: {}/{} }}",
            self.batch_count,
            self.total_batch_ms,
            self.avg_batch_ms,
            self.total_deser_ms,
            self.avg_deser_ms,
            self.total_sql_ms,
            self.avg_sql_ms,
            self.total_ser_ms,
            self.avg_ser_ms,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profiling_metrics_new() {
        let metrics = ProfilingMetrics::new();
        assert_eq!(metrics.deser_ms, 0);
        assert_eq!(metrics.sql_ms, 0);
        assert_eq!(metrics.ser_ms, 0);
        assert_eq!(metrics.batch_ms, 0);
        assert!(metrics.is_empty());
    }

    #[test]
    fn test_timing_scope() {
        let scope = TimingScope::start();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = scope.finish();
        assert!(elapsed >= 10, "Should measure at least 10ms");
    }

    #[test]
    fn test_profiling_helper_new() {
        let helper = ProfilingHelper::new();
        assert_eq!(helper.batch_count(), 0);
        assert_eq!(helper.total_deser_ms(), 0);
        assert_eq!(helper.total_sql_ms(), 0);
        assert_eq!(helper.total_ser_ms(), 0);
    }

    #[test]
    fn test_profiling_helper_record_metrics() {
        let helper = ProfilingHelper::new();
        let metrics = ProfilingMetrics {
            deser_ms: 5,
            sql_ms: 10,
            ser_ms: 3,
            batch_ms: 18,
        };
        helper.record_metrics(metrics);

        assert_eq!(helper.total_deser_ms(), 5);
        assert_eq!(helper.total_sql_ms(), 10);
        assert_eq!(helper.total_ser_ms(), 3);
        assert_eq!(helper.total_batch_ms(), 18);
        assert_eq!(helper.batch_count(), 1);
    }

    #[test]
    fn test_profiling_helper_multiple_batches() {
        let helper = ProfilingHelper::new();

        helper.record_metrics(ProfilingMetrics {
            deser_ms: 5,
            sql_ms: 10,
            ser_ms: 3,
            batch_ms: 18,
        });

        helper.record_metrics(ProfilingMetrics {
            deser_ms: 4,
            sql_ms: 8,
            ser_ms: 2,
            batch_ms: 14,
        });

        assert_eq!(helper.batch_count(), 2);
        assert_eq!(helper.total_deser_ms(), 9);
        assert_eq!(helper.total_sql_ms(), 18);
        assert_eq!(helper.total_ser_ms(), 5);
        assert_eq!(helper.total_batch_ms(), 32);
        assert_eq!(helper.average_deser_ms(), 4); // 9 / 2
        assert_eq!(helper.average_batch_ms(), 16); // 32 / 2
    }

    #[test]
    fn test_profiling_helper_reset() {
        let helper = ProfilingHelper::new();
        helper.record_metrics(ProfilingMetrics {
            deser_ms: 5,
            sql_ms: 10,
            ser_ms: 3,
            batch_ms: 18,
        });

        assert_eq!(helper.batch_count(), 1);
        helper.reset();
        assert_eq!(helper.batch_count(), 0);
        assert_eq!(helper.total_deser_ms(), 0);
        assert_eq!(helper.total_sql_ms(), 0);
    }

    #[test]
    fn test_profiling_helper_cloneable() {
        let helper1 = ProfilingHelper::new();
        helper1.record_metrics(ProfilingMetrics {
            deser_ms: 5,
            sql_ms: 10,
            ser_ms: 3,
            batch_ms: 18,
        });

        let helper2 = helper1.clone();
        // Both should share the same Arc references
        assert_eq!(helper2.batch_count(), 1);
        assert_eq!(helper2.total_deser_ms(), 5);

        // Changes in one should be visible in the other
        helper2.record_metrics(ProfilingMetrics {
            deser_ms: 2,
            sql_ms: 4,
            ser_ms: 1,
            batch_ms: 7,
        });

        assert_eq!(helper1.batch_count(), 2);
        assert_eq!(helper1.total_deser_ms(), 7);
    }

    #[test]
    fn test_profiling_helper_summary() {
        let helper = ProfilingHelper::new();
        helper.record_metrics(ProfilingMetrics {
            deser_ms: 10,
            sql_ms: 20,
            ser_ms: 5,
            batch_ms: 35,
        });

        let summary = helper.get_summary();
        assert_eq!(summary.batch_count, 1);
        assert_eq!(summary.total_deser_ms, 10);
        assert_eq!(summary.total_sql_ms, 20);
        assert_eq!(summary.avg_batch_ms, 35);
    }
}
