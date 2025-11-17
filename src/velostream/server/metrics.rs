//! StreamJobServer metrics and monitoring
//!
//! Metrics collection and monitoring utilities for the StreamJobServer

use crate::velostream::server::processors::DeadLetterQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Dead Letter Queue metrics for monitoring DLQ health
#[derive(Debug, Clone)]
pub struct DLQMetrics {
    /// Total number of entries added to DLQ
    entries_added: Arc<AtomicUsize>,
    /// Total number of entries rejected due to capacity
    entries_rejected: Arc<AtomicUsize>,
    /// Timestamp of last entry added
    last_entry_time: Arc<std::sync::Mutex<Option<Instant>>>,
    /// Timestamp of last capacity limit reached
    last_capacity_exceeded_time: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl DLQMetrics {
    /// Create new DLQ metrics tracker
    pub fn new() -> Self {
        Self {
            entries_added: Arc::new(AtomicUsize::new(0)),
            entries_rejected: Arc::new(AtomicUsize::new(0)),
            last_entry_time: Arc::new(std::sync::Mutex::new(None)),
            last_capacity_exceeded_time: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Record an entry being added to DLQ
    pub fn record_entry_added(&self) {
        self.entries_added.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut last_time) = self.last_entry_time.lock() {
            *last_time = Some(Instant::now());
        }
    }

    /// Record an entry being rejected due to capacity
    pub fn record_entry_rejected(&self) {
        self.entries_rejected.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut last_time) = self.last_capacity_exceeded_time.lock() {
            *last_time = Some(Instant::now());
        }
    }

    /// Get total entries added
    pub fn entries_added(&self) -> usize {
        self.entries_added.load(Ordering::SeqCst)
    }

    /// Get total entries rejected
    pub fn entries_rejected(&self) -> usize {
        self.entries_rejected.load(Ordering::SeqCst)
    }

    /// Get last entry addition time
    pub fn last_entry_time(&self) -> Option<Instant> {
        self.last_entry_time.lock().ok().and_then(|t| *t)
    }

    /// Get last capacity exceeded time
    pub fn last_capacity_exceeded_time(&self) -> Option<Instant> {
        self.last_capacity_exceeded_time
            .lock()
            .ok()
            .and_then(|t| *t)
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.entries_added.store(0, Ordering::SeqCst);
        self.entries_rejected.store(0, Ordering::SeqCst);
        if let Ok(mut last_time) = self.last_entry_time.lock() {
            *last_time = None;
        }
        if let Ok(mut last_time) = self.last_capacity_exceeded_time.lock() {
            *last_time = None;
        }
    }
}

impl Default for DLQMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Comprehensive metrics for job processing
#[derive(Debug, Clone)]
pub struct JobMetrics {
    /// DLQ-specific metrics
    pub dlq_metrics: DLQMetrics,
    /// Total records processed
    records_processed: Arc<AtomicUsize>,
    /// Total records failed
    records_failed: Arc<AtomicUsize>,
}

impl JobMetrics {
    /// Create new job metrics
    pub fn new() -> Self {
        Self {
            dlq_metrics: DLQMetrics::new(),
            records_processed: Arc::new(AtomicUsize::new(0)),
            records_failed: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Record records processed
    pub fn record_processed(&self, count: usize) {
        self.records_processed.fetch_add(count, Ordering::SeqCst);
    }

    /// Record records failed
    pub fn record_failed(&self, count: usize) {
        self.records_failed.fetch_add(count, Ordering::SeqCst);
    }

    /// Get total records processed
    pub fn records_processed(&self) -> usize {
        self.records_processed.load(Ordering::SeqCst)
    }

    /// Get total records failed
    pub fn records_failed(&self) -> usize {
        self.records_failed.load(Ordering::SeqCst)
    }

    /// Get failure rate as percentage
    pub fn failure_rate_percent(&self) -> f64 {
        let total = self.records_processed();
        let failed = self.records_failed();
        if total == 0 {
            0.0
        } else {
            (failed as f64 / total as f64) * 100.0
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.records_processed.store(0, Ordering::SeqCst);
        self.records_failed.store(0, Ordering::SeqCst);
        self.dlq_metrics.reset();
    }
}

impl Default for JobMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to create a summary of DLQ health
pub fn summarize_dlq_health(dlq: &DeadLetterQueue, metrics: &DLQMetrics) -> DLQHealthSummary {
    DLQHealthSummary {
        current_size: dlq.current_size(),
        max_size: dlq.max_size(),
        capacity_usage_percent: dlq.capacity_usage_percent(),
        is_at_capacity: dlq.is_at_capacity(),
        entries_added: metrics.entries_added(),
        entries_rejected: metrics.entries_rejected(),
        last_entry_time: metrics.last_entry_time(),
        last_capacity_exceeded_time: metrics.last_capacity_exceeded_time(),
    }
}

/// Summary of DLQ health metrics
#[derive(Debug, Clone)]
pub struct DLQHealthSummary {
    pub current_size: usize,
    pub max_size: Option<usize>,
    pub capacity_usage_percent: Option<f64>,
    pub is_at_capacity: bool,
    pub entries_added: usize,
    pub entries_rejected: usize,
    pub last_entry_time: Option<Instant>,
    pub last_capacity_exceeded_time: Option<Instant>,
}

impl DLQHealthSummary {
    /// Check if DLQ health is good (not near capacity)
    pub fn is_healthy(&self) -> bool {
        if self.is_at_capacity {
            return false;
        }
        if let Some(percent) = self.capacity_usage_percent {
            // Consider unhealthy if more than 90% full
            percent < 90.0
        } else {
            true // Unlimited DLQ is always healthy
        }
    }

    /// Get health status as a string
    pub fn status_str(&self) -> &'static str {
        if self.is_at_capacity {
            "CRITICAL - at capacity"
        } else if let Some(percent) = self.capacity_usage_percent {
            if percent >= 90.0 {
                "WARNING - >90% full"
            } else if percent >= 70.0 {
                "CAUTION - >70% full"
            } else {
                "HEALTHY"
            }
        } else {
            "HEALTHY"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_metrics_creation() {
        let metrics = DLQMetrics::new();
        assert_eq!(metrics.entries_added(), 0);
        assert_eq!(metrics.entries_rejected(), 0);
    }

    #[test]
    fn test_dlq_metrics_record_added() {
        let metrics = DLQMetrics::new();
        metrics.record_entry_added();
        metrics.record_entry_added();
        assert_eq!(metrics.entries_added(), 2);
    }

    #[test]
    fn test_dlq_metrics_record_rejected() {
        let metrics = DLQMetrics::new();
        metrics.record_entry_rejected();
        assert_eq!(metrics.entries_rejected(), 1);
    }

    #[test]
    fn test_dlq_metrics_reset() {
        let metrics = DLQMetrics::new();
        metrics.record_entry_added();
        metrics.record_entry_rejected();
        assert_eq!(metrics.entries_added(), 1);
        assert_eq!(metrics.entries_rejected(), 1);

        metrics.reset();
        assert_eq!(metrics.entries_added(), 0);
        assert_eq!(metrics.entries_rejected(), 0);
    }

    #[test]
    fn test_job_metrics_failure_rate() {
        let metrics = JobMetrics::new();
        metrics.record_processed(100);
        metrics.record_failed(10);
        assert!((metrics.failure_rate_percent() - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_dlq_health_summary_healthy() {
        let summary = DLQHealthSummary {
            current_size: 50,
            max_size: Some(1000),
            capacity_usage_percent: Some(5.0),
            is_at_capacity: false,
            entries_added: 50,
            entries_rejected: 0,
            last_entry_time: Some(Instant::now()),
            last_capacity_exceeded_time: None,
        };
        assert!(summary.is_healthy());
        assert_eq!(summary.status_str(), "HEALTHY");
    }

    #[test]
    fn test_dlq_health_summary_at_capacity() {
        let summary = DLQHealthSummary {
            current_size: 1000,
            max_size: Some(1000),
            capacity_usage_percent: Some(100.0),
            is_at_capacity: true,
            entries_added: 1000,
            entries_rejected: 50,
            last_entry_time: Some(Instant::now()),
            last_capacity_exceeded_time: Some(Instant::now()),
        };
        assert!(!summary.is_healthy());
        assert_eq!(summary.status_str(), "CRITICAL - at capacity");
    }

    #[test]
    fn test_dlq_health_summary_warning() {
        let summary = DLQHealthSummary {
            current_size: 950,
            max_size: Some(1000),
            capacity_usage_percent: Some(95.0),
            is_at_capacity: false,
            entries_added: 950,
            entries_rejected: 0,
            last_entry_time: Some(Instant::now()),
            last_capacity_exceeded_time: None,
        };
        assert!(!summary.is_healthy());
        assert_eq!(summary.status_str(), "WARNING - >90% full");
    }
}
