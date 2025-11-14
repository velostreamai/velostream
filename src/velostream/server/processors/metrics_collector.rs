//! Metrics collection for tracking actual runtime statistics
//!
//! Provides a thread-safe metrics collector for tracking:
//! - Record processing counts (total, failed)
//! - Uptime and throughput calculations
//! - Lifecycle state transitions

use super::job_processor_trait::LifecycleState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Thread-safe metrics collection for job processors
#[derive(Clone)]
pub struct MetricsCollector {
    inner: Arc<MetricsCollectorInner>,
}

struct MetricsCollectorInner {
    /// Start time of processor execution
    start_time: Instant,
    /// Total records processed
    total_records: AtomicU64,
    /// Total records that failed
    failed_records: AtomicU64,
    /// Current lifecycle state
    lifecycle_state: Mutex<LifecycleState>,
}

impl MetricsCollector {
    /// Create a new metrics collector starting at now
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsCollectorInner {
                start_time: Instant::now(),
                total_records: AtomicU64::new(0),
                failed_records: AtomicU64::new(0),
                lifecycle_state: Mutex::new(LifecycleState::Idle),
            }),
        }
    }

    /// Increment total records processed
    pub fn add_records(&self, count: u64) {
        self.inner.total_records.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment failed records
    pub fn add_failed_records(&self, count: u64) {
        self.inner
            .failed_records
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Get total records processed
    pub fn total_records(&self) -> u64 {
        self.inner.total_records.load(Ordering::Relaxed)
    }

    /// Get total failed records
    pub fn failed_records(&self) -> u64 {
        self.inner.failed_records.load(Ordering::Relaxed)
    }

    /// Get uptime in seconds since processor creation
    pub fn uptime_secs(&self) -> f64 {
        self.inner.start_time.elapsed().as_secs_f64()
    }

    /// Get throughput in records per second
    pub fn throughput_rps(&self) -> f64 {
        let uptime = self.uptime_secs();
        let total = self.total_records() as f64;
        if uptime > 0.0 { total / uptime } else { 0.0 }
    }

    /// Set lifecycle state
    pub fn set_lifecycle_state(&self, state: LifecycleState) {
        if let Ok(mut guard) = self.inner.lifecycle_state.lock() {
            *guard = state;
        }
    }

    /// Get current lifecycle state
    pub fn lifecycle_state(&self) -> LifecycleState {
        self.inner
            .lifecycle_state
            .lock()
            .map(|guard| *guard)
            .unwrap_or(LifecycleState::Idle)
    }

    /// Transition to Running state
    pub fn start(&self) {
        self.set_lifecycle_state(LifecycleState::Running);
    }

    /// Transition to Paused state
    pub fn pause(&self) {
        self.set_lifecycle_state(LifecycleState::Paused);
    }

    /// Transition back to Running from Paused state
    pub fn resume(&self) {
        self.set_lifecycle_state(LifecycleState::Running);
    }

    /// Transition to Stopping state
    pub fn stop(&self) {
        self.set_lifecycle_state(LifecycleState::Stopping);
    }

    /// Transition to Stopped state
    pub fn stopped(&self) {
        self.set_lifecycle_state(LifecycleState::Stopped);
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();
        assert_eq!(collector.total_records(), 0);
        assert_eq!(collector.failed_records(), 0);
        assert_eq!(collector.lifecycle_state(), LifecycleState::Idle);
    }

    #[test]
    fn test_add_records() {
        let collector = MetricsCollector::new();
        collector.add_records(10);
        assert_eq!(collector.total_records(), 10);
        collector.add_records(5);
        assert_eq!(collector.total_records(), 15);
    }

    #[test]
    fn test_add_failed_records() {
        let collector = MetricsCollector::new();
        collector.add_failed_records(3);
        assert_eq!(collector.failed_records(), 3);
        collector.add_failed_records(2);
        assert_eq!(collector.failed_records(), 5);
    }

    #[test]
    fn test_uptime_calculation() {
        let collector = MetricsCollector::new();
        let uptime1 = collector.uptime_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let uptime2 = collector.uptime_secs();
        assert!(uptime2 >= uptime1);
        assert!(uptime2 > 0.0);
    }

    #[test]
    fn test_throughput_calculation() {
        let collector = MetricsCollector::new();
        collector.start();
        collector.add_records(100);
        std::thread::sleep(std::time::Duration::from_millis(100));
        let throughput = collector.throughput_rps();
        assert!(throughput > 0.0);
    }

    #[test]
    fn test_lifecycle_state_transitions() {
        let collector = MetricsCollector::new();
        assert_eq!(collector.lifecycle_state(), LifecycleState::Idle);

        collector.start();
        assert_eq!(collector.lifecycle_state(), LifecycleState::Running);

        collector.pause();
        assert_eq!(collector.lifecycle_state(), LifecycleState::Paused);

        collector.resume();
        assert_eq!(collector.lifecycle_state(), LifecycleState::Running);

        collector.stop();
        assert_eq!(collector.lifecycle_state(), LifecycleState::Stopping);

        collector.stopped();
        assert_eq!(collector.lifecycle_state(), LifecycleState::Stopped);
    }

    #[test]
    fn test_cloneable() {
        let collector1 = MetricsCollector::new();
        collector1.add_records(10);

        let collector2 = collector1.clone();
        assert_eq!(collector2.total_records(), 10);

        collector2.add_records(5);
        assert_eq!(collector1.total_records(), 15); // Shared state
    }
}
