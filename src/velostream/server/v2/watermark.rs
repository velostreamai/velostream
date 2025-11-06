//! Watermark Management for Event-Time Processing
//!
//! Manages watermarks for window emission in partitioned execution.
//! Each partition maintains its own watermark to ensure correct event-time semantics.

use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

/// Watermark strategy for handling late records
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkStrategy {
    /// Drop records that arrive after watermark
    Drop,
    /// Process late records with a warning
    ProcessWithWarning,
    /// Process all late records silently
    ProcessAll,
}

impl Default for WatermarkStrategy {
    fn default() -> Self {
        WatermarkStrategy::ProcessWithWarning
    }
}

/// Configuration for watermark management
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Maximum allowed lateness before records are considered late
    pub max_lateness: Duration,

    /// Strategy for handling late records
    pub strategy: WatermarkStrategy,

    /// Minimum time to advance watermark (prevents rapid updates)
    pub min_advance: Duration,

    /// Idle timeout: advance watermark even without records
    pub idle_timeout: Option<Duration>,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            max_lateness: Duration::from_secs(60), // 1 minute default
            strategy: WatermarkStrategy::ProcessWithWarning,
            min_advance: Duration::from_millis(100), // 100ms minimum advance
            idle_timeout: Some(Duration::from_secs(30)), // 30s idle timeout
        }
    }
}

/// Per-partition watermark manager
///
/// Manages event-time watermarks for a single partition, enabling:
/// - Window emission based on event time
/// - Late record detection
/// - Out-of-order record handling
///
/// Thread-safe via atomic operations.
pub struct WatermarkManager {
    /// Partition ID for debugging
    partition_id: usize,

    /// Current watermark timestamp (milliseconds since epoch)
    /// -1 indicates no watermark set yet
    current_watermark: Arc<AtomicI64>,

    /// Last event time observed
    last_event_time: Arc<AtomicI64>,

    /// Configuration
    config: WatermarkConfig,

    /// Metrics: late records count
    late_records_count: Arc<AtomicI64>,

    /// Metrics: dropped records count
    dropped_records_count: Arc<AtomicI64>,
}

impl WatermarkManager {
    /// Create new watermark manager for a partition
    pub fn new(partition_id: usize, config: WatermarkConfig) -> Self {
        Self {
            partition_id,
            current_watermark: Arc::new(AtomicI64::new(-1)),
            last_event_time: Arc::new(AtomicI64::new(-1)),
            config,
            late_records_count: Arc::new(AtomicI64::new(0)),
            dropped_records_count: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Create with default configuration
    pub fn with_defaults(partition_id: usize) -> Self {
        Self::new(partition_id, WatermarkConfig::default())
    }

    /// Update watermark based on observed event time
    ///
    /// Returns true if watermark was advanced
    pub fn update(&self, event_time: DateTime<Utc>) -> bool {
        let event_time_millis = event_time.timestamp_millis();

        // Update last event time
        self.last_event_time
            .store(event_time_millis, Ordering::Relaxed);

        // Calculate new watermark (event_time - max_lateness)
        let max_lateness_millis = self.config.max_lateness.as_millis() as i64;
        let new_watermark = event_time_millis - max_lateness_millis;

        // Get current watermark
        let current = self.current_watermark.load(Ordering::Relaxed);

        // Only advance watermark if:
        // 1. No watermark set yet (current == -1), OR
        // 2. New watermark is significantly ahead (respects min_advance)
        if current == -1 {
            self.current_watermark
                .store(new_watermark, Ordering::Relaxed);
            return true;
        }

        let min_advance_millis = self.config.min_advance.as_millis() as i64;
        if new_watermark > current + min_advance_millis {
            self.current_watermark
                .store(new_watermark, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Get current watermark
    ///
    /// Returns None if no watermark has been set yet
    pub fn current_watermark(&self) -> Option<DateTime<Utc>> {
        let watermark = self.current_watermark.load(Ordering::Relaxed);
        if watermark == -1 {
            None
        } else {
            DateTime::from_timestamp_millis(watermark)
        }
    }

    /// Check if a record is late (arrives after watermark)
    ///
    /// Returns (is_late, should_drop)
    pub fn is_late(&self, event_time: DateTime<Utc>) -> (bool, bool) {
        let watermark = self.current_watermark.load(Ordering::Relaxed);

        // No watermark set yet, nothing is late
        if watermark == -1 {
            return (false, false);
        }

        let event_time_millis = event_time.timestamp_millis();

        if event_time_millis < watermark {
            // Record is late
            self.late_records_count.fetch_add(1, Ordering::Relaxed);

            match self.config.strategy {
                WatermarkStrategy::Drop => {
                    self.dropped_records_count.fetch_add(1, Ordering::Relaxed);
                    (true, true)
                }
                WatermarkStrategy::ProcessWithWarning => (true, false),
                WatermarkStrategy::ProcessAll => (false, false),
            }
        } else {
            (false, false)
        }
    }

    /// Force advance watermark to a specific time
    ///
    /// Used for handling idle partitions or administrative operations
    pub fn advance_to(&self, watermark: DateTime<Utc>) {
        let watermark_millis = watermark.timestamp_millis();
        self.current_watermark
            .store(watermark_millis, Ordering::Relaxed);
    }

    /// Get metrics snapshot
    pub fn metrics(&self) -> WatermarkMetrics {
        WatermarkMetrics {
            partition_id: self.partition_id,
            current_watermark: self.current_watermark(),
            late_records_count: self.late_records_count.load(Ordering::Relaxed) as u64,
            dropped_records_count: self.dropped_records_count.load(Ordering::Relaxed) as u64,
        }
    }

    /// Reset metrics counters
    pub fn reset_metrics(&self) {
        self.late_records_count.store(0, Ordering::Relaxed);
        self.dropped_records_count.store(0, Ordering::Relaxed);
    }

    /// Get partition ID
    pub fn partition_id(&self) -> usize {
        self.partition_id
    }
}

/// Watermark metrics snapshot
#[derive(Debug, Clone)]
pub struct WatermarkMetrics {
    pub partition_id: usize,
    pub current_watermark: Option<DateTime<Utc>>,
    pub late_records_count: u64,
    pub dropped_records_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    #[test]
    fn test_watermark_creation() {
        let wm = WatermarkManager::with_defaults(0);
        assert_eq!(wm.partition_id(), 0);
        assert!(wm.current_watermark().is_none());
    }

    #[test]
    fn test_watermark_update() {
        let config = WatermarkConfig {
            max_lateness: Duration::from_secs(10),
            min_advance: Duration::from_millis(100),
            ..Default::default()
        };
        let wm = WatermarkManager::new(0, config);

        let event_time = Utc::now();
        let advanced = wm.update(event_time);

        assert!(advanced);
        assert!(wm.current_watermark().is_some());

        // Watermark should be event_time - 10 seconds
        let watermark = wm.current_watermark().unwrap();
        let expected = event_time - ChronoDuration::seconds(10);

        // Allow 1ms tolerance for rounding
        assert!((watermark - expected).num_milliseconds().abs() <= 1);
    }

    #[test]
    fn test_late_record_detection() {
        let config = WatermarkConfig {
            max_lateness: Duration::from_secs(10),
            strategy: WatermarkStrategy::ProcessWithWarning,
            ..Default::default()
        };
        let wm = WatermarkManager::new(0, config);

        let now = Utc::now();
        wm.update(now);

        // Record from 15 seconds ago should be late
        let late_record = now - ChronoDuration::seconds(15);
        let (is_late, should_drop) = wm.is_late(late_record);

        assert!(is_late);
        assert!(!should_drop); // ProcessWithWarning strategy

        let metrics = wm.metrics();
        assert_eq!(metrics.late_records_count, 1);
        assert_eq!(metrics.dropped_records_count, 0);
    }

    #[test]
    fn test_drop_late_records() {
        let config = WatermarkConfig {
            max_lateness: Duration::from_secs(10),
            strategy: WatermarkStrategy::Drop,
            ..Default::default()
        };
        let wm = WatermarkManager::new(0, config);

        let now = Utc::now();
        wm.update(now);

        let late_record = now - ChronoDuration::seconds(15);
        let (is_late, should_drop) = wm.is_late(late_record);

        assert!(is_late);
        assert!(should_drop); // Drop strategy

        let metrics = wm.metrics();
        assert_eq!(metrics.late_records_count, 1);
        assert_eq!(metrics.dropped_records_count, 1);
    }

    #[test]
    fn test_watermark_monotonicity() {
        let config = WatermarkConfig {
            max_lateness: Duration::from_secs(10),
            min_advance: Duration::from_millis(100),
            ..Default::default()
        };
        let wm = WatermarkManager::new(0, config);

        let t1 = Utc::now();
        wm.update(t1);
        let wm1 = wm.current_watermark().unwrap();

        // Small advance shouldn't update watermark (min_advance = 100ms)
        let t2 = t1 + ChronoDuration::milliseconds(50);
        let advanced = wm.update(t2);
        assert!(!advanced);
        assert_eq!(wm.current_watermark().unwrap(), wm1);

        // Large advance should update watermark
        let t3 = t1 + ChronoDuration::seconds(1);
        let advanced = wm.update(t3);
        assert!(advanced);
        assert!(wm.current_watermark().unwrap() > wm1);
    }
}
