//! Join Watermark Tracker
//!
//! Tracks watermarks from multiple sources for stream-stream joins.
//! The combined watermark is the minimum of all source watermarks,
//! ensuring no late data is incorrectly expired.

use crate::velostream::sql::execution::join::JoinSide;

/// Configuration for watermark tracking
#[derive(Debug, Clone)]
pub struct WatermarkConfig {
    /// Maximum allowed lateness in milliseconds
    /// Records arriving within this window after watermark are still processed
    pub max_lateness_ms: i64,

    /// Idle timeout in milliseconds
    /// If a source is idle longer than this, its watermark advances automatically
    pub idle_timeout_ms: i64,

    /// Whether to allow out-of-order processing
    pub allow_late_data: bool,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            max_lateness_ms: 0,
            idle_timeout_ms: 60_000, // 1 minute default idle timeout
            allow_late_data: true,
        }
    }
}

impl WatermarkConfig {
    /// Create a strict watermark config (no lateness allowed)
    pub fn strict() -> Self {
        Self {
            max_lateness_ms: 0,
            idle_timeout_ms: 0,
            allow_late_data: false,
        }
    }

    /// Create a lenient watermark config with specified lateness
    pub fn with_lateness(max_lateness_ms: i64) -> Self {
        Self {
            max_lateness_ms,
            idle_timeout_ms: 60_000,
            allow_late_data: true,
        }
    }
}

/// Tracks watermarks from both sides of a stream-stream join
///
/// The watermark represents the point in event time up to which we expect
/// all data has arrived. Records with timestamps below the watermark are
/// considered "late" and may be dropped or handled specially.
#[derive(Debug)]
pub struct JoinWatermarkTracker {
    /// Configuration
    config: WatermarkConfig,

    /// Watermark from left source
    left_watermark: i64,

    /// Watermark from right source
    right_watermark: i64,

    /// Last observed event time from left
    left_last_event_time: i64,

    /// Last observed event time from right
    right_last_event_time: i64,

    /// Timestamp when left source was last active
    left_last_active: i64,

    /// Timestamp when right source was last active
    right_last_active: i64,

    /// Statistics
    late_records_left: u64,
    late_records_right: u64,
}

impl JoinWatermarkTracker {
    /// Create a new watermark tracker with default configuration
    pub fn new() -> Self {
        Self::with_config(WatermarkConfig::default())
    }

    /// Create a new watermark tracker with specific configuration
    pub fn with_config(config: WatermarkConfig) -> Self {
        Self {
            config,
            left_watermark: 0,
            right_watermark: 0,
            left_last_event_time: 0,
            right_last_event_time: 0,
            left_last_active: 0,
            right_last_active: 0,
            late_records_left: 0,
            late_records_right: 0,
        }
    }

    /// Update watermark for a source based on observed event time
    ///
    /// Returns true if the record is not late, false if it's considered late.
    pub fn observe_event_time(&mut self, side: JoinSide, event_time: i64, wall_clock: i64) -> bool {
        match side {
            JoinSide::Left => {
                self.left_last_active = wall_clock;
                if event_time > self.left_last_event_time {
                    self.left_last_event_time = event_time;
                    // Watermark advances with lateness buffer
                    self.left_watermark = event_time.saturating_sub(self.config.max_lateness_ms);
                }
                // Check if record is late
                let is_on_time = event_time >= self.left_watermark - self.config.max_lateness_ms;
                if !is_on_time {
                    self.late_records_left += 1;
                }
                is_on_time || self.config.allow_late_data
            }
            JoinSide::Right => {
                self.right_last_active = wall_clock;
                if event_time > self.right_last_event_time {
                    self.right_last_event_time = event_time;
                    self.right_watermark = event_time.saturating_sub(self.config.max_lateness_ms);
                }
                let is_on_time = event_time >= self.right_watermark - self.config.max_lateness_ms;
                if !is_on_time {
                    self.late_records_right += 1;
                }
                is_on_time || self.config.allow_late_data
            }
        }
    }

    /// Explicitly set watermark for a source
    pub fn set_watermark(&mut self, side: JoinSide, watermark: i64) {
        match side {
            JoinSide::Left => {
                if watermark > self.left_watermark {
                    self.left_watermark = watermark;
                }
            }
            JoinSide::Right => {
                if watermark > self.right_watermark {
                    self.right_watermark = watermark;
                }
            }
        }
    }

    /// Get the watermark for a specific side
    pub fn watermark(&self, side: JoinSide) -> i64 {
        match side {
            JoinSide::Left => self.left_watermark,
            JoinSide::Right => self.right_watermark,
        }
    }

    /// Get the combined (minimum) watermark across both sides
    ///
    /// This is the safe watermark for expiring state - only records
    /// older than this can be safely removed without missing joins.
    pub fn combined_watermark(&self) -> i64 {
        self.left_watermark.min(self.right_watermark)
    }

    /// Check if we should advance watermark due to idle source
    ///
    /// If a source has been idle for longer than the timeout, we can
    /// advance its watermark to match the other source.
    pub fn check_idle_advance(&mut self, wall_clock: i64) -> bool {
        if self.config.idle_timeout_ms == 0 {
            return false;
        }

        let mut advanced = false;

        // Check if left is idle
        if wall_clock - self.left_last_active > self.config.idle_timeout_ms {
            if self.left_watermark < self.right_watermark {
                self.left_watermark = self.right_watermark;
                advanced = true;
            }
        }

        // Check if right is idle
        if wall_clock - self.right_last_active > self.config.idle_timeout_ms {
            if self.right_watermark < self.left_watermark {
                self.right_watermark = self.left_watermark;
                advanced = true;
            }
        }

        advanced
    }

    /// Get the last event time observed from a side
    pub fn last_event_time(&self, side: JoinSide) -> i64 {
        match side {
            JoinSide::Left => self.left_last_event_time,
            JoinSide::Right => self.right_last_event_time,
        }
    }

    /// Get count of late records for a side
    pub fn late_record_count(&self, side: JoinSide) -> u64 {
        match side {
            JoinSide::Left => self.late_records_left,
            JoinSide::Right => self.late_records_right,
        }
    }

    /// Get total late record count
    pub fn total_late_records(&self) -> u64 {
        self.late_records_left + self.late_records_right
    }

    /// Check if a record would be considered late
    pub fn is_late(&self, side: JoinSide, event_time: i64) -> bool {
        let watermark = self.watermark(side);
        event_time < watermark - self.config.max_lateness_ms
    }

    /// Get statistics
    pub fn stats(&self) -> WatermarkStats {
        WatermarkStats {
            left_watermark: self.left_watermark,
            right_watermark: self.right_watermark,
            combined_watermark: self.combined_watermark(),
            late_records_left: self.late_records_left,
            late_records_right: self.late_records_right,
        }
    }
}

impl Default for JoinWatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for watermark tracking
#[derive(Debug, Clone, Default)]
pub struct WatermarkStats {
    pub left_watermark: i64,
    pub right_watermark: i64,
    pub combined_watermark: i64,
    pub late_records_left: u64,
    pub late_records_right: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_advancement() {
        let mut tracker = JoinWatermarkTracker::new();

        // Observe events on left side
        tracker.observe_event_time(JoinSide::Left, 1000, 0);
        assert_eq!(tracker.watermark(JoinSide::Left), 1000);

        // Advance left watermark
        tracker.observe_event_time(JoinSide::Left, 2000, 100);
        assert_eq!(tracker.watermark(JoinSide::Left), 2000);

        // Right is still at 0
        assert_eq!(tracker.watermark(JoinSide::Right), 0);

        // Combined is minimum
        assert_eq!(tracker.combined_watermark(), 0);
    }

    #[test]
    fn test_combined_watermark() {
        let mut tracker = JoinWatermarkTracker::new();

        tracker.set_watermark(JoinSide::Left, 1000);
        tracker.set_watermark(JoinSide::Right, 500);

        assert_eq!(tracker.combined_watermark(), 500);

        tracker.set_watermark(JoinSide::Right, 1500);
        assert_eq!(tracker.combined_watermark(), 1000);
    }

    #[test]
    fn test_lateness_detection() {
        let config = WatermarkConfig::with_lateness(100);
        let mut tracker = JoinWatermarkTracker::with_config(config);

        // Set watermark to 1000
        tracker.set_watermark(JoinSide::Left, 1000);

        // Event at 950 is within lateness window (1000 - 100 = 900)
        assert!(!tracker.is_late(JoinSide::Left, 950));

        // Event at 800 is late
        assert!(tracker.is_late(JoinSide::Left, 800));
    }

    #[test]
    fn test_late_record_counting() {
        let config = WatermarkConfig::strict();
        let mut tracker = JoinWatermarkTracker::with_config(config);

        // Advance watermark
        tracker.observe_event_time(JoinSide::Left, 1000, 0);

        // Late record (with allow_late_data = false, returns false)
        let result = tracker.observe_event_time(JoinSide::Left, 500, 100);
        assert!(!result); // Strict mode rejects late data

        assert_eq!(tracker.late_record_count(JoinSide::Left), 1);
    }

    #[test]
    fn test_allow_late_data() {
        let config = WatermarkConfig {
            max_lateness_ms: 100,
            idle_timeout_ms: 60_000,
            allow_late_data: true,
        };
        let mut tracker = JoinWatermarkTracker::with_config(config);

        // Advance watermark
        tracker.observe_event_time(JoinSide::Left, 1000, 0);

        // Late record (with allow_late_data = true, returns true anyway)
        let result = tracker.observe_event_time(JoinSide::Left, 500, 100);
        assert!(result); // Lenient mode accepts late data

        // Still counted as late
        assert_eq!(tracker.late_record_count(JoinSide::Left), 1);
    }

    #[test]
    fn test_idle_advancement() {
        let config = WatermarkConfig {
            max_lateness_ms: 0,
            idle_timeout_ms: 1000,
            allow_late_data: true,
        };
        let mut tracker = JoinWatermarkTracker::with_config(config);

        // Set watermarks unevenly
        tracker.observe_event_time(JoinSide::Left, 5000, 0);
        tracker.observe_event_time(JoinSide::Right, 3000, 0);

        assert_eq!(tracker.combined_watermark(), 3000);

        // Simulate right side being idle for longer than timeout
        let advanced = tracker.check_idle_advance(2000); // 2 seconds after last activity
        assert!(advanced);

        // Right watermark should advance to match left
        assert_eq!(tracker.watermark(JoinSide::Right), 5000);
        assert_eq!(tracker.combined_watermark(), 5000);
    }

    #[test]
    fn test_explicit_set_only_advances() {
        let mut tracker = JoinWatermarkTracker::new();

        tracker.set_watermark(JoinSide::Left, 1000);
        assert_eq!(tracker.watermark(JoinSide::Left), 1000);

        // Can't go backwards
        tracker.set_watermark(JoinSide::Left, 500);
        assert_eq!(tracker.watermark(JoinSide::Left), 1000);

        // Can advance
        tracker.set_watermark(JoinSide::Left, 1500);
        assert_eq!(tracker.watermark(JoinSide::Left), 1500);
    }

    #[test]
    fn test_stats() {
        let mut tracker = JoinWatermarkTracker::new();

        tracker.observe_event_time(JoinSide::Left, 1000, 0);
        tracker.observe_event_time(JoinSide::Right, 2000, 0);

        let stats = tracker.stats();
        assert_eq!(stats.left_watermark, 1000);
        assert_eq!(stats.right_watermark, 2000);
        assert_eq!(stats.combined_watermark, 1000);
    }
}
