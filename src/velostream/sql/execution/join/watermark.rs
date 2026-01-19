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
                // Check if record is late (watermark already accounts for lateness buffer)
                let is_on_time = event_time >= self.left_watermark;
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
                // Check if record is late (watermark already accounts for lateness buffer)
                let is_on_time = event_time >= self.right_watermark;
                if !is_on_time {
                    self.late_records_right += 1;
                }
                is_on_time || self.config.allow_late_data
            }
        }
    }

    /// Explicitly set the effective watermark for a source
    ///
    /// Note: This sets the effective watermark directly (the cutoff point).
    /// Records with event_time < watermark are considered late.
    /// This is different from `observe_event_time` which automatically
    /// subtracts `max_lateness_ms` from the observed event time.
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
    ///
    /// A record is late if its event_time is less than the effective watermark.
    /// The watermark already accounts for the lateness buffer when set via
    /// `observe_event_time`, so no additional lateness is subtracted here.
    #[must_use]
    pub fn is_late(&self, side: JoinSide, event_time: i64) -> bool {
        let watermark = self.watermark(side);
        event_time < watermark
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
