//! Sliding Window Strategy Implementation
//!
//! Overlapping fixed-size windows that advance by a smaller interval.
//!
//! Example: 60-second windows advancing every 30 seconds
//! ```text
//! [00:00-01:00]
//!       [00:30-01:30]
//!             [01:00-02:00]
//! ```

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{WindowStats, WindowStrategy};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;
use std::collections::VecDeque;

/// Sliding window strategy with overlapping windows.
///
/// Performance characteristics:
/// - O(1) record addition (amortized)
/// - O(1) window boundary check
/// - O(N) eviction where N = records outside window
/// - Memory: Bounded by window_size_ms * arrival_rate
///
/// # Overlap Calculation
/// - 50% overlap: advance_interval = window_size / 2
/// - 67% overlap: advance_interval = window_size / 3
/// - 75% overlap: advance_interval = window_size / 4
pub struct SlidingWindowStrategy {
    /// Window size in milliseconds
    window_size_ms: i64,

    /// Advance interval in milliseconds
    advance_interval_ms: i64,

    /// Circular buffer for efficient eviction (using VecDeque for ring buffer semantics)
    buffer: VecDeque<SharedRecord>,

    /// Next window start time (milliseconds)
    next_window_start: Option<i64>,

    /// Current window end time (milliseconds)
    current_window_end: Option<i64>,

    /// Number of emissions produced
    emission_count: usize,

    /// Time field name for extracting timestamps
    time_field: String,
}

impl SlidingWindowStrategy {
    /// Create a new sliding window strategy.
    ///
    /// # Arguments
    /// * `window_size_ms` - Window size in milliseconds
    /// * `advance_interval_ms` - How often to advance the window (controls overlap)
    /// * `time_field` - Field name containing event time
    ///
    /// # Example
    /// ```rust,ignore
    /// // 60-second windows advancing every 30 seconds (50% overlap)
    /// let strategy = SlidingWindowStrategy::new(60000, 30000, "_TIMESTAMP".to_string());
    /// ```
    pub fn new(window_size_ms: i64, advance_interval_ms: i64, time_field: String) -> Self {
        Self::with_estimated_capacity(window_size_ms, advance_interval_ms, time_field, 1000)
    }

    /// Create a new sliding window strategy with pre-allocated capacity hint.
    ///
    /// FR-082 Week 8 Optimization 3: Window Buffer Pre-allocation
    ///
    /// Pre-allocates buffer capacity to account for window overlap.
    /// Sliding windows retain records from overlapping windows, increasing expected record count.
    /// Typical sliding windows without pre-allocation experience 10-20 reallocations per advance.
    /// Pre-allocation reduces this to 0 reallocations.
    ///
    /// # Overlap Calculation
    /// Overlap ratio = window_size / advance_interval
    /// - advance_interval = window_size: no overlap (equivalent to tumbling)
    /// - advance_interval = window_size/2: 50% overlap (2x retention)
    /// - advance_interval = window_size/3: 67% overlap (3x retention)
    /// - advance_interval = window_size/10: 90% overlap (10x retention)
    ///
    /// # Arguments
    /// * `window_size_ms` - Window size in milliseconds
    /// * `advance_interval_ms` - Advance interval in milliseconds
    /// * `time_field` - Field name containing event time
    /// * `estimated_records_per_sec` - Heuristic for expected arrival rate
    ///
    /// # Example
    /// ```rust,ignore
    /// // 60-second window advancing every 30 seconds (50% overlap)
    /// // With 1000 events/sec, expects ~60K records with overlap
    /// let strategy = SlidingWindowStrategy::with_estimated_capacity(
    ///     60000,   // 60 second window
    ///     30000,   // advance every 30 seconds (50% overlap)
    ///     "_TIMESTAMP".to_string(),
    ///     1000     // 1000 events/sec
    /// );
    /// ```
    pub fn with_estimated_capacity(
        window_size_ms: i64,
        advance_interval_ms: i64,
        time_field: String,
        estimated_records_per_sec: usize,
    ) -> Self {
        // Calculate overlap ratio
        let overlap_ratio = (window_size_ms as f64) / (advance_interval_ms as f64);

        // Calculate expected record count accounting for overlap
        // For a sliding window, records are retained from multiple overlapping windows
        let window_size_secs = (window_size_ms as f64) / 1000.0;
        let base_records = window_size_secs * estimated_records_per_sec as f64;
        let estimated_window_records = (base_records * overlap_ratio).ceil() as usize;

        // Add 10% safety margin to avoid edge-case reallocations
        let capacity_with_margin = (estimated_window_records as f64 * 1.1).ceil() as usize;

        Self {
            window_size_ms,
            advance_interval_ms,
            buffer: VecDeque::with_capacity(capacity_with_margin),
            next_window_start: None,
            current_window_end: None,
            emission_count: 0,
            time_field,
        }
    }

    /// Extract timestamp from record using shared utility.
    ///
    /// FR-081: Uses shared extract_record_timestamp() which implements three-tier priority:
    /// 1. System columns (_TIMESTAMP, _EVENT_TIME) → metadata
    /// 2. Regular fields (timestamp, event_time) → get_event_time()
    /// 3. Legacy user fields → fields HashMap (backward compatibility)
    fn extract_timestamp(&self, record: &SharedRecord) -> Result<i64, SqlError> {
        crate::velostream::sql::execution::window_v2::timestamp_utils::extract_record_timestamp(
            record,
            &self.time_field,
        )
    }

    /// Initialize window boundaries based on first record.
    fn initialize_window(&mut self, timestamp: i64) {
        // Align to advance interval boundaries
        let window_index = timestamp / self.advance_interval_ms;
        self.next_window_start = Some(window_index.saturating_mul(self.advance_interval_ms));
        self.current_window_end = Some(
            self.next_window_start
                .unwrap()
                .saturating_add(self.window_size_ms),
        );
    }

    /// Check if timestamp is within current window range.
    fn is_in_current_window(&self, timestamp: i64) -> bool {
        match (self.next_window_start, self.current_window_end) {
            (Some(start), Some(end)) => timestamp >= start && timestamp < end,
            _ => false,
        }
    }

    /// Advance window to next position.
    fn advance_window(&mut self) {
        if let Some(start) = self.next_window_start {
            self.next_window_start = Some(start.saturating_add(self.advance_interval_ms));
            self.current_window_end = Some(
                self.next_window_start
                    .unwrap()
                    .saturating_add(self.window_size_ms),
            );
        }
    }

    /// Evict records that fall outside the current window.
    ///
    /// This is the key difference from tumbling windows - we keep records
    /// that overlap with the next window instead of clearing everything.
    fn evict_old_records(&mut self) {
        if let Some(start) = self.next_window_start {
            // Remove records older than window start
            while let Some(record) = self.buffer.front() {
                if let Ok(ts) = self.extract_timestamp(record) {
                    if ts < start {
                        self.buffer.pop_front();
                    } else {
                        break; // Records are time-ordered
                    }
                } else {
                    // If we can't extract timestamp, remove it
                    self.buffer.pop_front();
                }
            }
        }
    }
}

impl WindowStrategy for SlidingWindowStrategy {
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError> {
        let timestamp = self.extract_timestamp(&record)?;

        // Initialize window on first record
        if self.next_window_start.is_none() {
            self.initialize_window(timestamp);
            self.buffer.push_back(record);
            return Ok(false); // No emission on first record
        }

        // Check if we need to advance to next window
        let should_emit = if let Some(end) = self.current_window_end {
            timestamp >= end
        } else {
            false
        };

        // Always add record to buffer (it belongs to next window if beyond current)
        self.buffer.push_back(record);

        Ok(should_emit)
    }

    fn get_window_records(&self) -> Vec<SharedRecord> {
        // Filter records that are within the current window bounds
        if let (Some(start), Some(end)) = (self.next_window_start, self.current_window_end) {
            self.buffer
                .iter()
                .filter(|record| {
                    if let Ok(ts) = self.extract_timestamp(record) {
                        ts >= start && ts < end
                    } else {
                        false
                    }
                })
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    fn should_emit(&self, current_time: i64) -> bool {
        match self.current_window_end {
            Some(end) => current_time >= end,
            None => false,
        }
    }

    fn clear(&mut self) {
        // For sliding windows, "clear" means advance and evict old records
        self.advance_window();
        self.evict_old_records();
        self.emission_count += 1;
    }

    fn get_stats(&self) -> WindowStats {
        WindowStats {
            record_count: self.buffer.len(),
            window_start_time: self.next_window_start,
            window_end_time: self.current_window_end,
            emission_count: self.emission_count,
            buffer_size_bytes: self.buffer.len() * std::mem::size_of::<SharedRecord>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
    use std::collections::HashMap;

    fn create_test_record(timestamp: i64) -> SharedRecord {
        // FR-081: Set StreamRecord.timestamp metadata instead of fields HashMap
        // This is the correct way per the new architecture
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Integer(42));
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp; // Set processing-time metadata
        SharedRecord::new(record)
    }

    #[test]
    fn test_sliding_window_basic() {
        // 60-second windows advancing every 30 seconds (50% overlap)
        let mut strategy = SlidingWindowStrategy::new(60000, 30000, "_TIMESTAMP".to_string());

        // Add records within first window
        let r1 = create_test_record(1000);
        let r2 = create_test_record(30000);
        let r3 = create_test_record(59000);

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
        assert_eq!(strategy.add_record(r3).unwrap(), false);

        assert_eq!(strategy.buffer.len(), 3);
        assert_eq!(strategy.next_window_start, Some(0));
        assert_eq!(strategy.current_window_end, Some(60000));
    }

    #[test]
    fn test_sliding_window_overlap() {
        // 60-second windows advancing every 30 seconds
        let mut strategy = SlidingWindowStrategy::new(60000, 30000, "_TIMESTAMP".to_string());

        // Add records in first window
        let r1 = create_test_record(10000);
        let r2 = create_test_record(40000);
        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();

        // Advance to next window
        let r3 = create_test_record(70000);
        assert_eq!(strategy.add_record(r3).unwrap(), true); // Should emit

        strategy.clear(); // Advance window

        // After advance, window is [30000, 90000)
        assert_eq!(strategy.next_window_start, Some(30000));
        assert_eq!(strategy.current_window_end, Some(90000));

        // r2 at 40000 should still be in buffer (overlapping window)
        assert_eq!(strategy.buffer.len(), 2); // r2 and r3
    }

    #[test]
    fn test_sliding_window_eviction() {
        // 60-second windows advancing every 30 seconds
        let mut strategy = SlidingWindowStrategy::new(60000, 30000, "_TIMESTAMP".to_string());

        let r1 = create_test_record(5000);
        let r2 = create_test_record(35000);
        let r3 = create_test_record(65000);

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();
        assert_eq!(strategy.buffer.len(), 2);

        // Trigger emission and advance
        strategy.add_record(r3).unwrap();
        strategy.clear();

        // After clear, window is [30000, 90000)
        // r1 at 5000 should be evicted
        // r2 at 35000 should remain
        // r3 at 65000 should remain
        assert_eq!(strategy.buffer.len(), 2);

        let records = strategy.get_window_records();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_sliding_window_get_records_filters() {
        // 60-second windows advancing every 20 seconds (67% overlap)
        let mut strategy = SlidingWindowStrategy::new(60000, 20000, "_TIMESTAMP".to_string());

        let r1 = create_test_record(5000);
        let r2 = create_test_record(15000);
        let r3 = create_test_record(25000);
        let r4 = create_test_record(35000);

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();
        strategy.add_record(r3).unwrap();
        strategy.add_record(r4).unwrap();

        // Window is [0, 60000), all records should be included
        let records = strategy.get_window_records();
        assert_eq!(records.len(), 4);

        // Advance to [20000, 80000)
        strategy.clear();

        // Now only r3, r4 should be in window (r1 and r2 are < 20000)
        let records = strategy.get_window_records();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_sliding_window_stats() {
        let mut strategy = SlidingWindowStrategy::new(60000, 30000, "_TIMESTAMP".to_string());

        let r1 = create_test_record(10000);
        strategy.add_record(r1).unwrap();

        let stats = strategy.get_stats();
        assert_eq!(stats.record_count, 1);
        assert_eq!(stats.window_start_time, Some(0));
        assert_eq!(stats.window_end_time, Some(60000));
        assert_eq!(stats.emission_count, 0);

        strategy.clear();

        let stats = strategy.get_stats();
        assert_eq!(stats.emission_count, 1);
        assert_eq!(stats.window_start_time, Some(30000));
    }

    #[test]
    fn test_sliding_window_high_overlap() {
        // 120-second windows advancing every 30 seconds (75% overlap)
        let mut strategy = SlidingWindowStrategy::new(120000, 30000, "_TIMESTAMP".to_string());

        let r1 = create_test_record(10000);
        let r2 = create_test_record(50000);
        let r3 = create_test_record(90000);
        let r4 = create_test_record(130000);

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();
        strategy.add_record(r3).unwrap();

        // Should emit when r4 arrives (beyond window end)
        assert_eq!(strategy.add_record(r4).unwrap(), true);

        strategy.clear();

        // After advance, window is [30000, 150000)
        // r1 at 10000 should be evicted
        // r2, r3, r4 should remain (high overlap)
        assert_eq!(strategy.buffer.len(), 3);

        let records = strategy.get_window_records();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_sliding_window_alignment() {
        // Test window alignment to advance interval boundaries
        let mut strategy = SlidingWindowStrategy::new(60000, 30000, "_TIMESTAMP".to_string());

        // Record at timestamp 45000 should create window [30000, 90000)
        let r1 = create_test_record(45000);
        strategy.add_record(r1).unwrap();

        assert_eq!(strategy.next_window_start, Some(30000));
        assert_eq!(strategy.current_window_end, Some(90000));
    }
}
