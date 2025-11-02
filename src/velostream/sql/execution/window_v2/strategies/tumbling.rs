//! Tumbling Window Strategy Implementation
//!
//! Non-overlapping fixed-size windows that advance by their full size.
//!
//! Example: 5-minute tumbling windows
//! ```text
//! [00:00-05:00] [05:00-10:00] [10:00-15:00]
//! ```

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{WindowStats, WindowStrategy};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;
use std::collections::VecDeque;

/// Tumbling window strategy with fixed-size non-overlapping windows.
///
/// Performance characteristics:
/// - O(1) record addition
/// - O(1) window boundary check
/// - O(N) clear operation where N = records in window
/// - Memory: Bounded by window_size_ms * arrival_rate
pub struct TumblingWindowStrategy {
    /// Window size in milliseconds
    window_size_ms: i64,

    /// Current window buffer (using VecDeque for efficient front/back operations)
    buffer: VecDeque<SharedRecord>,

    /// Start time of current window (milliseconds)
    window_start_time: Option<i64>,

    /// End time of current window (milliseconds)
    window_end_time: Option<i64>,

    /// Number of emissions produced
    emission_count: usize,

    /// Time field name for extracting timestamps
    time_field: String,
}

impl TumblingWindowStrategy {
    /// Create a new tumbling window strategy.
    ///
    /// # Arguments
    /// * `window_size_ms` - Window size in milliseconds
    /// * `time_field` - Field name containing event time
    ///
    /// # Example
    /// ```rust,ignore
    /// let strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());
    /// ```
    pub fn new(window_size_ms: i64, time_field: String) -> Self {
        Self {
            window_size_ms,
            buffer: VecDeque::new(),
            window_start_time: None,
            window_end_time: None,
            emission_count: 0,
            time_field,
        }
    }

    /// Extract timestamp from record.
    fn extract_timestamp(&self, record: &SharedRecord) -> Result<i64, SqlError> {
        use crate::velostream::sql::execution::types::FieldValue;

        let rec = record.as_ref();
        match rec.fields.get(&self.time_field) {
            Some(FieldValue::Integer(ts)) => Ok(*ts),
            Some(FieldValue::Timestamp(dt)) => {
                // Convert NaiveDateTime to milliseconds since epoch
                Ok(dt.and_utc().timestamp_millis())
            }
            Some(other) => Err(SqlError::ExecutionError {
                message: format!(
                    "Time field '{}' has wrong type: {:?}",
                    self.time_field, other
                ),
                query: None,
            }),
            None => Err(SqlError::ExecutionError {
                message: format!("Time field '{}' not found in record", self.time_field),
                query: None,
            }),
        }
    }

    /// Initialize window boundaries based on first record.
    fn initialize_window(&mut self, timestamp: i64) {
        // Align to window boundaries
        let window_index = timestamp / self.window_size_ms;
        self.window_start_time = Some(window_index.saturating_mul(self.window_size_ms));
        self.window_end_time = Some(window_index.saturating_add(1).saturating_mul(self.window_size_ms));
    }

    /// Check if timestamp belongs to current window.
    fn is_in_current_window(&self, timestamp: i64) -> bool {
        match (self.window_start_time, self.window_end_time) {
            (Some(start), Some(end)) => timestamp >= start && timestamp < end,
            _ => false,
        }
    }

    /// Advance to next window.
    fn advance_window(&mut self) {
        if let (Some(start), Some(end)) = (self.window_start_time, self.window_end_time) {
            self.window_start_time = Some(end);
            self.window_end_time = Some(end + self.window_size_ms);
        }
    }
}

impl WindowStrategy for TumblingWindowStrategy {
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError> {
        let timestamp = self.extract_timestamp(&record)?;

        // Initialize window on first record
        if self.window_start_time.is_none() {
            self.initialize_window(timestamp);
            self.buffer.push_back(record);
            return Ok(false); // No emission on first record
        }

        // Check if record belongs to current window
        let should_emit = !self.is_in_current_window(timestamp);

        // Always add record to buffer (it belongs to next window if beyond current)
        self.buffer.push_back(record);

        Ok(should_emit)
    }

    fn get_window_records(&self) -> Vec<SharedRecord> {
        // Filter records that are within the current window bounds
        // This is important because add_record may add records beyond the current window
        if let (Some(start), Some(end)) = (self.window_start_time, self.window_end_time) {
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
        match self.window_end_time {
            Some(end) => current_time >= end,
            None => false,
        }
    }

    fn clear(&mut self) {
        // Advance window first
        self.advance_window();
        self.emission_count += 1;

        // Evict records that are before the new window start
        // (for tumbling windows, this should be all records from previous window)
        if let Some(start) = self.window_start_time {
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

    fn get_stats(&self) -> WindowStats {
        WindowStats {
            record_count: self.buffer.len(),
            window_start_time: self.window_start_time,
            window_end_time: self.window_end_time,
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
        let mut fields = HashMap::new();
        fields.insert("event_time".to_string(), FieldValue::Integer(timestamp));
        fields.insert("value".to_string(), FieldValue::Integer(42));
        SharedRecord::new(StreamRecord::new(fields))
    }

    #[test]
    fn test_tumbling_window_basic() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        // Add records within same window
        let r1 = create_test_record(1000);
        let r2 = create_test_record(30000);
        let r3 = create_test_record(59000);

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
        assert_eq!(strategy.add_record(r3).unwrap(), false);

        assert_eq!(strategy.buffer.len(), 3);
        assert_eq!(strategy.window_start_time, Some(0));
        assert_eq!(strategy.window_end_time, Some(60000));
    }

    #[test]
    fn test_tumbling_window_boundary() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        // Add record in first window
        let r1 = create_test_record(30000);
        assert_eq!(strategy.add_record(r1).unwrap(), false);

        // Add record in next window - should trigger emission
        let r2 = create_test_record(70000);
        assert_eq!(strategy.add_record(r2).unwrap(), true);
    }

    #[test]
    fn test_tumbling_window_clear() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        let r2 = create_test_record(20000);

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();

        assert_eq!(strategy.buffer.len(), 2);

        strategy.clear();

        assert_eq!(strategy.buffer.len(), 0);
        assert_eq!(strategy.emission_count, 1);
        assert_eq!(strategy.window_start_time, Some(60000));
        assert_eq!(strategy.window_end_time, Some(120000));
    }

    #[test]
    fn test_tumbling_window_stats() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        strategy.add_record(r1).unwrap();

        let stats = strategy.get_stats();
        assert_eq!(stats.record_count, 1);
        assert_eq!(stats.window_start_time, Some(0));
        assert_eq!(stats.window_end_time, Some(60000));
        assert_eq!(stats.emission_count, 0);
    }

    #[test]
    fn test_tumbling_window_should_emit() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        strategy.add_record(r1).unwrap();

        // Current time before window end
        assert_eq!(strategy.should_emit(30000), false);

        // Current time at window end
        assert_eq!(strategy.should_emit(60000), true);

        // Current time after window end
        assert_eq!(strategy.should_emit(70000), true);
    }

    #[test]
    fn test_tumbling_window_get_records() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        let r2 = create_test_record(20000);

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();

        let records = strategy.get_window_records();
        assert_eq!(records.len(), 2);

        // Verify records are SharedRecord (cheap clones)
        let records2 = strategy.get_window_records();
        assert_eq!(records2.len(), 2);
    }

    #[test]
    fn test_tumbling_window_alignment() {
        let mut strategy = TumblingWindowStrategy::new(60000, "event_time".to_string());

        // Record at timestamp 75000 should be in window [60000, 120000)
        let r1 = create_test_record(75000);
        strategy.add_record(r1).unwrap();

        assert_eq!(strategy.window_start_time, Some(60000));
        assert_eq!(strategy.window_end_time, Some(120000));
    }
}
