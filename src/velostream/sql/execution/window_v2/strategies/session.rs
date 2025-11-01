//! Session Window Strategy Implementation
//!
//! Dynamic windows that group events based on gaps in activity.
//!
//! Example: 5-minute session gap
//! ```text
//! Events: [00:00, 00:02, 00:03, 00:10, 00:12]
//! Sessions: [00:00-00:08] (events 1-3), [00:10-00:17] (events 4-5)
//! ```

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{WindowStats, WindowStrategy};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;
use std::collections::VecDeque;

/// Session window strategy with gap-based boundary detection.
///
/// Performance characteristics:
/// - O(1) record addition
/// - O(1) gap detection
/// - O(N) clear operation where N = records in session
/// - Memory: Unbounded by design (sessions can grow indefinitely)
///
/// # Gap Semantics
/// - Session closes when gap exceeds `gap_duration_ms`
/// - New session starts with first event after gap
/// - Session end time = last event time + gap_duration_ms
pub struct SessionWindowStrategy {
    /// Gap duration in milliseconds
    gap_duration_ms: i64,

    /// Current session buffer
    buffer: VecDeque<SharedRecord>,

    /// Timestamp of last event in current session
    last_event_time: Option<i64>,

    /// Start time of current session (milliseconds)
    session_start_time: Option<i64>,

    /// End time of current session (milliseconds)
    /// Calculated as last_event_time + gap_duration_ms
    session_end_time: Option<i64>,

    /// Number of sessions emitted
    emission_count: usize,

    /// Time field name for extracting timestamps
    time_field: String,
}

impl SessionWindowStrategy {
    /// Create a new session window strategy.
    ///
    /// # Arguments
    /// * `gap_duration_ms` - Maximum gap between events in milliseconds
    /// * `time_field` - Field name containing event time
    ///
    /// # Example
    /// ```rust,ignore
    /// // 5-minute session gap
    /// let strategy = SessionWindowStrategy::new(300000, "event_time".to_string());
    /// ```
    pub fn new(gap_duration_ms: i64, time_field: String) -> Self {
        Self {
            gap_duration_ms,
            buffer: VecDeque::new(),
            last_event_time: None,
            session_start_time: None,
            session_end_time: None,
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

    /// Initialize session with first event.
    fn initialize_session(&mut self, timestamp: i64) {
        self.session_start_time = Some(timestamp);
        self.last_event_time = Some(timestamp);
        self.session_end_time = Some(timestamp + self.gap_duration_ms);
    }

    /// Update session boundaries with new event.
    fn update_session(&mut self, timestamp: i64) {
        self.last_event_time = Some(timestamp);
        self.session_end_time = Some(timestamp + self.gap_duration_ms);
    }

    /// Check if timestamp exceeds session gap.
    fn exceeds_gap(&self, timestamp: i64) -> bool {
        match self.last_event_time {
            Some(last_time) => timestamp - last_time > self.gap_duration_ms,
            None => false,
        }
    }

    /// Reset session state for next session.
    fn reset_session(&mut self) {
        self.session_start_time = None;
        self.last_event_time = None;
        self.session_end_time = None;
    }
}

impl WindowStrategy for SessionWindowStrategy {
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError> {
        let timestamp = self.extract_timestamp(&record)?;

        // Initialize session on first record
        if self.session_start_time.is_none() {
            self.initialize_session(timestamp);
            self.buffer.push_back(record);
            return Ok(false); // No emission on first record
        }

        // Check if this event exceeds the session gap
        let should_emit = self.exceeds_gap(timestamp);

        // Always add record to buffer (it starts next session if gap exceeded)
        self.buffer.push_back(record);

        if should_emit {
            // Don't update session yet - wait for clear() to be called
            Ok(true)
        } else {
            // Event is within gap - extend current session
            self.update_session(timestamp);
            Ok(false)
        }
    }

    fn get_window_records(&self) -> Vec<SharedRecord> {
        // Filter records that belong to current session
        // This is important because add_record may add records beyond the current session
        if let (Some(start), Some(end)) = (self.session_start_time, self.session_end_time) {
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
        match self.session_end_time {
            Some(end) => current_time >= end,
            None => false,
        }
    }

    fn clear(&mut self) {
        self.emission_count += 1;

        // Evict records from completed session
        if let Some(end) = self.session_end_time {
            while let Some(record) = self.buffer.front() {
                if let Ok(ts) = self.extract_timestamp(record) {
                    if ts < end {
                        self.buffer.pop_front();
                    } else {
                        // This record starts the next session
                        self.reset_session();
                        self.initialize_session(ts);
                        break;
                    }
                } else {
                    // If we can't extract timestamp, remove it
                    self.buffer.pop_front();
                }
            }
        }

        // If buffer is empty after eviction, reset session
        if self.buffer.is_empty() {
            self.reset_session();
        }
    }

    fn get_stats(&self) -> WindowStats {
        WindowStats {
            record_count: self.buffer.len(),
            window_start_time: self.session_start_time,
            window_end_time: self.session_end_time,
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
    fn test_session_window_basic() {
        // 5-minute session gap
        let mut strategy = SessionWindowStrategy::new(300000, "event_time".to_string());

        // Add events within gap
        let r1 = create_test_record(1000);
        let r2 = create_test_record(60000);  // 1 minute later
        let r3 = create_test_record(120000); // 2 minutes later

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
        assert_eq!(strategy.add_record(r3).unwrap(), false);

        assert_eq!(strategy.buffer.len(), 3);
        assert_eq!(strategy.session_start_time, Some(1000));
        assert_eq!(strategy.session_end_time, Some(420000)); // 120000 + 300000
    }

    #[test]
    fn test_session_window_gap_detection() {
        // 5-minute session gap
        let mut strategy = SessionWindowStrategy::new(300000, "event_time".to_string());

        // First session
        let r1 = create_test_record(1000);
        let r2 = create_test_record(60000);

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);

        // Event beyond gap - should trigger emission
        let r3 = create_test_record(500000); // 8+ minutes after r2
        assert_eq!(strategy.add_record(r3).unwrap(), true);
    }

    #[test]
    fn test_session_window_clear() {
        // 5-minute session gap
        let mut strategy = SessionWindowStrategy::new(300000, "event_time".to_string());

        let r1 = create_test_record(10000);
        let r2 = create_test_record(20000);
        let r3 = create_test_record(500000); // Starts next session

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();
        strategy.add_record(r3).unwrap();

        assert_eq!(strategy.buffer.len(), 3);

        strategy.clear();

        // After clear, first session records evicted, new session starts with r3
        assert_eq!(strategy.buffer.len(), 1);
        assert_eq!(strategy.emission_count, 1);
        assert_eq!(strategy.session_start_time, Some(500000));
        assert_eq!(strategy.session_end_time, Some(800000)); // 500000 + 300000
    }

    #[test]
    fn test_session_window_multiple_sessions() {
        // 1-minute session gap
        let mut strategy = SessionWindowStrategy::new(60000, "event_time".to_string());

        // Session 1: 0-2s
        let r1 = create_test_record(0);
        let r2 = create_test_record(2000);
        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();

        // Session 2: 100s
        let r3 = create_test_record(100000);
        assert_eq!(strategy.add_record(r3).unwrap(), true); // Should emit session 1

        strategy.clear();

        // Verify session 2 is active
        assert_eq!(strategy.buffer.len(), 1);
        assert_eq!(strategy.session_start_time, Some(100000));
        assert_eq!(strategy.emission_count, 1);
    }

    #[test]
    fn test_session_window_stats() {
        let mut strategy = SessionWindowStrategy::new(300000, "event_time".to_string());

        let r1 = create_test_record(10000);
        strategy.add_record(r1).unwrap();

        let stats = strategy.get_stats();
        assert_eq!(stats.record_count, 1);
        assert_eq!(stats.window_start_time, Some(10000));
        assert_eq!(stats.window_end_time, Some(310000)); // 10000 + 300000
        assert_eq!(stats.emission_count, 0);
    }

    #[test]
    fn test_session_window_should_emit() {
        let mut strategy = SessionWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        strategy.add_record(r1).unwrap();

        // Current time before session end
        assert_eq!(strategy.should_emit(50000), false);

        // Current time at session end
        assert_eq!(strategy.should_emit(70000), true);

        // Current time after session end
        assert_eq!(strategy.should_emit(100000), true);
    }

    #[test]
    fn test_session_window_get_records() {
        let mut strategy = SessionWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        let r2 = create_test_record(20000);
        let r3 = create_test_record(200000); // Beyond gap

        strategy.add_record(r1).unwrap();
        strategy.add_record(r2).unwrap();

        // Get records from first session
        let records = strategy.get_window_records();
        assert_eq!(records.len(), 2);

        strategy.add_record(r3).unwrap();

        // Still returns only first session records
        let records = strategy.get_window_records();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_session_window_empty_after_clear() {
        let mut strategy = SessionWindowStrategy::new(60000, "event_time".to_string());

        let r1 = create_test_record(10000);
        strategy.add_record(r1).unwrap();

        // No events beyond gap - just emit current session
        strategy.clear();

        // Buffer should be empty
        assert_eq!(strategy.buffer.len(), 0);
        assert_eq!(strategy.session_start_time, None);
        assert_eq!(strategy.session_end_time, None);
        assert_eq!(strategy.emission_count, 1);
    }

    #[test]
    fn test_session_window_short_gap() {
        // 10-second session gap
        let mut strategy = SessionWindowStrategy::new(10000, "event_time".to_string());

        let r1 = create_test_record(1000);
        let r2 = create_test_record(5000);   // 4s later - within gap
        let r3 = create_test_record(20000);  // 15s later - beyond gap

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
        assert_eq!(strategy.add_record(r3).unwrap(), true); // Should emit

        // Session end should be extended to last event + gap
        assert_eq!(strategy.session_end_time, Some(15000)); // 5000 + 10000
    }
}
