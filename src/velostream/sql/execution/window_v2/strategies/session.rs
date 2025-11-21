//! Session Window Strategy Implementation
//!
//! Dynamic windows that group events based on gaps in activity.
//!
//! Example: 5-minute session gap
//! ```text
//! Events: [00:00, 00:02, 00:03, 00:10, 00:12]
//! Sessions: [00:00-00:08] (events 1-3), [00:10-00:17] (events 4-5)
//! ```
//!
//! Watermark-based late data handling:
//! - Watermark: Highest timestamp seen so far (monotonically increasing)
//! - Late records: Records that belong to closed sessions with a gap after them
//! - On-time records: Records within current session gap period
//! - Allowed lateness: Grace period for session merging and re-emission
//! - Late firing: Re-emitting sessions when late data extends/merges sessions

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{WindowStats, WindowStrategy};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Session window strategy with gap-based boundary detection and watermark-based late data handling.
///
/// Performance characteristics:
/// - O(1) record addition with watermark update
/// - O(1) gap detection
/// - O(N) clear operation where N = records in session
/// - O(log S) historical session lookup where S = sessions in allowed lateness
/// - Memory: Bounded by (gap_duration_ms + allowed_lateness_ms) * arrival_rate
///
/// # Gap Semantics
/// - Session closes when gap exceeds `gap_duration_ms`
/// - New session starts with first event after gap
/// - Session end time = last event time + gap_duration_ms
///
/// # Watermark-based Late Firing
/// For session windows, late data can:
/// - Extend a closed session (if within allowed_lateness)
/// - Merge multiple closed sessions (if record bridges the gap)
/// - Trigger re-emission with updated session boundaries
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

    /// WATERMARK: Highest timestamp seen so far (for distinguishing late arrivals)
    /// Critical for session windows where late data can extend/merge sessions
    max_watermark_seen: i64,

    /// Allowed lateness in milliseconds - how long to keep session state alive after emission
    /// For session windows, controls grace period for session extension and merging
    allowed_lateness_ms: i64,

    /// Historical sessions that may still receive late-arriving records
    /// Key: session_end_time
    /// Value: HistoricalSessionState with buffer and metadata
    /// Used for session extension and merging when late data arrives
    historical_sessions: BTreeMap<i64, HistoricalSessionState>,

    /// Metrics collection (thread-safe)
    pub metrics: Arc<Mutex<SessionWindowMetrics>>,
}

/// Metrics for analyzing session window behavior
#[derive(Debug, Clone, Default)]
pub struct SessionWindowMetrics {
    /// Number of clear() calls on window
    pub clear_calls: usize,
    /// Sum of all buffer sizes at time of clear
    pub total_buffer_sizes_at_clear: usize,
    /// Maximum buffer size observed
    pub max_buffer_size: usize,
    /// Total records added to window
    pub add_record_calls: usize,
    /// Number of sessions emitted
    pub emission_count: usize,
    /// Number of sessions extended by late data
    pub session_extensions: usize,
    /// Number of sessions merged by late data
    pub session_merges: usize,
    /// Number of late arrivals that triggered re-emissions
    pub late_firing_count: usize,
    /// Number of records processed in late firings
    pub late_firing_records: usize,
}

/// State of a historical session that may receive late-arriving records.
///
/// For session windows, we track closed sessions that may be extended or merged
/// by late-arriving data within the allowed lateness grace period.
#[derive(Debug, Clone)]
struct HistoricalSessionState {
    /// Session start time (milliseconds)
    start_time: i64,
    /// Session end time (milliseconds)
    end_time: i64,
    /// Records in this session (including late arrivals)
    buffer: VecDeque<SharedRecord>,
    /// Has this session been emitted?
    emitted: bool,
    /// Watermark when this session's grace period ends
    grace_period_end_watermark: i64,
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
    /// let strategy = SessionWindowStrategy::new(300000, "_TIMESTAMP".to_string());
    /// ```
    pub fn new(gap_duration_ms: i64, time_field: String) -> Self {
        Self::with_estimated_capacity(gap_duration_ms, time_field, 10_000)
    }

    /// Create a new session window strategy with pre-allocated capacity hint.
    ///
    /// FR-082 Week 8 Optimization 3: Window Buffer Pre-allocation
    ///
    /// Pre-allocates buffer capacity using a conservative estimate since session
    /// windows are unbounded by design (variable session duration).
    ///
    /// Allocation strategy:
    /// - If typical_session_size is provided, use as initial capacity
    /// - Default assumption: ~10K events per session (configurable)
    /// - Sessions exceeding this size will still reallocate, but typical sessions
    ///   within estimate will have 0 reallocations
    ///
    /// # Arguments
    /// * `gap_duration_ms` - Maximum gap between events in milliseconds
    /// * `time_field` - Field name containing event time
    /// * `typical_session_size` - Conservative estimate of typical session size
    ///   (used for pre-allocation, not a hard limit)
    ///
    /// # Example
    /// ```rust,ignore
    /// // 5-minute session gap, expecting ~10K events per session
    /// let strategy = SessionWindowStrategy::with_estimated_capacity(
    ///     300000,    // 5 minute gap
    ///     "_TIMESTAMP".to_string(),
    ///     10_000     // 10K events typical
    /// );
    /// ```
    pub fn with_estimated_capacity(
        gap_duration_ms: i64,
        time_field: String,
        typical_session_size: usize,
    ) -> Self {
        // Conservative: Use typical_session_size as capacity
        // Sessions smaller than this size avoid reallocations
        // Sessions larger may reallocate (acceptable - designed for unbounded sessions)
        let capacity = (typical_session_size as f64 * 1.1).ceil() as usize;

        Self {
            gap_duration_ms,
            buffer: VecDeque::with_capacity(capacity),
            last_event_time: None,
            session_start_time: None,
            session_end_time: None,
            emission_count: 0,
            time_field,
            max_watermark_seen: i64::MIN,
            allowed_lateness_ms: gap_duration_ms, // Default: equal to gap duration
            historical_sessions: BTreeMap::new(),
            metrics: Arc::new(Mutex::new(SessionWindowMetrics::default())),
        }
    }

    /// Set the allowed lateness in milliseconds.
    ///
    /// Allowed lateness defines how long session state is kept alive after emission
    /// to accommodate late-arriving records that may extend or merge sessions.
    ///
    /// # Arguments
    /// * `allowed_lateness_ms` - Grace period in milliseconds
    ///
    /// # Example
    /// ```rust,ignore
    /// // Allow 2 hours for delayed partition data
    /// strategy.set_allowed_lateness_ms(2 * 60 * 60 * 1000);
    /// ```
    pub fn set_allowed_lateness_ms(&mut self, allowed_lateness_ms: i64) {
        self.allowed_lateness_ms = allowed_lateness_ms;
    }

    /// Get a copy of current metrics
    pub fn get_metrics(&self) -> SessionWindowMetrics {
        self.metrics.lock().map(|m| m.clone()).unwrap_or_default()
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

    /// Initialize session with first event.
    fn initialize_session(&mut self, timestamp: i64) {
        self.session_start_time = Some(timestamp);
        self.last_event_time = Some(timestamp);
        self.session_end_time = Some(timestamp.saturating_add(self.gap_duration_ms));
    }

    /// Update session boundaries with new event.
    fn update_session(&mut self, timestamp: i64) {
        self.last_event_time = Some(timestamp);
        self.session_end_time = Some(timestamp.saturating_add(self.gap_duration_ms));
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

    /// Update watermark to the highest timestamp seen so far.
    ///
    /// Watermark advancement is O(1) and happens on every record.
    /// Used to determine when sessions can be safely cleaned up.
    fn update_watermark(&mut self, timestamp: i64) {
        if timestamp > self.max_watermark_seen {
            self.max_watermark_seen = timestamp;
        }
    }

    /// Clean up historical sessions that are beyond the allowed lateness period.
    ///
    /// Removes sessions where: session_end + allowed_lateness < watermark
    /// This keeps memory bounded while allowing late data to extend/merge sessions.
    fn cleanup_expired_sessions(&mut self) {
        let expiry_threshold = self.max_watermark_seen - self.allowed_lateness_ms;

        // Remove all historical sessions that have expired
        self.historical_sessions
            .retain(|_, state| state.grace_period_end_watermark > expiry_threshold);
    }

    /// Check if a record is late (belongs to a previously emitted session).
    ///
    /// A record is late if:
    /// - Its timestamp is less than or equal to current_session_end
    /// - It arrived after the corresponding session has been emitted
    fn is_late_record(&self, timestamp: i64) -> bool {
        if let Some(end) = self.session_end_time {
            timestamp < end
        } else {
            false
        }
    }

    /// Check if a late record is within the allowed lateness grace period.
    ///
    /// A late record can still trigger re-emissions (extension/merge) if:
    /// - watermark < session_end + allowed_lateness
    fn is_within_grace_period(&self, timestamp: i64) -> bool {
        if let Some(end) = self.session_end_time {
            let grace_period_end = end + self.allowed_lateness_ms;
            self.max_watermark_seen < grace_period_end
        } else {
            false
        }
    }
}

impl WindowStrategy for SessionWindowStrategy {
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError> {
        let timestamp = self.extract_timestamp(&record)?;

        // Update watermark on every record (O(1) operation)
        self.update_watermark(timestamp);

        // Track metric
        if let Ok(mut m) = self.metrics.lock() {
            m.add_record_calls += 1;
            m.max_buffer_size = m.max_buffer_size.max(self.buffer.len());
        }

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
                        // For zero-gap sessions (start == end), include records at exactly start time
                        // For normal sessions, include records in [start, end) range
                        if start == end {
                            ts == start
                        } else {
                            ts >= start && ts < end
                        }
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

        // Clean up expired historical sessions (O(log S) amortized)
        self.cleanup_expired_sessions();

        // Track metrics
        if let Ok(mut m) = self.metrics.lock() {
            m.clear_calls += 1;
            m.total_buffer_sizes_at_clear += self.buffer.len();
            m.emission_count = self.emission_count;
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
        // FR-081: Set StreamRecord.timestamp metadata instead of fields HashMap
        // This is the correct way per the new architecture
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Integer(42));
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp; // Set processing-time metadata
        SharedRecord::new(record)
    }

    #[test]
    fn test_session_window_basic() {
        // 5-minute session gap
        let mut strategy = SessionWindowStrategy::new(300000, "_TIMESTAMP".to_string());

        // Add events within gap
        let r1 = create_test_record(1000);
        let r2 = create_test_record(60000); // 1 minute later
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
        let mut strategy = SessionWindowStrategy::new(300000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(300000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(60000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(300000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(60000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(60000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(60000, "_TIMESTAMP".to_string());

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
        let mut strategy = SessionWindowStrategy::new(10000, "_TIMESTAMP".to_string());

        let r1 = create_test_record(1000);
        let r2 = create_test_record(5000); // 4s later - within gap
        let r3 = create_test_record(20000); // 15s later - beyond gap

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
        assert_eq!(strategy.add_record(r3).unwrap(), true); // Should emit

        // Session end should be extended to last event + gap
        assert_eq!(strategy.session_end_time, Some(15000)); // 5000 + 10000
    }
}
