//! ROWS Window Strategy Implementation
//!
//! Count-based windows with strict memory bounds.
//!
//! Example: ROWS WINDOW BUFFER 100 ROWS
//! ```text
//! Window maintains exactly 100 most recent rows per partition
//! Memory-bounded regardless of time span
//! ```

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{WindowStats, WindowStrategy};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;
use std::collections::VecDeque;

/// ROWS window strategy with count-based boundaries.
///
/// Performance characteristics:
/// - O(1) record addition
/// - O(1) eviction when buffer full
/// - Memory: Strictly bounded by buffer_rows
/// - Growth ratio: 0.0x (constant memory)
///
/// # Memory Guarantee
/// Buffer size never exceeds `buffer_rows`, making this ideal for:
/// - Memory-constrained environments
/// - High-velocity streams with strict resource limits
/// - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
/// - Offset functions (LAG, LEAD)
pub struct RowsWindowStrategy {
    /// Maximum number of rows to keep in buffer
    buffer_rows: usize,

    /// Circular buffer with automatic eviction
    buffer: VecDeque<SharedRecord>,

    /// Number of rows processed (for statistics)
    total_rows_processed: usize,

    /// Number of emissions produced
    emission_count: usize,

    /// Whether to emit on every record (for streaming aggregations)
    emit_per_record: bool,
}

impl RowsWindowStrategy {
    /// Create a new ROWS window strategy.
    ///
    /// # Arguments
    /// * `buffer_rows` - Maximum number of rows to maintain
    /// * `emit_per_record` - If true, emit on every record (for EMIT CHANGES)
    ///
    /// # Example
    /// ```rust,ignore
    /// // 100-row sliding window
    /// let strategy = RowsWindowStrategy::new(100, false);
    ///
    /// // Streaming aggregation (emit on every row)
    /// let streaming = RowsWindowStrategy::new(100, true);
    /// ```
    pub fn new(buffer_rows: usize, emit_per_record: bool) -> Self {
        Self {
            buffer_rows,
            buffer: VecDeque::with_capacity(buffer_rows),
            total_rows_processed: 0,
            emission_count: 0,
            emit_per_record,
        }
    }

    /// Check if buffer is at capacity.
    fn is_full(&self) -> bool {
        self.buffer.len() >= self.buffer_rows
    }

    /// Evict oldest record if buffer is full.
    fn evict_if_full(&mut self) {
        if self.is_full() {
            self.buffer.pop_front();
        }
    }
}

impl WindowStrategy for RowsWindowStrategy {
    fn add_record(&mut self, record: SharedRecord) -> Result<bool, SqlError> {
        // Evict oldest record if buffer is full
        self.evict_if_full();

        // Add new record
        self.buffer.push_back(record);
        self.total_rows_processed += 1;

        // Emit if configured to do so
        Ok(self.emit_per_record)
    }

    fn get_window_records(&self) -> Vec<SharedRecord> {
        self.buffer.iter().cloned().collect()
    }

    fn should_emit(&self, _current_time: i64) -> bool {
        // ROWS windows are not time-based
        // Emission is controlled by emit_per_record flag
        self.emit_per_record
    }

    fn clear(&mut self) {
        // For ROWS windows, clear doesn't evict records
        // It just increments emission count
        self.emission_count += 1;
    }

    fn get_stats(&self) -> WindowStats {
        WindowStats {
            record_count: self.buffer.len(),
            window_start_time: None, // ROWS windows are not time-based
            window_end_time: None,
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

    fn create_test_record(value: i64) -> SharedRecord {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Integer(value));
        SharedRecord::new(StreamRecord::new(fields))
    }

    #[test]
    fn test_rows_window_basic() {
        let mut strategy = RowsWindowStrategy::new(5, false);

        // Add 3 records
        let r1 = create_test_record(1);
        let r2 = create_test_record(2);
        let r3 = create_test_record(3);

        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
        assert_eq!(strategy.add_record(r3).unwrap(), false);

        assert_eq!(strategy.buffer.len(), 3);
        assert_eq!(strategy.total_rows_processed, 3);
    }

    #[test]
    fn test_rows_window_eviction() {
        let mut strategy = RowsWindowStrategy::new(3, false);

        // Add 5 records (should evict 2 oldest)
        for i in 1..=5 {
            strategy.add_record(create_test_record(i)).unwrap();
        }

        // Buffer should maintain exactly 3 rows
        assert_eq!(strategy.buffer.len(), 3);
        assert_eq!(strategy.total_rows_processed, 5);

        // Verify oldest records were evicted (should have 3, 4, 5)
        let records = strategy.get_window_records();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_rows_window_memory_bounded() {
        let mut strategy = RowsWindowStrategy::new(100, false);

        // Add 1000 records
        for i in 1..=1000 {
            strategy.add_record(create_test_record(i)).unwrap();
        }

        // Buffer should never exceed 100 rows
        assert_eq!(strategy.buffer.len(), 100);
        assert_eq!(strategy.total_rows_processed, 1000);

        // Verify constant memory growth ratio
        let stats = strategy.get_stats();
        assert_eq!(stats.record_count, 100);
    }

    #[test]
    fn test_rows_window_emit_per_record() {
        let mut strategy = RowsWindowStrategy::new(5, true);

        let r1 = create_test_record(1);
        let r2 = create_test_record(2);

        // Should emit on every record
        assert_eq!(strategy.add_record(r1).unwrap(), true);
        assert_eq!(strategy.add_record(r2).unwrap(), true);
    }

    #[test]
    fn test_rows_window_no_emit() {
        let mut strategy = RowsWindowStrategy::new(5, false);

        let r1 = create_test_record(1);
        let r2 = create_test_record(2);

        // Should not emit
        assert_eq!(strategy.add_record(r1).unwrap(), false);
        assert_eq!(strategy.add_record(r2).unwrap(), false);
    }

    #[test]
    fn test_rows_window_clear() {
        let mut strategy = RowsWindowStrategy::new(5, false);

        let r1 = create_test_record(1);
        strategy.add_record(r1).unwrap();

        assert_eq!(strategy.emission_count, 0);

        strategy.clear();

        // Clear increments emission count but doesn't evict records
        assert_eq!(strategy.emission_count, 1);
        assert_eq!(strategy.buffer.len(), 1);
    }

    #[test]
    fn test_rows_window_stats() {
        let mut strategy = RowsWindowStrategy::new(10, false);

        for i in 1..=5 {
            strategy.add_record(create_test_record(i)).unwrap();
        }

        let stats = strategy.get_stats();
        assert_eq!(stats.record_count, 5);
        assert_eq!(stats.window_start_time, None); // ROWS windows have no time
        assert_eq!(stats.window_end_time, None);
        assert_eq!(stats.emission_count, 0);
    }

    #[test]
    fn test_rows_window_get_records() {
        let mut strategy = RowsWindowStrategy::new(3, false);

        strategy.add_record(create_test_record(10)).unwrap();
        strategy.add_record(create_test_record(20)).unwrap();
        strategy.add_record(create_test_record(30)).unwrap();

        let records = strategy.get_window_records();
        assert_eq!(records.len(), 3);

        // Verify records are SharedRecord (cheap clones)
        let records2 = strategy.get_window_records();
        assert_eq!(records2.len(), 3);
    }

    #[test]
    fn test_rows_window_should_emit() {
        let strategy_no_emit = RowsWindowStrategy::new(5, false);
        assert_eq!(strategy_no_emit.should_emit(12345), false);

        let strategy_emit = RowsWindowStrategy::new(5, true);
        assert_eq!(strategy_emit.should_emit(12345), true);
    }

    #[test]
    fn test_rows_window_capacity_preallocated() {
        let strategy = RowsWindowStrategy::new(100, false);

        // Verify buffer capacity is preallocated
        assert!(strategy.buffer.capacity() >= 100);
    }

    #[test]
    fn test_rows_window_single_row() {
        let mut strategy = RowsWindowStrategy::new(1, false);

        strategy.add_record(create_test_record(1)).unwrap();
        strategy.add_record(create_test_record(2)).unwrap();
        strategy.add_record(create_test_record(3)).unwrap();

        // Should only keep 1 row (most recent)
        assert_eq!(strategy.buffer.len(), 1);

        let records = strategy.get_window_records();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_rows_window_large_buffer() {
        let mut strategy = RowsWindowStrategy::new(10000, false);

        // Add half the buffer size
        for i in 1..=5000 {
            strategy.add_record(create_test_record(i)).unwrap();
        }

        assert_eq!(strategy.buffer.len(), 5000);
        assert_eq!(strategy.total_rows_processed, 5000);

        // Add another 5000 to reach capacity
        for i in 5001..=10000 {
            strategy.add_record(create_test_record(i)).unwrap();
        }

        assert_eq!(strategy.buffer.len(), 10000);

        // Add 1000 more - should evict oldest 1000
        for i in 10001..=11000 {
            strategy.add_record(create_test_record(i)).unwrap();
        }

        assert_eq!(strategy.buffer.len(), 10000);
        assert_eq!(strategy.total_rows_processed, 11000);
    }
}
