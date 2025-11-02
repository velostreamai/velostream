//! Emit Changes Strategy Implementation
//!
//! Emits window results continuously as aggregations change.
//!
//! This is the streaming emission strategy for real-time analytics.

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{
    EmissionStrategy, EmitDecision, WindowStrategy,
};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;

/// Emit changes strategy - emits on every record.
///
/// Characteristics:
/// - Continuous emissions as window state changes
/// - High output volume for real-time monitoring
/// - Suitable for streaming dashboards
/// - Every record triggers a window recalculation and emission
///
/// # Use Cases
/// - Real-time dashboards requiring instant updates
/// - Streaming aggregations (running totals, moving averages)
/// - Change data capture (CDC) pipelines
/// - Event-driven analytics
pub struct EmitChangesStrategy {
    /// Track total emissions for statistics
    emission_count: usize,

    /// Emit on every Nth record (1 = every record, 10 = every 10th record)
    emit_frequency: usize,

    /// Current record count since last emission
    record_count: usize,
}

impl EmitChangesStrategy {
    /// Create a new emit changes strategy.
    ///
    /// # Arguments
    /// * `emit_frequency` - Emit on every Nth record (1 = every record)
    ///
    /// # Example
    /// ```rust,ignore
    /// // Emit on every record
    /// let strategy = EmitChangesStrategy::new(1);
    ///
    /// // Emit on every 10th record (throttled streaming)
    /// let throttled = EmitChangesStrategy::new(10);
    /// ```
    pub fn new(emit_frequency: usize) -> Self {
        Self {
            emission_count: 0,
            emit_frequency: emit_frequency.max(1), // Minimum frequency is 1
            record_count: 0,
        }
    }

    /// Check if it's time to emit based on frequency.
    fn should_emit_now(&mut self) -> bool {
        self.record_count += 1;
        if self.record_count >= self.emit_frequency {
            self.record_count = 0;
            true
        } else {
            false
        }
    }
}

impl Default for EmitChangesStrategy {
    fn default() -> Self {
        Self::new(1)
    }
}

impl EmissionStrategy for EmitChangesStrategy {
    fn should_emit_for_record(&self, _record: &SharedRecord, _window_complete: bool) -> bool {
        // EMIT CHANGES emits on every record (or based on frequency)
        true
    }

    fn process_record(
        &mut self,
        _record: SharedRecord,
        _window_strategy: &dyn WindowStrategy,
    ) -> Result<EmitDecision, SqlError> {
        // Check if we should emit based on frequency
        if self.should_emit_now() {
            self.emission_count += 1;
            // Don't clear buffer - EMIT CHANGES maintains running state
            Ok(EmitDecision::Emit)
        } else {
            Ok(EmitDecision::Skip)
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
        SharedRecord::new(StreamRecord::new(fields))
    }

    #[test]
    fn test_emit_changes_strategy_creation() {
        let strategy = EmitChangesStrategy::new(1);
        assert_eq!(strategy.emission_count, 0);
        assert_eq!(strategy.emit_frequency, 1);
        assert_eq!(strategy.record_count, 0);
    }

    #[test]
    fn test_emit_changes_default() {
        let strategy = EmitChangesStrategy::default();
        assert_eq!(strategy.emit_frequency, 1);
    }

    #[test]
    fn test_emit_changes_should_emit() {
        let strategy = EmitChangesStrategy::new(1);
        let record = create_test_record(1000);

        // Should emit for every record
        assert_eq!(strategy.should_emit_for_record(&record, false), true);
        assert_eq!(strategy.should_emit_for_record(&record, true), true);
    }

    #[test]
    fn test_emit_changes_frequency_1() {
        let mut strategy = EmitChangesStrategy::new(1);

        // Should emit on every record
        assert_eq!(strategy.should_emit_now(), true);
        assert_eq!(strategy.should_emit_now(), true);
        assert_eq!(strategy.should_emit_now(), true);
    }

    #[test]
    fn test_emit_changes_frequency_10() {
        let mut strategy = EmitChangesStrategy::new(10);

        // Should emit on 10th, 20th, 30th records
        for i in 1..=9 {
            assert_eq!(
                strategy.should_emit_now(),
                false,
                "Record {} should not emit",
                i
            );
        }
        assert_eq!(strategy.should_emit_now(), true, "Record 10 should emit");

        for i in 1..=9 {
            assert_eq!(
                strategy.should_emit_now(),
                false,
                "Record {} should not emit",
                i + 10
            );
        }
        assert_eq!(strategy.should_emit_now(), true, "Record 20 should emit");
    }

    #[test]
    fn test_emit_changes_zero_frequency() {
        // Zero frequency should be treated as 1
        let strategy = EmitChangesStrategy::new(0);
        assert_eq!(strategy.emit_frequency, 1);
    }

    #[test]
    fn test_emit_changes_process_record() {
        use crate::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;

        let mut strategy = EmitChangesStrategy::new(1);
        let window = TumblingWindowStrategy::new(60000, "event_time".to_string());

        let record = create_test_record(1000);

        // Process record - should emit
        let decision = strategy.process_record(record, &window).unwrap();
        assert!(matches!(decision, EmitDecision::Emit));
        assert_eq!(strategy.emission_count, 1);
    }

    #[test]
    fn test_emit_changes_throttled() {
        use crate::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;

        let mut strategy = EmitChangesStrategy::new(5);
        let window = TumblingWindowStrategy::new(60000, "event_time".to_string());

        // Process 4 records - should skip
        for i in 1..=4 {
            let record = create_test_record(i * 1000);
            let decision = strategy.process_record(record, &window).unwrap();
            assert!(
                matches!(decision, EmitDecision::Skip),
                "Record {} should skip",
                i
            );
        }

        // 5th record - should emit
        let record = create_test_record(5000);
        let decision = strategy.process_record(record, &window).unwrap();
        assert!(matches!(decision, EmitDecision::Emit));
        assert_eq!(strategy.emission_count, 1);
    }

    #[test]
    fn test_emit_changes_emission_count() {
        use crate::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;

        let mut strategy = EmitChangesStrategy::new(3);
        let window = TumblingWindowStrategy::new(60000, "event_time".to_string());

        // Process 9 records - should emit 3 times (on 3rd, 6th, 9th)
        for i in 1..=9 {
            let record = create_test_record(i * 1000);
            strategy.process_record(record, &window).unwrap();
        }

        assert_eq!(strategy.emission_count, 3);
    }

    #[test]
    fn test_emit_changes_no_clear() {
        use crate::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;

        let mut strategy = EmitChangesStrategy::new(1);
        let window = TumblingWindowStrategy::new(60000, "event_time".to_string());

        let record = create_test_record(1000);

        // Process record - should return Emit, not EmitAndClear
        let decision = strategy.process_record(record, &window).unwrap();
        assert!(matches!(decision, EmitDecision::Emit));
        assert!(!matches!(decision, EmitDecision::EmitAndClear));
    }

    #[test]
    fn test_emit_changes_continuous_streaming() {
        use crate::velostream::sql::execution::window_v2::strategies::SlidingWindowStrategy;

        let mut strategy = EmitChangesStrategy::new(1);
        let window = SlidingWindowStrategy::new(60000, 30000, "event_time".to_string());

        // Simulate continuous stream - every record should emit
        for i in 1..=100 {
            let record = create_test_record(i * 1000);
            let decision = strategy.process_record(record, &window).unwrap();
            assert!(
                matches!(decision, EmitDecision::Emit),
                "Record {} should emit",
                i
            );
        }

        assert_eq!(strategy.emission_count, 100);
    }

    #[test]
    fn test_emit_changes_large_frequency() {
        let mut strategy = EmitChangesStrategy::new(1000);

        // Should not emit until 1000th record
        for i in 1..=999 {
            assert_eq!(
                strategy.should_emit_now(),
                false,
                "Record {} should not emit",
                i
            );
        }
        assert_eq!(strategy.should_emit_now(), true, "Record 1000 should emit");
    }
}
