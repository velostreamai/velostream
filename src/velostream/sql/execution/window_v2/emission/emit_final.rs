//! Emit Final Strategy Implementation
//!
//! Emits window results once when the window completes.
//!
//! This is the standard emission strategy for batch-oriented window processing.

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::window_v2::traits::{
    EmissionStrategy, EmitDecision, WindowStrategy,
};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;

/// Emit final strategy - emits once per window at completion.
///
/// Characteristics:
/// - One emission per window
/// - Emits when window boundary reached
/// - Suitable for batch analytics
/// - Lower output volume than EMIT CHANGES
pub struct EmitFinalStrategy {
    /// Track if we're in the middle of a window
    window_active: bool,
}

impl EmitFinalStrategy {
    /// Create a new emit final strategy.
    pub fn new() -> Self {
        Self {
            window_active: false,
        }
    }
}

impl Default for EmitFinalStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl EmissionStrategy for EmitFinalStrategy {
    fn should_emit_for_record(&self, _record: &SharedRecord, window_complete: bool) -> bool {
        // Only emit when window completes
        window_complete
    }

    fn process_record(
        &mut self,
        record: SharedRecord,
        window_strategy: &mut dyn WindowStrategy,
    ) -> Result<EmitDecision, SqlError> {
        // CRITICAL: Add the record to the window buffer
        // add_record() returns true if window boundary is crossed and should emit
        let window_needs_emit = window_strategy.add_record(record)?;

        if window_needs_emit {
            // Window is complete - emit and clear
            // Keep window_active true because a new window starts after clear()
            self.window_active = true;
            Ok(EmitDecision::EmitAndClear)
        } else {
            // Window still active
            self.window_active = true;
            Ok(EmitDecision::Skip)
        }
    }
}

/// Helper to extract timestamp from record.
///
/// FR-081: Use StreamRecord.get_event_time() for proper event-time/processing-time handling
/// This aligns with window strategies and provides correct semantics
fn extract_timestamp(record: &SharedRecord) -> Result<i64, SqlError> {
    // FR-081: Use StreamRecord's get_event_time() method which provides:
    // - Event-time if set (record.event_time)
    // - Processing-time fallback (record.timestamp)
    // This is the canonical way to get timestamps for windowing per StreamRecord documentation
    Ok(record.as_ref().get_event_time().timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
    use std::collections::HashMap;

    fn create_test_record(timestamp: i64) -> SharedRecord {
        // FR-081: Set StreamRecord.timestamp metadata instead of fields HashMap
        // This is the correct way per the new architecture
        let fields = HashMap::new();
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp; // Set processing-time metadata
        SharedRecord::new(record)
    }

    #[test]
    fn test_emit_final_strategy_creation() {
        let strategy = EmitFinalStrategy::new();
        assert!(!strategy.window_active);
    }

    #[test]
    fn test_emit_final_should_emit() {
        let strategy = EmitFinalStrategy::new();
        let record = create_test_record(1000);

        // Should not emit for incomplete window
        assert!(!strategy.should_emit_for_record(&record, false));

        // Should emit for complete window
        assert!(strategy.should_emit_for_record(&record, true));
    }

    #[test]
    fn test_extract_timestamp() {
        let record = create_test_record(12345);
        assert_eq!(extract_timestamp(&record).unwrap(), 12345);
    }

    #[test]
    fn test_extract_timestamp_without_user_fields() {
        // FR-081: extract_timestamp now uses get_event_time() which always succeeds
        // It uses record.event_time if set, otherwise fallback to record.timestamp
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Integer(42));
        let mut record_data = StreamRecord::new(fields);
        record_data.timestamp = 9999; // Set processing-time
        let record = SharedRecord::new(record_data);

        // Should succeed and return processing-time
        assert_eq!(extract_timestamp(&record).unwrap(), 9999);
    }
}
