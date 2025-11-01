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
        window_strategy: &dyn WindowStrategy,
    ) -> Result<EmitDecision, SqlError> {
        // Check if adding this record triggers window completion
        let window_needs_emit = window_strategy.should_emit(extract_timestamp(&record)?);

        if window_needs_emit && self.window_active {
            // Window is complete - emit and clear
            self.window_active = false;
            Ok(EmitDecision::EmitAndClear)
        } else {
            // Window still active or starting new window
            self.window_active = true;
            Ok(EmitDecision::Skip)
        }
    }
}

/// Helper to extract timestamp from record.
fn extract_timestamp(record: &SharedRecord) -> Result<i64, SqlError> {
    use crate::velostream::sql::execution::types::FieldValue;

    // Try common timestamp field names
    let field_names = vec!["event_time", "timestamp", "ts", "time"];

    for field_name in field_names {
        if let Some(value) = record.as_ref().fields.get(field_name) {
            match value {
                FieldValue::Integer(ts) => return Ok(*ts),
                FieldValue::Timestamp(dt) => {
                    return Ok(dt.and_utc().timestamp_millis());
                }
                _ => continue,
            }
        }
    }

    Err(SqlError::ExecutionError {
        message: "No valid timestamp field found in record".to_string(),
        query: None,
    })
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
    fn test_emit_final_strategy_creation() {
        let strategy = EmitFinalStrategy::new();
        assert_eq!(strategy.window_active, false);
    }

    #[test]
    fn test_emit_final_should_emit() {
        let strategy = EmitFinalStrategy::new();
        let record = create_test_record(1000);

        // Should not emit for incomplete window
        assert_eq!(strategy.should_emit_for_record(&record, false), false);

        // Should emit for complete window
        assert_eq!(strategy.should_emit_for_record(&record, true), true);
    }

    #[test]
    fn test_extract_timestamp() {
        let record = create_test_record(12345);
        assert_eq!(extract_timestamp(&record).unwrap(), 12345);
    }

    #[test]
    fn test_extract_timestamp_missing_field() {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Integer(42));
        let record = SharedRecord::new(StreamRecord::new(fields));

        assert!(extract_timestamp(&record).is_err());
    }
}
