/// Time Extraction Utilities
///
/// Handles extraction of timestamps from records for windowing operations.
/// These are pure extraction functions with no engine state dependency.
use crate::velostream::sql::execution::{FieldValue, StreamRecord};

/// Utility class for extracting time values from records
pub struct TimeExtractor;

impl TimeExtractor {
    /// Extract event time from a record, either from a specific column or using record timestamp
    pub fn extract_event_time(record: &StreamRecord, time_column: Option<&str>) -> i64 {
        if let Some(column_name) = time_column {
            // Extract time from specified column
            if let Some(field_value) = record.fields.get(column_name) {
                match field_value {
                    FieldValue::Integer(ts) => *ts,
                    FieldValue::Float(ts) => *ts as i64,
                    FieldValue::Timestamp(ts) => ts.and_utc().timestamp_millis(),
                    FieldValue::String(s) => {
                        // Try to parse as timestamp
                        s.parse::<i64>().unwrap_or(record.timestamp)
                    }
                    _ => record.timestamp, // Fallback to record timestamp
                }
            } else {
                record.timestamp // Column not found, use record timestamp
            }
        } else {
            record.timestamp // Use record timestamp (processing time)
        }
    }

    /// Align timestamp to window boundary for tumbling windows
    pub fn align_to_window_boundary(timestamp: i64, window_size_ms: i64) -> i64 {
        (timestamp / window_size_ms) * window_size_ms
    }
}
