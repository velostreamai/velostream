//! Timestamp Extraction Utilities for Window Strategies
//!
//! Provides shared timestamp extraction logic following FR-081 three-tier priority system:
//! 1. System columns (_TIMESTAMP, _EVENT_TIME) → access metadata directly
//! 2. Regular fields (timestamp, event_time) → use get_event_time() for proper event-time semantics
//! 3. Legacy user fields → extract from fields HashMap (backward compatibility)

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::types::{FieldValue, system_columns};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;

/// Extract timestamp from a StreamRecord using the three-tier priority system.
///
/// # FR-081 Three-Tier Priority System
///
/// ## Priority 1: System Columns
/// System columns (_TIMESTAMP, _EVENT_TIME) access StreamRecord metadata directly:
/// - `_TIMESTAMP`: Processing-time (record.timestamp)
/// - `_EVENT_TIME`: Event-time with processing-time fallback (record.event_time)
///
/// ## Priority 2: Regular Fields
/// Standard timestamp field names use StreamRecord.get_event_time():
/// - "timestamp": Processing-time field
/// - "event_time": Event-time field
/// - Uses get_event_time() which provides proper event-time with fallback
///
/// ## Priority 3: Legacy Fields
/// User-defined timestamp fields extracted from fields HashMap:
/// - Supports backward compatibility with existing queries
/// - Handles FieldValue::Integer (milliseconds) and FieldValue::Timestamp
///
/// # Arguments
/// * `record` - The stream record to extract timestamp from
/// * `time_field` - The field name to use for timestamp extraction
///
/// # Returns
/// * `Ok(i64)` - Timestamp in milliseconds since epoch
/// * `Err(SqlError)` - If timestamp extraction fails
///
/// # Example
/// ```rust,ignore
/// let timestamp = extract_record_timestamp(&record, "event_time")?;
/// ```
pub(crate) fn extract_record_timestamp(
    record: &SharedRecord,
    time_field: &str,
) -> Result<i64, SqlError> {
    let rec = record.as_ref();

    // FR-081: Priority 1 - System columns access metadata directly
    if time_field.starts_with('_') {
        return match time_field {
            system_columns::TIMESTAMP => Ok(rec.timestamp),
            system_columns::EVENT_TIME => {
                if let Some(event_time) = rec.event_time {
                    Ok(event_time.timestamp_millis())
                } else {
                    // Fall back to processing-time if event-time not set
                    Ok(rec.timestamp)
                }
            }
            _ => Err(SqlError::ExecutionError {
                message: format!("Unknown system column '{}'", time_field),
                query: None,
            }),
        };
    }

    // FR-081: Priority 2 - Regular fields use get_event_time()
    // This provides event-time with processing-time fallback
    // NOTE: This is the recommended approach per StreamRecord documentation
    if time_field == "timestamp" || time_field == "event_time" {
        return Ok(rec.get_event_time().timestamp_millis());
    }

    // Priority 3 - Legacy: Extract from user payload fields (backward compatibility)
    match rec.fields.get(time_field) {
        Some(FieldValue::Integer(ts)) => Ok(*ts),
        Some(FieldValue::Timestamp(dt)) => {
            // Convert NaiveDateTime to milliseconds since epoch
            Ok(dt.and_utc().timestamp_millis())
        }
        Some(other) => Err(SqlError::ExecutionError {
            message: format!("Time field '{}' has wrong type: {:?}", time_field, other),
            query: None,
        }),
        None => Err(SqlError::ExecutionError {
            message: format!("Time field '{}' not found in record", time_field),
            query: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::sql::execution::types::StreamRecord;
    use std::collections::HashMap;

    fn create_test_record(timestamp: i64) -> SharedRecord {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), FieldValue::Integer(42));
        let mut record = StreamRecord::new(fields);
        record.timestamp = timestamp;
        SharedRecord::new(record)
    }

    #[test]
    fn test_system_column_timestamp() {
        let record = create_test_record(1234567890);
        let ts = extract_record_timestamp(&record, "_TIMESTAMP").unwrap();
        assert_eq!(ts, 1234567890);
    }

    #[test]
    fn test_regular_field_timestamp() {
        let record = create_test_record(9876543210);
        let ts = extract_record_timestamp(&record, "timestamp").unwrap();
        assert_eq!(ts, 9876543210);
    }

    #[test]
    fn test_regular_field_event_time() {
        let record = create_test_record(5555555555);
        let ts = extract_record_timestamp(&record, "event_time").unwrap();
        assert_eq!(ts, 5555555555);
    }

    #[test]
    fn test_legacy_field_integer() {
        let mut fields = HashMap::new();
        fields.insert("custom_time".to_string(), FieldValue::Integer(1111111111));
        let record = SharedRecord::new(StreamRecord::new(fields));
        let ts = extract_record_timestamp(&record, "custom_time").unwrap();
        assert_eq!(ts, 1111111111);
    }

    #[test]
    fn test_unknown_system_column_error() {
        let record = create_test_record(1000);
        let result = extract_record_timestamp(&record, "_UNKNOWN");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown system column")
        );
    }

    #[test]
    fn test_missing_field_error() {
        let record = create_test_record(1000);
        let result = extract_record_timestamp(&record, "nonexistent");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not found in record")
        );
    }

    #[test]
    fn test_wrong_type_error() {
        let mut fields = HashMap::new();
        fields.insert(
            "bad_time".to_string(),
            FieldValue::String("not a timestamp".to_string()),
        );
        let record = SharedRecord::new(StreamRecord::new(fields));
        let result = extract_record_timestamp(&record, "bad_time");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("wrong type"));
    }
}
