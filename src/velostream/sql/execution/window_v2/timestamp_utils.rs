//! Timestamp Extraction Utilities for Window Strategies
//!
//! Provides shared timestamp extraction logic following FR-081 two-tier priority system:
//! 1. System columns (_TIMESTAMP, _EVENT_TIME) → access metadata directly (zero overhead)
//! 2. User payload fields → extract from fields HashMap (single lookup overhead)

use crate::velostream::sql::SqlError;
use crate::velostream::sql::execution::types::{FieldValue, system_columns};
use crate::velostream::sql::execution::window_v2::types::SharedRecord;

/// Extract timestamp from a StreamRecord using the two-tier priority system.
///
/// # FR-081 Two-Tier Priority System
///
/// ## Priority 1: System Columns (Zero Overhead)
/// System columns (prefixed with `_`) access StreamRecord metadata directly:
/// - `_TIMESTAMP`: Processing-time from Kafka message (rec.timestamp)
/// - `_EVENT_TIME`: Event-time with processing-time fallback (rec.event_time)
/// - Performance: Direct struct field access, no HashMap lookup
///
/// ## Priority 2: User Payload Fields (Single HashMap Lookup)
/// All non-prefixed fields are extracted from user payload:
/// - `"timestamp"`: User's timestamp field from JSON payload
/// - `"event_time"`: User's event_time field from JSON payload
/// - `"custom_time"`: Any custom timestamp field
/// - Supports FieldValue::Integer (milliseconds) and FieldValue::Timestamp
/// - Performance: O(1) HashMap lookup (~30-50 CPU cycles)
///
/// # Performance Characteristics
/// - System columns: O(1) struct field access (~5 CPU cycles)
/// - User fields: O(1) HashMap lookup (~30-50 CPU cycles)
/// - Memory: Zero allocations, all operations are read-only
///
/// # Arguments
/// * `record` - The stream record to extract timestamp from
/// * `time_field` - The field name to use for timestamp extraction
///
/// # Returns
/// * `Ok(i64)` - Timestamp in milliseconds since epoch
/// * `Err(SqlError)` - If timestamp extraction fails or field not found
///
/// # Example
/// ```rust,ignore
/// // Use system processing-time (zero overhead)
/// let ts = extract_record_timestamp(&record, "_TIMESTAMP")?;
///
/// // Use user's event_time field from JSON payload
/// let ts = extract_record_timestamp(&record, "event_time")?;
/// ```
pub(crate) fn extract_record_timestamp(
    record: &SharedRecord,
    time_field: &str,
) -> Result<i64, SqlError> {
    let rec = record.as_ref();

    // Priority 1: System columns - Direct metadata access (zero overhead)
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

    // Priority 2: User payload fields - HashMap lookup (single lookup overhead)
    // All non-prefixed fields are treated as user payload data
    match rec.fields.get(time_field) {
        Some(FieldValue::Integer(ts)) => Ok(*ts),
        Some(FieldValue::Timestamp(dt)) => {
            // Convert NaiveDateTime to milliseconds since epoch
            Ok(dt.and_utc().timestamp_millis())
        }
        Some(other) => Err(SqlError::ExecutionError {
            message: format!(
                "Time field '{}' has invalid type: {:?} (expected Integer or Timestamp)",
                time_field, other
            ),
            query: None,
        }),
        None => Err(SqlError::ExecutionError {
            message: format!(
                "Time field '{}' not found in record. Use _TIMESTAMP for system processing-time.",
                time_field
            ),
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
    fn test_user_payload_field_timestamp() {
        // User provides their own "timestamp" field in JSON payload
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(9876543210));
        let record = SharedRecord::new(StreamRecord::new(fields));

        let ts = extract_record_timestamp(&record, "timestamp").unwrap();
        assert_eq!(
            ts, 9876543210,
            "Should extract user's timestamp field from payload"
        );
    }

    #[test]
    fn test_user_payload_field_event_time() {
        // User provides their own "event_time" field in JSON payload
        let mut fields = HashMap::new();
        fields.insert("event_time".to_string(), FieldValue::Integer(5555555555));
        let record = SharedRecord::new(StreamRecord::new(fields));

        let ts = extract_record_timestamp(&record, "event_time").unwrap();
        assert_eq!(
            ts, 5555555555,
            "Should extract user's event_time field from payload"
        );
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
        assert!(result.unwrap_err().to_string().contains("invalid type"));
    }

    #[test]
    fn test_system_and_user_fields_coexist() {
        // User provides their own "timestamp" field that differs from system timestamp
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(1000000000));
        let mut record = StreamRecord::new(fields);
        record.timestamp = 9999999999; // System processing-time is different
        let shared_record = SharedRecord::new(record);

        // System column uses system metadata
        let system_ts = extract_record_timestamp(&shared_record, "_TIMESTAMP").unwrap();
        assert_eq!(system_ts, 9999999999, "System column should use metadata");

        // User field uses payload data
        let user_ts = extract_record_timestamp(&shared_record, "timestamp").unwrap();
        assert_eq!(user_ts, 1000000000, "User field should use payload data");

        // They are independent and don't interfere
        assert_ne!(
            system_ts, user_ts,
            "System and user timestamps should be independent"
        );
    }

    #[test]
    fn test_missing_field_helpful_error() {
        let record = create_test_record(1000);
        let result = extract_record_timestamp(&record, "nonexistent");
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("not found in record"));
        assert!(
            error_msg.contains("_TIMESTAMP"),
            "Error should suggest using _TIMESTAMP"
        );
    }
}
