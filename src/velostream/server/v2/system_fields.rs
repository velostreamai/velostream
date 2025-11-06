//! System field injection helpers for Phase 4
//!
//! Provides utilities to inject system fields into StreamRecords:
//! - _WINDOW_START: Window start timestamp
//! - _WINDOW_END: Window end timestamp
//!
//! These fields are injected by window processors and consumed by the SQL engine
//! for SELECT, WHERE, GROUP BY, and ORDER BY clauses.

use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use chrono::{DateTime, Utc};

/// System field names (uppercase, with leading underscore)
pub const WINDOW_START: &str = "_WINDOW_START";
pub const WINDOW_END: &str = "_WINDOW_END";

/// Inject window boundary fields into a StreamRecord
///
/// ## Phase 4 Implementation
///
/// Window processors (TUMBLING, SLIDING, SESSION) inject window boundary timestamps
/// into records for SQL access:
///
/// ```sql
/// SELECT
///     symbol,
///     _WINDOW_START,  -- Accessible in SELECT
///     _WINDOW_END,
///     COUNT(*) as trades
/// FROM market_data
/// GROUP BY symbol, _WINDOW_START, _WINDOW_END  -- Accessible in GROUP BY
/// ORDER BY _WINDOW_END DESC  -- Accessible in ORDER BY
/// ```
///
/// ## Arguments
///
/// * `record` - Mutable reference to StreamRecord to modify
/// * `window_start` - Window start timestamp (UTC)
/// * `window_end` - Window end timestamp (UTC)
///
/// ## Example
///
/// ```rust
/// use chrono::Utc;
/// use velostream::velostream::server::v2::system_fields;
/// use velostream::velostream::sql::execution::types::StreamRecord;
/// use std::collections::HashMap;
///
/// let mut record = StreamRecord::new(HashMap::new());
/// let window_start = Utc::now();
/// let window_end = window_start + chrono::Duration::minutes(5);
///
/// system_fields::inject_window_fields(&mut record, window_start, window_end);
/// ```
pub fn inject_window_fields(
    record: &mut StreamRecord,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) {
    // Convert DateTime<Utc> to milliseconds since epoch for storage
    record.fields.insert(
        WINDOW_START.to_string(),
        FieldValue::Integer(window_start.timestamp_millis()),
    );
    record.fields.insert(
        WINDOW_END.to_string(),
        FieldValue::Integer(window_end.timestamp_millis()),
    );
}

/// Inject only window start field (for partial window emission scenarios)
///
/// ## Use Case
///
/// Early window emission in SESSION windows where end time is not yet determined.
pub fn inject_window_start_field(record: &mut StreamRecord, window_start: DateTime<Utc>) {
    record.fields.insert(
        WINDOW_START.to_string(),
        FieldValue::Integer(window_start.timestamp_millis()),
    );
}

/// Inject only window end field (for completion scenarios)
///
/// ## Use Case
///
/// Window closing/finalization in SESSION windows.
pub fn inject_window_end_field(record: &mut StreamRecord, window_end: DateTime<Utc>) {
    record.fields.insert(
        WINDOW_END.to_string(),
        FieldValue::Integer(window_end.timestamp_millis()),
    );
}

/// Extract window fields from a StreamRecord (if present)
///
/// ## Returns
///
/// - `(Some(start), Some(end))` if both fields present
/// - `(Some(start), None)` if only start present
/// - `(None, Some(end))` if only end present
/// - `(None, None)` if neither present
///
/// ## Example
///
/// ```rust
/// use velostream::velostream::server::v2::system_fields;
/// use velostream::velostream::sql::execution::types::StreamRecord;
/// use std::collections::HashMap;
///
/// let record = StreamRecord::new(HashMap::new());
/// let (start, end) = system_fields::extract_window_fields(&record);
/// assert!(start.is_none());
/// assert!(end.is_none());
/// ```
pub fn extract_window_fields(
    record: &StreamRecord,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let window_start = record.fields.get(WINDOW_START).and_then(|v| match v {
        FieldValue::Integer(millis) => DateTime::from_timestamp_millis(*millis),
        _ => None,
    });

    let window_end = record.fields.get(WINDOW_END).and_then(|v| match v {
        FieldValue::Integer(millis) => DateTime::from_timestamp_millis(*millis),
        _ => None,
    });

    (window_start, window_end)
}

/// Check if a record has window fields injected
///
/// ## Returns
///
/// - `true` if both _WINDOW_START and _WINDOW_END are present
/// - `false` otherwise
pub fn has_window_fields(record: &StreamRecord) -> bool {
    record.fields.contains_key(WINDOW_START) && record.fields.contains_key(WINDOW_END)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::collections::HashMap;

    #[test]
    fn test_inject_window_fields() {
        let mut record = StreamRecord::new(HashMap::new());
        let window_start = Utc::now();
        let window_end = window_start + Duration::minutes(5);

        inject_window_fields(&mut record, window_start, window_end);

        assert!(record.fields.contains_key(WINDOW_START));
        assert!(record.fields.contains_key(WINDOW_END));

        // Verify values are integers (milliseconds)
        match record.fields.get(WINDOW_START) {
            Some(FieldValue::Integer(millis)) => {
                assert_eq!(*millis, window_start.timestamp_millis());
            }
            _ => panic!("Expected Integer for _WINDOW_START"),
        }

        match record.fields.get(WINDOW_END) {
            Some(FieldValue::Integer(millis)) => {
                assert_eq!(*millis, window_end.timestamp_millis());
            }
            _ => panic!("Expected Integer for _WINDOW_END"),
        }
    }

    #[test]
    fn test_inject_partial_window_fields() {
        let mut record = StreamRecord::new(HashMap::new());
        let window_start = Utc::now();

        inject_window_start_field(&mut record, window_start);

        assert!(record.fields.contains_key(WINDOW_START));
        assert!(!record.fields.contains_key(WINDOW_END));
    }

    #[test]
    fn test_extract_window_fields() {
        let mut record = StreamRecord::new(HashMap::new());
        let window_start = Utc::now();
        let window_end = window_start + Duration::minutes(5);

        inject_window_fields(&mut record, window_start, window_end);

        let (extracted_start, extracted_end) = extract_window_fields(&record);

        // Compare timestamps with millisecond precision (microsecond precision is lost in conversion)
        assert!(extracted_start.is_some());
        assert!(extracted_end.is_some());

        let start_diff =
            (extracted_start.unwrap().timestamp_millis() - window_start.timestamp_millis()).abs();
        let end_diff =
            (extracted_end.unwrap().timestamp_millis() - window_end.timestamp_millis()).abs();

        assert_eq!(
            start_diff, 0,
            "Window start should match with millisecond precision"
        );
        assert_eq!(
            end_diff, 0,
            "Window end should match with millisecond precision"
        );
    }

    #[test]
    fn test_extract_window_fields_missing() {
        let record = StreamRecord::new(HashMap::new());

        let (start, end) = extract_window_fields(&record);

        assert!(start.is_none());
        assert!(end.is_none());
    }

    #[test]
    fn test_has_window_fields() {
        let mut record = StreamRecord::new(HashMap::new());
        assert!(!has_window_fields(&record));

        let window_start = Utc::now();
        inject_window_start_field(&mut record, window_start);
        assert!(!has_window_fields(&record)); // Only start, not both

        let window_end = window_start + Duration::minutes(5);
        inject_window_end_field(&mut record, window_end);
        assert!(has_window_fields(&record)); // Both present
    }

    #[test]
    fn test_window_field_constants() {
        assert_eq!(WINDOW_START, "_WINDOW_START");
        assert_eq!(WINDOW_END, "_WINDOW_END");
    }
}
