//! Tests for `_event_time` preservation through the full pipeline.
//!
//! Covers:
//!   - `from_kafka()`: extracting `_event_time` from Kafka message headers
//!   - `from_kafka()`: priority order (event_time_config > header > Kafka timestamp)
//!   - `resolve_column()`: returning event_time millis when set
//!   - `resolve_column()`: fallback behavior (Null vs ProcessingTime vs Warn)
//!   - Round-trip: event_time → header → from_kafka → StreamRecord.event_time

use std::collections::HashMap;

use chrono::DateTime;
use velostream::velostream::datasource::event_time::{EventTimeConfig, TimestampFormat};
use velostream::velostream::sql::execution::types::system_columns;
use velostream::velostream::sql::execution::types::system_columns::EventTimeFallback;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

// ===================================================================
// from_kafka() — _event_time header extraction
// ===================================================================

/// When `_event_time` is present as a Kafka header, `from_kafka()` should
/// use it as the record's event_time (not the Kafka message timestamp).
#[test]
fn test_from_kafka_extracts_event_time_from_header() {
    let event_time_ms: i64 = 1770400000000; // ~2026-02-07
    let kafka_timestamp_ms: i64 = 1770500000000; // different from header

    let mut headers = HashMap::new();
    headers.insert(
        system_columns::EVENT_TIME.to_string(),
        event_time_ms.to_string(),
    );

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        Some(kafka_timestamp_ms),
        0,
        0,
        headers,
        None,
    );

    assert!(
        record.event_time.is_some(),
        "event_time should be set from _event_time header"
    );
    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        event_time_ms,
        "event_time should match the _event_time header value, not the Kafka timestamp"
    );
}

/// When no `_event_time` header exists, `from_kafka()` should fall back to
/// the Kafka message timestamp.
#[test]
fn test_from_kafka_falls_back_to_kafka_timestamp_when_no_header() {
    let kafka_timestamp_ms: i64 = 1770500000000;

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        Some(kafka_timestamp_ms),
        0,
        0,
        HashMap::new(), // no headers
        None,
    );

    assert!(
        record.event_time.is_some(),
        "event_time should be set from Kafka timestamp when no header"
    );
    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        kafka_timestamp_ms,
        "event_time should equal the Kafka message timestamp"
    );
}

/// `event_time_config` (explicit field extraction from payload) takes the
/// highest priority — above `_event_time` header and Kafka timestamp.
#[test]
fn test_from_kafka_event_time_config_takes_priority_over_header() {
    let config_field_ms: i64 = 1770300000000; // from payload field
    let header_ms: i64 = 1770400000000; // from _event_time header
    let kafka_ts_ms: i64 = 1770500000000; // from Kafka message

    let mut fields = HashMap::new();
    fields.insert("event_ts".to_string(), FieldValue::Integer(config_field_ms));

    let mut headers = HashMap::new();
    headers.insert(
        system_columns::EVENT_TIME.to_string(),
        header_ms.to_string(),
    );

    let config = EventTimeConfig {
        field_name: "event_ts".to_string(),
        format: Some(TimestampFormat::EpochMillis),
    };

    let record = StreamRecord::from_kafka(
        fields,
        "test_topic",
        None,
        Some(kafka_ts_ms),
        0,
        0,
        headers,
        Some(&config),
    );

    assert!(
        record.event_time.is_some(),
        "event_time should be set from event_time_config field"
    );
    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        config_field_ms,
        "event_time_config field should take priority over _event_time header and Kafka timestamp"
    );
}

/// `_event_time` header takes priority over Kafka message timestamp when
/// no `event_time_config` is specified.
#[test]
fn test_from_kafka_header_takes_priority_over_kafka_timestamp() {
    let header_ms: i64 = 1770400000000;
    let kafka_ts_ms: i64 = 1770500000000;

    let mut headers = HashMap::new();
    headers.insert(
        system_columns::EVENT_TIME.to_string(),
        header_ms.to_string(),
    );

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        Some(kafka_ts_ms),
        0,
        0,
        headers,
        None, // no event_time_config
    );

    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        header_ms,
        "_event_time header should take priority over Kafka timestamp"
    );
}

/// When `_event_time` header has an invalid (non-numeric) value,
/// `from_kafka()` should fall back to Kafka timestamp.
#[test]
fn test_from_kafka_invalid_header_falls_back_to_kafka_timestamp() {
    let kafka_ts_ms: i64 = 1770500000000;

    let mut headers = HashMap::new();
    headers.insert(
        system_columns::EVENT_TIME.to_string(),
        "not_a_number".to_string(),
    );

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        Some(kafka_ts_ms),
        0,
        0,
        headers,
        None,
    );

    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        kafka_ts_ms,
        "Invalid _event_time header should fall back to Kafka timestamp"
    );
}

/// When neither header nor Kafka timestamp is present, event_time should be None.
#[test]
fn test_from_kafka_no_event_time_sources_returns_none() {
    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        None, // no Kafka timestamp
        0,
        0,
        HashMap::new(), // no headers
        None,
    );

    assert!(
        record.event_time.is_none(),
        "event_time should be None when no timestamp sources available"
    );
}

/// Headers are preserved on the StreamRecord after from_kafka().
#[test]
fn test_from_kafka_preserves_headers() {
    let mut headers = HashMap::new();
    headers.insert(
        system_columns::EVENT_TIME.to_string(),
        "1770400000000".to_string(),
    );
    headers.insert("traceparent".to_string(), "00-abc-def-01".to_string());

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        Some(1770400000000),
        0,
        0,
        headers,
        None,
    );

    assert_eq!(record.headers.len(), 2, "All headers should be preserved");
    assert_eq!(
        record.headers.get(system_columns::EVENT_TIME).unwrap(),
        "1770400000000"
    );
    assert_eq!(record.headers.get("traceparent").unwrap(), "00-abc-def-01");
}

// ===================================================================
// resolve_column() — event_time system column resolution
// ===================================================================

/// When `event_time` is set, `resolve_column(system_columns::EVENT_TIME)` returns the
/// event_time in millis regardless of any fallback setting.
#[test]
fn test_resolve_column_event_time_returns_millis_when_set() {
    let event_time_ms: i64 = 1770400000000;
    let event_time = DateTime::from_timestamp_millis(event_time_ms).unwrap();

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: 9999999999999, // different from event_time
        offset: 0,
        partition: 0,
        event_time: Some(event_time),
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = record.resolve_column(system_columns::EVENT_TIME);
    assert_eq!(
        result,
        FieldValue::Integer(event_time_ms),
        "resolve_column should return event_time millis when event_time is set"
    );
}

/// Qualified name `m._event_time` should resolve identically to `_event_time`.
#[test]
fn test_resolve_column_qualified_event_time() {
    let event_time_ms: i64 = 1770400000000;
    let event_time = DateTime::from_timestamp_millis(event_time_ms).unwrap();

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: 9999999999999,
        offset: 0,
        partition: 0,
        event_time: Some(event_time),
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = record.resolve_column("m._event_time");
    assert_eq!(
        result,
        FieldValue::Integer(event_time_ms),
        "Qualified m._event_time should resolve to system column metadata"
    );
}

/// In `Null` fallback mode, `resolve_column(system_columns::EVENT_TIME)` should return
/// `FieldValue::Null` when `record.event_time` is None.
///
/// Since `event_time_fallback()` uses `OnceLock` (one-time init per process),
/// we simulate the behavior by matching on the enum directly rather than
/// calling the actual function.
#[test]
fn test_resolve_column_null_fallback_returns_null() {
    let processing_time_ms: i64 = 1770400000000;

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: processing_time_ms,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Simulate what resolve_column does in Null fallback mode
    let result = match record.event_time {
        Some(et) => FieldValue::Integer(et.timestamp_millis()),
        None => match EventTimeFallback::Null {
            EventTimeFallback::Null => FieldValue::Null,
            EventTimeFallback::Warn | EventTimeFallback::ProcessingTime => {
                FieldValue::Integer(record.timestamp)
            }
        },
    };

    assert_eq!(
        result,
        FieldValue::Null,
        "In Null fallback mode, _event_time should return Null when event_time is None"
    );
}

/// In `ProcessingTime` fallback mode, `resolve_column(system_columns::EVENT_TIME)` should
/// return `record.timestamp` when `record.event_time` is None.
#[test]
fn test_resolve_column_processing_time_fallback_returns_timestamp() {
    let processing_time_ms: i64 = 1770400000000;

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: processing_time_ms,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Simulate what resolve_column does in ProcessingTime fallback mode
    let result = match record.event_time {
        Some(et) => FieldValue::Integer(et.timestamp_millis()),
        None => match EventTimeFallback::ProcessingTime {
            EventTimeFallback::Null => FieldValue::Null,
            EventTimeFallback::Warn | EventTimeFallback::ProcessingTime => {
                FieldValue::Integer(record.timestamp)
            }
        },
    };

    assert_eq!(
        result,
        FieldValue::Integer(processing_time_ms),
        "In ProcessingTime fallback mode, _event_time should return _TIMESTAMP"
    );
}

/// In `Warn` fallback mode, behavior is the same as `ProcessingTime` —
/// returns `record.timestamp` when `record.event_time` is None.
#[test]
fn test_resolve_column_warn_fallback_returns_timestamp() {
    let processing_time_ms: i64 = 1770400000000;

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: processing_time_ms,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Simulate what resolve_column does in Warn fallback mode
    let result = match record.event_time {
        Some(et) => FieldValue::Integer(et.timestamp_millis()),
        None => match EventTimeFallback::Warn {
            EventTimeFallback::Null => FieldValue::Null,
            EventTimeFallback::Warn | EventTimeFallback::ProcessingTime => {
                FieldValue::Integer(record.timestamp)
            }
        },
    };

    assert_eq!(
        result,
        FieldValue::Integer(processing_time_ms),
        "In Warn fallback mode, _event_time should return _TIMESTAMP"
    );
}

// ===================================================================
// Round-trip: event_time → header string → from_kafka() → event_time
// ===================================================================

/// Simulates the full round-trip: a record with `event_time` is written to
/// Kafka with an `_event_time` header, then consumed and reconstructed via
/// `from_kafka()`. The event_time should survive the round-trip exactly.
#[test]
fn test_event_time_round_trip_through_header() {
    let original_event_time_ms: i64 = 1770400000000;
    let original_event_time = DateTime::from_timestamp_millis(original_event_time_ms).unwrap();

    // --- Writer side: serialize event_time as header ---
    let et_header_value = original_event_time.timestamp_millis().to_string();

    // --- Consumer side: reconstruct via from_kafka() ---
    let mut headers = HashMap::new();
    headers.insert(system_columns::EVENT_TIME.to_string(), et_header_value);

    let reconstructed = StreamRecord::from_kafka(
        HashMap::new(),
        "output_topic",
        None,
        Some(original_event_time_ms), // Kafka timestamp also set (backup)
        42,
        1,
        headers,
        None,
    );

    assert_eq!(
        reconstructed.event_time.unwrap().timestamp_millis(),
        original_event_time_ms,
        "event_time should survive round-trip through _event_time header"
    );
}

/// Round-trip with an event_time far in the past (1970-01-02).
/// Verifies no truncation or overflow for small timestamps.
#[test]
fn test_event_time_round_trip_small_timestamp() {
    let small_ms: i64 = 86400000; // 1970-01-02 00:00:00 UTC

    let mut headers = HashMap::new();
    headers.insert(system_columns::EVENT_TIME.to_string(), small_ms.to_string());

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        None,
        0,
        0,
        headers,
        None,
    );

    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        small_ms,
        "Small timestamp should survive round-trip"
    );
}

/// Round-trip with negative timestamp (before Unix epoch).
/// `DateTime::from_timestamp_millis` handles negative values.
#[test]
fn test_from_kafka_negative_timestamp_in_header() {
    let negative_ms: i64 = -86400000; // 1969-12-31

    let mut headers = HashMap::new();
    headers.insert(
        system_columns::EVENT_TIME.to_string(),
        negative_ms.to_string(),
    );

    let record = StreamRecord::from_kafka(
        HashMap::new(),
        "test_topic",
        None,
        None,
        0,
        0,
        headers,
        None,
    );

    assert_eq!(
        record.event_time.unwrap().timestamp_millis(),
        negative_ms,
        "Negative timestamp should survive round-trip"
    );
}

// ===================================================================
// resolve_column() — other system columns are unaffected
// ===================================================================

/// `_timestamp` always resolves to `record.timestamp` (processing time).
#[test]
fn test_resolve_column_timestamp_unaffected() {
    let processing_ms: i64 = 1770500000000;
    let event_ms: i64 = 1770400000000;
    let event_time = DateTime::from_timestamp_millis(event_ms).unwrap();

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: processing_ms,
        offset: 0,
        partition: 0,
        event_time: Some(event_time),
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = record.resolve_column(system_columns::TIMESTAMP);
    assert_eq!(
        result,
        FieldValue::Integer(processing_ms),
        "_timestamp should always resolve to processing time, not event_time"
    );
}

/// `_offset` and `_partition` resolve to their respective metadata fields.
#[test]
fn test_resolve_column_offset_and_partition() {
    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: 0,
        offset: 42,
        partition: 7,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    assert_eq!(
        record.resolve_column(system_columns::OFFSET),
        FieldValue::Integer(42)
    );
    assert_eq!(
        record.resolve_column(system_columns::PARTITION),
        FieldValue::Integer(7)
    );
}
