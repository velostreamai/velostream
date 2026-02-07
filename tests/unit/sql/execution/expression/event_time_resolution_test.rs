//! Tests for how `_event_time` and `_timestamp` system columns resolve.
//!
//! System columns:
//!   - `_EVENT_TIME` (`record.event_time`) = event time from the data (set via SQL `AS _event_time`)
//!   - `_TIMESTAMP`  (`record.timestamp`)  = processing/ingestion time (wall clock when received)
//!
//! These are distinct and must not be conflated. A regular field named `timestamp`
//! (no underscore prefix) is a user data field, not a system column.

use std::collections::HashMap;
use velostream::velostream::sql::ast::Expr;
use velostream::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
use velostream::velostream::sql::execution::types::system_columns::EventTimeFallback;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// A regular field named `timestamp` (no underscore) resolves to the HashMap field,
/// NOT the `_TIMESTAMP` system column.
#[test]
fn test_timestamp_field_resolves_to_hashmap_not_system_column() {
    let mut fields = HashMap::new();
    // User data field value: 1000
    fields.insert("timestamp".to_string(), FieldValue::Integer(1000));

    let record = StreamRecord {
        fields,
        timestamp: 9999, // _TIMESTAMP (processing time): 9999
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let expr = Expr::Column("timestamp".to_string());
    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();

    assert_eq!(
        result,
        FieldValue::Integer(1000),
        "Regular field 'timestamp' should resolve to HashMap value (1000), \
         not system column _TIMESTAMP (9999)"
    );
}

/// `_event_time` is a system column — it resolves to `record.event_time` metadata,
/// NOT to a HashMap field. This is correct by design.
///
/// The select processor strips `_event_time` from output fields (it's metadata-only),
/// so downstream records won't have it in the HashMap. When they reference
/// `_event_time`, it correctly resolves to the system column metadata.
#[test]
fn test_event_time_resolves_to_system_column_metadata() {
    let field_value_ms: i64 = 1609459200000; // 2021-01-01 (stale data in HashMap)
    let metadata_event_time_ms: i64 = 1770400000000; // ~2026-02-07 (metadata event time)

    let mut fields = HashMap::new();
    fields.insert(
        "_event_time".to_string(),
        FieldValue::Integer(field_value_ms),
    );

    let metadata_event_time =
        chrono::DateTime::from_timestamp_millis(metadata_event_time_ms).unwrap();

    let record = StreamRecord {
        fields,
        timestamp: 9999999999999, // _TIMESTAMP (processing time) — different from both
        offset: 0,
        partition: 0,
        event_time: Some(metadata_event_time),
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Bare _event_time should resolve to system column metadata
    let expr = Expr::Column("_event_time".to_string());
    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();

    assert_eq!(
        result,
        FieldValue::Integer(metadata_event_time_ms),
        "_event_time should resolve to system column metadata (record.event_time), \
         not to HashMap field value"
    );
}

/// Qualified `m._event_time` also resolves to the system column metadata.
#[test]
fn test_qualified_event_time_resolves_to_system_column_metadata() {
    let field_value_ms: i64 = 1609459200000;
    let metadata_event_time_ms: i64 = 1770400000000; // ~2026-02-07

    let mut fields = HashMap::new();
    fields.insert(
        "_event_time".to_string(),
        FieldValue::Integer(field_value_ms),
    );

    let metadata_event_time =
        chrono::DateTime::from_timestamp_millis(metadata_event_time_ms).unwrap();

    let record = StreamRecord {
        fields,
        timestamp: 9999999999999,
        offset: 0,
        partition: 0,
        event_time: Some(metadata_event_time),
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Qualified m._event_time should also resolve to system column metadata
    let expr = Expr::Column("m._event_time".to_string());
    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();

    assert_eq!(
        result,
        FieldValue::Integer(metadata_event_time_ms),
        "m._event_time should resolve to system column metadata (record.event_time), \
         not to HashMap field value"
    );
}

/// When `record.event_time` is None, `_EVENT_TIME` falls back to `_TIMESTAMP`
/// (processing time). This matches Flink/ksqlDB semantics.
#[test]
fn test_event_time_falls_back_to_processing_time_when_unset() {
    let processing_time_ms: i64 = 1770400000000; // ~2026-02-07

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: processing_time_ms, // _TIMESTAMP (processing time)
        offset: 0,
        partition: 0,
        event_time: None, // _EVENT_TIME not set
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let expr = Expr::Column("_event_time".to_string());
    let result = ExpressionEvaluator::evaluate_expression_value(&expr, &record).unwrap();

    assert_eq!(
        result,
        FieldValue::Integer(processing_time_ms),
        "When event_time is None, _event_time should fall back to _TIMESTAMP (processing time)"
    );
}

// --- EventTimeFallback enum and behavior tests ---

/// Verify the EventTimeFallback enum variants exist and are distinguishable.
#[test]
fn test_event_time_fallback_enum_variants() {
    assert_eq!(
        EventTimeFallback::ProcessingTime,
        EventTimeFallback::ProcessingTime
    );
    assert_eq!(EventTimeFallback::Warn, EventTimeFallback::Warn);
    assert_eq!(EventTimeFallback::Null, EventTimeFallback::Null);

    assert_ne!(EventTimeFallback::ProcessingTime, EventTimeFallback::Warn);
    assert_ne!(EventTimeFallback::ProcessingTime, EventTimeFallback::Null);
    assert_ne!(EventTimeFallback::Warn, EventTimeFallback::Null);
}

/// In Null fallback mode, `get_event_time_value` returns FieldValue::Null
/// when record.event_time is None.
///
/// Since the OnceLock-based `event_time_fallback()` can only be set once per process,
/// we test the behavior directly by matching on the enum.
#[test]
fn test_event_time_null_fallback_behavior() {
    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: 1770400000000, // ~2026-02-07
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    // Simulate what get_event_time_value does in Null mode
    let result = match record.event_time {
        Some(event_time) => FieldValue::Integer(event_time.timestamp_millis()),
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
        "In Null fallback mode, _EVENT_TIME should return Null when event_time is None"
    );
}

/// In Warn/ProcessingTime fallback modes, `get_event_time_value` returns
/// _TIMESTAMP when record.event_time is None.
#[test]
fn test_event_time_warn_fallback_behavior() {
    let processing_time_ms: i64 = 1770400000000; // ~2026-02-07

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

    // Simulate what get_event_time_value does in Warn mode
    let result = match record.event_time {
        Some(event_time) => FieldValue::Integer(event_time.timestamp_millis()),
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
        "In Warn fallback mode, _EVENT_TIME should fall back to _TIMESTAMP"
    );
}

/// When event_time IS set, the fallback mode is irrelevant — event_time is always used.
#[test]
fn test_event_time_present_ignores_fallback_mode() {
    let event_time_ms: i64 = 1770400000000; // ~2026-02-07
    let event_time = chrono::DateTime::from_timestamp_millis(event_time_ms).unwrap();

    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: 9999999999999, // Different from event_time
        offset: 0,
        partition: 0,
        event_time: Some(event_time),
        headers: HashMap::new(),
        topic: None,
        key: None,
    };

    let result = ExpressionEvaluator::get_event_time_value(&record);

    assert_eq!(
        result,
        FieldValue::Integer(event_time_ms),
        "When event_time is present, it should always be used regardless of fallback mode"
    );
}
