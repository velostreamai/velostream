//! Tests for temporal type comparisons in ExpressionEvaluator.

use chrono::{NaiveDate, NaiveDateTime};
use std::collections::HashMap;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, LiteralValue};
use velostream::velostream::sql::execution::expression::evaluator::ExpressionEvaluator;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

fn make_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
    StreamRecord {
        fields: fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        timestamp: 0,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: None,
        key: None,
    }
}

fn eval_literal_bool(
    record: &StreamRecord,
    left: &str,
    op: BinaryOperator,
    right_literal: Expr,
) -> bool {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column(left.to_string())),
        op,
        right: Box::new(right_literal),
    };
    ExpressionEvaluator::evaluate_expression(&expr, record).unwrap()
}

fn eval_bool(record: &StreamRecord, left: &str, op: BinaryOperator, right: &str) -> bool {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column(left.to_string())),
        op,
        right: Box::new(Expr::Column(right.to_string())),
    };
    ExpressionEvaluator::evaluate_expression(&expr, record).unwrap()
}

#[test]
fn test_date_date_less_than() {
    let d1 = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
    let record = make_record(vec![
        ("a", FieldValue::Date(d1)),
        ("b", FieldValue::Date(d2)),
    ]);
    assert!(eval_bool(&record, "a", BinaryOperator::LessThan, "b"));
    assert!(!eval_bool(&record, "b", BinaryOperator::LessThan, "a"));
}

#[test]
fn test_date_date_equality() {
    let d = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
    let record = make_record(vec![("a", FieldValue::Date(d)), ("b", FieldValue::Date(d))]);
    assert!(eval_bool(&record, "a", BinaryOperator::Equal, "b"));
}

#[test]
fn test_timestamp_timestamp_less_than() {
    let dt1 = NaiveDateTime::parse_from_str("2025-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let dt2 = NaiveDateTime::parse_from_str("2025-06-15T12:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let record = make_record(vec![
        ("a", FieldValue::Timestamp(dt1)),
        ("b", FieldValue::Timestamp(dt2)),
    ]);
    assert!(eval_bool(&record, "a", BinaryOperator::LessThan, "b"));
}

#[test]
fn test_date_timestamp_cross_comparison() {
    let d = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
    // Noon on same day — Date (midnight) < Timestamp (noon)
    let dt = NaiveDateTime::parse_from_str("2025-06-15T12:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let record = make_record(vec![
        ("the_date", FieldValue::Date(d)),
        ("the_ts", FieldValue::Timestamp(dt)),
    ]);
    assert!(eval_bool(
        &record,
        "the_date",
        BinaryOperator::LessThan,
        "the_ts"
    ));
}

#[test]
fn test_timestamp_date_cross_comparison() {
    let dt = NaiveDateTime::parse_from_str("2025-06-15T23:59:59", "%Y-%m-%dT%H:%M:%S").unwrap();
    let d = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    let record = make_record(vec![
        ("the_ts", FieldValue::Timestamp(dt)),
        ("the_date", FieldValue::Date(d)),
    ]);
    // 2025-06-15 23:59:59 < 2025-06-16 00:00:00
    assert!(eval_bool(
        &record,
        "the_ts",
        BinaryOperator::LessThan,
        "the_date"
    ));
}

#[test]
fn test_date_timestamp_equality_at_midnight() {
    let d = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
    let dt = NaiveDateTime::parse_from_str("2025-06-15T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
    let record = make_record(vec![
        ("the_date", FieldValue::Date(d)),
        ("the_ts", FieldValue::Timestamp(dt)),
    ]);
    assert!(eval_bool(
        &record,
        "the_date",
        BinaryOperator::Equal,
        "the_ts"
    ));
}

/// Test that StreamRecord::resolve_column handles qualified system column references.
/// This was the root cause of the empty "Enrichment latency" panel — `m._event_time`
/// resolved to Null because normalize_if_system_column("m._event_time") didn't match.
#[test]
fn test_resolve_column_qualified_system_columns() {
    let event_millis: i64 = 1750000000000;
    let record = StreamRecord {
        fields: HashMap::new(),
        timestamp: 999,
        offset: 42,
        partition: 3,
        headers: HashMap::new(),
        event_time: Some(chrono::DateTime::from_timestamp_millis(event_millis).unwrap()),
        topic: None,
        key: None,
    };

    // Unqualified system columns
    assert_eq!(
        record.resolve_column("_event_time"),
        FieldValue::Integer(event_millis)
    );
    assert_eq!(
        record.resolve_column("_EVENT_TIME"),
        FieldValue::Integer(event_millis)
    );
    assert_eq!(
        record.resolve_column("_timestamp"),
        FieldValue::Integer(999)
    );
    assert_eq!(record.resolve_column("_offset"), FieldValue::Integer(42));
    assert_eq!(record.resolve_column("_partition"), FieldValue::Integer(3));

    // Qualified system columns (table.column) — the bug that caused the empty panel
    assert_eq!(
        record.resolve_column("m._event_time"),
        FieldValue::Integer(event_millis)
    );
    assert_eq!(
        record.resolve_column("m._EVENT_TIME"),
        FieldValue::Integer(event_millis)
    );
    assert_eq!(
        record.resolve_column("t._timestamp"),
        FieldValue::Integer(999)
    );
    assert_eq!(record.resolve_column("x._offset"), FieldValue::Integer(42));
    assert_eq!(
        record.resolve_column("x._partition"),
        FieldValue::Integer(3)
    );

    // Regular fields still work
    let mut fields = HashMap::new();
    fields.insert("price".to_string(), FieldValue::Float(100.0));
    let record2 = StreamRecord { fields, ..record };
    assert_eq!(record2.resolve_column("price"), FieldValue::Float(100.0));
    assert_eq!(record2.resolve_column("m.price"), FieldValue::Float(100.0));
    assert_eq!(record2.resolve_column("missing"), FieldValue::Null);
}

/// Reproduces the exact production error: Date(2025-01-15) <= Integer(epoch_millis).
/// This is what happens when a table has FieldValue::Date and _event_time is epoch millis.
#[test]
fn test_date_vs_integer_epoch_millis_comparison() {
    let d = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let epoch_millis: i64 = 1750000000000; // ~2025-06-15
    let record = make_record(vec![("effective_date", FieldValue::Date(d))]);

    // Date(2025-01-15) <= Integer(~2025-06-15) should be true
    assert!(eval_literal_bool(
        &record,
        "effective_date",
        BinaryOperator::LessThanOrEqual,
        Expr::Literal(LiteralValue::Integer(epoch_millis)),
    ));

    // Date(2025-01-15) > Integer(~2025-06-15) should be false
    assert!(!eval_literal_bool(
        &record,
        "effective_date",
        BinaryOperator::GreaterThan,
        Expr::Literal(LiteralValue::Integer(epoch_millis)),
    ));

    // Use a millis value before the date: Integer(~2024-06-15) < Date(2025-01-15)
    let early_millis: i64 = 1718400000000; // ~2024-06-15
    let record2 = make_record(vec![("event_ms", FieldValue::Integer(early_millis))]);
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Column("event_ms".to_string())),
        op: BinaryOperator::LessThan,
        right: Box::new(Expr::Literal(LiteralValue::String(
            "2025-01-15".to_string(),
        ))),
    };
    // This tests Integer vs String-date — different path, just ensure no crash
    let _result = ExpressionEvaluator::evaluate_expression(&expr, &record2);
}
