/*!
# Extended CAST Support Tests

Tests for the enhanced CAST operation supporting DATE, TIMESTAMP, and DECIMAL types.
These tests verify that all type conversions work correctly and handle edge cases.
*/

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::f64::consts::PI;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_engine() -> StreamExecutionEngine {
    let (output_tx, _output_rx) = mpsc::unbounded_channel();
    StreamExecutionEngine::new(output_tx, Arc::new(JsonFormat))
}

fn create_mock_record() -> ferrisstreams::ferris::sql::execution::StreamRecord {
    ferrisstreams::ferris::sql::execution::StreamRecord {
        fields: HashMap::new(),
        timestamp: 1640995200000, // 2022-01-01 00:00:00 UTC
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
    }
}

#[tokio::test]
async fn test_cast_to_date_from_string() {
    let engine = create_test_engine();
    let _record = create_mock_record();

    // Test various date string formats
    let test_cases = vec![
        ("2023-12-25", true),
        ("2023/12/25", true),
        ("12/25/2023", true),
        ("25-12-2023", true),
        ("invalid-date", false),
        ("2023-13-45", false),
    ];

    for (date_str, should_succeed) in test_cases {
        let result = engine.cast_value(FieldValue::String(date_str.to_string()), "DATE");

        if should_succeed {
            assert!(
                result.is_ok(),
                "Should successfully cast '{}' to DATE",
                date_str
            );
            if let Ok(FieldValue::Date(_)) = result {
                // Success case verified
            } else {
                panic!("Cast result should be a Date type for '{}'", date_str);
            }
        } else {
            assert!(
                result.is_err(),
                "Should fail to cast '{}' to DATE",
                date_str
            );
        }
    }
}

#[tokio::test]
async fn test_cast_to_date_from_timestamp() {
    let engine = create_test_engine();

    let timestamp =
        NaiveDateTime::parse_from_str("2023-12-25 14:30:45", "%Y-%m-%d %H:%M:%S").unwrap();
    let expected_date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();

    let result = engine.cast_value(FieldValue::Timestamp(timestamp), "DATE");

    assert!(result.is_ok(), "Should successfully cast TIMESTAMP to DATE");
    assert_eq!(result.unwrap(), FieldValue::Date(expected_date));
}

#[tokio::test]
async fn test_cast_to_timestamp_from_string() {
    let engine = create_test_engine();

    let test_cases = vec![
        ("2023-12-25 14:30:45", true),
        ("2023-12-25 14:30:45.123", true),
        ("2023-12-25T14:30:45", true),
        ("2023-12-25T14:30:45.123", true),
        ("2023/12/25 14:30:45", true),
        ("2023-12-25", true), // Date only, should add 00:00:00
        ("invalid-timestamp", false),
        ("2023-13-45 25:70:80", false),
    ];

    for (timestamp_str, should_succeed) in test_cases {
        let result = engine.cast_value(FieldValue::String(timestamp_str.to_string()), "TIMESTAMP");

        if should_succeed {
            assert!(
                result.is_ok(),
                "Should successfully cast '{}' to TIMESTAMP",
                timestamp_str
            );
            if let Ok(FieldValue::Timestamp(_)) = result {
                // Success case verified
            } else {
                panic!(
                    "Cast result should be a Timestamp type for '{}'",
                    timestamp_str
                );
            }
        } else {
            assert!(
                result.is_err(),
                "Should fail to cast '{}' to TIMESTAMP",
                timestamp_str
            );
        }
    }
}

#[tokio::test]
async fn test_cast_to_timestamp_from_date() {
    let engine = create_test_engine();

    let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
    let expected_timestamp = date.and_hms_opt(0, 0, 0).unwrap();

    let result = engine.cast_value(FieldValue::Date(date), "TIMESTAMP");

    assert!(result.is_ok(), "Should successfully cast DATE to TIMESTAMP");
    assert_eq!(result.unwrap(), FieldValue::Timestamp(expected_timestamp));
}

#[tokio::test]
async fn test_cast_to_timestamp_from_integer() {
    let engine = create_test_engine();

    // Unix timestamp for 2023-01-01 00:00:00 UTC
    let unix_timestamp = 1672531200i64;

    let result = engine.cast_value(FieldValue::Integer(unix_timestamp), "TIMESTAMP");

    assert!(
        result.is_ok(),
        "Should successfully cast INTEGER to TIMESTAMP"
    );
    if let Ok(FieldValue::Timestamp(ts)) = result {
        // Verify that the timestamp is reasonable (should be 2023-01-01)
        assert_eq!(ts.date().year(), 2023);
        assert_eq!(ts.date().month(), 1);
        assert_eq!(ts.date().day(), 1);
    } else {
        panic!("Cast result should be a Timestamp type");
    }
}

#[tokio::test]
async fn test_cast_to_decimal_from_various_types() {
    let engine = create_test_engine();

    // Test from Integer
    let result = engine.cast_value(FieldValue::Integer(42), "DECIMAL");
    assert!(result.is_ok(), "Should cast INTEGER to DECIMAL");
    assert_eq!(result.unwrap(), FieldValue::Decimal(Decimal::from(42)));

    // Test from Float
    let result = engine.cast_value(FieldValue::Float(PI), "DECIMAL");
    assert!(result.is_ok(), "Should cast FLOAT to DECIMAL");
    if let Ok(FieldValue::Decimal(d)) = result {
        // Allow for some floating point precision differences
        let expected = Decimal::try_from(PI).unwrap();
        let diff = (d - expected).abs();
        assert!(
            diff < Decimal::from_str("0.00001").unwrap(),
            "Decimal should be close to PI"
        );
    }

    // Test from String
    let result = engine.cast_value(FieldValue::String("123.456".to_string()), "DECIMAL");
    assert!(
        result.is_ok(),
        "Should cast valid numeric STRING to DECIMAL"
    );
    assert_eq!(
        result.unwrap(),
        FieldValue::Decimal(Decimal::from_str("123.456").unwrap())
    );

    // Test from Boolean
    let result = engine.cast_value(FieldValue::Boolean(true), "DECIMAL");
    assert!(result.is_ok(), "Should cast BOOLEAN to DECIMAL");
    assert_eq!(result.unwrap(), FieldValue::Decimal(Decimal::ONE));

    let result = engine.cast_value(FieldValue::Boolean(false), "DECIMAL");
    assert!(result.is_ok(), "Should cast BOOLEAN to DECIMAL");
    assert_eq!(result.unwrap(), FieldValue::Decimal(Decimal::ZERO));

    // Test invalid string
    let result = engine.cast_value(FieldValue::String("not-a-number".to_string()), "DECIMAL");
    assert!(
        result.is_err(),
        "Should fail to cast invalid STRING to DECIMAL"
    );
}

#[tokio::test]
async fn test_cast_decimal_to_other_types() {
    let engine = create_test_engine();

    let decimal_value = Decimal::from_str("123.456").unwrap();

    // Test DECIMAL to INTEGER (should truncate)
    let result = engine.cast_value(FieldValue::Decimal(decimal_value), "INTEGER");
    assert!(result.is_ok(), "Should cast DECIMAL to INTEGER");
    assert_eq!(result.unwrap(), FieldValue::Integer(123));

    // Test DECIMAL to FLOAT
    let result = engine.cast_value(FieldValue::Decimal(decimal_value), "FLOAT");
    assert!(result.is_ok(), "Should cast DECIMAL to FLOAT");
    if let Ok(FieldValue::Float(f)) = result {
        assert!(
            (f - 123.456).abs() < 0.001,
            "Float should be close to 123.456"
        );
    }

    // Test DECIMAL to STRING
    let result = engine.cast_value(FieldValue::Decimal(decimal_value), "STRING");
    assert!(result.is_ok(), "Should cast DECIMAL to STRING");
    if let Ok(FieldValue::String(s)) = result {
        assert!(
            s.starts_with("123.456"),
            "String should represent the decimal value"
        );
    }
}

#[tokio::test]
async fn test_cast_null_values() {
    let engine = create_test_engine();

    // All NULL casts should return NULL, except STRING which returns "NULL"
    let null_target_types = vec![
        "DATE",
        "TIMESTAMP",
        "DECIMAL",
        "INTEGER",
        "FLOAT",
        "BOOLEAN",
    ];

    for target_type in null_target_types {
        let result = engine.cast_value(FieldValue::Null, target_type);
        assert!(
            result.is_ok(),
            "Should successfully cast NULL to {}",
            target_type
        );
        assert_eq!(result.unwrap(), FieldValue::Null);
    }

    // Special case for STRING - NULL to STRING should return "NULL" string
    let result = engine.cast_value(FieldValue::Null, "STRING");
    assert!(result.is_ok(), "Should successfully cast NULL to STRING");
    assert_eq!(result.unwrap(), FieldValue::String("NULL".to_string()));
}

#[tokio::test]
async fn test_cast_error_cases() {
    let engine = create_test_engine();

    // Test invalid cast combinations that should fail
    let invalid_casts = vec![
        (FieldValue::Array(vec![]), "DATE"),
        (FieldValue::Map(HashMap::new()), "TIMESTAMP"),
        (FieldValue::Struct(HashMap::new()), "DECIMAL"),
        (
            FieldValue::Date(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()),
            "INTEGER",
        ),
        (
            FieldValue::Timestamp(
                NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            ),
            "INTEGER",
        ),
    ];

    for (value, target_type) in invalid_casts {
        let result = engine.cast_value(value.clone(), target_type);
        assert!(
            result.is_err(),
            "Should fail to cast {:?} to {}",
            value.type_name(),
            target_type
        );
    }
}

#[tokio::test]
async fn test_cast_datetime_alias() {
    let engine = create_test_engine();

    // Test that DATETIME works as an alias for TIMESTAMP
    let date_str = "2023-12-25 14:30:45";
    let result = engine.cast_value(FieldValue::String(date_str.to_string()), "DATETIME");

    assert!(result.is_ok(), "Should successfully cast to DATETIME");
    if let Ok(FieldValue::Timestamp(_)) = result {
        // Success case verified
    } else {
        panic!("DATETIME cast should return Timestamp type");
    }
}

#[tokio::test]
async fn test_cast_numeric_alias() {
    let engine = create_test_engine();

    // Test that NUMERIC works as an alias for DECIMAL
    let result = engine.cast_value(FieldValue::String("123.456".to_string()), "NUMERIC");

    assert!(result.is_ok(), "Should successfully cast to NUMERIC");
    if let Ok(FieldValue::Decimal(_)) = result {
        // Success case verified
    } else {
        panic!("NUMERIC cast should return Decimal type");
    }
}

#[tokio::test]
async fn test_large_decimal_values() {
    let engine = create_test_engine();

    // Test large but reasonable decimal numbers (within Decimal's precision limits)
    let large_decimal_str = "123456789012345.123456789";
    let result = engine.cast_value(FieldValue::String(large_decimal_str.to_string()), "DECIMAL");

    assert!(result.is_ok(), "Should handle large DECIMAL values");
    if let Ok(FieldValue::Decimal(d)) = result {
        assert!(
            d.to_string().starts_with("123456789012345"),
            "Should preserve large decimal precision"
        );
    }
}

#[tokio::test]
async fn test_date_boundary_values() {
    let engine = create_test_engine();

    // Test boundary dates
    let boundary_dates = vec![
        "1970-01-01", // Unix epoch
        "2000-02-29", // Leap year
        "2023-02-28", // Non-leap year
        "2023-12-31", // Year end
    ];

    for date_str in boundary_dates {
        let result = engine.cast_value(FieldValue::String(date_str.to_string()), "DATE");
        assert!(result.is_ok(), "Should handle boundary date {}", date_str);
    }
}

#[tokio::test]
async fn test_timestamp_precision() {
    let engine = create_test_engine();

    // Test that millisecond precision is preserved
    let timestamp_with_ms = "2023-12-25 14:30:45.123";
    let result = engine.cast_value(
        FieldValue::String(timestamp_with_ms.to_string()),
        "TIMESTAMP",
    );

    assert!(result.is_ok(), "Should handle timestamps with milliseconds");
    if let Ok(FieldValue::Timestamp(ts)) = result {
        // Verify millisecond precision
        assert_eq!(
            ts.and_utc().timestamp_subsec_millis(),
            123,
            "Should preserve millisecond precision"
        );
    }
}

#[tokio::test]
async fn test_cast_display_string_format() {
    let engine = create_test_engine();

    // Test that new types display correctly when cast to string
    let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
    let timestamp =
        NaiveDateTime::parse_from_str("2023-12-25 14:30:45.123", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
    let decimal = Decimal::from_str("123.456").unwrap();

    let result = engine.cast_value(FieldValue::Date(date), "STRING");
    assert!(result.is_ok());
    if let Ok(FieldValue::String(s)) = result {
        assert_eq!(s, "2023-12-25");
    }

    let result = engine.cast_value(FieldValue::Timestamp(timestamp), "STRING");
    assert!(result.is_ok());
    if let Ok(FieldValue::String(s)) = result {
        assert!(s.starts_with("2023-12-25 14:30:45"));
    }

    let result = engine.cast_value(FieldValue::Decimal(decimal), "STRING");
    assert!(result.is_ok());
    if let Ok(FieldValue::String(s)) = result {
        assert_eq!(s, "123.456");
    }
}
