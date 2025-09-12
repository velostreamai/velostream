/*!
# Enhanced Date Functions Tests

Comprehensive tests for the newly added EXTRACT and DATEDIFF units.
Tests EPOCH, WEEK, QUARTER, MILLISECOND, MICROSECOND, NANOSECOND for EXTRACT,
and weeks, months, quarters, years for DATEDIFF.
*/

use chrono::{Datelike, Timelike};
use ferrisstreams::ferris::serialization::JsonFormat;
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record_with_timestamps() -> StreamRecord {
    let mut fields = HashMap::new();

    // Test timestamps: Jan 1, 2023 12:30:45.123 UTC and Jul 15, 2024 18:45:30.987 UTC
    fields.insert("start_time".to_string(), FieldValue::Integer(1672576245123)); // 2023-01-01 12:30:45.123
    fields.insert("end_time".to_string(), FieldValue::Integer(1721064330987)); // 2024-07-15 18:45:30.987
    fields.insert(
        "quarter_test".to_string(),
        FieldValue::Integer(1677675600000),
    ); // 2023-03-01 (Q1)
    fields.insert("week_test".to_string(), FieldValue::Integer(1673226000000)); // 2023-01-09 (Monday, Week 2)

    let mut headers = HashMap::new();
    headers.insert("test_source".to_string(), "date_functions_test".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1672576245123, // 2023-01-01 12:30:45.123 UTC
        offset: 1000,
        partition: 0,
        event_time: None,
    }
}

async fn execute_date_query(query: &str) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record_with_timestamps();

    // Execute the query with StreamRecord
    engine.execute_with_record(&parsed_query, record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_extract_epoch() {
    let results =
        execute_date_query("SELECT EXTRACT('EPOCH', start_time) as epoch_time FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // EPOCH should return Unix timestamp in seconds
    assert_eq!(
        results[0].fields.get("epoch_time"),
        Some(&FieldValue::Integer(1672576245))
    ); // seconds, not milliseconds
}

#[tokio::test]
async fn test_extract_week() {
    let results =
        execute_date_query("SELECT EXTRACT('WEEK', week_test) as week_number FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // January 9, 2023 should be week 2 (ISO week)
    assert_eq!(
        results[0].fields.get("week_number"),
        Some(&FieldValue::Integer(2))
    );
}

#[tokio::test]
async fn test_extract_quarter() {
    // Test Q1 (March)
    let results = execute_date_query(
        "SELECT EXTRACT('QUARTER', quarter_test) as quarter_num FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    // March should be Q1 (quarter 1)
    assert_eq!(
        results[0].fields.get("quarter_num"),
        Some(&FieldValue::Integer(1))
    );

    // Test Q3 (July) using end_time
    let results =
        execute_date_query("SELECT EXTRACT('QUARTER', end_time) as quarter_num FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // July should be Q3 (quarter 3)
    assert_eq!(
        results[0].fields.get("quarter_num"),
        Some(&FieldValue::Integer(3))
    );
}

#[tokio::test]
async fn test_extract_millisecond() {
    let results =
        execute_date_query("SELECT EXTRACT('MILLISECOND', start_time) as ms FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // Should extract millisecond component (123)
    assert_eq!(results[0].fields.get("ms"), Some(&FieldValue::Integer(123)));
}

#[tokio::test]
async fn test_extract_microsecond() {
    let results =
        execute_date_query("SELECT EXTRACT('MICROSECOND', start_time) as us FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // Should extract microsecond component (123000 - milliseconds converted to microseconds)
    assert_eq!(
        results[0].fields.get("us"),
        Some(&FieldValue::Integer(123000))
    );
}

#[tokio::test]
async fn test_extract_nanosecond() {
    let results =
        execute_date_query("SELECT EXTRACT('NANOSECOND', start_time) as ns FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // Should extract nanosecond component (123000000 - milliseconds converted to nanoseconds)
    assert_eq!(
        results[0].fields.get("ns"),
        Some(&FieldValue::Integer(123000000))
    );
}

#[tokio::test]
async fn test_datediff_weeks() {
    let results = execute_date_query(
        "SELECT DATEDIFF('weeks', start_time, end_time) as week_diff FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    // From Jan 1, 2023 to Jul 15, 2024 should be approximately 80 weeks (80.17 exactly)
    let week_diff = match results[0].fields.get("week_diff") {
        Some(FieldValue::Integer(w)) => *w,
        _ => panic!("Expected integer for week_diff"),
    };
    assert!(
        (80..=81).contains(&week_diff),
        "Week difference should be ~80 weeks, got {}",
        week_diff
    );
}

#[tokio::test]
async fn test_datediff_months() {
    let results = execute_date_query(
        "SELECT DATEDIFF('months', start_time, end_time) as month_diff FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    // From Jan 1, 2023 to Jul 15, 2024 should be 18 months (Jan 2023 to Jul 2024)
    assert_eq!(
        results[0].fields.get("month_diff"),
        Some(&FieldValue::Integer(18))
    );
}

#[tokio::test]
async fn test_datediff_quarters() {
    let results = execute_date_query(
        "SELECT DATEDIFF('quarters', start_time, end_time) as quarter_diff FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    // From Q1 2023 to Q3 2024 should be 6 quarters
    assert_eq!(
        results[0].fields.get("quarter_diff"),
        Some(&FieldValue::Integer(6))
    );
}

#[tokio::test]
async fn test_datediff_years() {
    let results = execute_date_query(
        "SELECT DATEDIFF('years', start_time, end_time) as year_diff FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    // From 2023 to 2024 should be 1 year
    assert_eq!(
        results[0].fields.get("year_diff"),
        Some(&FieldValue::Integer(1))
    );
}

#[tokio::test]
async fn test_datediff_month_precision() {
    // Test month calculation precision with a case where day matters
    // Use specific dates: Feb 1 to Feb 28 (should be 0 months) vs Feb 1 to Mar 1 (should be 1 month)
    let feb_1 = 1675209600000i64; // 2023-02-01
    let feb_28 = 1677542400000i64; // 2023-02-28
    let mar_1 = 1677628800000i64; // 2023-03-01

    let results = execute_date_query(&format!(
        "SELECT DATEDIFF('months', {}, {}) as feb_diff FROM test_stream",
        feb_1, feb_28
    ))
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("feb_diff"),
        Some(&FieldValue::Integer(0))
    ); // Same month

    let results = execute_date_query(&format!(
        "SELECT DATEDIFF('months', {}, {}) as mar_diff FROM test_stream",
        feb_1, mar_1
    ))
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("mar_diff"),
        Some(&FieldValue::Integer(1))
    ); // Next month
}

#[tokio::test]
async fn test_extract_all_new_units_in_single_query() {
    let query = "
        SELECT 
            EXTRACT('EPOCH', start_time) as epoch_val,
            EXTRACT('WEEK', start_time) as week_val,
            EXTRACT('QUARTER', start_time) as quarter_val,
            EXTRACT('MILLISECOND', start_time) as ms_val,
            EXTRACT('MICROSECOND', start_time) as us_val,
            EXTRACT('NANOSECOND', start_time) as ns_val
        FROM test_stream
    ";

    let results = execute_date_query(query).await.unwrap();
    assert_eq!(results.len(), 1);

    // Verify all values are present and reasonable
    // assert!(matches!(results[0].fields.get("epoch_val"), Some(&FieldValue::Integer(_)));
    // assert!(matches!(results[0].fields.get("week_val"), Some(&FieldValue::Integer(_)));
    // assert!(matches!(
    //     results[0].fields.get("quarter_val"),
    //     Some(&FieldValue::Integer(_))
    // ));
    assert!(matches!(
        results[0].fields.get("ms_val"),
        Some(&FieldValue::Integer(_))
    ));
    assert!(matches!(
        results[0].fields.get("us_val"),
        Some(&FieldValue::Integer(_))
    ));
    assert!(matches!(
        results[0].fields.get("ns_val"),
        Some(&FieldValue::Integer(_))
    ));
}

#[tokio::test]
async fn test_datediff_all_new_units_in_single_query() {
    let query = "
        SELECT 
            DATEDIFF('weeks', start_time, end_time) as weeks_diff,
            DATEDIFF('months', start_time, end_time) as months_diff,
            DATEDIFF('quarters', start_time, end_time) as quarters_diff,
            DATEDIFF('years', start_time, end_time) as years_diff
        FROM test_stream
    ";

    let results = execute_date_query(query).await.unwrap();
    assert_eq!(results.len(), 1);

    // Verify all values are present and reasonable
    let weeks = match results[0].fields.get("weeks_diff") {
        Some(FieldValue::Integer(w)) => *w,
        _ => panic!("Expected integer for weeks_diff"),
    };
    let months = match results[0].fields.get("months_diff") {
        Some(FieldValue::Integer(m)) => *m,
        _ => panic!("Expected integer for months_diff"),
    };
    let quarters = match results[0].fields.get("quarters_diff") {
        Some(FieldValue::Integer(q)) => *q,
        _ => panic!("Expected integer for quarters_diff"),
    };
    let years = match results[0].fields.get("years_diff") {
        Some(FieldValue::Integer(y)) => *y,
        _ => panic!("Expected integer for years_diff"),
    };

    // Sanity checks on relationships
    assert!(weeks > 0, "Weeks should be positive");
    assert!(months > 0, "Months should be positive");
    assert!(quarters > 0, "Quarters should be positive");
    assert!(years > 0, "Years should be positive");
    assert!(
        months <= weeks / 4,
        "Months should be roughly weeks/4 or less"
    );
    assert!(
        quarters == months / 3,
        "Quarters should be roughly months/3"
    );
}

#[tokio::test]
async fn test_extract_error_cases() {
    // Test unsupported extract unit
    let result = execute_date_query(
        "SELECT EXTRACT('INVALID_UNIT', start_time) as invalid FROM test_stream",
    )
    .await;
    assert!(result.is_err(), "Should fail for invalid EXTRACT unit");
}

#[tokio::test]
async fn test_datediff_error_cases() {
    // Test unsupported datediff unit
    let result = execute_date_query(
        "SELECT DATEDIFF('invalid_unit', start_time, end_time) as invalid FROM test_stream",
    )
    .await;
    assert!(result.is_err(), "Should fail for invalid DATEDIFF unit");
}

#[tokio::test]
async fn test_edge_case_same_timestamps() {
    // Test DATEDIFF with same timestamps (should return 0)
    let results = execute_date_query(
        "SELECT DATEDIFF('days', start_time, start_time) as same_day FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("same_day"),
        Some(&FieldValue::Integer(0))
    );
}

#[tokio::test]
async fn test_negative_time_differences() {
    // Test DATEDIFF with reversed timestamps (should return negative values)
    let results = execute_date_query(
        "SELECT DATEDIFF('days', end_time, start_time) as negative_diff FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let diff = match results[0].fields.get("negative_diff") {
        Some(FieldValue::Integer(d)) => *d,
        _ => panic!("Expected integer for negative_diff"),
    };
    assert!(
        diff < 0,
        "Reversed time difference should be negative, got {}",
        diff
    );
}

// ==================== FROM_UNIXTIME() and UNIX_TIMESTAMP() Tests ====================

fn create_test_record_for_timestamp_functions() -> StreamRecord {
    let mut fields = HashMap::new();

    // Test Unix timestamps
    fields.insert(
        "unix_timestamp".to_string(),
        FieldValue::Integer(1672576245),
    ); // 2023-01-01 12:30:45 UTC
    fields.insert(
        "unix_timestamp_float".to_string(),
        FieldValue::Float(1672576245.123),
    ); // With fractional seconds
    fields.insert("invalid_timestamp".to_string(), FieldValue::Integer(-1)); // Invalid timestamp
    fields.insert("null_timestamp".to_string(), FieldValue::Null);
    fields.insert(
        "string_field".to_string(),
        FieldValue::String("not_a_timestamp".to_string()),
    );

    // Pre-converted timestamps for testing UNIX_TIMESTAMP conversion back
    use chrono::{NaiveDate, NaiveTime};
    let naive_dt = NaiveDate::from_ymd_opt(2023, 1, 1)
        .unwrap()
        .and_time(NaiveTime::from_hms_opt(12, 30, 45).unwrap());
    fields.insert(
        "datetime_field".to_string(),
        FieldValue::Timestamp(naive_dt),
    );

    let mut headers = HashMap::new();
    headers.insert(
        "test_source".to_string(),
        "timestamp_functions_test".to_string(),
    );

    StreamRecord {
        fields,
        headers,
        timestamp: 1672576245000, // 2023-01-01 12:30:45 UTC in milliseconds
        offset: 1001,
        partition: 0,
        event_time: None,
    }
}

#[tokio::test]
async fn test_from_unixtime_basic() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT FROM_UNIXTIME(unix_timestamp) AS transaction_time FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Check that we got a timestamp back
    match results[0].fields.get("transaction_time") {
        Some(FieldValue::Timestamp(dt)) => {
            // Should be 2023-01-01 12:30:45
            assert_eq!(dt.year(), 2023);
            assert_eq!(dt.month(), 1);
            assert_eq!(dt.day(), 1);
            assert_eq!(dt.hour(), 12);
            assert_eq!(dt.minute(), 30);
            assert_eq!(dt.second(), 45);
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

#[tokio::test]
async fn test_from_unixtime_float() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT FROM_UNIXTIME(unix_timestamp_float) AS precise_time FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Check that we got a timestamp back with nanosecond precision
    match results[0].fields.get("precise_time") {
        Some(FieldValue::Timestamp(dt)) => {
            // Should be 2023-01-01 12:30:45.123
            assert_eq!(dt.year(), 2023);
            assert_eq!(dt.month(), 1);
            assert_eq!(dt.day(), 1);
            assert_eq!(dt.hour(), 12);
            assert_eq!(dt.minute(), 30);
            assert_eq!(dt.second(), 45);
            // Check nanoseconds (123ms = ~123,000,000ns, but floating point precision may cause small variations)
            let actual_nanos = dt.nanosecond();
            assert!(
                (122_000_000..=124_000_000).contains(&actual_nanos),
                "Nanoseconds should be approximately 123,000,000, got {}",
                actual_nanos
            );
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}

#[tokio::test]
async fn test_from_unixtime_null_handling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT FROM_UNIXTIME(null_timestamp) AS null_result FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("null_result"),
        Some(&FieldValue::Null)
    );
}

#[tokio::test]
async fn test_from_unixtime_invalid_type() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT FROM_UNIXTIME(string_field) AS error_result FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    // This should result in an error
    let result = engine.execute_with_record(&parsed_query, record).await;
    assert!(result.is_err(), "Should fail with non-numeric input");
}

#[tokio::test]
async fn test_unix_timestamp_no_args() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT UNIX_TIMESTAMP() AS current_timestamp FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Should get a current Unix timestamp (integer)
    match results[0].fields.get("current_timestamp") {
        Some(FieldValue::Integer(ts)) => {
            // Should be reasonable current timestamp (after 2020, before 2030)
            assert!(*ts > 1577836800, "Timestamp should be after 2020"); // 2020-01-01
            assert!(*ts < 1893456000, "Timestamp should be before 2030"); // 2030-01-01
        }
        other => panic!("Expected Integer timestamp, got {:?}", other),
    }
}

#[tokio::test]
async fn test_unix_timestamp_with_datetime() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT UNIX_TIMESTAMP(datetime_field) AS converted_timestamp FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Should convert back to Unix timestamp (2023-01-01 12:30:45 = 1672576245)
    assert_eq!(
        results[0].fields.get("converted_timestamp"),
        Some(&FieldValue::Integer(1672576245))
    );
}

#[tokio::test]
async fn test_unix_timestamp_null_handling() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT UNIX_TIMESTAMP(null_timestamp) AS null_result FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].fields.get("null_result"),
        Some(&FieldValue::Null)
    );
}

#[tokio::test]
async fn test_unix_timestamp_invalid_type() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "SELECT UNIX_TIMESTAMP(string_field) AS error_result FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    // This should result in an error
    let result = engine.execute_with_record(&parsed_query, record).await;
    assert!(result.is_err(), "Should fail with non-timestamp input");
}

#[tokio::test]
async fn test_roundtrip_conversion() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Test roundtrip: unix timestamp -> datetime -> unix timestamp
    let query =
        "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(unix_timestamp)) AS roundtrip FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Should get back the original timestamp
    assert_eq!(
        results[0].fields.get("roundtrip"),
        Some(&FieldValue::Integer(1672576245))
    );
}

#[tokio::test]
async fn test_practical_usage_example() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Test the exact usage from the original request
    let query = "SELECT FROM_UNIXTIME(unix_timestamp) AS transaction_time, 'exported_' || UNIX_TIMESTAMP() AS export_id FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Check transaction_time is a proper timestamp
    match results[0].fields.get("transaction_time") {
        Some(FieldValue::Timestamp(dt)) => {
            assert_eq!(dt.year(), 2023);
            assert_eq!(dt.month(), 1);
            assert_eq!(dt.day(), 1);
        }
        other => panic!("Expected Timestamp for transaction_time, got {:?}", other),
    }

    // Check export_id is a string starting with 'exported_'
    match results[0].fields.get("export_id") {
        Some(FieldValue::String(s)) => {
            assert!(
                s.starts_with("exported_"),
                "export_id should start with 'exported_', got '{}'",
                s
            );
            // Extract the timestamp part and verify it's a valid number
            let timestamp_part = &s[9..]; // Remove 'exported_' prefix
            let timestamp: i64 = timestamp_part.parse().expect("Should be a valid timestamp");
            assert!(timestamp > 1577836800, "Timestamp should be reasonable");
        }
        other => panic!("Expected String for export_id, got {:?}", other),
    }
}

#[tokio::test]
async fn test_multiple_timestamp_functions_in_query() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = "
        SELECT 
            FROM_UNIXTIME(unix_timestamp) AS converted_time,
            UNIX_TIMESTAMP() AS current_time,
            UNIX_TIMESTAMP(datetime_field) AS back_converted,
            FROM_UNIXTIME(unix_timestamp_float) AS precise_time
        FROM test_stream
    ";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Verify all functions worked
    assert!(matches!(
        results[0].fields.get("converted_time"),
        Some(FieldValue::Timestamp(_))
    ));
    assert!(matches!(
        results[0].fields.get("current_time"),
        Some(FieldValue::Integer(_))
    ));
    assert_eq!(
        results[0].fields.get("back_converted"),
        Some(&FieldValue::Integer(1672576245))
    );
    assert!(matches!(
        results[0].fields.get("precise_time"),
        Some(FieldValue::Timestamp(_))
    ));
}

#[tokio::test]
async fn test_from_unixtime_wrong_arg_count() {
    let (tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Test with no arguments
    let query = "SELECT FROM_UNIXTIME() AS error FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    let result = engine.execute_with_record(&parsed_query, record).await;
    assert!(result.is_err(), "Should fail with no arguments");
}

#[tokio::test]
async fn test_unix_timestamp_wrong_arg_count() {
    let (tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Test with too many arguments
    let query = "SELECT UNIX_TIMESTAMP(datetime_field, unix_timestamp) AS error FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    let result = engine.execute_with_record(&parsed_query, record).await;
    assert!(result.is_err(), "Should fail with too many arguments");
}

#[tokio::test]
async fn test_edge_case_timestamps() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Test with epoch (0)
    let query = "SELECT FROM_UNIXTIME(0) AS epoch_time FROM test_stream";
    let parsed_query = parser.parse(query).unwrap();
    let record = create_test_record_for_timestamp_functions();

    engine
        .execute_with_record(&parsed_query, record)
        .await
        .unwrap();

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }

    assert_eq!(results.len(), 1);

    // Should be 1970-01-01 00:00:00
    match results[0].fields.get("epoch_time") {
        Some(FieldValue::Timestamp(dt)) => {
            assert_eq!(dt.year(), 1970);
            assert_eq!(dt.month(), 1);
            assert_eq!(dt.day(), 1);
            assert_eq!(dt.hour(), 0);
            assert_eq!(dt.minute(), 0);
            assert_eq!(dt.second(), 0);
        }
        other => panic!("Expected Timestamp, got {:?}", other),
    }
}
