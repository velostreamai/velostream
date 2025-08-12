/*!
# Enhanced Date Functions Tests

Comprehensive tests for the newly added EXTRACT and DATEDIFF units.
Tests EPOCH, WEEK, QUARTER, MILLISECOND, MICROSECOND, NANOSECOND for EXTRACT,
and weeks, months, quarters, years for DATEDIFF.
*/

use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

fn create_test_record_with_timestamps() -> StreamRecord {
    let mut fields = HashMap::new();

    // Test timestamps: Jan 1, 2023 12:30:45.123 UTC and Jul 15, 2024 18:45:30.987 UTC
    fields.insert("start_time".to_string(), FieldValue::Integer(1672575045123)); // 2023-01-01 12:30:45.123
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
        timestamp: 1672575045123, // 2023-01-01 12:30:45.123 UTC
        offset: 1000,
        partition: 0,
    }
}

fn convert_stream_record_to_internal(record: &StreamRecord) -> HashMap<String, InternalValue> {
    record
        .fields
        .iter()
        .map(|(k, v)| {
            let internal_val = match v {
                FieldValue::Integer(i) => InternalValue::Integer(*i),
                FieldValue::Float(f) => InternalValue::Number(*f),
                FieldValue::String(s) => InternalValue::String(s.clone()),
                FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                FieldValue::Null => InternalValue::Null,
                _ => InternalValue::String(format!("{:?}", v)),
            };
            (k.clone(), internal_val)
        })
        .collect()
}

async fn execute_date_query(
    query: &str,
) -> Result<Vec<HashMap<String, InternalValue>>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx, Arc::new(JsonFormat));
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record_with_timestamps();

    // Convert StreamRecord to HashMap<String, InternalValue>
    let internal_record = convert_stream_record_to_internal(&record);

    // Execute the query with internal record, including metadata
    engine
        .execute_with_metadata(
            &parsed_query,
            internal_record,
            record.headers,
            Some(record.timestamp),
            Some(record.offset),
            Some(record.partition),
        )
        .await?;

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
    assert_eq!(results[0]["epoch_time"], InternalValue::Integer(1672575045)); // seconds, not milliseconds
}

#[tokio::test]
async fn test_extract_week() {
    let results =
        execute_date_query("SELECT EXTRACT('WEEK', week_test) as week_number FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // January 9, 2023 should be week 2 (ISO week)
    assert_eq!(results[0]["week_number"], InternalValue::Integer(2));
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
    assert_eq!(results[0]["quarter_num"], InternalValue::Integer(1));

    // Test Q3 (July) using end_time
    let results =
        execute_date_query("SELECT EXTRACT('QUARTER', end_time) as quarter_num FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // July should be Q3 (quarter 3)
    assert_eq!(results[0]["quarter_num"], InternalValue::Integer(3));
}

#[tokio::test]
async fn test_extract_millisecond() {
    let results =
        execute_date_query("SELECT EXTRACT('MILLISECOND', start_time) as ms FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // Should extract millisecond component (123)
    assert_eq!(results[0]["ms"], InternalValue::Integer(123));
}

#[tokio::test]
async fn test_extract_microsecond() {
    let results =
        execute_date_query("SELECT EXTRACT('MICROSECOND', start_time) as us FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // Should extract microsecond component (123000 - milliseconds converted to microseconds)
    assert_eq!(results[0]["us"], InternalValue::Integer(123000));
}

#[tokio::test]
async fn test_extract_nanosecond() {
    let results =
        execute_date_query("SELECT EXTRACT('NANOSECOND', start_time) as ns FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    // Should extract nanosecond component (123000000 - milliseconds converted to nanoseconds)
    assert_eq!(results[0]["ns"], InternalValue::Integer(123000000));
}

#[tokio::test]
async fn test_datediff_weeks() {
    let results = execute_date_query(
        "SELECT DATEDIFF('weeks', start_time, end_time) as week_diff FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    // From Jan 1, 2023 to Jul 15, 2024 should be approximately 80+ weeks
    let week_diff = match &results[0]["week_diff"] {
        InternalValue::Integer(w) => *w,
        _ => panic!("Expected integer for week_diff"),
    };
    assert!(
        week_diff > 80 && week_diff < 85,
        "Week difference should be ~82 weeks, got {}",
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
    assert_eq!(results[0]["month_diff"], InternalValue::Integer(18));
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
    assert_eq!(results[0]["quarter_diff"], InternalValue::Integer(6));
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
    assert_eq!(results[0]["year_diff"], InternalValue::Integer(1));
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
    assert_eq!(results[0]["feb_diff"], InternalValue::Integer(0)); // Same month

    let results = execute_date_query(&format!(
        "SELECT DATEDIFF('months', {}, {}) as mar_diff FROM test_stream",
        feb_1, mar_1
    ))
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["mar_diff"], InternalValue::Integer(1)); // Next month
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
    assert!(matches!(results[0]["epoch_val"], InternalValue::Integer(_)));
    assert!(matches!(results[0]["week_val"], InternalValue::Integer(_)));
    assert!(matches!(
        results[0]["quarter_val"],
        InternalValue::Integer(_)
    ));
    assert!(matches!(results[0]["ms_val"], InternalValue::Integer(_)));
    assert!(matches!(results[0]["us_val"], InternalValue::Integer(_)));
    assert!(matches!(results[0]["ns_val"], InternalValue::Integer(_)));
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
    let weeks = match &results[0]["weeks_diff"] {
        InternalValue::Integer(w) => *w,
        _ => panic!("Expected integer for weeks_diff"),
    };
    let months = match &results[0]["months_diff"] {
        InternalValue::Integer(m) => *m,
        _ => panic!("Expected integer for months_diff"),
    };
    let quarters = match &results[0]["quarters_diff"] {
        InternalValue::Integer(q) => *q,
        _ => panic!("Expected integer for quarters_diff"),
    };
    let years = match &results[0]["years_diff"] {
        InternalValue::Integer(y) => *y,
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
    assert_eq!(results[0]["same_day"], InternalValue::Integer(0));
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
    let diff = match &results[0]["negative_diff"] {
        InternalValue::Integer(d) => *d,
        _ => panic!("Expected integer for negative_diff"),
    };
    assert!(
        diff < 0,
        "Reversed time difference should be negative, got {}",
        diff
    );
}
