/*!
# Tests for INTERVAL Arithmetic Support

Comprehensive test suite for INTERVAL literal parsing and arithmetic operations.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::ast::TimeUnit;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "timestamp_field".to_string(),
        FieldValue::Integer(1734652800000),
    ); // 2024-12-20 00:00:00 UTC
    fields.insert("amount".to_string(), FieldValue::Float(123.456));
    fields.insert("quantity".to_string(), FieldValue::Integer(42));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test-system".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp: 1734652800000, // 2024-12-20 00:00:00 UTC
        offset: 100,
        partition: 0,
        event_time: None,
        topic: None,
        key: None,
    }
}

// Removed conversion helper - now using StreamRecord directly

async fn execute_query(query: &str) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let parsed_query = parser.parse(query)?;
    let record = create_test_record();

    // Execute the query with StreamRecord directly
    engine.execute_with_record(&parsed_query, &record).await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

#[tokio::test]
async fn test_interval_literal_parsing() {
    // Test INTERVAL literal parsing - basic functionality
    let parser = StreamingSqlParser::new();

    // Test different time units
    let test_queries = vec![
        "SELECT INTERVAL '5' MINUTES as interval_min FROM test_stream",
        "SELECT INTERVAL '30' SECONDS as interval_sec FROM test_stream",
        "SELECT INTERVAL '2' HOURS as interval_hour FROM test_stream",
        "SELECT INTERVAL '1' DAY as interval_day FROM test_stream",
        "SELECT INTERVAL '500' MILLISECONDS as interval_ms FROM test_stream",
    ];

    for query in test_queries {
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse query: {}", query);

        // Extract the query and verify it contains interval
        match result.unwrap() {
            velostream::velostream::sql::ast::StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 1);
                match &fields[0] {
                    velostream::velostream::sql::ast::SelectField::Expression { expr, .. } => {
                        match expr {
                            velostream::velostream::sql::ast::Expr::Literal(
                                velostream::velostream::sql::ast::LiteralValue::Interval { .. },
                            ) => {
                                // Successfully parsed as interval literal
                            }
                            _ => panic!("Expected INTERVAL literal, got: {:?}", expr),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}

#[tokio::test]
async fn test_interval_arithmetic_with_timestamps() {
    // Test adding intervals to timestamps
    let results =
        execute_query("SELECT timestamp_field + INTERVAL '1' HOUR as future_time FROM test_stream")
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
    let future_time = match results[0].fields.get("future_time") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for future_time"),
    };

    // Should be 1 hour (3600 seconds = 3600000 ms) later
    assert_eq!(future_time, 1734652800000 + 3600000);
}

#[tokio::test]
async fn test_interval_arithmetic_subtraction() {
    // Test subtracting intervals from timestamps
    let results = execute_query(
        "SELECT timestamp_field - INTERVAL '30' MINUTES as past_time FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let past_time = match results[0].fields.get("past_time") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for past_time"),
    };

    // Should be 30 minutes (1800 seconds = 1800000 ms) earlier
    assert_eq!(past_time, 1734652800000 - 1800000);
}

#[tokio::test]
async fn test_interval_with_system_columns() {
    // Test INTERVAL arithmetic with system columns
    let results = execute_query(
        "SELECT _timestamp + INTERVAL '5' MINUTES as event_plus_5min FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let event_plus_5min = match results[0].fields.get("event_plus_5min") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for event_plus_5min"),
    };

    // Should be 5 minutes (300 seconds = 300000 ms) later
    assert_eq!(event_plus_5min, 1734652800000 + 300000);
}

#[tokio::test]
async fn test_multiple_interval_operations() {
    // Test multiple INTERVAL operations in one query
    let results = execute_query(
        "SELECT 
            timestamp_field + INTERVAL '1' HOUR as plus_hour,
            timestamp_field - INTERVAL '30' MINUTES as minus_30min,
            timestamp_field + INTERVAL '1' DAY as plus_day
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    let plus_hour = match results[0].fields.get("plus_hour") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for plus_hour"),
    };
    assert_eq!(plus_hour, 1734652800000 + 3600000); // +1 hour

    let minus_30min = match results[0].fields.get("minus_30min") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for minus_30min"),
    };
    assert_eq!(minus_30min, 1734652800000 - 1800000); // -30 minutes

    let plus_day = match results[0].fields.get("plus_day") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for plus_day"),
    };
    assert_eq!(plus_day, 1734652800000 + 86400000); // +1 day
}

#[tokio::test]
async fn test_interval_in_where_clause() {
    // Test INTERVAL in WHERE clause filtering
    let results = execute_query(
        "SELECT timestamp_field 
         FROM test_stream 
         WHERE timestamp_field > (timestamp_field - INTERVAL '1' HOUR)",
    )
    .await
    .unwrap();

    // Should return the record since timestamp > (timestamp - 1 hour) is always true
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_interval_conversion_accuracy() {
    // Test that intervals convert to correct millisecond values
    let test_cases = vec![
        ("INTERVAL '1' SECOND", 1000),
        ("INTERVAL '1' MINUTE", 60000),
        ("INTERVAL '1' HOUR", 3600000),
        ("INTERVAL '1' DAY", 86400000),
        ("INTERVAL '1000' MILLISECONDS", 1000),
        ("INTERVAL '5' MINUTES", 300000),
        ("INTERVAL '2' HOURS", 7200000),
    ];

    for (interval_expr, expected_millis) in test_cases {
        let query = format!(
            "SELECT {} as interval_value FROM test_stream",
            interval_expr
        );
        let results = execute_query(&query).await.unwrap();

        assert_eq!(results.len(), 1);
        let interval_value = match results[0].fields.get("interval_value") {
            Some(FieldValue::Integer(val)) => *val,
            Some(FieldValue::Float(val)) => *val as i64,
            Some(FieldValue::Interval { value, unit }) => {
                // Convert interval to milliseconds based on unit
                match unit {
                    TimeUnit::Nanosecond => *value / 1_000_000,
                    TimeUnit::Microsecond => *value / 1000,
                    TimeUnit::Millisecond => *value,
                    TimeUnit::Second => *value * 1000,
                    TimeUnit::Minute => *value * 60 * 1000,
                    TimeUnit::Hour => *value * 60 * 60 * 1000,
                    TimeUnit::Day => *value * 24 * 60 * 60 * 1000,
                    TimeUnit::Week => *value * 7 * 24 * 60 * 60 * 1000,
                    TimeUnit::Month => *value * 30 * 24 * 60 * 60 * 1000, // Approximate: 30 days
                    TimeUnit::Year => *value * 365 * 24 * 60 * 60 * 1000, // Approximate: 365 days
                }
            }
            Some(other) => panic!("Unexpected field type for interval_value: {:?}", other),
            None => panic!(
                "interval_value field not found in result: {:?}",
                results[0].fields
            ),
        };

        assert_eq!(
            interval_value, expected_millis,
            "Interval {} should convert to {} milliseconds",
            interval_expr, expected_millis
        );
    }
}

#[tokio::test]
async fn test_interval_with_computed_expressions() {
    // Test INTERVAL in computed expressions
    let results = execute_query(
        "SELECT 
            (timestamp_field + INTERVAL '1' HOUR) - (timestamp_field + INTERVAL '30' MINUTES) as time_diff
         FROM test_stream"
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);
    let time_diff = match results[0].fields.get("time_diff") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for time_diff"),
    };

    // Should be 30 minutes difference (1 hour - 30 minutes = 30 minutes = 1800000 ms)
    assert_eq!(time_diff, 1800000);
}

#[tokio::test]
async fn test_interval_error_cases() {
    // Test invalid INTERVAL usage
    let parser = StreamingSqlParser::new();

    let invalid_queries = vec![
        "SELECT INTERVAL 'abc' MINUTES FROM test", // Invalid number
        "SELECT INTERVAL '5' INVALID_UNIT FROM test", // Invalid time unit
        "SELECT INTERVAL FROM test",               // Missing value and unit
        "SELECT INTERVAL '5' FROM test",           // Missing unit
    ];

    for query in invalid_queries {
        let result = parser.parse(query);
        assert!(result.is_err(), "Query should have failed: {}", query);
    }
}

#[tokio::test]
async fn test_interval_with_null_values() {
    // Test INTERVAL arithmetic with NULL values
    let results =
        execute_query("SELECT NULL + INTERVAL '1' HOUR as null_plus_interval FROM test_stream")
            .await;

    // This might fail or return NULL depending on implementation - both are acceptable
    match results {
        Ok(res) => {
            if !res.is_empty() {
                // If it succeeds, result should be NULL
                assert_eq!(
                    res[0].fields.get("null_plus_interval"),
                    Some(&FieldValue::Null)
                );
            }
        }
        Err(_) => {
            // It's also acceptable for this to fail
        }
    }
}

#[tokio::test]
async fn test_interval_edge_cases() {
    // Test edge cases like zero intervals and large values
    let results = execute_query(
        "SELECT 
            timestamp_field + INTERVAL '0' SECONDS as zero_interval,
            timestamp_field + INTERVAL '999999' MILLISECONDS as large_interval
         FROM test_stream",
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 1);

    let zero_interval = match results[0].fields.get("zero_interval") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for zero_interval"),
    };
    assert_eq!(zero_interval, 1734652800000); // Should be unchanged

    let large_interval = match results[0].fields.get("large_interval") {
        Some(FieldValue::Integer(val)) => *val,
        _ => panic!("Expected integer result for large_interval"),
    };
    assert_eq!(large_interval, 1734652800000 + 999999);
}
