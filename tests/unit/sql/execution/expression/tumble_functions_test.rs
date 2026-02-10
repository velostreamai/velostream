/*!
# Comprehensive Tests for TUMBLE_START and TUMBLE_END Functions

Tests for the TUMBLE_START and TUMBLE_END functions that return window boundary timestamps
for tumbling window queries.

## Test Coverage

- TUMBLE_START reads _window_start metadata correctly
- TUMBLE_END reads _window_end metadata correctly
- Fallback behavior when metadata is missing
- Integration with actual window processor
- Edge cases and error conditions
*/

use std::collections::HashMap;
use velostream::velostream::sql::ast::{Expr, LiteralValue};
use velostream::velostream::sql::execution::expression::functions::BuiltinFunctions;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord, system_columns};

/// Create test record with window metadata
fn create_windowed_record(
    id: i64,
    value: f64,
    timestamp: i64,
    window_start: i64,
    window_end: i64,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Float(value));
    fields.insert(
        system_columns::WINDOW_START.to_string(),
        FieldValue::Integer(window_start),
    );
    fields.insert(
        system_columns::WINDOW_END.to_string(),
        FieldValue::Integer(window_end),
    );

    StreamRecord {
        fields,
        timestamp,
        offset: id,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

/// Create test record without window metadata (non-windowed query)
fn create_non_windowed_record(id: i64, value: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Float(value));

    StreamRecord {
        fields,
        timestamp,
        offset: id,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

#[cfg(test)]
mod tumble_function_tests {
    use super::*;

    #[test]
    fn test_tumble_start_with_window_metadata() {
        // Test that TUMBLE_START correctly reads _window_start from metadata
        let record = create_windowed_record(
            1, 100.0, 5000, // record timestamp
            0,    // window start (beginning of first 5-second window)
            5000, // window end
        );

        let args = vec![];
        let result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &args, &record);

        assert!(result.is_ok(), "TUMBLE_START should succeed");
        match result.unwrap() {
            FieldValue::Integer(ts) => {
                assert_eq!(ts, 0, "TUMBLE_START should return window start timestamp");
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_tumble_end_with_window_metadata() {
        // Test that TUMBLE_END correctly reads _window_end from metadata
        let record = create_windowed_record(
            1, 100.0, 5000, // record timestamp
            0,    // window start
            5000, // window end (end of first 5-second window)
        );

        let args = vec![];
        let result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &args, &record);

        assert!(result.is_ok(), "TUMBLE_END should succeed");
        match result.unwrap() {
            FieldValue::Integer(ts) => {
                assert_eq!(ts, 5000, "TUMBLE_END should return window end timestamp");
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_tumble_start_fallback_without_metadata() {
        // Test fallback behavior when _window_start metadata is missing
        let record = create_non_windowed_record(1, 100.0, 12345);

        let args = vec![];
        let result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &args, &record);

        assert!(
            result.is_ok(),
            "TUMBLE_START should succeed even without metadata"
        );
        match result.unwrap() {
            FieldValue::Integer(ts) => {
                assert_eq!(
                    ts, 12345,
                    "TUMBLE_START should fall back to record timestamp"
                );
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_tumble_end_fallback_without_metadata() {
        // Test fallback behavior when _window_end metadata is missing
        let record = create_non_windowed_record(1, 100.0, 67890);

        let args = vec![];
        let result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &args, &record);

        assert!(
            result.is_ok(),
            "TUMBLE_END should succeed even without metadata"
        );
        match result.unwrap() {
            FieldValue::Integer(ts) => {
                assert_eq!(ts, 67890, "TUMBLE_END should fall back to record timestamp");
            }
            other => panic!("Expected Integer, got {:?}", other),
        }
    }

    #[test]
    fn test_tumble_functions_with_multiple_windows() {
        // Test TUMBLE functions across different window boundaries
        let test_cases = vec![
            // (window_start, window_end, expected_start, expected_end)
            (0, 1000, 0, 1000),                   // First 1-second window
            (1000, 2000, 1000, 2000),             // Second window
            (5000, 6000, 5000, 6000),             // Later window
            (3600000, 3660000, 3600000, 3660000), // 1-minute window at 1 hour mark
        ];

        for (i, (start, end, expected_start, expected_end)) in test_cases.iter().enumerate() {
            let record = create_windowed_record(i as i64, 100.0, *end, *start, *end);

            // Test TUMBLE_START
            let result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &[], &record);
            assert!(result.is_ok(), "TUMBLE_START failed for window {}", i);
            match result.unwrap() {
                FieldValue::Integer(ts) => {
                    assert_eq!(ts, *expected_start, "Window {} start mismatch", i);
                }
                other => panic!("Expected Integer for window {}, got {:?}", i, other),
            }

            // Test TUMBLE_END
            let result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &[], &record);
            assert!(result.is_ok(), "TUMBLE_END failed for window {}", i);
            match result.unwrap() {
                FieldValue::Integer(ts) => {
                    assert_eq!(ts, *expected_end, "Window {} end mismatch", i);
                }
                other => panic!("Expected Integer for window {}, got {:?}", i, other),
            }
        }
    }

    #[test]
    fn test_tumble_functions_accept_arguments_but_ignore_them() {
        // In SQL, TUMBLE_START(event_time, INTERVAL '1' SECOND) is valid syntax
        // The functions accept arguments for compatibility but read from metadata
        let record = create_windowed_record(1, 100.0, 5000, 0, 5000);

        // Test with arguments (should be ignored)
        let args_with_column = vec![Expr::Column("event_time".to_string())];
        let args_with_interval = vec![
            Expr::Column("event_time".to_string()),
            Expr::Literal(LiteralValue::Integer(1000)),
        ];

        // TUMBLE_START with arguments
        let result =
            BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &args_with_column, &record);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FieldValue::Integer(0));

        let result = BuiltinFunctions::evaluate_function_by_name(
            "TUMBLE_START",
            &args_with_interval,
            &record,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FieldValue::Integer(0));

        // TUMBLE_END with arguments
        let result =
            BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &args_with_column, &record);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FieldValue::Integer(5000));

        let result =
            BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &args_with_interval, &record);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FieldValue::Integer(5000));
    }

    #[test]
    fn test_tumble_functions_with_financial_trading_scenario() {
        // Simulate a realistic financial trading scenario with tick buckets
        // Each bucket is 1 second (1000ms)
        let windows = vec![
            // Simulating the tick_buckets query from financial_trading.sql
            (0, 1000),    // First tick bucket: 0-1s
            (1000, 2000), // Second tick bucket: 1-2s
            (2000, 3000), // Third tick bucket: 2-3s
            (3000, 4000), // Fourth tick bucket: 3-4s
        ];

        for (start, end) in windows {
            let record = create_windowed_record(1, 125.50, end, start, end);

            // TUMBLE_START should give bucket start
            let start_result =
                BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &[], &record);
            assert!(start_result.is_ok());
            match start_result.unwrap() {
                FieldValue::Integer(ts) => {
                    assert_eq!(
                        ts, start,
                        "Bucket start mismatch for window [{}, {}]",
                        start, end
                    );
                }
                other => panic!("Expected Integer, got {:?}", other),
            }

            // TUMBLE_END should give bucket end
            let end_result =
                BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &[], &record);
            assert!(end_result.is_ok());
            match end_result.unwrap() {
                FieldValue::Integer(ts) => {
                    assert_eq!(
                        ts, end,
                        "Bucket end mismatch for window [{}, {}]",
                        start, end
                    );
                }
                other => panic!("Expected Integer, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_tumble_functions_zero_timestamp_handling() {
        // Edge case: window starting at timestamp 0
        let record = create_windowed_record(1, 100.0, 0, 0, 1000);

        let start_result =
            BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &[], &record);
        assert!(start_result.is_ok());
        assert_eq!(start_result.unwrap(), FieldValue::Integer(0));

        let end_result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &[], &record);
        assert!(end_result.is_ok());
        assert_eq!(end_result.unwrap(), FieldValue::Integer(1000));
    }

    #[test]
    fn test_tumble_functions_large_timestamp_values() {
        // Test with realistic large timestamps (e.g., actual epoch milliseconds)
        let epoch_2024 = 1704067200000_i64; // January 1, 2024 00:00:00 UTC in milliseconds
        let window_size = 60000; // 1 minute window

        let record = create_windowed_record(
            1,
            100.0,
            epoch_2024 + window_size,
            epoch_2024,
            epoch_2024 + window_size,
        );

        let start_result =
            BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &[], &record);
        assert!(start_result.is_ok());
        assert_eq!(start_result.unwrap(), FieldValue::Integer(epoch_2024));

        let end_result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &[], &record);
        assert!(end_result.is_ok());
        assert_eq!(
            end_result.unwrap(),
            FieldValue::Integer(epoch_2024 + window_size)
        );
    }

    #[test]
    fn test_tumble_functions_with_additional_metadata_fields() {
        // Test that TUMBLE functions work correctly even when record has other metadata
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        fields.insert("value".to_string(), FieldValue::Float(100.0));
        fields.insert(
            system_columns::WINDOW_START.to_string(),
            FieldValue::Integer(0),
        );
        fields.insert(
            system_columns::WINDOW_END.to_string(),
            FieldValue::Integer(5000),
        );
        fields.insert("_window_record_count".to_string(), FieldValue::Integer(42)); // Additional metadata
        fields.insert(
            "custom_field".to_string(),
            FieldValue::String("test".to_string()),
        );

        let record = StreamRecord {
            fields,
            timestamp: 5000,
            offset: 1,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
            topic: None,
            key: None,
        };

        // Should still read the correct window boundaries
        let start_result =
            BuiltinFunctions::evaluate_function_by_name("TUMBLE_START", &[], &record);
        assert_eq!(start_result.unwrap(), FieldValue::Integer(0));

        let end_result = BuiltinFunctions::evaluate_function_by_name("TUMBLE_END", &[], &record);
        assert_eq!(end_result.unwrap(), FieldValue::Integer(5000));
    }
}
