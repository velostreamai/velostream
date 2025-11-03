use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};
use std::collections::HashMap;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Create record with null values for edge case testing
fn create_null_record(id: i64, timestamp_seconds: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("customer_id".to_string(), FieldValue::Null);
    fields.insert("amount".to_string(), FieldValue::Null);
    fields.insert("status".to_string(), FieldValue::Null);
    fields.insert(
        "timestamp".to_string(),
        FieldValue::Integer(timestamp_seconds * 1000),
    );
    StreamRecord::new(fields)
}

/// Create record with extreme values
fn create_extreme_record(
    id: i64,
    customer_id: i64,
    amount: f64,
    timestamp_seconds: i64,
) -> StreamRecord {
    TestDataBuilder::order_record(id, customer_id, amount, "completed", timestamp_seconds)
}

#[cfg(test)]
mod window_edge_cases_tests {
    use super::*;

    // TIMESTAMP EDGE CASES
    #[tokio::test]
    async fn test_zero_timestamp() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0), // Timestamp = 0
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for zero timestamp"
        );

        if let Some(record) = results.first() {
            assert_eq!(
                record.fields.get("count_result"),
                Some(&FieldValue::Integer(1)),
                "COUNT should be 1 for single record at timestamp 0"
            );
        }

        WindowTestAssertions::print_results(&results, "Zero timestamp");
    }

    #[tokio::test]
    async fn test_negative_timestamp() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders 
            WINDOW TUMBLING(1m)
        "#;

        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(1));
        fields.insert("customer_id".to_string(), FieldValue::Integer(100));
        fields.insert("amount".to_string(), FieldValue::Float(25.0));
        fields.insert(
            "status".to_string(),
            FieldValue::String("completed".to_string()),
        );
        fields.insert("timestamp".to_string(), FieldValue::Integer(-60000)); // -1 minute

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let mut record = StreamRecord::new(fields);
        record.timestamp = -60000;

        let records = vec![record];
        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Negative timestamp");
    }

    #[tokio::test]
    async fn test_far_future_timestamp() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders 
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 4102444800), // Year 2100
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Far future timestamp");
    }

    #[tokio::test]
    async fn test_same_exact_timestamp() {
        let sql = r#"
            SELECT COUNT(*) as count_result, SUM(amount) as total
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 30), // All same timestamp
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 30),
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 30),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for identical timestamps"
        );

        if let Some(record) = results.first() {
            assert_eq!(
                record.fields.get("count_result"),
                Some(&FieldValue::Integer(3)),
                "COUNT should be 3 for three records at same timestamp"
            );
            assert_eq!(
                record.fields.get("total"),
                Some(&FieldValue::Float(105.0)),
                "SUM should be 105.0 (25+35+45) for three records at same timestamp"
            );
        }

        WindowTestAssertions::print_results(&results, "Identical timestamps");
    }

    // NULL VALUE EDGE CASES
    #[tokio::test]
    async fn test_null_aggregation_fields() {
        let sql = r#"
            SELECT
                COUNT(*) as total_count,
                COUNT(amount) as non_null_amount_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM orders
            WINDOW TUMBLING(2m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            create_null_record(2, 30), // Null amount
            TestDataBuilder::order_record(3, 102, 35.0, "completed", 60),
            create_null_record(4, 90), // Another null amount
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for null aggregation fields"
        );

        if let Some(record) = results.first() {
            assert_eq!(
                record.fields.get("total_count"),
                Some(&FieldValue::Integer(4)),
                "COUNT(*) should count all 4 records including nulls"
            );
            assert_eq!(
                record.fields.get("non_null_amount_count"),
                Some(&FieldValue::Integer(2)),
                "COUNT(amount) should only count 2 non-null amounts"
            );
            assert_eq!(
                record.fields.get("total_amount"),
                Some(&FieldValue::Float(60.0)),
                "SUM should be 60.0 (25+35) excluding nulls"
            );
        }

        WindowTestAssertions::print_results(&results, "Null aggregation fields");
    }

    #[tokio::test]
    async fn test_null_partition_keys() {
        let sql = r#"
            SELECT
                customer_id,
                COUNT(*) as session_count
            FROM orders
            GROUP BY customer_id
            WINDOW SESSION(2m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            create_null_record(2, 30), // Null customer_id
            TestDataBuilder::order_record(3, 100, 35.0, "completed", 60), // Same customer
            create_null_record(4, 90), // Another null customer_id
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for null partition keys"
        );

        // Results should have sessions for customer_id 100 and null records grouped separately
        if let Some(record) = results.first() {
            // Verify that session_count is a positive integer
            if let Some(FieldValue::Integer(count)) = record.fields.get("session_count") {
                assert!(
                    *count > 0 && *count <= 4,
                    "Session count should be 1-4, got {}",
                    count
                );
            }
        }

        WindowTestAssertions::print_results(&results, "Null partition keys");
    }

    // EXTREME VALUE EDGE CASES
    #[tokio::test]
    async fn test_extreme_large_values() {
        let sql = r#"
            SELECT
                SUM(amount) as total,
                AVG(amount) as average,
                MAX(amount) as maximum
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            create_extreme_record(1, 100, f64::MAX / 1e10, 0), // Very large but not overflow
            TestDataBuilder::order_record(2, 101, 1.0, "completed", 30),
            create_extreme_record(3, 102, 1e15, 45), // Extremely large
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for extreme large values"
        );

        if let Some(record) = results.first() {
            // Should have 3 records in first window and 0 in second
            assert!(
                record.fields.get("maximum").is_some(),
                "MAX function should return a value for extreme large numbers"
            );
        }

        WindowTestAssertions::print_results(&results, "Extreme large values");
    }

    #[tokio::test]
    async fn test_extreme_small_values() {
        let sql = r#"
            SELECT
                SUM(amount) as total,
                AVG(amount) as average,
                MIN(amount) as minimum
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            create_extreme_record(1, 100, f64::MIN_POSITIVE, 0), // Smallest positive
            create_extreme_record(2, 101, -1e15, 30),            // Very negative
            TestDataBuilder::order_record(3, 102, 0.000001, "completed", 45),
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for extreme small values"
        );

        if let Some(record) = results.first() {
            assert!(
                record.fields.get("minimum").is_some(),
                "MIN function should return a value for extreme small numbers"
            );
        }

        WindowTestAssertions::print_results(&results, "Extreme small values");
    }

    #[tokio::test]
    async fn test_special_float_values() {
        let sql = r#"
            SELECT
                COUNT(*) as total_count,
                SUM(amount) as total_sum,
                AVG(amount) as average
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let mut records = vec![TestDataBuilder::order_record(1, 100, 25.0, "completed", 0)];

        // Add NaN record (if supported)
        let mut nan_fields = HashMap::new();
        nan_fields.insert("id".to_string(), FieldValue::Integer(2));
        nan_fields.insert("customer_id".to_string(), FieldValue::Integer(101));
        nan_fields.insert("amount".to_string(), FieldValue::Float(f64::NAN));
        nan_fields.insert(
            "status".to_string(),
            FieldValue::String("completed".to_string()),
        );
        nan_fields.insert("timestamp".to_string(), FieldValue::Integer(30000));

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let mut nan_record = StreamRecord::new(nan_fields);
        nan_record.timestamp = 30000;
        records.push(nan_record);

        // Add infinity record (if supported)
        let mut inf_fields = HashMap::new();
        inf_fields.insert("id".to_string(), FieldValue::Integer(3));
        inf_fields.insert("customer_id".to_string(), FieldValue::Integer(102));
        inf_fields.insert("amount".to_string(), FieldValue::Float(f64::INFINITY));
        inf_fields.insert(
            "status".to_string(),
            FieldValue::String("completed".to_string()),
        );
        inf_fields.insert("timestamp".to_string(), FieldValue::Integer(45000));

        // FR-081: Set StreamRecord metadata timestamp for proper window calculations
        let mut inf_record = StreamRecord::new(inf_fields);
        inf_record.timestamp = 45000;
        records.push(inf_record);

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for special float values"
        );

        if let Some(record) = results.first() {
            // Should count at least the normal record
            assert_eq!(
                record.fields.get("total_count"),
                Some(&FieldValue::Integer(3)),
                "COUNT should be 3 for three records (including NaN and Infinity)"
            );
        }

        WindowTestAssertions::print_results(&results, "Special float values (NaN, Infinity)");
    }

    // WINDOW SIZE EDGE CASES
    #[tokio::test]
    async fn test_very_small_window_size() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders
            WINDOW TUMBLING(1s)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 1), // 1 second later
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 2), // 2 seconds later
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for 1-second window size"
        );

        // Verify at least one result exists and has a positive count
        if let Some(result) = results.first() {
            if let Some(FieldValue::Integer(count)) = result.fields.get("count_result") {
                assert!(*count > 0, "COUNT should be positive, got {}", count);
                // With 1s windows, total should be 3 records across all windows
                assert!(
                    *count <= 3,
                    "COUNT per window should not exceed total records"
                );
            }
        }

        WindowTestAssertions::print_results(&results, "1-second window size");
    }

    #[tokio::test]
    async fn test_very_large_window_size() {
        let sql = r#"
            SELECT COUNT(*) as count_result, AVG(amount) as avg_amount
            FROM orders
            WINDOW TUMBLING(24h)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 43200), // 12 hours later
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 86400), // 24 hours later (next window)
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for 24-hour window"
        );

        if let Some(first_window) = results.first() {
            // Window should have a count and average
            if let Some(FieldValue::Integer(count)) = first_window.fields.get("count_result") {
                assert!(
                    *count > 0 && *count <= 3,
                    "Window should have 1-3 records, got {}",
                    count
                );
            }
            // Verify average is a reasonable value if present
            if let Some(FieldValue::Float(avg)) = first_window.fields.get("avg_amount") {
                assert!(
                    *avg >= 25.0 && *avg <= 1e15,
                    "Average should be within data range, got {}",
                    avg
                );
            }
        }

        WindowTestAssertions::print_results(&results, "24-hour window size");
    }

    // SESSION WINDOW EDGE CASES
    #[tokio::test]
    async fn test_zero_gap_session_window() {
        let sql = r#"
            SELECT
                customer_id,
                COUNT(*) as action_count
            FROM orders
            GROUP BY customer_id
            WINDOW SESSION(0s)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "action1", 0),
            TestDataBuilder::order_record(2, 100, 35.0, "action2", 1), // Should be separate session
            TestDataBuilder::order_record(3, 100, 45.0, "action3", 2), // Should be separate session
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for zero-gap session window"
        );

        // With 0s gap, each record creates a new session, so we should have 3 results
        if results.len() >= 1 {
            let single_count_results = results
                .iter()
                .filter(|r| r.fields.get("action_count") == Some(&FieldValue::Integer(1)))
                .count();
            assert!(
                single_count_results >= 1,
                "Should have at least one session with single record"
            );
        }

        WindowTestAssertions::print_results(&results, "Zero gap session window");
    }

    #[tokio::test]
    async fn test_very_large_session_gap() {
        let sql = r#"
            SELECT
                customer_id,
                COUNT(*) as action_count
            FROM orders
            GROUP BY customer_id
            WINDOW SESSION(1w)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "day1", 0),
            TestDataBuilder::order_record(2, 100, 35.0, "day3", 259200), // 3 days later (day 3)
            TestDataBuilder::order_record(3, 100, 45.0, "day6", 518400), // 3 days later (day 6)
            TestDataBuilder::order_record(4, 100, 55.0, "day16", 1382400), // 10 days later (day 16) - exceeds 1w gap → new session
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for 1-week session gap"
        );

        // First session should have 3 records (within 1 week), second session should have 1 record
        if results.len() >= 1 {
            let three_count_results = results
                .iter()
                .filter(|r| r.fields.get("action_count") == Some(&FieldValue::Integer(3)))
                .count();
            assert!(
                three_count_results >= 1,
                "Should have at least one session with 3 records"
            );
        }

        WindowTestAssertions::print_results(&results, "1-week session gap");
    }

    // SLIDING WINDOW EDGE CASES
    #[tokio::test]
    async fn test_advance_larger_than_window_size() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders
            WINDOW SLIDING(1m, 2m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 60), // 1 min
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 120), // 2 min
            TestDataBuilder::order_record(4, 103, 55.0, "completed", 180), // 3 min
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for sliding window with advance > window"
        );

        // With 1m window and 2m advance, verify results are reasonable
        if let Some(result) = results.first() {
            if let Some(FieldValue::Integer(count)) = result.fields.get("count_result") {
                assert!(*count > 0, "Window count should be positive, got {}", count);
            }
        }

        WindowTestAssertions::print_results(&results, "Advance > window size");
    }

    #[tokio::test]
    async fn test_advance_equal_to_window_size() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders
            WINDOW SLIDING(2m, 2m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0),
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 60), // 1 min
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 120), // 2 min (should behave like tumbling)
            TestDataBuilder::order_record(4, 103, 55.0, "completed", 180), // 3 min
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for sliding window with advance = window size"
        );

        // With 2m window and 2m advance, verify results are reasonable
        if let Some(result) = results.first() {
            if let Some(FieldValue::Integer(count)) = result.fields.get("count_result") {
                assert!(*count > 0, "Window count should be positive, got {}", count);
            }
        }

        WindowTestAssertions::print_results(&results, "Advance = window size (tumbling behavior)");
    }

    // EMPTY DATA EDGE CASES
    #[tokio::test]
    async fn test_no_records() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let records: Vec<StreamRecord> = vec![];
        let results = SqlExecutor::execute_query(sql, records).await;

        // Empty input should produce no results or results with 0 count
        if !results.is_empty() {
            if let Some(record) = results.first() {
                assert_eq!(
                    record.fields.get("count_result"),
                    Some(&FieldValue::Integer(0)),
                    "COUNT should be 0 for no records"
                );
            }
        }

        WindowTestAssertions::print_results(&results, "No records");
    }

    #[tokio::test]
    async fn test_single_record() {
        let sql = r#"
            SELECT
                COUNT(*) as count_result,
                SUM(amount) as total,
                AVG(amount) as average,
                MIN(amount) as minimum,
                MAX(amount) as maximum
            FROM orders
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![TestDataBuilder::order_record(1, 100, 42.0, "completed", 30)];

        let results = SqlExecutor::execute_query(sql, records).await;
        assert!(
            !results.is_empty(),
            "Should produce results for single record"
        );

        if let Some(record) = results.first() {
            assert_eq!(
                record.fields.get("count_result"),
                Some(&FieldValue::Integer(1)),
                "COUNT should be 1 for single record"
            );
            assert_eq!(
                record.fields.get("total"),
                Some(&FieldValue::Float(42.0)),
                "SUM should be 42.0"
            );
            assert_eq!(
                record.fields.get("average"),
                Some(&FieldValue::Float(42.0)),
                "AVG should be 42.0"
            );
            assert_eq!(
                record.fields.get("minimum"),
                Some(&FieldValue::Float(42.0)),
                "MIN should be 42.0"
            );
            assert_eq!(
                record.fields.get("maximum"),
                Some(&FieldValue::Float(42.0)),
                "MAX should be 42.0"
            );
        }

        WindowTestAssertions::print_results(&results, "Single record");
    }

    // HIGH VOLUME EDGE CASES
    #[tokio::test]
    async fn test_high_frequency_records() {
        let sql = r#"
            SELECT COUNT(*) as count_result
            FROM orders 
            WINDOW SLIDING(1s, 100ms)
        "#;

        // Generate 100 records in 1 second (every 10ms)
        let mut records = Vec::new();
        for i in 0..100 {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i + 1));
            fields.insert("customer_id".to_string(), FieldValue::Integer(100));
            fields.insert("amount".to_string(), FieldValue::Float(10.0));
            fields.insert(
                "status".to_string(),
                FieldValue::String("completed".to_string()),
            );
            fields.insert("timestamp".to_string(), FieldValue::Integer(i * 10)); // Every 10ms

            // FR-081: Set StreamRecord metadata timestamp for proper window calculations
            let mut record = StreamRecord::new(fields);
            record.timestamp = i * 10;
            records.push(record);
        }

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "High frequency (10ms intervals)");
    }

    // COMPLEX AGGREGATION EDGE CASES
    #[tokio::test]
    async fn test_division_by_zero_scenarios() {
        let sql = r#"
            SELECT 
                AVG(amount) as avg_amount,
                COUNT(*) as record_count,
                SUM(amount) / COUNT(*) as manual_average
            FROM orders 
            WHERE amount > 0  -- Filter to create potential empty windows
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 0.0, "completed", 0), // Filtered out
            TestDataBuilder::order_record(2, 101, -5.0, "completed", 30), // Filtered out
            TestDataBuilder::order_record(3, 102, 25.0, "completed", 60), // Only valid record
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Division by zero scenarios");
    }

    // LATE ARRIVING DATA AND OUT-OF-ORDER EDGE CASES
    #[tokio::test]
    async fn test_late_arriving_data() {
        let sql = r#"
            SELECT COUNT(*) as count_result, SUM(amount) as total
            FROM orders 
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 0), // Window 1: 0-60s
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 30), // Window 1: 0-60s
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 90), // Window 2: 60-120s
            TestDataBuilder::order_record(4, 103, 15.0, "completed", 45), // LATE: belongs to Window 1 (0-60s)
            TestDataBuilder::order_record(5, 104, 55.0, "completed", 120), // Window 3: 120-180s
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Late arriving data handling");
    }

    #[tokio::test]
    async fn test_severely_out_of_order_data() {
        let sql = r#"
            SELECT 
                COUNT(*) as count_result,
                MIN(timestamp) as window_start,
                MAX(timestamp) as window_end
            FROM orders 
            WINDOW TUMBLING(30s)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "completed", 90), // Future data first
            TestDataBuilder::order_record(2, 101, 35.0, "completed", 10), // Very late arriving
            TestDataBuilder::order_record(3, 102, 45.0, "completed", 120), // Even further future
            TestDataBuilder::order_record(4, 103, 15.0, "completed", 5),  // Extremely late
            TestDataBuilder::order_record(5, 104, 55.0, "completed", 75), // Moderately out of order
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Severely out-of-order data");
    }

    #[tokio::test]
    async fn test_mixed_early_and_late_data() {
        let sql = r#"
            SELECT 
                customer_id,
                COUNT(*) as session_events,
                AVG(amount) as avg_amount
            FROM orders 
            GROUP BY customer_id
            WINDOW SESSION(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 50.0, "start", 60), // Normal time
            TestDataBuilder::order_record(2, 100, 75.0, "action1", 30), // Early data (should extend session backwards)
            TestDataBuilder::order_record(3, 100, 25.0, "action2", 90), // Continue session
            TestDataBuilder::order_record(4, 100, 100.0, "action3", 45), // Late data in middle of session
            TestDataBuilder::order_record(5, 100, 200.0, "end", 150), // Gap > 1 min = new session
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Mixed early and late data in sessions");
    }

    #[tokio::test]
    async fn test_late_data_beyond_window_grace_period() {
        let sql = r#"
            SELECT 
                COUNT(*) as count_result,
                SUM(amount) as total_amount
            FROM orders 
            WINDOW SLIDING(2m, 1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 100.0, "completed", 0), // Window 1
            TestDataBuilder::order_record(2, 101, 200.0, "completed", 60), // Window 2
            TestDataBuilder::order_record(3, 102, 300.0, "completed", 120), // Window 3
            TestDataBuilder::order_record(4, 103, 50.0, "completed", 180), // Window 4
            TestDataBuilder::order_record(5, 104, 25.0, "late1", 15),     // Very late for Window 1
            TestDataBuilder::order_record(6, 105, 75.0, "late2", 30),     // Late for Window 1
            TestDataBuilder::order_record(7, 106, 150.0, "late3", 90),    // Late for Window 2
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Late data beyond grace period");
    }

    #[tokio::test]
    async fn test_duplicate_timestamps_out_of_order() {
        let sql = r#"
            SELECT 
                COUNT(*) as total_count,
                COUNT(customer_id) as unique_customers,
                SUM(amount) as total_amount
            FROM orders 
            WINDOW TUMBLING(1m)
        "#;

        let records = vec![
            TestDataBuilder::order_record(1, 100, 25.0, "order1", 30), // Same timestamp
            TestDataBuilder::order_record(2, 101, 35.0, "order2", 30), // Same timestamp
            TestDataBuilder::order_record(3, 102, 45.0, "order3", 30), // Same timestamp
            TestDataBuilder::order_record(4, 103, 55.0, "order4", 90), // Later
            TestDataBuilder::order_record(5, 104, 15.0, "order5", 30), // Late with same timestamp
            TestDataBuilder::order_record(6, 105, 65.0, "order6", 30), // Another late with same timestamp
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Duplicate timestamps out of order");
    }

    #[tokio::test]
    async fn test_watermark_simulation_with_late_data() {
        let sql = r#"
            SELECT 
                COUNT(*) as event_count,
                AVG(amount) as avg_amount,
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts
            FROM orders 
            WINDOW TUMBLING(30s)
        "#;

        // Simulate a streaming scenario where data arrives with some delay
        // and we need to handle watermarks for late data
        let records = vec![
            TestDataBuilder::order_record(1, 100, 100.0, "on_time", 10), // On time
            TestDataBuilder::order_record(2, 101, 200.0, "on_time", 20), // On time
            TestDataBuilder::order_record(3, 102, 300.0, "future", 60),  // Advances watermark
            TestDataBuilder::order_record(4, 103, 150.0, "late1", 5),    // Late by ~5s
            TestDataBuilder::order_record(5, 104, 175.0, "late2", 15),   // Late but acceptable
            TestDataBuilder::order_record(6, 105, 400.0, "future2", 90), // Advances watermark further
            TestDataBuilder::order_record(7, 106, 50.0, "very_late", 2), // Very late data
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Watermark simulation with late data");
    }

    #[tokio::test]
    async fn test_session_window_late_data_merging() {
        let sql = r#"
            SELECT 
                customer_id,
                COUNT(*) as total_actions,
                MIN(timestamp) as session_start,
                MAX(timestamp) as session_end,
                SUM(amount) as session_value
            FROM orders 
            GROUP BY customer_id
            WINDOW SESSION(2m)
        "#;

        let records = vec![
            // Customer 100: Create initial session
            TestDataBuilder::order_record(1, 100, 50.0, "action1", 60), // Session start
            TestDataBuilder::order_record(2, 100, 75.0, "action2", 90), // Continue session
            // Customer 100: Late data that should merge sessions
            TestDataBuilder::order_record(3, 100, 100.0, "late_action", 30), // Late data that extends session backwards
            // Customer 100: More future actions
            TestDataBuilder::order_record(4, 100, 125.0, "action3", 120), // Continue session
            TestDataBuilder::order_record(5, 100, 200.0, "action4", 300), // New session (gap > 2 min)
            // Customer 100: Very late data that might merge the two sessions
            TestDataBuilder::order_record(6, 100, 80.0, "bridge_action", 180), // Bridges sessions?
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Session window late data merging");
    }

    #[tokio::test]
    async fn test_sliding_window_with_chaotic_arrival_order() {
        let sql = r#"
            SELECT 
                AVG(amount) as moving_avg,
                COUNT(*) as window_count,
                MAX(amount) - MIN(amount) as price_range
            FROM orders 
            WINDOW SLIDING(3m, 1m)
        "#;

        // Completely chaotic arrival order to stress test the windowing system
        let records = vec![
            TestDataBuilder::order_record(1, 100, 500.0, "chaos", 300), // Way in future
            TestDataBuilder::order_record(2, 101, 100.0, "chaos", 0),   // Back to start
            TestDataBuilder::order_record(3, 102, 400.0, "chaos", 240), // Moderately future
            TestDataBuilder::order_record(4, 103, 200.0, "chaos", 60),  // Early middle
            TestDataBuilder::order_record(5, 104, 450.0, "chaos", 360), // Even further future
            TestDataBuilder::order_record(6, 105, 150.0, "chaos", 30),  // Early
            TestDataBuilder::order_record(7, 106, 350.0, "chaos", 180), // Middle
            TestDataBuilder::order_record(8, 107, 250.0, "chaos", 90),  // Early middle
            TestDataBuilder::order_record(9, 108, 600.0, "chaos", 420), // Far future
            TestDataBuilder::order_record(10, 109, 50.0, "chaos", 15),  // Very early
        ];

        let results = SqlExecutor::execute_query(sql, records).await;
        WindowTestAssertions::print_results(&results, "Sliding window with chaotic arrival order");
    }
}
