/*!
# Window Frame Execution Tests with Value Assertions

**IMPORTANT FINDING**: Window frame execution is NOT currently implemented.

The Velostream streaming SQL engine supports parsing of window frame specifications (ROWS BETWEEN,
RANGE BETWEEN) but does NOT apply them during query execution. This means:

- ✅ Parser accepts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW syntax
- ❌ Aggregation does NOT filter records based on frame bounds
- ❌ All records in the window partition are aggregated (frame is effectively ignored)

**Current Behavior**: Each window aggregation includes ALL rows from the start of the window partition,
regardless of the ROWS BETWEEN specification.

**Example Issue**:
```sql
SELECT AVG(value) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM data
```

For input [10.0, 20.0, 30.0]:
- Expected Row 2: AVG(10.0, 20.0) = 15.0 (rows 1-2)
- Actual Row 2: 20.0 (only current row, or miscalculated)

**Root Cause**: The `calculate_frame_bounds()` function exists in window_functions.rs (lines 281-353)
but is never called. The aggregation accumulator in aggregation/accumulator.rs includes ALL records
without filtering by frame bounds.

**To Fix**: Requires comprehensive changes to:
1. Apply window frame bounds during record aggregation
2. Filter records based on start_offset and end_offset for each frame bound type
3. Rebuild aggregations for each row considering only frame-filtered records

These are comprehensive execution tests that document the current gap and the expected behavior
once window frame execution is implemented.
*/

use std::collections::HashMap;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

use super::shared_test_utils::SqlExecutor;

/// Helper function to extract floating-point value from StreamRecord result string
/// Parses StreamRecord debug output with fields like: Float(10.0), Integer(1), etc.
fn extract_value_from_result(result: &str, field_name: &str) -> Option<f64> {
    // Look for pattern: "field_name": Float(value) or similar in the result string
    let pattern = format!("\"{}\": Float(", field_name);
    if let Some(start_idx) = result.find(&pattern) {
        let float_start = start_idx + pattern.len();
        if let Some(end_idx) = result[float_start..].find(')') {
            let value_str = &result[float_start..float_start + end_idx];
            return value_str.parse::<f64>().ok();
        }
    }
    None
}

/// Helper function to extract integer value from StreamRecord result string
fn extract_int_from_result(result: &str, field_name: &str) -> Option<i64> {
    // Look for pattern: "field_name": Integer(value) in the result string
    let pattern = format!("\"{}\": Integer(", field_name);
    if let Some(start_idx) = result.find(&pattern) {
        let int_start = start_idx + pattern.len();
        if let Some(end_idx) = result[int_start..].find(')') {
            let value_str = &result[int_start..int_start + end_idx];
            return value_str.parse::<i64>().ok();
        }
    }
    None
}

fn create_test_record(id: i64, value: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Float(value));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp,
        offset: id,
        partition: 0,
    }
}

/// Test ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
/// Window includes all rows from start through current row
/// For value [10,20,30,40,50]:
/// - Row 1: AVG(10) = 10.0
/// - Row 2: AVG(10,20) = 15.0
/// - Row 3: AVG(10,20,30) = 20.0
/// - Row 4: AVG(10,20,30,40) = 25.0
/// - Row 5: AVG(10,20,30,40,50) = 30.0
#[tokio::test]
async fn test_rows_between_unbounded_preceding_execution() {
    let query = "SELECT id, value, AVG(value) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_avg FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected running averages
    let expected_avgs = vec![10.0, 15.0, 20.0, 25.0, 30.0];
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!("✅ UNBOUNDED PRECEDING - Result {}: {}", i + 1, result);

        if let Some(actual_avg) = extract_value_from_result(result, "running_avg") {
            let expected = expected_avgs[i];
            assert!(
                (actual_avg - expected).abs() < tolerance,
                "Row {} AVG mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_avg
            );
        } else {
            panic!(
                "Could not extract running_avg value from result: {}",
                result
            );
        }
    }
}

/// Test ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
/// Window includes current row and 1 row ahead
/// For value [10,20,30,40,50]:
/// - Row 1: AVG(10,20) = 15.0
/// - Row 2: AVG(20,30) = 25.0
/// - Row 3: AVG(30,40) = 35.0
/// - Row 4: AVG(40,50) = 45.0
/// - Row 5: AVG(50) = 50.0 (no following row)
#[tokio::test]
async fn test_rows_between_current_and_following_execution() {
    let query = "SELECT id, value, AVG(value) OVER (ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) as forward_avg FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected forward averages
    let expected_avgs = vec![15.0, 25.0, 35.0, 45.0, 50.0];
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!(
            "✅ CURRENT ROW AND FOLLOWING - Result {}: {}",
            i + 1,
            result
        );

        if let Some(actual_avg) = extract_value_from_result(result, "forward_avg") {
            let expected = expected_avgs[i];
            assert!(
                (actual_avg - expected).abs() < tolerance,
                "Row {} AVG mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_avg
            );
        } else {
            panic!(
                "Could not extract forward_avg value from result: {}",
                result
            );
        }
    }
}

/// Test ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
/// Window includes 2 rows back and 1 row back (excludes current row)
/// For value [10,20,30,40,50]:
/// - Row 1: No window (< 2 rows exist)
/// - Row 2: No window (< 2 rows exist)
/// - Row 3: AVG(10,20) = 15.0 (rows 1,2)
/// - Row 4: AVG(20,30) = 25.0 (rows 2,3)
/// - Row 5: AVG(30,40) = 35.0 (rows 3,4)
#[tokio::test]
async fn test_rows_between_preceding_execution() {
    let query = "SELECT id, value, AVG(value) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) as prev_avg FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected values - only rows 3-5 have valid windows
    // Rows 1-2 have no window (not enough preceding rows)
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!(
            "✅ ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING - Result {}: {}",
            i + 1,
            result
        );

        // Only rows 3-5 (indices 2-4) should have valid window values
        if i >= 2 {
            // Rows 3-5: expected values 15.0, 25.0, 35.0
            let expected_avgs = vec![15.0, 25.0, 35.0];
            if let Some(actual_avg) = extract_value_from_result(result, "prev_avg") {
                let expected = expected_avgs[i - 2];
                assert!(
                    (actual_avg - expected).abs() < tolerance,
                    "Row {} AVG mismatch: expected {}, got {}",
                    i + 1,
                    expected,
                    actual_avg
                );
            } else {
                panic!("Could not extract prev_avg value from result: {}", result);
            }
        }
    }
}

/// Test ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
/// Window includes current row and all rows after
/// For value [10,20,30,40,50]:
/// - Row 1: AVG(10,20,30,40,50) = 30.0
/// - Row 2: AVG(20,30,40,50) = 35.0
/// - Row 3: AVG(30,40,50) = 40.0
/// - Row 4: AVG(40,50) = 45.0
/// - Row 5: AVG(50) = 50.0
#[tokio::test]
async fn test_rows_between_unbounded_following_execution() {
    let query = "SELECT id, value, AVG(value) OVER (ORDER BY timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as trailing_avg FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected trailing averages
    let expected_avgs = vec![30.0, 35.0, 40.0, 45.0, 50.0];
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!("✅ UNBOUNDED FOLLOWING - Result {}: {}", i + 1, result);

        if let Some(actual_avg) = extract_value_from_result(result, "trailing_avg") {
            let expected = expected_avgs[i];
            assert!(
                (actual_avg - expected).abs() < tolerance,
                "Row {} AVG mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_avg
            );
        } else {
            panic!(
                "Could not extract trailing_avg value from result: {}",
                result
            );
        }
    }
}

/// Test COUNT aggregation with ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
/// For 5 records:
/// - Row 1: COUNT(1) = 1 (only current)
/// - Row 2: COUNT(2) = 2 (1 preceding + current)
/// - Row 3: COUNT(2) = 2 (1 preceding + current)
/// - Row 4: COUNT(2) = 2 (1 preceding + current)
/// - Row 5: COUNT(2) = 2 (1 preceding + current)
#[tokio::test]
async fn test_rows_between_count_execution() {
    let query = "SELECT id, COUNT(*) OVER (ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as count_window FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected COUNT results: 1, 2, 2, 2, 2
    let expected_counts = vec![1i64, 2i64, 2i64, 2i64, 2i64];

    for (i, result) in results.iter().enumerate() {
        println!("✅ COUNT with window frame - Result {}: {}", i + 1, result);

        if let Some(actual_count) = extract_int_from_result(result, "count_window") {
            let expected = expected_counts[i];
            assert_eq!(
                actual_count,
                expected,
                "Row {} COUNT mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_count
            );
        } else {
            panic!(
                "Could not extract count_window value from result: {}",
                result
            );
        }
    }
}

/// Test SUM aggregation with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
/// For value [10,20,30,40,50]:
/// - Row 1: SUM(10) = 10 (only current, no preceding)
/// - Row 2: SUM(10,20) = 30 (1 preceding + current)
/// - Row 3: SUM(10,20,30) = 60 (2 preceding + current)
/// - Row 4: SUM(20,30,40) = 90 (2 preceding + current)
/// - Row 5: SUM(30,40,50) = 120 (2 preceding + current)
#[tokio::test]
async fn test_rows_between_sum_execution() {
    let query = "SELECT id, value, SUM(value) OVER (ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum_window FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected SUM results: 10, 30, 60, 90, 120
    let expected_sums = vec![10.0, 30.0, 60.0, 90.0, 120.0];
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!("✅ SUM with window frame - Result {}: {}", i + 1, result);

        if let Some(actual_sum) = extract_value_from_result(result, "sum_window") {
            let expected = expected_sums[i];
            assert!(
                (actual_sum - expected).abs() < tolerance,
                "Row {} SUM mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_sum
            );
        } else {
            panic!("Could not extract sum_window value from result: {}", result);
        }
    }
}

/// Test MAX aggregation with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
/// Window includes 1 row before, current row, and 1 row after
/// For value [10,20,30,40,50]:
/// - Row 1: MAX(10,20) = 20
/// - Row 2: MAX(10,20,30) = 30
/// - Row 3: MAX(20,30,40) = 40
/// - Row 4: MAX(30,40,50) = 50
/// - Row 5: MAX(40,50) = 50
#[tokio::test]
async fn test_rows_between_max_execution() {
    let query = "SELECT id, value, MAX(value) OVER (ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as max_window FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected MAX results: 20, 30, 40, 50, 50
    let expected_maxes = vec![20.0, 30.0, 40.0, 50.0, 50.0];
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!("✅ MAX with window frame - Result {}: {}", i + 1, result);

        if let Some(actual_max) = extract_value_from_result(result, "max_window") {
            let expected = expected_maxes[i];
            assert!(
                (actual_max - expected).abs() < tolerance,
                "Row {} MAX mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_max
            );
        } else {
            panic!("Could not extract max_window value from result: {}", result);
        }
    }
}

/// Test MIN aggregation with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
/// For value [10,20,30,40,50]:
/// - Row 1: MIN(10,20) = 10
/// - Row 2: MIN(10,20,30) = 10
/// - Row 3: MIN(20,30,40) = 20
/// - Row 4: MIN(30,40,50) = 30
/// - Row 5: MIN(40,50) = 40
#[tokio::test]
async fn test_rows_between_min_execution() {
    let query = "SELECT id, value, MIN(value) OVER (ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as min_window FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // Expected MIN results: 10, 10, 20, 30, 40
    let expected_mins = vec![10.0, 10.0, 20.0, 30.0, 40.0];
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!("✅ MIN with window frame - Result {}: {}", i + 1, result);

        if let Some(actual_min) = extract_value_from_result(result, "min_window") {
            let expected = expected_mins[i];
            assert!(
                (actual_min - expected).abs() < tolerance,
                "Row {} MIN mismatch: expected {}, got {}",
                i + 1,
                expected,
                actual_min
            );
        } else {
            panic!("Could not extract min_window value from result: {}", result);
        }
    }
}

/// Test rolling average pattern from financial_trading.sql
/// ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING creates a 20-row rolling window
/// This test uses smaller window (ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) for simpler verification
/// For value [10,20,30,40,50]:
/// - Row 1-3: Not enough preceding rows, window undefined
/// - Row 4: AVG(10,20,30) = 20.0 (3 rows before current, excluding current)
/// - Row 5: AVG(20,30,40) = 30.0 (3 rows before current, excluding current)
#[tokio::test]
async fn test_rolling_window_pattern_execution() {
    let query = "SELECT id, value, AVG(value) OVER (ORDER BY timestamp ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) as rolling_avg FROM test_data";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert_eq!(
        results.len(),
        5,
        "Should have 5 results for 5 input records"
    );

    // This is the exact pattern used in financial_trading.sql for rolling_avg_20
    // Verifies that the window frame produces correct rolling averages
    let tolerance = 0.001;

    for (i, result) in results.iter().enumerate() {
        println!("✅ Rolling window pattern - Result {}: {}", i + 1, result);

        // Only rows 4-5 (indices 3-4) have valid windows for ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        if i >= 3 {
            // Rows 4-5: expected values 20.0, 30.0
            let expected_avgs = vec![20.0, 30.0];
            if let Some(actual_avg) = extract_value_from_result(result, "rolling_avg") {
                let expected = expected_avgs[i - 3];
                assert!(
                    (actual_avg - expected).abs() < tolerance,
                    "Row {} AVG mismatch: expected {}, got {}",
                    i + 1,
                    expected,
                    actual_avg
                );
            } else {
                panic!(
                    "Could not extract rolling_avg value from result: {}",
                    result
                );
            }
        }
    }
    println!("   (This mimics the ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING pattern from financial_trading.sql)");
}
