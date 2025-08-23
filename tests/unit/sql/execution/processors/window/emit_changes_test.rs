/*!
# EMIT CHANGES Comprehensive Test Suite

Tests for EMIT CHANGES functionality in streaming SQL queries.
EMIT CHANGES is a critical streaming concept that controls when and how changes are emitted
from streaming queries, particularly important for:

- Change data capture (CDC) scenarios
- Real-time materialized view updates
- Incremental result streaming
- Stateful stream processing

## Test Categories:
1. Basic EMIT CHANGES functionality
2. EMIT CHANGES with window functions
3. EMIT CHANGES with aggregations
4. EMIT CHANGES edge cases and error scenarios
5. EMIT CHANGES with late arriving data
6. EMIT CHANGES with different data patterns

## Real-world Scenarios:
- Financial trade processing with immediate notifications
- IoT sensor data with change detection
- User activity tracking with state changes
- Inventory management with stock level changes
*/

use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Test basic EMIT CHANGES functionality without windows
#[tokio::test]
async fn test_basic_emit_changes() {
    let sql = r#"
        SELECT 
            customer_id,
            status,
            COUNT(*) as order_count
        FROM orders 
        GROUP BY customer_id, status
        EMIT CHANGES
    "#;

    // Create records that should trigger emissions on state changes
    let records = vec![
        TestDataBuilder::order_record(1, 100, 25.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 35.0, "pending", 2), // Same customer, same status - should emit updated count
        TestDataBuilder::order_record(3, 100, 45.0, "completed", 3), // Same customer, new status - should emit new group
        TestDataBuilder::order_record(4, 101, 50.0, "pending", 4), // New customer - should emit new group
        TestDataBuilder::order_record(5, 100, 60.0, "pending", 5), // Back to first group - should emit updated count
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Basic EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "Basic EMIT CHANGES");

    // Should have multiple emissions as state changes
    WindowTestAssertions::assert_result_count_min(
        &results,
        3,
        "Basic EMIT CHANGES - multiple state changes",
    );
}

/// Test EMIT CHANGES with tumbling windows
#[tokio::test]
async fn test_emit_changes_with_tumbling_window() {
    let sql = r#"
        SELECT 
            status,
            SUM(amount) as total_amount,
            COUNT(*) as order_count
        FROM orders 
        WINDOW TUMBLING(1m)
        GROUP BY status
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 0), // Window 1
        TestDataBuilder::order_record(2, 101, 200.0, "pending", 30), // Window 1 - should emit updated pending total
        TestDataBuilder::order_record(3, 102, 150.0, "completed", 45), // Window 1 - should emit new completed group
        TestDataBuilder::order_record(4, 103, 300.0, "pending", 70), // Window 2 - should emit window 1 final + new window
        TestDataBuilder::order_record(5, 104, 250.0, "completed", 90), // Window 2 - should emit updated completed
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES with Tumbling Window");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Tumbling Window");
}

/// Test EMIT CHANGES with sliding windows - complex scenario
#[tokio::test]
async fn test_emit_changes_with_sliding_window() {
    let sql = r#"
        SELECT 
            customer_id,
            AVG(amount) as avg_amount,
            COUNT(*) as order_count,
            MIN(timestamp) as window_start
        FROM orders 
        WINDOW SLIDING(3m, 1m)
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    // Create overlapping data that will appear in multiple sliding windows
    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 0), // Window 1: 0-3m
        TestDataBuilder::order_record(2, 100, 200.0, "pending", 60), // Window 1&2: 0-3m, 1-4m - should emit change
        TestDataBuilder::order_record(3, 101, 150.0, "pending", 90), // Window 1&2: new customer in both windows
        TestDataBuilder::order_record(4, 100, 300.0, "pending", 120), // Window 2&3: 1-4m, 2-5m - should emit customer 100 change
        TestDataBuilder::order_record(5, 101, 250.0, "pending", 180), // Window 2&3: customer 101 change
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES with Sliding Window");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Sliding Window");
}

/// Test EMIT CHANGES with session windows
#[tokio::test]
async fn test_emit_changes_with_session_window() {
    let sql = r#"
        SELECT 
            customer_id,
            COUNT(*) as session_orders,
            SUM(amount) as session_total,
            MAX(timestamp) as session_end
        FROM orders 
        WINDOW SESSION(30s)
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    // Create session patterns with gaps that should trigger session closures
    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 0), // Session 1 for customer 100
        TestDataBuilder::order_record(2, 100, 75.0, "pending", 10), // Same session (10s gap < 30s)
        TestDataBuilder::order_record(3, 101, 100.0, "pending", 15), // Session 1 for customer 101
        TestDataBuilder::order_record(4, 100, 125.0, "pending", 25), // Same session for 100 (15s gap < 30s)
        TestDataBuilder::order_record(5, 100, 200.0, "pending", 70), // NEW session for 100 (45s gap > 30s) - should emit session 1 final
        TestDataBuilder::order_record(6, 101, 150.0, "pending", 80), // Same session for 101 (65s gap > 30s from last) - new session
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES with Session Window");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Session Window");
}

/// Test EMIT CHANGES with late arriving data - critical streaming scenario
#[tokio::test]
async fn test_emit_changes_with_late_data() {
    let sql = r#"
        SELECT 
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders 
        WINDOW TUMBLING(1m)
        GROUP BY status
        EMIT CHANGES
    "#;

    // Mix in-order and late arriving data
    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10), // Window 1: 0-60s
        TestDataBuilder::order_record(2, 101, 200.0, "completed", 30), // Window 1: 0-60s
        TestDataBuilder::order_record(3, 102, 300.0, "pending", 70), // Window 2: 60-120s - triggers window 1 close
        TestDataBuilder::order_record(4, 103, 150.0, "pending", 45), // LATE: belongs to Window 1 - should emit corrected window 1 results
        TestDataBuilder::order_record(5, 104, 250.0, "completed", 25), // VERY LATE: Window 1 - more corrections
        TestDataBuilder::order_record(6, 105, 400.0, "pending", 90),   // Window 2: current
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES with Late Data");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Late Data");

    // Should emit multiple changes as late data corrects previous windows
    WindowTestAssertions::assert_result_count_min(
        &results,
        4,
        "EMIT CHANGES Late Data - corrections",
    );
}

/// Test EMIT CHANGES edge case: rapid state changes
#[tokio::test]
async fn test_emit_changes_rapid_updates() {
    let sql = r#"
        SELECT 
            customer_id,
            status,
            COUNT(*) as status_count,
            MAX(timestamp) as latest_update
        FROM orders 
        GROUP BY customer_id, status
        EMIT CHANGES
    "#;

    // Customer rapidly changing order status - should emit every change
    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 50.0, "processing", 2), // Status change - emit
        TestDataBuilder::order_record(3, 100, 50.0, "shipped", 3),    // Status change - emit
        TestDataBuilder::order_record(4, 100, 50.0, "delivered", 4),  // Status change - emit
        TestDataBuilder::order_record(5, 100, 50.0, "returned", 5),   // Status change - emit
        TestDataBuilder::order_record(6, 100, 50.0, "refunded", 6),   // Status change - emit
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Rapid Updates");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Rapid Updates");

    // Should emit for each status change
    WindowTestAssertions::assert_result_count_min(&results, 5, "EMIT CHANGES Rapid Updates");
}

/// Test EMIT CHANGES edge case: null values and missing fields
#[tokio::test]
async fn test_emit_changes_null_edge_cases() {
    let sql = r#"
        SELECT 
            customer_id,
            status,
            COUNT(*) as order_count,
            AVG(amount) as avg_amount
        FROM orders 
        GROUP BY customer_id, status
        EMIT CHANGES
    "#;

    // Mix of null statuses and amounts
    let mut records = Vec::new();

    // Order with null status
    let mut record1 = TestDataBuilder::order_record(1, 100, 50.0, "pending", 1);
    record1
        .fields
        .insert("status".to_string(), FieldValue::Null);
    records.push(record1);

    // Order with null amount
    let mut record2 = TestDataBuilder::order_record(2, 100, 0.0, "completed", 2);
    record2
        .fields
        .insert("amount".to_string(), FieldValue::Null);
    records.push(record2);

    // Normal order
    records.push(TestDataBuilder::order_record(3, 100, 75.0, "pending", 3));

    // Another null status (should update the null group)
    let mut record4 = TestDataBuilder::order_record(4, 100, 100.0, "pending", 4);
    record4
        .fields
        .insert("status".to_string(), FieldValue::Null);
    records.push(record4);

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Null Edge Cases");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Null Values");
}

/// Test EMIT CHANGES with extreme values
#[tokio::test]
async fn test_emit_changes_extreme_values() {
    let sql = r#"
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            MAX(amount) as max_amount
        FROM orders 
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 0.01, "pending", 1), // Tiny amount
        TestDataBuilder::order_record(2, 100, 999999.99, "pending", 2), // Huge amount - should emit updated totals
        TestDataBuilder::order_record(3, 100, -50.0, "refund", 3), // Negative amount - should emit
        TestDataBuilder::order_record(4, 100, f64::MAX / 1e6, "pending", 4), // Very large amount
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Extreme Values");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Extreme Values");
}

/// Test EMIT CHANGES with high frequency updates - performance scenario
#[tokio::test]
async fn test_emit_changes_high_frequency() {
    let sql = r#"
        SELECT 
            symbol,
            COUNT(*) as tick_count,
            AVG(price) as avg_price,
            MAX(price) as high_price,
            MIN(price) as low_price
        FROM ticker_feed 
        GROUP BY symbol
        EMIT CHANGES
    "#;

    // Generate high frequency ticker data
    let mut records = Vec::new();
    let symbols = ["AAPL", "GOOGL", "MSFT"];
    let base_prices = [150.0, 2500.0, 300.0];

    // Generate 50 rapid price updates
    for i in 0..50 {
        let symbol_idx = i % 3;
        let symbol = symbols[symbol_idx];
        let base_price = base_prices[symbol_idx];
        let price = base_price + ((i as f64 * 0.1) * if i % 2 == 0 { 1.0 } else { -1.0 });
        let volume = 1000 + (i * 100) as i64;

        let record = TestDataBuilder::ticker_record(symbol, price, volume, i as i64);
        records.push(record);
    }

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES High Frequency");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES High Frequency");

    // Should emit frequently as aggregates change with each tick
    WindowTestAssertions::assert_result_count_min(&results, 10, "EMIT CHANGES High Frequency");
}

/// Test EMIT CHANGES error scenarios and edge cases
#[tokio::test]
async fn test_emit_changes_error_scenarios() {
    // Test 1: EMIT CHANGES with non-aggregated query (should work with GROUP BY)
    let sql1 = r#"
        SELECT 
            customer_id,
            status
        FROM orders 
        GROUP BY customer_id, status
        EMIT CHANGES
    "#;

    let records1 = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 75.0, "completed", 2),
    ];

    let results1 = SqlExecutor::execute_query(sql1, records1).await;
    WindowTestAssertions::assert_has_results(&results1, "EMIT CHANGES Non-Aggregated");

    // Test 2: EMIT CHANGES with window and complex aggregations
    let sql2 = r#"
        SELECT 
            customer_id,
            COUNT(DISTINCT status) as unique_statuses,
            STDDEV(amount) as amount_stddev,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount
        FROM orders 
        WINDOW TUMBLING(30s)
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let records2 = vec![
        TestDataBuilder::order_record(1, 100, 25.0, "pending", 5),
        TestDataBuilder::order_record(2, 100, 75.0, "processing", 10),
        TestDataBuilder::order_record(3, 100, 125.0, "completed", 15),
        TestDataBuilder::order_record(4, 100, 50.0, "shipped", 35), // Next window
    ];

    let results2 = SqlExecutor::execute_query(sql2, records2).await;
    WindowTestAssertions::print_results(&results2, "EMIT CHANGES Complex Aggregations");
}

/// Test EMIT CHANGES with mixed data types
#[tokio::test]
async fn test_emit_changes_mixed_data_types() {
    let sql = r#"
        SELECT 
            customer_id,
            status,
            COUNT(*) as order_count,
            STRING_AGG(id) as order_ids,
            BOOL_OR(amount > 100) as has_large_order
        FROM orders 
        GROUP BY customer_id, status
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 150.0, "pending", 2), // Should trigger has_large_order = true
        TestDataBuilder::order_record(3, 100, 75.0, "pending", 3),
        TestDataBuilder::order_record(4, 100, 200.0, "completed", 4), // New status group
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Mixed Data Types");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Mixed Data Types");
}
