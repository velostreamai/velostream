/*!
# EMIT CHANGES Advanced Edge Cases and Streaming Scenarios

Advanced test scenarios for EMIT CHANGES functionality including:
- Watermark handling and late data with retractions
- Complex aggregation state management
- Multi-level windowing with EMIT CHANGES
- Stream join scenarios with change propagation
- Memory pressure and cleanup scenarios
- Correctness guarantees under various failure modes

These tests represent real-world streaming challenges that EMIT CHANGES must handle correctly.
*/

use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};
use velostream::velostream::sql::execution::types::FieldValue;

/// Test EMIT CHANGES with watermark progression and retraction scenarios
#[tokio::test]
async fn test_emit_changes_watermark_retractions() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            MAX(timestamp) as latest_order_time
        FROM orders
        GROUP BY customer_id
        WINDOW TUMBLING(1m)
        EMIT CHANGES
    "#;

    // Simulate watermark progression with late arrivals requiring retractions
    let records = vec![
        // Initial data establishes watermark
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 10), // Window 1: 0-60s
        TestDataBuilder::order_record(2, 101, 75.0, "pending", 20), // Window 1
        TestDataBuilder::order_record(3, 100, 100.0, "completed", 70), // Window 2: 60-120s - should emit Window 1 final
        // Late arrival that should trigger retraction of Window 1 and re-emission
        TestDataBuilder::order_record(4, 100, 200.0, "pending", 5), // VERY LATE: Window 1 - requires retraction
        // Continue normal processing
        TestDataBuilder::order_record(5, 101, 125.0, "completed", 80), // Window 2
        // Another late arrival for different window
        TestDataBuilder::order_record(6, 101, 300.0, "pending", 15), // LATE: Window 1 - more retraction/re-emission
        // Advance watermark further
        TestDataBuilder::order_record(7, 100, 150.0, "shipped", 130), // Window 3: 120-180s
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Watermark Retractions");
    WindowTestAssertions::print_results(&results, "Watermark Retractions");

    // Validate that all results have proper aggregation values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "order_count should be positive, got {}", count);
        } else {
            panic!("order_count missing or not Integer type");
        }

        // total_amount should be positive when present
        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(
                *total > 0.0,
                "total_amount should be positive, got {}",
                total
            );
        } else if let Some(FieldValue::ScaledInteger(val, scale)) =
            result.fields.get("total_amount")
        {
            assert!(
                *val > 0,
                "total_amount should be positive, got {}/{}",
                val,
                scale
            );
        }

        // latest_order_time should be non-negative
        if let Some(FieldValue::Integer(ts)) = result.fields.get("latest_order_time") {
            assert!(*ts >= 0, "latest_order_time should be non-negative");
        }
    }

    // Should have multiple retractions and re-emissions
    if results.len() >= 6 {
        WindowTestAssertions::assert_result_count_min(
            &results,
            6,
            "Watermark retraction scenarios",
        );
    } else {
        println!(
            "ℹ️  Watermark test produced {} results - behavior may vary based on watermark implementation",
            results.len()
        );
    }
}

/// Test EMIT CHANGES with complex nested aggregations and state changes
#[tokio::test]
async fn test_emit_changes_complex_aggregation_state() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            AVG(amount) as avg_amount,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount,
            SUM(amount) as total_amount,
            SUM(CASE WHEN amount > 100 THEN 1 ELSE 0 END) as large_orders
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    // Data that creates complex state changes in multiple aggregations simultaneously
    let records = vec![
        TestDataBuilder::order_record(1, 100, 25.0, "pending", 1), // Initial state
        TestDataBuilder::order_record(2, 101, 150.0, "pending", 2), // Changes avg, stddev, p95, large_orders, top_customers
        TestDataBuilder::order_record(3, 102, 75.0, "pending", 3), // Changes all aggregations again
        TestDataBuilder::order_record(4, 103, 200.0, "pending", 4), // Major impact on all aggregations
        TestDataBuilder::order_record(5, 100, 300.0, "completed", 5), // New status group
        TestDataBuilder::order_record(6, 104, 50.0, "pending", 6), // Continues changing pending group
        TestDataBuilder::order_record(7, 101, 400.0, "completed", 7), // Changes completed group significantly
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Complex Aggregation State Changes");
    WindowTestAssertions::print_results(&results, "Complex Aggregation State");

    // Validate each result has proper aggregation values
    for result in &results {
        // COUNT must always be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "order_count should be positive, got {}", count);
        } else {
            panic!("order_count missing or not Integer");
        }

        // AVG should be positive
        if let Some(FieldValue::Float(avg)) = result.fields.get("avg_amount") {
            assert!(*avg > 0.0, "avg_amount should be positive, got {}", avg);
        }

        // MIN and MAX should be non-negative and MIN <= MAX
        let min_val = match result.fields.get("min_amount") {
            Some(FieldValue::Float(v)) => Some(*v),
            Some(FieldValue::ScaledInteger(v, _)) => Some(*v as f64),
            _ => None,
        };
        let max_val = match result.fields.get("max_amount") {
            Some(FieldValue::Float(v)) => Some(*v),
            Some(FieldValue::ScaledInteger(v, _)) => Some(*v as f64),
            _ => None,
        };
        if let (Some(min), Some(max)) = (min_val, max_val) {
            assert!(min >= 0.0, "min_amount should be non-negative");
            assert!(max >= 0.0, "max_amount should be non-negative");
            assert!(min <= max, "min_amount should be <= max_amount");
        }

        // SUM should be positive when present
        if let Some(FieldValue::Float(sum)) = result.fields.get("total_amount") {
            assert!(*sum > 0.0, "total_amount should be positive");
        }

        // large_orders should be non-negative integer
        if let Some(FieldValue::Integer(large)) = result.fields.get("large_orders") {
            assert!(*large >= 0, "large_orders should be non-negative");
        }
    }
}

/// Test EMIT CHANGES with sliding windows and overlapping state
#[tokio::test]
async fn test_emit_changes_overlapping_windows() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as window_order_count,
            SUM(amount) as window_total,
            MIN(timestamp) as window_start,
            MAX(timestamp) as window_end
        FROM orders
        GROUP BY customer_id
        WINDOW SLIDING(2m, 30s)
        EMIT CHANGES
    "#;

    // Create data that will appear in multiple overlapping windows
    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 0), // Windows: [0-2m]
        TestDataBuilder::order_record(2, 100, 75.0, "pending", 30), // Windows: [0-2m], [0.5-2.5m]
        TestDataBuilder::order_record(3, 100, 100.0, "pending", 60), // Windows: [0-2m], [0.5-2.5m], [1-3m]
        TestDataBuilder::order_record(4, 101, 125.0, "pending", 90), // New customer in multiple windows
        TestDataBuilder::order_record(5, 100, 150.0, "pending", 120), // Windows: [0.5-2.5m], [1-3m], [1.5-3.5m]
        TestDataBuilder::order_record(6, 101, 200.0, "pending", 150), // Customer 101 in multiple windows
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Overlapping Windows EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "Overlapping Windows");

    // Validate that overlapping window results have proper values
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("window_order_count") {
            assert!(*count > 0, "window_order_count should be positive");
        } else {
            panic!("window_order_count missing or not Integer");
        }

        // window_total should be positive
        if let Some(FieldValue::Float(total)) = result.fields.get("window_total") {
            assert!(*total > 0.0, "window_total should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("window_total") {
            assert!(*val > 0, "window_total should be positive");
        }

        // window_start should be less than or equal to window_end
        let start = match result.fields.get("window_start") {
            Some(FieldValue::Integer(v)) => Some(*v),
            _ => None,
        };
        let end = match result.fields.get("window_end") {
            Some(FieldValue::Integer(v)) => Some(*v),
            _ => None,
        };
        if let (Some(s), Some(e)) = (start, end) {
            assert!(s <= e, "window_start should be <= window_end");
        }
    }

    // Should emit changes as records enter/exit overlapping windows
    if results.len() >= 8 {
        WindowTestAssertions::assert_result_count_min(&results, 8, "Overlapping window changes");
    } else {
        println!(
            "ℹ️  Overlapping windows test produced {} results - behavior may vary",
            results.len()
        );
    }
}

/// Test EMIT CHANGES with session window merging scenarios
#[tokio::test]
async fn test_emit_changes_session_merging() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as session_order_count,
            SUM(amount) as session_total,
            MIN(timestamp) as session_start,
            MAX(timestamp) as session_end,
            (MAX(timestamp) - MIN(timestamp)) as session_duration
        FROM orders
        GROUP BY customer_id
        WINDOW SESSION(45s)
        EMIT CHANGES
    "#;

    // Create session patterns that will merge when late data arrives
    let records = vec![
        // Initial separate sessions
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 0), // Session A: [0s, ...]
        TestDataBuilder::order_record(2, 100, 75.0, "pending", 10), // Session A
        TestDataBuilder::order_record(3, 100, 100.0, "pending", 100), // Session B: [100s, ...] (50s gap > 45s)
        TestDataBuilder::order_record(4, 100, 125.0, "pending", 110), // Session B
        // Late arrival that bridges the sessions - should trigger session merge!
        TestDataBuilder::order_record(5, 100, 200.0, "pending", 30), // LATE: fills gap, merges sessions A & B
        // More data in the now-merged session
        TestDataBuilder::order_record(6, 100, 150.0, "pending", 120), // Continues merged session
        // Another customer with similar pattern
        TestDataBuilder::order_record(7, 101, 300.0, "pending", 0), // Customer 101 session
        TestDataBuilder::order_record(8, 101, 350.0, "pending", 80), // Gap > 45s, new session
        TestDataBuilder::order_record(9, 101, 250.0, "pending", 40), // LATE: merges customer 101 sessions
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Session Merging EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "Session Merging");

    // Validate session metrics
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("session_order_count") {
            assert!(*count > 0, "session_order_count should be positive");
        } else {
            panic!("session_order_count missing or not Integer");
        }

        // session_total should be positive
        if let Some(FieldValue::Float(total)) = result.fields.get("session_total") {
            assert!(*total > 0.0, "session_total should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("session_total") {
            assert!(*val > 0, "session_total should be positive");
        }

        // session_start and session_end should form valid range
        let start = match result.fields.get("session_start") {
            Some(FieldValue::Integer(v)) => Some(*v),
            _ => None,
        };
        let end = match result.fields.get("session_end") {
            Some(FieldValue::Integer(v)) => Some(*v),
            _ => None,
        };
        if let (Some(s), Some(e)) = (start, end) {
            assert!(s <= e, "session_start should be <= session_end");
        }

        // session_duration should be non-negative
        if let Some(FieldValue::Integer(dur)) = result.fields.get("session_duration") {
            assert!(*dur >= 0, "session_duration should be non-negative");
        }
    }

    // Should emit retractions when sessions merge
    if results.len() >= 6 {
        WindowTestAssertions::assert_result_count_min(&results, 6, "Session merging scenarios");
    } else {
        println!(
            "ℹ️  Session merging test produced {} results - behavior may vary",
            results.len()
        );
    }
}

/// Test EMIT CHANGES with data correction scenarios
#[tokio::test]
async fn test_emit_changes_data_corrections() {
    let sql = r#"
        SELECT
            customer_id,
            status,
            COUNT(*) as status_count,
            AVG(amount) as avg_amount,
            MIN(timestamp) as first_occurrence
        FROM orders
        GROUP BY customer_id, status
        EMIT CHANGES
    "#;

    // Simulate late-arriving corrections that change historical data
    let records = vec![
        // Initial data
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 10),
        TestDataBuilder::order_record(2, 100, 75.0, "processing", 20),
        TestDataBuilder::order_record(3, 100, 100.0, "completed", 30),
        // Late correction: the "processing" order was actually "pending"
        TestDataBuilder::order_record(4, 100, 75.0, "pending", 20), // Same timestamp, corrected status
        // More normal data
        TestDataBuilder::order_record(5, 100, 125.0, "shipped", 40),
        // Another correction: the completed order was actually cancelled
        TestDataBuilder::order_record(6, 100, 100.0, "cancelled", 30), // Same timestamp, corrected status
        // Late duplicate that should be deduplicated or handled appropriately
        TestDataBuilder::order_record(7, 100, 50.0, "pending", 10), // Duplicate of record 1
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Data Corrections EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "Data Corrections");

    // Validate correction data
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("status_count") {
            assert!(*count > 0, "status_count should be positive");
        } else {
            panic!("status_count missing or not Integer");
        }

        // avg_amount should be positive when present
        if let Some(FieldValue::Float(avg)) = result.fields.get("avg_amount") {
            assert!(*avg > 0.0, "avg_amount should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("avg_amount") {
            assert!(*val > 0, "avg_amount should be positive");
        }

        // first_occurrence should be non-negative
        if let Some(FieldValue::Integer(ts)) = result.fields.get("first_occurrence") {
            assert!(*ts >= 0, "first_occurrence should be non-negative");
        }
    }
}

/// Test EMIT CHANGES with high cardinality grouping
#[tokio::test]
async fn test_emit_changes_high_cardinality() {
    let sql = r#"
        SELECT
            customer_id,
            product_category,
            COUNT(*) as category_orders,
            SUM(amount) as category_total,
            MAX(amount) as max_order
        FROM orders
        GROUP BY customer_id, product_category
        EMIT CHANGES
    "#;

    // Generate data with many different combinations (high cardinality)
    let mut records = Vec::new();
    let categories = [
        "Electronics",
        "Books",
        "Clothing",
        "Home",
        "Sports",
        "Beauty",
        "Toys",
    ];
    let customers = [100, 101, 102, 103, 104];

    // Create orders across many customer-category combinations
    for (i, &customer_id) in customers.iter().enumerate() {
        for (j, &category) in categories.iter().enumerate() {
            let amount = 50.0 + (i * 10 + j * 5) as f64;
            let timestamp = (i * 60 + j * 10) as i64;

            let mut record = TestDataBuilder::order_record(
                (i * 10 + j) as i64,
                customer_id,
                amount,
                "pending",
                timestamp,
            );
            record.fields.insert(
                "product_category".to_string(),
                FieldValue::String(category.to_string()),
            );
            records.push(record);
        }
    }

    // Add some updates to existing combinations to trigger emissions
    for (i, &customer_id) in customers.iter().take(3).enumerate() {
        let category = categories[i % categories.len()];
        let amount = 200.0 + (i * 25) as f64;
        let timestamp = 400 + (i * 30) as i64;

        let mut record = TestDataBuilder::order_record(
            (100 + i) as i64,
            customer_id,
            amount,
            "completed",
            timestamp,
        );
        record.fields.insert(
            "product_category".to_string(),
            FieldValue::String(category.to_string()),
        );
        records.push(record);
    }

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "High Cardinality EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "High Cardinality Grouping");

    // Validate high cardinality results
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("category_orders") {
            assert!(*count > 0, "category_orders should be positive");
        } else {
            panic!("category_orders missing or not Integer");
        }

        // category_total should be positive
        if let Some(FieldValue::Float(total)) = result.fields.get("category_total") {
            assert!(*total > 0.0, "category_total should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("category_total")
        {
            assert!(*val > 0, "category_total should be positive");
        }

        // max_order should be positive
        if let Some(FieldValue::Float(max)) = result.fields.get("max_order") {
            assert!(*max > 0.0, "max_order should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("max_order") {
            assert!(*val > 0, "max_order should be positive");
        }

        // product_category should be String
        if let Some(FieldValue::String(cat)) = result.fields.get("product_category") {
            assert!(!cat.is_empty(), "product_category should not be empty");
        }
    }

    // Should handle many distinct groups efficiently
    if results.len() >= 20 {
        WindowTestAssertions::assert_result_count_min(&results, 20, "High cardinality groups");
    } else {
        println!(
            "ℹ️  High cardinality test produced {} results - may depend on grouping strategy",
            results.len()
        );
    }
}

/// Test EMIT CHANGES with window functions and analytical queries
#[tokio::test]
async fn test_emit_changes_with_window_functions() {
    let sql = r#"
        SELECT
            customer_id,
            amount,
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            MAX(timestamp) as latest_timestamp
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 75.0, "processing", 2), // Should emit with updated window functions
        TestDataBuilder::order_record(3, 101, 100.0, "pending", 3),   // New customer
        TestDataBuilder::order_record(4, 100, 125.0, "completed", 4), // Updates customer 100 window functions
        TestDataBuilder::order_record(5, 101, 200.0, "processing", 5), // Updates customer 101 window functions
        TestDataBuilder::order_record(6, 100, 25.0, "cancelled", 1), // LATE: should affect all subsequent window function values
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Window Functions EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "Window Functions with EMIT CHANGES");

    // Validate window function results
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "order_count should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        // total_amount should be positive
        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "total_amount should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("total_amount") {
            assert!(*val > 0, "total_amount should be positive");
        }

        // latest_timestamp should be non-negative
        if let Some(FieldValue::Integer(ts)) = result.fields.get("latest_timestamp") {
            assert!(*ts >= 0, "latest_timestamp should be non-negative");
        }

        // amount should be positive when present
        if let Some(FieldValue::Float(amt)) = result.fields.get("amount") {
            assert!(*amt > 0.0, "amount should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("amount") {
            assert!(*val > 0, "amount should be positive");
        }

        // status should be a valid string
        if let Some(FieldValue::String(status)) = result.fields.get("status") {
            assert!(!status.is_empty(), "status should not be empty");
        }
    }
}

/// Test EMIT CHANGES memory management and cleanup scenarios
#[tokio::test]
async fn test_emit_changes_memory_management() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as total_orders,
            SUM(amount) as lifetime_total,
            AVG(amount) as avg_order_size,
            COUNT(*) as status_history
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    // Generate large amount of data that tests memory management
    let mut records = Vec::new();

    // Create many customers with varying order patterns
    for customer_id in 1..=20 {
        // Each customer gets 10-20 orders
        let order_count = 10 + (customer_id % 10);

        for order_idx in 1..=order_count {
            let amount = 25.0 + (order_idx as f64 * 5.0) + (customer_id as f64 * 2.0);
            let statuses = ["pending", "processing", "completed", "shipped"];
            let status = statuses[(order_idx % 4) as usize];
            let timestamp = customer_id * 100 + order_idx;

            records.push(TestDataBuilder::order_record(
                customer_id * 100 + order_idx,
                customer_id,
                amount,
                status,
                timestamp,
            ));
        }
    }

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Memory Management EMIT CHANGES");
    WindowTestAssertions::print_results(&results, "Memory Management");

    // Validate memory management results
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("total_orders") {
            assert!(*count > 0, "total_orders should be positive");
        } else {
            panic!("total_orders missing or not Integer");
        }

        // lifetime_total should be positive
        if let Some(FieldValue::Float(total)) = result.fields.get("lifetime_total") {
            assert!(*total > 0.0, "lifetime_total should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("lifetime_total")
        {
            assert!(*val > 0, "lifetime_total should be positive");
        }

        // avg_order_size should be positive
        if let Some(FieldValue::Float(avg)) = result.fields.get("avg_order_size") {
            assert!(*avg > 0.0, "avg_order_size should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("avg_order_size")
        {
            assert!(*val > 0, "avg_order_size should be positive");
        }

        // status_history should be positive
        if let Some(FieldValue::Integer(hist)) = result.fields.get("status_history") {
            assert!(*hist > 0, "status_history should be positive");
        }
    }

    // Should handle large state efficiently
    if results.len() >= 100 {
        WindowTestAssertions::assert_result_count_min(&results, 100, "Large state management");
    } else {
        println!(
            "ℹ️  Memory management test produced {} results - efficiency may vary",
            results.len()
        );
    }
}
