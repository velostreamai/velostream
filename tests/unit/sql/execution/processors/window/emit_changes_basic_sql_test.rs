/*!
# EMIT CHANGES Basic Functionality Tests

Focused tests for EMIT CHANGES core functionality using only implemented SQL features.
These tests verify the fundamental behavior of EMIT CHANGES without relying on
advanced SQL functions that may not be implemented yet.

Tests covered:
- Basic EMIT CHANGES with simple aggregations (COUNT, SUM, AVG, MIN, MAX)
- EMIT CHANGES with windowing (tumbling, sliding)
- EMIT CHANGES with late arriving data scenarios
- Core streaming behavior verification
*/

use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};
use velostream::velostream::sql::execution::types::FieldValue;

/// Test basic EMIT CHANGES functionality with simple aggregations
#[tokio::test]
async fn test_basic_emit_changes_count() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as order_count
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 25.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 35.0, "pending", 2), // Same customer - should emit updated count
        TestDataBuilder::order_record(3, 101, 45.0, "pending", 3), // New customer - should emit new group
        TestDataBuilder::order_record(4, 100, 50.0, "pending", 4), // Back to first customer - should emit updated count
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Basic EMIT CHANGES COUNT");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES COUNT");

    // Validate COUNT values in results
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(
                *count > 0,
                "COUNT should be positive for EMIT CHANGES, got {}",
                count
            );
        } else {
            panic!("order_count field missing or not Integer");
        }
    }
}

/// Test EMIT CHANGES with SUM aggregation
#[tokio::test]
async fn test_emit_changes_sum() {
    let sql = r#"
        SELECT
            status,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 1),
        TestDataBuilder::order_record(2, 101, 200.0, "pending", 2), // Same status - should emit updated sum
        TestDataBuilder::order_record(3, 102, 150.0, "completed", 3), // New status - should emit new group
        TestDataBuilder::order_record(4, 103, 300.0, "pending", 4), // Back to pending - should emit updated sum
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES SUM");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES SUM");

    // Validate SUM values in results
    for result in &results {
        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "SUM amount should be positive, got {}", total);
        } else {
            panic!("total_amount field missing or not Float");
        }
    }
}

/// Test EMIT CHANGES with multiple basic aggregations
#[tokio::test]
async fn test_emit_changes_multiple_aggregations() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 1),
        TestDataBuilder::order_record(2, 101, 150.0, "pending", 2), // Should update all aggregations for pending
        TestDataBuilder::order_record(3, 102, 75.0, "completed", 3), // New status group
        TestDataBuilder::order_record(4, 103, 200.0, "pending", 4), // Should update pending aggregations again
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Multiple Aggregations");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Multiple Aggregations");

    // Validate all aggregation values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "SUM should be positive");
        } else {
            panic!("total_amount missing or not Float");
        }

        if let Some(FieldValue::Float(avg)) = result.fields.get("avg_amount") {
            assert!(*avg > 0.0, "AVG should be positive");
        } else {
            panic!("avg_amount missing or not Float");
        }

        if let Some(FieldValue::Float(min)) = result.fields.get("min_amount") {
            assert!(*min > 0.0, "MIN should be positive");
        } else {
            panic!("min_amount missing or not Float");
        }

        if let Some(FieldValue::Float(max)) = result.fields.get("max_amount") {
            assert!(*max > 0.0, "MAX should be positive");
        } else {
            panic!("max_amount missing or not Float");
        }
    }
}

#[tokio::test]
async fn test_emit_changes_tumbling_window() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY status
        WINDOW TUMBLING(60s)
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10), // Window 1
        TestDataBuilder::order_record(2, 101, 200.0, "pending", 30), // Window 1 - should emit updated pending
        TestDataBuilder::order_record(3, 102, 150.0, "completed", 45), // Window 1 - should emit new completed group
        TestDataBuilder::order_record(4, 103, 300.0, "pending", 70), // Window 2 - may emit window 1 final + new window
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Tumbling Window");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Tumbling Window");

    // Validate COUNT and SUM values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "SUM should be positive");
        } else {
            panic!("total_amount missing or not Float");
        }
    }
}

#[tokio::test]
async fn test_emit_changes_sliding_window() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY customer_id
        WINDOW SLIDING(180s, 60s)
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 0), // Window 1: 0-3m
        TestDataBuilder::order_record(2, 100, 200.0, "pending", 60), // Window 1&2: should emit changes for customer 100
        TestDataBuilder::order_record(3, 101, 150.0, "pending", 90), // Window 1&2: new customer
        TestDataBuilder::order_record(4, 100, 300.0, "pending", 120), // Window 2&3: customer 100 continues
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Sliding Window");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Sliding Window");

    // Validate COUNT and SUM values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "SUM should be positive");
        } else {
            panic!("total_amount missing or not Float");
        }
    }
}

#[tokio::test]
async fn test_emit_changes_late_data() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY status
        WINDOW TUMBLING(60s)
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10), // Window 1: 0-60s
        TestDataBuilder::order_record(2, 101, 200.0, "completed", 30), // Window 1: 0-60s
        TestDataBuilder::order_record(3, 102, 300.0, "pending", 70), // Window 2: 60-120s - may trigger window 1 close
        TestDataBuilder::order_record(4, 103, 150.0, "pending", 45), // LATE: belongs to Window 1 - should emit corrections
        TestDataBuilder::order_record(5, 104, 250.0, "completed", 25), // VERY LATE: Window 1 - more corrections
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Late Data");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Late Data");

    // Validate COUNT and SUM values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "SUM should be positive");
        } else {
            panic!("total_amount missing or not Float");
        }
    }
}

/// Test EMIT CHANGES with rapid state changes
#[tokio::test]
async fn test_emit_changes_rapid_updates() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as order_count,
            MAX(amount) as max_amount
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    // Same customer rapidly placing orders - should emit every change
    let records = vec![
        TestDataBuilder::order_record(1, 100, 50.0, "pending", 1),
        TestDataBuilder::order_record(2, 100, 75.0, "processing", 2), // Should emit updated count and max
        TestDataBuilder::order_record(3, 100, 125.0, "shipped", 3), // Should emit updated count and max
        TestDataBuilder::order_record(4, 100, 25.0, "delivered", 4), // Should emit updated count (max stays same)
        TestDataBuilder::order_record(5, 100, 200.0, "completed", 5), // Should emit updated count and max
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Rapid Updates");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Rapid Updates");

    // Validate COUNT and MAX values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        if let Some(FieldValue::Float(max)) = result.fields.get("max_amount") {
            assert!(*max > 0.0, "MAX should be positive");
        } else {
            panic!("max_amount missing or not Float");
        }
    }
}

/// Test EMIT CHANGES with null values
#[tokio::test]
async fn test_emit_changes_null_handling() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    let mut records = Vec::new();

    // Order with null status
    let mut record1 = TestDataBuilder::order_record(1, 100, 50.0, "pending", 1);
    record1
        .fields
        .insert("status".to_string(), FieldValue::Null);
    records.push(record1);

    // Normal order
    records.push(TestDataBuilder::order_record(2, 101, 75.0, "completed", 2));

    // Another null status (should update the null group)
    let mut record3 = TestDataBuilder::order_record(3, 102, 100.0, "pending", 3);
    record3
        .fields
        .insert("status".to_string(), FieldValue::Null);
    records.push(record3);

    // Order with null amount
    let mut record4 = TestDataBuilder::order_record(4, 103, 0.0, "pending", 4);
    record4
        .fields
        .insert("amount".to_string(), FieldValue::Null);
    records.push(record4);

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Null Handling");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Null Handling");

    // Validate COUNT values exist (SUM may be NULL for null amount records)
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }
    }
}

/// Test EMIT CHANGES without GROUP BY (should work with implicit grouping)
#[tokio::test]
async fn test_emit_changes_no_groupby() {
    let sql = r#"
        SELECT
            COUNT(*) as total_orders,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM orders
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 1),
        TestDataBuilder::order_record(2, 101, 200.0, "completed", 2), // Should emit updated totals
        TestDataBuilder::order_record(3, 102, 150.0, "shipped", 3),   // Should emit updated totals
        TestDataBuilder::order_record(4, 103, 300.0, "delivered", 4), // Should emit updated totals
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES No GROUP BY");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES No GROUP BY");

    // Validate all aggregation values
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("total_orders") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("total_orders missing or not Integer");
        }

        if let Some(FieldValue::Float(total)) = result.fields.get("total_amount") {
            assert!(*total > 0.0, "SUM should be positive");
        } else {
            panic!("total_amount missing or not Float");
        }

        if let Some(FieldValue::Float(avg)) = result.fields.get("avg_amount") {
            assert!(*avg > 0.0, "AVG should be positive");
        } else {
            panic!("avg_amount missing or not Float");
        }
    }
}

/// Test EMIT CHANGES with extreme values
#[tokio::test]
async fn test_emit_changes_extreme_values() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            MAX(amount) as max_amount,
            MIN(amount) as min_amount
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::order_record(1, 100, 0.01, "pending", 1), // Tiny amount
        TestDataBuilder::order_record(2, 100, 999999.99, "pending", 2), // Huge amount
        TestDataBuilder::order_record(3, 100, -50.0, "refund", 3), // Negative amount
        TestDataBuilder::order_record(4, 101, 0.0, "free", 4),     // Zero amount
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES Extreme Values");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES Extreme Values");

    // Validate COUNT exists and is positive (SUM/MAX/MIN may be negative or zero)
    for result in &results {
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "COUNT should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }
    }
}
