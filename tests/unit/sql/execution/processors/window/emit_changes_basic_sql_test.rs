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

/// Test EMIT CHANGES with CASE expressions using alias + IN operator
/// This tests the pattern used in trading signals: spike_classification IN ('HIGH', 'MEDIUM')
#[tokio::test]
async fn test_emit_changes_case_alias_with_in_operator() {
    let sql = r#"
        SELECT
            symbol,
            COUNT(*) AS trade_count,
            AVG(volume) AS avg_volume,
            MAX(volume) AS max_volume,
            CASE
                WHEN MAX(volume) > 2000 THEN 'EXTREME_SPIKE'
                WHEN MAX(volume) > 1000 THEN 'HIGH_SPIKE'
                WHEN MAX(volume) > 500 THEN 'STATISTICAL_ANOMALY'
                ELSE 'NORMAL'
            END AS spike_classification,
            CASE
                WHEN MAX(volume) > 3000 THEN 'TRIGGER_BREAKER'
                WHEN spike_classification IN ('EXTREME_SPIKE', 'STATISTICAL_ANOMALY') THEN 'PAUSE_FEED'
                WHEN spike_classification = 'HIGH_SPIKE' THEN 'SLOW_MODE'
                ELSE 'ALLOW'
            END AS circuit_state
        FROM trades
        GROUP BY symbol
        WINDOW SLIDING(300s, 60s)
        EMIT CHANGES
    "#;

    let records = vec![
        TestDataBuilder::trade_record(1, "AAPL", 150.0, 800, 0), // volume > 500 -> STATISTICAL_ANOMALY
        TestDataBuilder::trade_record(2, "AAPL", 151.0, 1200, 60000), // volume > 1000 -> HIGH_SPIKE
        TestDataBuilder::trade_record(3, "GOOGL", 2800.0, 600, 120000), // volume > 500 -> STATISTICAL_ANOMALY
        TestDataBuilder::trade_record(4, "AAPL", 152.0, 2500, 180000), // volume > 2000 -> EXTREME_SPIKE
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "EMIT CHANGES CASE + alias + IN");
    WindowTestAssertions::print_results(&results, "EMIT CHANGES CASE + alias + IN");

    // Validate that spike_classification and circuit_state are computed correctly
    // AND that they are properly correlated based on the CASE logic
    for result in &results {
        let spike_classification = result.fields.get("spike_classification");
        let circuit_state = result.fields.get("circuit_state");
        let max_volume = result.fields.get("max_volume");

        // Ensure spike_classification exists and is one of the expected values
        if let Some(FieldValue::String(classification)) = spike_classification {
            assert!(
                [
                    "EXTREME_SPIKE",
                    "HIGH_SPIKE",
                    "STATISTICAL_ANOMALY",
                    "NORMAL"
                ]
                .contains(&classification.as_str()),
                "Unexpected spike_classification: {}",
                classification
            );

            // Verify circuit_state is correctly correlated with spike_classification
            // Based on the CASE logic in the query:
            // - EXTREME_SPIKE or STATISTICAL_ANOMALY -> PAUSE_FEED (unless max_volume > 3000 -> TRIGGER_BREAKER)
            // - HIGH_SPIKE -> SLOW_MODE
            // - NORMAL -> ALLOW
            if let Some(FieldValue::String(state)) = circuit_state {
                match classification.as_str() {
                    "EXTREME_SPIKE" | "STATISTICAL_ANOMALY" => {
                        // Check if max_volume > 3000 (TRIGGER_BREAKER takes precedence)
                        let is_trigger = match max_volume {
                            Some(FieldValue::Integer(v)) => *v > 3000,
                            Some(FieldValue::Float(v)) => *v > 3000.0,
                            _ => false,
                        };
                        if is_trigger {
                            assert_eq!(
                                state, "TRIGGER_BREAKER",
                                "max_volume > 3000 should trigger TRIGGER_BREAKER, got {}",
                                state
                            );
                        } else {
                            assert_eq!(
                                state, "PAUSE_FEED",
                                "{} should map to PAUSE_FEED, got {}",
                                classification, state
                            );
                        }
                    }
                    "HIGH_SPIKE" => {
                        // Check if max_volume > 3000 (TRIGGER_BREAKER takes precedence)
                        let is_trigger = match max_volume {
                            Some(FieldValue::Integer(v)) => *v > 3000,
                            Some(FieldValue::Float(v)) => *v > 3000.0,
                            _ => false,
                        };
                        if is_trigger {
                            assert_eq!(
                                state, "TRIGGER_BREAKER",
                                "max_volume > 3000 should trigger TRIGGER_BREAKER, got {}",
                                state
                            );
                        } else {
                            assert_eq!(
                                state, "SLOW_MODE",
                                "HIGH_SPIKE should map to SLOW_MODE, got {}",
                                state
                            );
                        }
                    }
                    "NORMAL" => {
                        // Check if max_volume > 3000 (TRIGGER_BREAKER takes precedence)
                        let is_trigger = match max_volume {
                            Some(FieldValue::Integer(v)) => *v > 3000,
                            Some(FieldValue::Float(v)) => *v > 3000.0,
                            _ => false,
                        };
                        if is_trigger {
                            assert_eq!(
                                state, "TRIGGER_BREAKER",
                                "max_volume > 3000 should trigger TRIGGER_BREAKER, got {}",
                                state
                            );
                        } else {
                            assert_eq!(state, "ALLOW", "NORMAL should map to ALLOW, got {}", state);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Ensure circuit_state exists and is one of the expected values
        if let Some(FieldValue::String(state)) = circuit_state {
            assert!(
                ["TRIGGER_BREAKER", "PAUSE_FEED", "SLOW_MODE", "ALLOW"].contains(&state.as_str()),
                "Unexpected circuit_state: {}",
                state
            );
        }
    }
}
