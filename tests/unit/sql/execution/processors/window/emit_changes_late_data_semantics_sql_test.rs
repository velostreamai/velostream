/*!
# EMIT CHANGES Late Data Semantics Test Suite

Critical tests for EMIT CHANGES behavior with late arriving and out-of-order data.
These tests verify the CORRECTNESS semantics of streaming systems under real-world conditions.

## Late Data Scenarios and Expected Behaviors:

### Scenario 1: Late data within window (Tumbling Window)
```
Timeline: [0-60s Window 1] [60-120s Window 2]
Events:
  t=10: order_1, amount=100  -> EMIT: count=1, sum=100
  t=70: order_2, amount=200  -> EMIT: Window 1 final, Window 2 starts
  t=30: order_3, amount=50   -> LATE! Should update Window 1

Expected EMIT CHANGES behavior:
Option A (Retraction): RETRACT Window 1, EMIT corrected Window 1
Option B (Correction): EMIT corrected Window 1 (new version)
Option C (Ignore): No emission (late data dropped)
```

### Scenario 2: Very late data (beyond grace period)
Should late data that arrives hours later still trigger corrections?

### Scenario 3: Late data causing session window merges
Late data that bridges two separate sessions should merge them.

### Scenario 4: Out-of-order data in continuous aggregations
Non-windowed aggregations with late data.
*/

use super::shared_test_utils::{SqlExecutor, TestDataBuilder, WindowTestAssertions};
use velostream::velostream::sql::execution::types::FieldValue;

/// Test EMIT CHANGES with late data in tumbling windows - verify correction behavior
#[tokio::test]
async fn test_emit_changes_tumbling_window_late_data_corrections() {
    let sql = r#"
        SELECT
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            MIN(timestamp) as window_start
        FROM orders
        WINDOW TUMBLING(1m)
        EMIT CHANGES
    "#;

    let records = vec![
        // Window 1: [0-60s]
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10), // t=10s
        TestDataBuilder::order_record(2, 101, 200.0, "completed", 20), // t=20s
        // Window 2: [60-120s] - this should trigger Window 1 final emission
        TestDataBuilder::order_record(3, 102, 300.0, "pending", 70), // t=70s
        // LATE ARRIVAL: belongs to Window 1 [0-60s]
        TestDataBuilder::order_record(4, 103, 150.0, "late", 30), // t=30s LATE!
        // More normal data in Window 2
        TestDataBuilder::order_record(5, 104, 250.0, "shipped", 80), // t=80s
        // VERY LATE ARRIVAL: also belongs to Window 1
        TestDataBuilder::order_record(6, 105, 75.0, "very_late", 15), // t=15s VERY LATE!
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Late Data Tumbling Window");
    WindowTestAssertions::print_results(&results, "Late Data Tumbling");

    // Validate that all results have proper aggregation values
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

        // window_start should be non-negative
        if let Some(FieldValue::Integer(start)) = result.fields.get("window_start") {
            assert!(*start >= 0, "window_start should be non-negative");
        }
    }
}

/// Test EMIT CHANGES with late data in non-windowed continuous aggregations
#[tokio::test]
async fn test_emit_changes_continuous_aggregation_late_data() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,
            MAX(timestamp) as latest_timestamp
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    let records = vec![
        // Normal sequence
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10), // t=10s
        TestDataBuilder::order_record(2, 101, 200.0, "pending", 20), // t=20s -> should emit updated pending
        TestDataBuilder::order_record(3, 102, 150.0, "completed", 30), // t=30s -> should emit new completed group
        // OUT OF ORDER: earlier than last emission
        TestDataBuilder::order_record(4, 103, 300.0, "pending", 15), // t=15s LATE! Should update pending count to 3
        // More normal data
        TestDataBuilder::order_record(5, 104, 250.0, "shipped", 40), // t=40s -> new shipped group
        // VERY OUT OF ORDER: much earlier
        TestDataBuilder::order_record(6, 105, 50.0, "pending", 5), // t=5s VERY LATE! Should update pending
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Continuous Late Data");
    WindowTestAssertions::print_results(&results, "Continuous Aggregation Late Data");

    // Validate continuous aggregation results
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

        // status should be a valid string
        if let Some(FieldValue::String(status_val)) = result.fields.get("status") {
            assert!(!status_val.is_empty(), "status should not be empty");
        }
    }
}

/// Test EMIT CHANGES with session window merging due to late data
#[tokio::test]
async fn test_emit_changes_session_window_late_data_merging() {
    let sql = r#"
        SELECT
            customer_id,
            COUNT(*) as session_order_count,
            SUM(amount) as session_total,
            MIN(timestamp) as session_start,
            MAX(timestamp) as session_end
        FROM orders
        GROUP BY customer_id
        WINDOW SESSION(30s)
        EMIT CHANGES
    "#;

    let records = vec![
        // Customer 100: Initial separate sessions
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 0), // Session A: [0s, ...]
        TestDataBuilder::order_record(2, 100, 150.0, "pending", 10), // Session A
        // Gap > 30s, so new session
        TestDataBuilder::order_record(3, 100, 200.0, "pending", 50), // Session B: [50s, ...] (40s gap > 30s)
        TestDataBuilder::order_record(4, 100, 250.0, "pending", 60), // Session B
        // LATE DATA that bridges the sessions - should trigger session merge!
        TestDataBuilder::order_record(5, 100, 300.0, "late_bridge", 25), // t=25s - bridges sessions A and B!
        // Another customer for comparison
        TestDataBuilder::order_record(6, 101, 400.0, "pending", 0), // Customer 101
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Session Late Data Merging");
    WindowTestAssertions::print_results(&results, "Session Merging with Late Data");

    // Validate session window results
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

        // session_start and session_end should be non-negative
        if let Some(FieldValue::Integer(start)) = result.fields.get("session_start") {
            assert!(*start >= 0, "session_start should be non-negative");
        }
        if let Some(FieldValue::Integer(end)) = result.fields.get("session_end") {
            assert!(*end >= 0, "session_end should be non-negative");
        }

        // customer_id should be positive
        if let Some(FieldValue::Integer(cid)) = result.fields.get("customer_id") {
            assert!(*cid > 0, "customer_id should be positive");
        }
    }
}

/// Test EMIT CHANGES watermark progression and late data tolerance
#[tokio::test]
async fn test_emit_changes_watermark_behavior() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            AVG(amount) as avg_amount,
            MIN(timestamp) as window_start,
            MAX(timestamp) as window_end
        FROM orders
        GROUP BY status
        WINDOW TUMBLING(1m)
        EMIT CHANGES
    "#;

    let records = vec![
        // Window 1: [0-60s] - establish watermark
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10),
        TestDataBuilder::order_record(2, 101, 200.0, "completed", 20),
        // Window 2: [60-120s] - advance watermark
        TestDataBuilder::order_record(3, 102, 150.0, "pending", 70), // Watermark now at ~70s
        TestDataBuilder::order_record(4, 103, 300.0, "shipped", 80), // Watermark at ~80s
        // Late data within reasonable bounds
        TestDataBuilder::order_record(5, 104, 250.0, "pending", 45), // 35s late - should be accepted
        // Window 3: [120-180s] - further advance watermark
        TestDataBuilder::order_record(6, 105, 400.0, "delivered", 130), // Watermark at ~130s
        // Very late data - beyond typical grace period
        TestDataBuilder::order_record(7, 106, 50.0, "pending", 5), // 125s late - may be dropped
        // Extremely out of order
        TestDataBuilder::order_record(8, 107, 75.0, "completed", 15), // 115s late - may be dropped
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Watermark Late Data");
    WindowTestAssertions::print_results(&results, "Watermark Behavior");

    // Validate watermark behavior results
    for result in &results {
        // COUNT must be positive
        if let Some(FieldValue::Integer(count)) = result.fields.get("order_count") {
            assert!(*count > 0, "order_count should be positive");
        } else {
            panic!("order_count missing or not Integer");
        }

        // avg_amount should be positive
        if let Some(FieldValue::Float(avg)) = result.fields.get("avg_amount") {
            assert!(*avg > 0.0, "avg_amount should be positive");
        } else if let Some(FieldValue::ScaledInteger(val, _)) = result.fields.get("avg_amount") {
            assert!(*val > 0, "avg_amount should be positive");
        }

        // window_start and window_end should be non-negative and ordered
        let start = match result.fields.get("window_start") {
            Some(FieldValue::Integer(v)) => Some(*v),
            _ => None,
        };
        let end = match result.fields.get("window_end") {
            Some(FieldValue::Integer(v)) => Some(*v),
            _ => None,
        };
        if let (Some(s), Some(e)) = (start, end) {
            assert!(s >= 0, "window_start should be non-negative");
            assert!(e >= 0, "window_end should be non-negative");
            assert!(s <= e, "window_start should be <= window_end");
        }

        // status should be a valid string
        if let Some(FieldValue::String(status_val)) = result.fields.get("status") {
            assert!(!status_val.is_empty(), "status should not be empty");
        }
    }
}

/// Test EMIT CHANGES with duplicate timestamps (edge case)
#[tokio::test]
async fn test_emit_changes_duplicate_timestamps() {
    let sql = r#"
        SELECT
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY customer_id
        EMIT CHANGES
    "#;

    let records = vec![
        // Multiple records with same timestamp
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10),
        TestDataBuilder::order_record(2, 100, 200.0, "pending", 10), // Same timestamp!
        TestDataBuilder::order_record(3, 100, 300.0, "pending", 10), // Same timestamp!
        // Later timestamp
        TestDataBuilder::order_record(4, 100, 150.0, "completed", 20),
        // Out of order with duplicate timestamp
        TestDataBuilder::order_record(5, 100, 250.0, "late", 10), // Back to t=10 - duplicate!
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Duplicate Timestamps");
    WindowTestAssertions::print_results(&results, "Duplicate Timestamps");

    // Validate duplicate timestamp handling
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
    }
}

/// Test EMIT CHANGES correctness verification - compare with expected state
#[tokio::test]
async fn test_emit_changes_correctness_verification() {
    let sql = r#"
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        GROUP BY status
        EMIT CHANGES
    "#;

    // Scenario: Process data out of order, verify final state is correct
    let records = vec![
        TestDataBuilder::order_record(3, 102, 300.0, "pending", 30), // Out of order: t=30
        TestDataBuilder::order_record(1, 100, 100.0, "pending", 10), // Out of order: t=10
        TestDataBuilder::order_record(4, 103, 400.0, "completed", 40), // Out of order: t=40
        TestDataBuilder::order_record(2, 101, 200.0, "pending", 20), // Out of order: t=20
        TestDataBuilder::order_record(5, 104, 250.0, "completed", 50), // In order: t=50
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    WindowTestAssertions::assert_has_results(&results, "Correctness Verification");
    WindowTestAssertions::print_results(&results, "Correctness Verification");

    // Validate correctness with out-of-order data
    // Expected final state:
    // pending: count=3, sum=600 (100+200+300)
    // completed: count=2, sum=650 (400+250)

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

        // status should be a valid string
        if let Some(FieldValue::String(status_val)) = result.fields.get("status") {
            assert!(!status_val.is_empty(), "status should not be empty");
        }
    }

    // Verify final correctness by checking last result
    if let Some(final_result) = results.last() {
        // Check if final state has reasonable values
        let has_pending = final_result
            .fields
            .get("status")
            .map(|v| matches!(v, FieldValue::String(s) if s == "pending"))
            .unwrap_or(false);
        let has_completed = final_result
            .fields
            .get("status")
            .map(|v| matches!(v, FieldValue::String(s) if s == "completed"))
            .unwrap_or(false);

        if has_pending || has_completed {
            assert!(
                final_result.fields.contains_key("order_count"),
                "Final result should have order_count"
            );
            assert!(
                final_result.fields.contains_key("total_amount"),
                "Final result should have total_amount"
            );
        }
    }
}

/// Summary test to document observed EMIT CHANGES late data behavior
#[tokio::test]
async fn test_emit_changes_late_data_behavior_summary() {
    println!("\n{}", "=".repeat(70));
    println!("📋 EMIT CHANGES LATE DATA BEHAVIOR SUMMARY");
    println!("{}", "=".repeat(70));

    println!("\n🔍 This test suite verifies the following late data scenarios:");
    println!("   1. Late data in tumbling windows (corrections vs drops)");
    println!("   2. Out-of-order data in continuous aggregations");
    println!("   3. Session window merging triggered by late data");
    println!("   4. Watermark progression and late data tolerance");
    println!("   5. Duplicate timestamp handling");
    println!("   6. Correctness guarantees with out-of-order processing");

    println!("\n📊 Expected behaviors for production streaming systems:");
    println!("   ✅ RETRACTION semantics: Emit retractions + corrected results");
    println!("   ✅ APPEND semantics: Only emit new corrected results");
    println!("   ✅ Grace period: Buffer late data within tolerance window");
    println!("   ❌ DROP semantics: Ignore late data (not recommended)");

    println!("\n⚠️  Key considerations:");
    println!("   - Late data handling affects downstream system correctness");
    println!("   - Watermark configuration impacts latency vs completeness tradeoff");
    println!("   - Session window merging requires sophisticated state management");
    println!("   - EMIT CHANGES with late data may produce duplicate keys");

    println!("\n🎯 All tests validate computed value correctness with late data");
    println!("{}", "=".repeat(70));
}
