//! Tests for window timestamp boundary handling.
//!
//! These tests verify that window_start and window_end timestamps are correctly
//! bounded to the actual event times in the data, not extrapolated into the future.
//!
//! BUG REPRODUCTION (FR-XXX): Sliding windows were producing window_end timestamps
//! far into the future when processing accumulated data with varying timestamp ranges.
//! This caused Prometheus remote-write to reject metrics with "timestamp is too far
//! in the future" errors.

use super::shared_test_utils::{SqlExecutor, TestDataBuilder};
use chrono::DateTime;
use velostream::velostream::sql::execution::types::FieldValue;

/// Test that sliding window window_end does not exceed the maximum event time in the data.
///
/// This reproduces the bug where accumulated data with wide timestamp ranges caused
/// window_end values to extend far into the future relative to current time.
#[tokio::test]
async fn test_sliding_window_end_bounded_by_max_event_time() {
    let sql = r#"
        CREATE STREAM volume_analysis AS
        SELECT
            symbol,
            COUNT(*) as trade_count,
            AVG(volume) as avg_volume,
            _WINDOW_START as window_start,
            _WINDOW_END as window_end
        FROM trades
        GROUP BY symbol
        WINDOW SLIDING(_event_time, 5m, 1m)
        EMIT CHANGES
        WITH (
            'trades.type' = 'memory',
            'volume_analysis.type' = 'memory'
        )
    "#;

    // Create records with event times spanning a range
    // All timestamps should be in the "past" relative to the test
    let base_time = chrono::Utc::now().timestamp_millis();
    let past_1h = base_time - 3600_000; // 1 hour ago
    let past_30m = base_time - 1800_000; // 30 min ago
    let past_10m = base_time - 600_000; // 10 min ago

    let records = vec![
        // Early records (1 hour ago)
        create_trade_with_event_time("AAPL", 100.0, 1000, past_1h),
        create_trade_with_event_time("AAPL", 101.0, 1100, past_1h + 30_000),
        // Middle records (30 min ago)
        create_trade_with_event_time("AAPL", 105.0, 1200, past_30m),
        create_trade_with_event_time("AAPL", 106.0, 1300, past_30m + 60_000),
        // Recent records (10 min ago)
        create_trade_with_event_time("AAPL", 110.0, 1400, past_10m),
        create_trade_with_event_time("AAPL", 111.0, 1500, past_10m + 120_000),
        // Trigger window emission
        create_trade_with_event_time("AAPL", 115.0, 1600, past_10m + 400_000),
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    assert!(
        !results.is_empty(),
        "Should have at least one aggregation result"
    );

    let now = chrono::Utc::now().timestamp_millis();

    for result in &results {
        // Check window_end is not in the future
        if let Some(FieldValue::Integer(window_end)) = result.fields.get("window_end") {
            let diff_ms = *window_end - now;
            let diff_min = diff_ms as f64 / 60_000.0;

            // Window end should not be more than 10 minutes in the future
            // (allowing some tolerance for test execution time)
            assert!(
                diff_ms < 600_000, // 10 minutes
                "BUG: window_end is {} minutes in the future! \
                 Window timestamps should be bounded by actual event times. \
                 window_end={}, now={}, diff={}ms",
                diff_min,
                window_end,
                now,
                diff_ms
            );

            // Also check the record's event_time metadata (used for metrics)
            if let Some(event_time) = result.event_time {
                let event_time_ms = event_time.timestamp_millis();
                let event_diff_ms = event_time_ms - now;

                assert!(
                    event_diff_ms < 600_000,
                    "BUG: record.event_time is {} minutes in the future! \
                     This will cause Prometheus remote-write rejection. \
                     event_time={}, now={}",
                    event_diff_ms as f64 / 60_000.0,
                    event_time_ms,
                    now
                );
            }
        }
    }

    println!("✅ All window_end values are properly bounded (not in the distant future)");
}

/// Test that tumbling window window_end is bounded correctly.
#[tokio::test]
async fn test_tumbling_window_end_bounded_by_event_time() {
    let sql = r#"
        CREATE STREAM agg AS
        SELECT
            symbol,
            COUNT(*) as cnt,
            _WINDOW_END as window_end
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(1m)
        EMIT CHANGES
        WITH (
            'trades.type' = 'memory',
            'agg.type' = 'memory'
        )
    "#;

    let past_time = chrono::Utc::now().timestamp_millis() - 300_000; // 5 min ago

    let records = vec![
        create_trade_with_event_time("MSFT", 200.0, 500, past_time),
        create_trade_with_event_time("MSFT", 201.0, 600, past_time + 30_000),
        // Trigger emission
        create_trade_with_event_time("MSFT", 205.0, 700, past_time + 90_000),
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    let now = chrono::Utc::now().timestamp_millis();

    for result in &results {
        if let Some(FieldValue::Integer(window_end)) = result.fields.get("window_end") {
            assert!(
                *window_end <= now + 120_000, // Allow 2 min tolerance
                "window_end should not be far in the future. Got {} (now+{}ms)",
                window_end,
                window_end - now
            );
        }
    }

    println!("✅ Tumbling window_end is properly bounded");
}

/// Test that event_time on aggregation output matches window_end.
#[tokio::test]
async fn test_aggregation_event_time_matches_window_end() {
    let sql = r#"
        CREATE STREAM check AS
        SELECT
            symbol,
            COUNT(*) as cnt,
            _WINDOW_END as we
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(30s)
        EMIT CHANGES
        WITH (
            'trades.type' = 'memory',
            'check.type' = 'memory'
        )
    "#;

    let past_time = chrono::Utc::now().timestamp_millis() - 120_000; // 2 min ago

    let records = vec![
        create_trade_with_event_time("GOOGL", 150.0, 100, past_time),
        create_trade_with_event_time("GOOGL", 151.0, 200, past_time + 10_000),
        // Trigger
        create_trade_with_event_time("GOOGL", 155.0, 300, past_time + 50_000),
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    for result in &results {
        if let (Some(event_time), Some(FieldValue::Integer(window_end))) =
            (result.event_time, result.fields.get("we"))
        {
            let event_time_ms = event_time.timestamp_millis();
            assert_eq!(
                event_time_ms, *window_end,
                "record.event_time should equal _WINDOW_END for aggregations. \
                 event_time={}, window_end={}",
                event_time_ms, window_end
            );
        }
    }

    println!("✅ Aggregation event_time matches window_end");
}

/// Helper to create a trade record with explicit event_time.
fn create_trade_with_event_time(
    symbol: &str,
    price: f64,
    volume: i64,
    event_time_ms: i64,
) -> velostream::velostream::sql::execution::types::StreamRecord {
    let mut record = TestDataBuilder::trade_record(1, symbol, price, volume, event_time_ms);
    record.event_time = DateTime::from_timestamp_millis(event_time_ms);
    record
}
