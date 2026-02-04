//! Tests that verify event_time is preserved through window aggregation operations.
//!
//! This is critical for metrics and downstream operations that depend on
//! accurate event timestamps rather than processing time.

use super::shared_test_utils::{SqlExecutor, TestDataBuilder};
use chrono::DateTime;
use velostream::velostream::sql::execution::types::FieldValue;

/// Test that event_time is set on aggregation output records.
///
/// After a GROUP BY with window aggregation, the output record should have
/// event_time set to the window_end_time (standard stream processing semantics).
#[tokio::test]
async fn test_event_time_preserved_through_tumbling_window_aggregation() {
    let sql = r#"
        CREATE STREAM aggregated_trades AS
        SELECT
            symbol,
            AVG(price) as avg_price,
            COUNT(*) as trade_count,
            _WINDOW_END as window_end
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(1m)
        EMIT CHANGES
        WITH (
            'trades.type' = 'memory',
            'aggregated_trades.type' = 'memory'
        )
    "#;

    // Create test records with explicit event_time
    // All records are in the same 1-minute window (minute 0: 0-60000ms)
    let base_time_ms: i64 = 30_000; // 30 seconds into epoch

    let records = vec![
        create_trade_record("AAPL", 100.0, base_time_ms),
        create_trade_record("AAPL", 110.0, base_time_ms + 1000),
        create_trade_record("AAPL", 105.0, base_time_ms + 2000),
        // Add a record in next window to trigger emission of first window
        create_trade_record("AAPL", 120.0, 70_000), // 70 seconds - in minute 1 window
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    // Should have at least one aggregation result
    assert!(
        !results.is_empty(),
        "Should have at least one aggregation result. Got {} results.",
        results.len()
    );

    // Find the AAPL result from the first window
    let aapl_result = results
        .iter()
        .find(|r| {
            r.fields.get("symbol") == Some(&FieldValue::String("AAPL".to_string()))
                && r.fields
                    .get("window_end")
                    .map(|v| matches!(v, FieldValue::Integer(60_000)))
                    .unwrap_or(false)
        })
        .expect("Should have AAPL result from first window");

    // CRITICAL: Verify event_time is set on the output record
    assert!(
        aapl_result.event_time.is_some(),
        "Aggregation output record MUST have event_time set. \
         This is required for metrics to display correct timestamps in Grafana. \
         Got: event_time = None"
    );

    let output_event_time = aapl_result.event_time.unwrap();

    // The event_time should be the window_end_time (60000ms = end of first minute)
    let expected_window_end = DateTime::from_timestamp_millis(60_000).unwrap();
    assert_eq!(
        output_event_time, expected_window_end,
        "Aggregation output event_time should be window_end_time. \
         Expected: {:?}, Got: {:?}",
        expected_window_end, output_event_time
    );

    // Also verify the _WINDOW_END field matches
    if let Some(FieldValue::Integer(window_end_ms)) = aapl_result.fields.get("window_end") {
        assert_eq!(
            *window_end_ms,
            output_event_time.timestamp_millis(),
            "_WINDOW_END field should match record.event_time"
        );
    }

    println!(
        "✅ event_time preserved through aggregation: {:?}",
        output_event_time
    );
}

/// Test that event_time is preserved through sliding windows.
#[tokio::test]
async fn test_event_time_preserved_through_sliding_window() {
    let sql = r#"
        CREATE STREAM sliding_agg AS
        SELECT
            symbol,
            SUM(volume) as total_volume,
            _WINDOW_END as window_end
        FROM trades
        GROUP BY symbol
        WINDOW SLIDING(2m, 1m)
        EMIT CHANGES
        WITH (
            'trades.type' = 'memory',
            'sliding_agg.type' = 'memory'
        )
    "#;

    let records = vec![
        create_trade_record_with_volume("MSFT", 200.0, 1000, 30_000),
        create_trade_record_with_volume("MSFT", 205.0, 2000, 45_000),
        // Trigger window close
        create_trade_record_with_volume("MSFT", 210.0, 500, 130_000), // 2+ minutes later
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    // Find any MSFT result with event_time set
    let msft_with_event_time = results.iter().find(|r| {
        r.fields.get("symbol") == Some(&FieldValue::String("MSFT".to_string()))
            && r.event_time.is_some()
    });

    assert!(
        msft_with_event_time.is_some(),
        "At least one MSFT aggregation result should have event_time set. \
         Results: {:?}",
        results
            .iter()
            .map(|r| (&r.fields, r.event_time))
            .collect::<Vec<_>>()
    );

    let result = msft_with_event_time.unwrap();
    println!(
        "✅ Sliding window preserved event_time: {:?}",
        result.event_time
    );
}

/// Test that event_time matches _WINDOW_END in aggregation output.
#[tokio::test]
async fn test_event_time_equals_window_end() {
    let sql = r#"
        CREATE STREAM window_check AS
        SELECT
            symbol,
            COUNT(*) as cnt,
            _WINDOW_START as ws,
            _WINDOW_END as we
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(30s)
        EMIT CHANGES
        WITH (
            'trades.type' = 'memory',
            'window_check.type' = 'memory'
        )
    "#;

    let records = vec![
        create_trade_record("GOOGL", 150.0, 10_000), // In window [0, 30000)
        create_trade_record("GOOGL", 151.0, 15_000),
        // Trigger emission
        create_trade_record("GOOGL", 155.0, 40_000), // In window [30000, 60000)
    ];

    let results = SqlExecutor::execute_query(sql, records).await;

    for result in &results {
        if let (Some(event_time), Some(FieldValue::Integer(window_end))) =
            (result.event_time, result.fields.get("we"))
        {
            let event_time_ms = event_time.timestamp_millis();
            assert_eq!(
                event_time_ms, *window_end,
                "record.event_time ({}) should equal _WINDOW_END ({})",
                event_time_ms, window_end
            );
            println!(
                "✅ event_time ({}) == _WINDOW_END ({})",
                event_time_ms, window_end
            );
        }
    }
}

fn create_trade_record(
    symbol: &str,
    price: f64,
    timestamp_ms: i64,
) -> velostream::velostream::sql::execution::types::StreamRecord {
    create_trade_record_with_volume(symbol, price, 100, timestamp_ms)
}

fn create_trade_record_with_volume(
    symbol: &str,
    price: f64,
    volume: i64,
    timestamp_ms: i64,
) -> velostream::velostream::sql::execution::types::StreamRecord {
    let mut record = TestDataBuilder::trade_record(1, symbol, price, volume, timestamp_ms);
    // Set event_time from the timestamp
    record.event_time = DateTime::from_timestamp_millis(timestamp_ms);
    record
}
