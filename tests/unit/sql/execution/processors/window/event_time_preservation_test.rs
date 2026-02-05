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

// =============================================================================
// Tests for qualified column reference event_time propagation (FR-081)
// =============================================================================
// These tests verify that SELECT queries with qualified column references like
// `m._event_time` properly set the output record's event_time metadata.

/// Test that direct `_event_time` column selection sets output event_time.
/// This is the simplest case - SELECT _event_time FROM source.
#[tokio::test]
async fn test_direct_event_time_column_selection() {
    let sql = r#"
        CREATE STREAM output AS
        SELECT
            symbol,
            price,
            _event_time
        FROM market_data
        EMIT CHANGES
        WITH (
            'market_data.type' = 'memory',
            'output.type' = 'memory'
        )
    "#;

    // Create record with event_time set to 1 hour in the past
    let past_time_ms = chrono::Utc::now().timestamp_millis() - 3600_000; // 1 hour ago
    let mut record = TestDataBuilder::trade_record(1, "AAPL", 150.0, 100, past_time_ms);
    record.event_time = DateTime::from_timestamp_millis(past_time_ms);

    let results = SqlExecutor::execute_query(sql, vec![record]).await;

    assert!(!results.is_empty(), "Should have at least one result");

    let output = &results[0];
    assert!(
        output.event_time.is_some(),
        "Output record MUST have event_time set when _event_time is selected. Got: None"
    );

    let output_event_time_ms = output.event_time.unwrap().timestamp_millis();
    let now_ms = chrono::Utc::now().timestamp_millis();

    // The output event_time should be the PAST time from the input, not current time
    assert!(
        output_event_time_ms < now_ms - 3000_000, // At least 50 minutes ago
        "Output event_time should be in the past (from input), not current time. \
         Input: {}ms, Output: {}ms, Now: {}ms, Diff: {}s",
        past_time_ms,
        output_event_time_ms,
        now_ms,
        (output_event_time_ms - now_ms) / 1000
    );

    // Should match the input event_time
    assert_eq!(
        output_event_time_ms, past_time_ms,
        "Output event_time should equal input event_time. Expected: {}ms, Got: {}ms",
        past_time_ms, output_event_time_ms
    );

    println!(
        "✅ Direct _event_time selection preserved: input={}ms, output={}ms",
        past_time_ms, output_event_time_ms
    );
}

/// Test that qualified column reference `m._event_time` sets output event_time.
/// This is the pattern used in JOIN queries like `SELECT m._event_time FROM market_data m`.
#[tokio::test]
async fn test_qualified_event_time_column_reference() {
    let sql = r#"
        CREATE STREAM output AS
        SELECT
            m.symbol,
            m.price,
            m._event_time
        FROM market_data m
        EMIT CHANGES
        WITH (
            'market_data.type' = 'memory',
            'output.type' = 'memory'
        )
    "#;

    // Create record with event_time set to 1 hour in the past
    let past_time_ms = chrono::Utc::now().timestamp_millis() - 3600_000;
    let mut record = TestDataBuilder::trade_record(1, "GOOGL", 175.0, 200, past_time_ms);
    record.event_time = DateTime::from_timestamp_millis(past_time_ms);

    let results = SqlExecutor::execute_query(sql, vec![record]).await;

    assert!(!results.is_empty(), "Should have at least one result");

    let output = &results[0];
    assert!(
        output.event_time.is_some(),
        "Output record MUST have event_time set when m._event_time is selected. Got: None"
    );

    let output_event_time_ms = output.event_time.unwrap().timestamp_millis();

    // The output event_time should match the input, not be in the future
    assert_eq!(
        output_event_time_ms, past_time_ms,
        "Output event_time should equal input event_time for qualified column m._event_time. \
         Expected: {}ms, Got: {}ms",
        past_time_ms, output_event_time_ms
    );

    println!(
        "✅ Qualified m._event_time selection preserved: input={}ms, output={}ms",
        past_time_ms, output_event_time_ms
    );
}

/// Test that _event_time is preserved alongside NOW() in the same query.
/// This reproduces the enriched_market_data scenario where both m._event_time
/// and NOW() as enrichment_time are in the SELECT.
#[tokio::test]
async fn test_event_time_with_now_function() {
    let sql = r#"
        CREATE STREAM enriched AS
        SELECT
            m.symbol,
            m.price,
            m._event_time,
            NOW() as processing_time
        FROM market_data m
        EMIT CHANGES
        WITH (
            'market_data.type' = 'memory',
            'enriched.type' = 'memory'
        )
    "#;

    // Create record with event_time set to 1 hour in the past
    let past_time_ms = chrono::Utc::now().timestamp_millis() - 3600_000;
    let mut record = TestDataBuilder::trade_record(1, "MSFT", 400.0, 150, past_time_ms);
    record.event_time = DateTime::from_timestamp_millis(past_time_ms);

    let results = SqlExecutor::execute_query(sql, vec![record]).await;

    assert!(!results.is_empty(), "Should have at least one result");

    let output = &results[0];

    // CRITICAL: event_time should come from m._event_time (in the past),
    // NOT from NOW() (current time) or some future value
    assert!(
        output.event_time.is_some(),
        "Output record MUST have event_time set. Got: None"
    );

    let output_event_time_ms = output.event_time.unwrap().timestamp_millis();
    let now_ms = chrono::Utc::now().timestamp_millis();

    // The event_time should be the PAST input time, not current or future
    assert!(
        output_event_time_ms <= now_ms,
        "Output event_time should NOT be in the future! \
         Got: {}ms, Now: {}ms, Diff: {}s in future",
        output_event_time_ms,
        now_ms,
        (output_event_time_ms - now_ms) / 1000
    );

    assert_eq!(
        output_event_time_ms, past_time_ms,
        "Output event_time should equal input event_time, not NOW(). \
         Expected: {}ms (past), Got: {}ms",
        past_time_ms, output_event_time_ms
    );

    // Also verify the processing_time field contains a recent timestamp (from NOW())
    if let Some(FieldValue::Integer(processing_time)) = output.fields.get("processing_time") {
        assert!(
            (*processing_time - now_ms).abs() < 5000, // Within 5 seconds
            "processing_time field should be close to current time (from NOW()). \
             Got: {}ms, Now: {}ms",
            processing_time,
            now_ms
        );
    }

    println!(
        "✅ event_time preserved with NOW() in query: event_time={}ms (past), now={}ms",
        output_event_time_ms, now_ms
    );
}

/// Test event_time propagation in a scenario similar to enriched_market_data.
/// This includes a JOIN and multiple fields including NOW().
#[tokio::test]
async fn test_event_time_in_join_with_enrichment() {
    // This test simulates the enriched_market_data query pattern
    let sql = r#"
        CREATE STREAM enriched_market_data AS
        SELECT
            m.symbol,
            m.price,
            m._event_time,
            m.timestamp,
            NOW() as enrichment_time,
            (NOW() - m._event_time) / 1000 as latency_seconds
        FROM market_data m
        EMIT CHANGES
        WITH (
            'market_data.type' = 'memory',
            'enriched_market_data.type' = 'memory'
        )
    "#;

    // Create record with event_time 2 minutes in the past
    let past_time_ms = chrono::Utc::now().timestamp_millis() - 120_000;
    let mut record = TestDataBuilder::trade_record(1, "AAPL", 150.0, 100, past_time_ms);
    record.event_time = DateTime::from_timestamp_millis(past_time_ms);

    let results = SqlExecutor::execute_query(sql, vec![record]).await;

    assert!(!results.is_empty(), "Should have at least one result");

    let output = &results[0];
    let now_ms = chrono::Utc::now().timestamp_millis();

    // CRITICAL ASSERTION: event_time must be set and must be in the PAST
    assert!(
        output.event_time.is_some(),
        "Output event_time MUST be set for metrics to work correctly"
    );

    let output_event_time_ms = output.event_time.unwrap().timestamp_millis();

    // This is the key assertion that would catch the "future timestamp" bug
    assert!(
        output_event_time_ms <= now_ms,
        "BUG DETECTED: Output event_time is in the FUTURE! \
         This causes Prometheus remote-write to reject metrics. \
         event_time: {}ms, now: {}ms, diff: {}s in future",
        output_event_time_ms,
        now_ms,
        (output_event_time_ms - now_ms) / 1000
    );

    // event_time should match the input (past time)
    assert_eq!(
        output_event_time_ms,
        past_time_ms,
        "Output event_time should match input event_time. \
         Expected: {}ms, Got: {}ms, Diff: {}s",
        past_time_ms,
        output_event_time_ms,
        (output_event_time_ms - past_time_ms) / 1000
    );

    // The latency_seconds field should be positive (since input is in the past)
    if let Some(latency) = output.fields.get("latency_seconds") {
        match latency {
            FieldValue::Integer(l) => {
                assert!(
                    *l > 0,
                    "latency_seconds should be positive for past events. Got: {}",
                    l
                );
                println!("  latency_seconds: {}s", l);
            }
            FieldValue::Float(l) => {
                assert!(
                    *l > 0.0,
                    "latency_seconds should be positive for past events. Got: {}",
                    l
                );
                println!("  latency_seconds: {:.2}s", l);
            }
            _ => {}
        }
    }

    println!(
        "✅ JOIN enrichment preserved event_time: input={}ms, output={}ms ({}s ago)",
        past_time_ms,
        output_event_time_ms,
        (now_ms - output_event_time_ms) / 1000
    );
}
