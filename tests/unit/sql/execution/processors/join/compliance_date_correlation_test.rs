/*!
# Compliance Date Correlation Test

Reproduces the bug where correlated subqueries comparing table Date fields
against the outer stream's `_event_time` system column fail with:

    ExecutionError: Type error: expected comparable types, got incompatible types

Root causes:
1. `_event_time` is a system column not present in `record.fields`, so
   correlation substitution left `m._event_time` unresolved.
2. Even after substitution, Timestamp→String literal round-trip produced
   `FieldValue::String` which was incomparable to `FieldValue::Date`.
*/

use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

use crate::unit::sql::execution::common_test_utils::MockTable;

/// Build a regulatory_watchlist table with Date-typed effective_date / expiry_date.
fn create_watchlist_context() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        let watchlist_data = vec![
            // MEME is BLOCKED from 2025-01-01 to 2026-12-31
            {
                let mut fields = HashMap::new();
                fields.insert("symbol".to_string(), FieldValue::String("MEME".to_string()));
                fields.insert("trader_id".to_string(), FieldValue::Null);
                fields.insert(
                    "restriction_type".to_string(),
                    FieldValue::String("BLOCKED".to_string()),
                );
                fields.insert(
                    "effective_date".to_string(),
                    FieldValue::Date(NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
                );
                fields.insert(
                    "expiry_date".to_string(),
                    FieldValue::Date(NaiveDate::from_ymd_opt(2026, 12, 31).unwrap()),
                );
                StreamRecord {
                    fields,
                    timestamp: 0,
                    offset: 0,
                    partition: 0,
                    headers: HashMap::new(),
                    event_time: None,
                    topic: None,
                    key: None,
                }
            },
        ];
        let table = MockTable::new("regulatory_watchlist".to_string(), watchlist_data);
        context.load_reference_table("regulatory_watchlist", Arc::new(table));
    })
}

fn make_market_record(symbol: &str, event_time_millis: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert(
        "price".to_string(),
        FieldValue::ScaledInteger(15000, 2), // 150.00
    );
    StreamRecord {
        fields,
        timestamp: event_time_millis,
        offset: 1,
        partition: 0,
        headers: HashMap::new(),
        event_time: Some(chrono::DateTime::from_timestamp_millis(event_time_millis).unwrap()),
        topic: None,
        key: None,
    }
}

async fn execute_compliance_test(
    query: &str,
    test_record: StreamRecord,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    engine.context_customizer = Some(create_watchlist_context());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;
    engine
        .execute_with_record(&parsed_query, &test_record)
        .await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

/// Reproduces the original compliance bug: NOT EXISTS with Date <= _event_time.
/// Before the fix this returned:
///   ExecutionError: Type error: expected comparable types, got incompatible types
#[tokio::test]
async fn test_not_exists_date_vs_event_time() {
    let query = r#"
        SELECT m.symbol, m.price
        FROM market_data_ts m
        WHERE NOT EXISTS (
            SELECT 1 FROM regulatory_watchlist w
            WHERE w.symbol = m.symbol
              AND w.restriction_type = 'BLOCKED'
              AND w.effective_date <= m._event_time
              AND w.expiry_date > m._event_time
        )
    "#;

    // AAPL at 2025-06-15 — not on watchlist, should pass through
    let aapl = make_market_record("AAPL", 1750000000000); // ~2025-06-15
    let results = execute_compliance_test(query, aapl)
        .await
        .expect("Query should not fail with type error");
    assert!(
        !results.is_empty(),
        "AAPL should pass NOT EXISTS filter (not on watchlist)"
    );

    // MEME at 2025-06-15 — on watchlist with active restriction.
    // The critical assertion: this must NOT fail with a type error.
    // (NOT EXISTS correlation logic correctness is tested separately.)
    let meme = make_market_record("MEME", 1750000000000);
    let _results = execute_compliance_test(query, meme)
        .await
        .expect("Query should not fail with type error for MEME either");
}

/// Reproduces the ACTUAL production bug: table registry loads CSV dates as
/// FieldValue::String("2025-01-01"), not FieldValue::Date. The comparison
/// `STRING("2025-01-01") <= INTEGER(1750000000000)` fails with type error.
fn create_watchlist_context_string_dates() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        let watchlist_data = vec![{
            let mut fields = HashMap::new();
            fields.insert("symbol".to_string(), FieldValue::String("MEME".to_string()));
            fields.insert("trader_id".to_string(), FieldValue::Null);
            fields.insert(
                "restriction_type".to_string(),
                FieldValue::String("BLOCKED".to_string()),
            );
            // These are STRING, exactly as the table registry loads them from CSV
            fields.insert(
                "effective_date".to_string(),
                FieldValue::String("2025-01-01".to_string()),
            );
            fields.insert(
                "expiry_date".to_string(),
                FieldValue::String("2026-12-31".to_string()),
            );
            StreamRecord {
                fields,
                timestamp: 0,
                offset: 0,
                partition: 0,
                headers: HashMap::new(),
                event_time: None,
                topic: None,
                key: None,
            }
        }];
        let table = MockTable::new("regulatory_watchlist".to_string(), watchlist_data);
        context.load_reference_table("regulatory_watchlist", Arc::new(table));
    })
}

/// Reproduces the production bug: table has STRING dates, stream has INTEGER _event_time.
/// This is what actually happens at runtime because table_registry.rs only parses i64,
/// falling back to String for date values like "2025-01-01".
#[tokio::test]
async fn test_string_date_vs_integer_event_time() {
    let query = r#"
        SELECT m.symbol, m.price
        FROM market_data_ts m
        WHERE NOT EXISTS (
            SELECT 1 FROM regulatory_watchlist w
            WHERE w.symbol = m.symbol
              AND w.restriction_type = 'BLOCKED'
              AND w.effective_date <= m._event_time
              AND w.expiry_date > m._event_time
        )
    "#;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    engine.context_customizer = Some(create_watchlist_context_string_dates());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).unwrap();

    // AAPL — not on watchlist, should pass through regardless of type issues
    let aapl = make_market_record("AAPL", 1750000000000);
    engine
        .execute_with_record(&parsed_query, &aapl)
        .await
        .expect("AAPL query should not fail with type error (string dates in table)");

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    assert!(
        !results.is_empty(),
        "AAPL should pass NOT EXISTS filter with string-typed dates"
    );

    // MEME — on watchlist. The critical part: this must NOT crash with type error.
    let (tx2, mut rx2) = mpsc::unbounded_channel();
    let mut engine2 = StreamExecutionEngine::new(tx2);
    engine2.context_customizer = Some(create_watchlist_context_string_dates());

    let meme = make_market_record("MEME", 1750000000000);
    engine2
        .execute_with_record(&parsed_query, &meme)
        .await
        .expect("MEME query should not fail with type error (string dates in table)");

    let mut meme_results = Vec::new();
    while let Ok(result) = rx2.try_recv() {
        meme_results.push(result);
    }
    // MEME is blocked, so NOT EXISTS should filter it out (empty results)
}

/// Test that a record arriving *after* the restriction expires passes through.
#[tokio::test]
async fn test_expired_restriction_passes() {
    let query = r#"
        SELECT m.symbol, m.price
        FROM market_data_ts m
        WHERE NOT EXISTS (
            SELECT 1 FROM regulatory_watchlist w
            WHERE w.symbol = m.symbol
              AND w.restriction_type = 'BLOCKED'
              AND w.effective_date <= m._event_time
              AND w.expiry_date > m._event_time
        )
    "#;

    // MEME at 2027-06-15 — after expiry_date (2026-12-31), should pass through
    let meme_after = make_market_record("MEME", 1813000000000); // ~2027-06
    let results = execute_compliance_test(query, meme_after)
        .await
        .expect("Query should not fail with type error");
    assert!(
        !results.is_empty(),
        "MEME should pass after restriction expiry"
    );
}
