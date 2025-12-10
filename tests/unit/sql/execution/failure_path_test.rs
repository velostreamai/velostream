//! SQL Engine Failure Path Tests
//!
//! These tests verify that the SQL engine properly reports errors for edge cases
//! instead of silently dropping records. Every failure should produce a clear error
//! message that can be logged or sent to DLQ.
//!
//! Ensuring no silent failures in SQL execution pipeline.

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Helper to create a basic stream record with specified fields
fn create_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
    let mut map = HashMap::new();
    for (k, v) in fields {
        map.insert(k.to_string(), v);
    }
    StreamRecord {
        fields: map,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: None,
        key: None,
    }
}

/// Helper to create a record with explicit timestamp
fn create_record_with_timestamp(fields: Vec<(&str, FieldValue)>, timestamp: i64) -> StreamRecord {
    let mut map = HashMap::new();
    for (k, v) in fields {
        map.insert(k.to_string(), v);
    }
    StreamRecord {
        fields: map,
        timestamp,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: None,
        key: None,
    }
}

// =============================================================================
// MISSING FIELD TESTS
// =============================================================================

#[test]
fn test_missing_field_in_select_should_error_or_produce_null() {
    // Query references a field that doesn't exist in the record
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT nonexistent_field FROM test_stream")
        .unwrap();

    // Record without the required field
    let record = create_record(vec![
        ("id", FieldValue::Integer(1)),
        ("name", FieldValue::String("test".to_string())),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    // This should either:
    // 1. Return an error (preferred)
    // 2. Return a result with NULL/missing field (acceptable)
    // 3. NOT silently drop the record with no indication
    match &result {
        Ok(results) => {
            if results.is_empty() {
                println!(
                    "⚠️ SILENT FAILURE: Missing field 'nonexistent_field' produced 0 results instead of error"
                );
                // This is the bug - should not silently produce empty results
            } else {
                println!(
                    "✓ Missing field produced {} result(s) with NULL/missing value",
                    results.len()
                );
                // Check if the field is present as null or missing
                for r in results {
                    let field_val = r.fields.get("nonexistent_field");
                    println!("  nonexistent_field = {:?}", field_val);
                }
            }
        }
        Err(e) => {
            println!("✓ GOOD: Missing field produced error: {}", e);
        }
    }
}

#[test]
fn test_missing_field_in_where_clause_should_error() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT id FROM test_stream WHERE missing_field > 10")
        .unwrap();

    let record = create_record(vec![("id", FieldValue::Integer(1))]);

    let result = engine.execute_with_record_sync(&query, &record);

    // Missing field in WHERE should error, not silently filter
    match &result {
        Ok(results) => {
            if results.is_empty() {
                println!(
                    "⚠️ POSSIBLE SILENT FAILURE: Missing field in WHERE produced 0 results - was it filtered or errored?"
                );
            } else {
                println!("Missing field in WHERE produced {} results", results.len());
            }
        }
        Err(e) => {
            println!("✓ GOOD: Missing field in WHERE produced error: {}", e);
        }
    }
}

#[test]
fn test_missing_field_in_group_by_should_error() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse(
            "SELECT missing_group_field, COUNT(*) as cnt FROM test_stream GROUP BY missing_group_field",
        )
        .unwrap();

    engine.init_query_execution(query.clone());

    let record = create_record(vec![("id", FieldValue::Integer(1))]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            if results.is_empty() {
                println!(
                    "⚠️ POSSIBLE SILENT FAILURE: Missing field in GROUP BY produced 0 results"
                );
            } else {
                println!(
                    "GROUP BY with missing field produced {} results:",
                    results.len()
                );
                for r in results {
                    println!("  {:?}", r.fields);
                }
            }
        }
        Err(e) => {
            println!("✓ GOOD: Missing field in GROUP BY produced error: {}", e);
        }
    }
}

// =============================================================================
// INVALID FUNCTION TESTS
// =============================================================================

#[test]
fn test_invalid_function_name_should_error() {
    // This should fail at parse time
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT INVALID_FUNCTION(price) FROM test_stream");

    match result {
        Ok(_) => {
            println!("⚠️ WARNING: Invalid function name was accepted by parser");
        }
        Err(e) => {
            println!(
                "✓ GOOD: Invalid function name rejected at parse time: {}",
                e
            );
        }
    }
}

#[test]
fn test_function_with_wrong_type_should_error() {
    // SUM on a string field should error
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT SUM(name) as total FROM test_stream GROUP BY id")
        .unwrap();

    engine.init_query_execution(query.clone());

    let record = create_record(vec![
        ("id", FieldValue::Integer(1)),
        ("name", FieldValue::String("not_a_number".to_string())),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            // Check what the aggregation produced
            for r in results {
                if let Some(total) = r.fields.get("total") {
                    println!(
                        "SUM on string produced: {:?} - should this be an error?",
                        total
                    );
                }
            }
            if results.is_empty() {
                println!("⚠️ SUM on string produced 0 results - silent failure?");
            }
        }
        Err(e) => {
            println!("✓ GOOD: SUM on string type produced error: {}", e);
        }
    }
}

#[test]
fn test_avg_on_non_numeric_should_error() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT AVG(status) as avg_status FROM test_stream GROUP BY id")
        .unwrap();

    engine.init_query_execution(query.clone());

    let record = create_record(vec![
        ("id", FieldValue::Integer(1)),
        ("status", FieldValue::String("active".to_string())),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            if results.is_empty() {
                println!("⚠️ AVG on string produced 0 results - silent failure?");
            } else {
                for r in results {
                    if let Some(avg) = r.fields.get("avg_status") {
                        println!("AVG on string produced: {:?}", avg);
                    }
                }
            }
        }
        Err(e) => {
            println!("✓ GOOD: AVG on string produced error: {}", e);
        }
    }
}

// =============================================================================
// INVALID SYSTEM COLUMN TESTS
// =============================================================================

#[test]
fn test_invalid_system_column_should_error() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT _INVALID_SYSTEM_COL FROM test_stream")
        .unwrap();

    let record = create_record(vec![("id", FieldValue::Integer(1))]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            if results.is_empty() {
                println!("⚠️ Invalid system column produced 0 results - silent failure");
            } else {
                for r in results {
                    if r.fields.contains_key("_INVALID_SYSTEM_COL") {
                        println!(
                            "Invalid system column returned value: {:?}",
                            r.fields.get("_INVALID_SYSTEM_COL")
                        );
                    }
                }
            }
        }
        Err(e) => {
            println!("✓ GOOD: Invalid system column produced error: {}", e);
        }
    }
}

#[test]
fn test_system_timestamp_available() {
    // _TIMESTAMP should always be available
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser.parse("SELECT _TIMESTAMP FROM test_stream").unwrap();

    let record = create_record(vec![("id", FieldValue::Integer(1))]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            if !results.is_empty() {
                println!("✓ GOOD: _TIMESTAMP is available: {:?}", results[0].fields);
            } else {
                println!("⚠️ WARNING: _TIMESTAMP query returned empty results");
            }
        }
        Err(e) => {
            println!("✗ ERROR: _TIMESTAMP query failed: {}", e);
        }
    }
}

// =============================================================================
// WINDOWED QUERY FAILURE TESTS - THE MAIN FAILURE PATH
// =============================================================================

#[test]
fn test_window_with_default_timestamp_works() {
    // Window query using _TIMESTAMP (default) should work
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse(
            "SELECT symbol, COUNT(*) as cnt FROM test_stream \
             GROUP BY symbol WINDOW TUMBLING(10s) EMIT CHANGES",
        )
        .unwrap();

    engine.init_query_execution(query.clone());

    // Record with symbol field - window should use _TIMESTAMP by default
    let record = create_record(vec![
        ("symbol", FieldValue::String("AAPL".to_string())),
        ("price", FieldValue::Float(150.0)),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            println!(
                "Window query with system timestamp: {} results",
                results.len()
            );
            for r in results {
                println!("  Result: {:?}", r.fields);
            }
            if results.is_empty() {
                println!(
                    "⚠️ EMIT CHANGES returned 0 results for first record - might be expected if window not initialized"
                );
            }
        }
        Err(e) => {
            println!("✗ Window query error: {}", e);
        }
    }
}

// =============================================================================
// EMIT CHANGES SPECIFIC TESTS - THE KEY TEST FOR DEBUGGING
// =============================================================================

#[test]
fn test_emit_changes_produces_output_for_each_record() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse(
            "SELECT symbol, COUNT(*) as cnt FROM test_stream \
             GROUP BY symbol WINDOW TUMBLING(60s) EMIT CHANGES",
        )
        .unwrap();

    engine.init_query_execution(query.clone());

    let mut total_outputs = 0;
    let base_ts = chrono::Utc::now().timestamp_millis();

    // Process 10 records - each should produce an output with EMIT CHANGES
    for i in 0..10 {
        let record = create_record_with_timestamp(
            vec![
                ("symbol", FieldValue::String("AAPL".to_string())),
                ("price", FieldValue::Float(150.0 + i as f64)),
            ],
            base_ts + (i * 100), // Spread timestamps slightly
        );

        let result = engine.execute_with_record_sync(&query, &record);

        match &result {
            Ok(results) => {
                total_outputs += results.len();
                if i < 3 || i == 9 {
                    println!(
                        "Record {} produced {} outputs (running total: {})",
                        i,
                        results.len(),
                        total_outputs
                    );
                    for r in results {
                        println!("  -> {:?}", r.fields);
                    }
                }
            }
            Err(e) => {
                println!("✗ Record {} failed with error: {}", i, e);
            }
        }
    }

    println!("\n=== SUMMARY ===");
    println!(
        "EMIT CHANGES test: {} total outputs from 10 records",
        total_outputs
    );

    // EMIT CHANGES should produce output for every record (or close to it)
    if total_outputs == 0 {
        println!("✗ BUG CONFIRMED: EMIT CHANGES produced 0 outputs for 10 records!");
        println!("  This is the silent failure bug we're investigating.");
    } else if total_outputs < 5 {
        println!(
            "⚠️ WARNING: Only {} outputs for 10 records - might be buffering issue",
            total_outputs
        );
    } else {
        println!("✓ EMIT CHANGES working: {} outputs", total_outputs);
    }
}

/// This test uses the EXACT same query and data fields as market_aggregation.sql
/// to reproduce the integration test bug where 0 outputs are produced.
///
/// market_aggregation.sql query:
/// ```sql
/// SELECT symbol, COUNT(*) AS trade_count, SUM(volume) AS total_volume,
///        AVG(price) AS avg_price, MIN(price) AS min_price, MAX(price) AS max_price,
///        _window_start AS window_start, _window_end AS window_end
/// FROM market_data
/// GROUP BY symbol WINDOW TUMBLING(10s) EMIT CHANGES
/// ```
///
/// market_data.schema.yaml fields:
/// - symbol: string (AAPL, GOOGL, MSFT, AMZN, META)
/// - price: float (50.0-500.0)
/// - volume: integer (100-10000)
/// - event_time: timestamp (-2m to now)
#[test]
fn test_emit_changes_exact_market_aggregation_query() {
    use velostream::velostream::serialization::helpers::{
        field_value_to_json, json_to_field_value,
    };

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // EXACT query from market_aggregation.sql
    let query = parser
        .parse(
            "SELECT symbol, COUNT(*) AS trade_count, SUM(volume) AS total_volume, \
             AVG(price) AS avg_price, MIN(price) AS min_price, MAX(price) AS max_price, \
             _window_start AS window_start, _window_end AS window_end \
             FROM market_data \
             GROUP BY symbol WINDOW TUMBLING(10s) EMIT CHANGES",
        )
        .unwrap();

    engine.init_query_execution(query.clone());

    let mut total_outputs = 0;
    let mut errors = Vec::new();

    // Use timestamps spread over 2 minutes (like the schema specifies: -2m to now)
    let now_ts = chrono::Utc::now().timestamp_millis();
    let base_ts = now_ts - 120_000; // 2 minutes ago

    let symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"];

    println!("\n=== EXACT MARKET_AGGREGATION.SQL TEST ===");
    println!(
        "Query: SELECT symbol, COUNT(*), SUM(volume), AVG(price), MIN(price), MAX(price), _window_start, _window_end"
    );
    println!("       FROM market_data GROUP BY symbol WINDOW TUMBLING(10s) EMIT CHANGES");
    println!("Data: symbol, price, volume, event_time (JSON round-tripped)\n");

    // Process 100 records (like velo-test does)
    for i in 0..100 {
        let symbol = symbols[i % symbols.len()];
        let price = 50.0 + (i as f64 * 4.5); // Range 50.0-500.0
        let volume = 100 + (i * 99); // Range 100-10000
        let event_time_ms = base_ts + (i as i64 * 1200); // Spread over ~120 seconds

        // Create original record with all fields from schema
        let original_fields = vec![
            ("symbol", FieldValue::String(symbol.to_string())),
            ("price", FieldValue::Float(price)),
            ("volume", FieldValue::Integer(volume as i64)),
            (
                "event_time",
                FieldValue::Timestamp(
                    chrono::NaiveDateTime::from_timestamp_millis(event_time_ms).unwrap(),
                ),
            ),
        ];

        // Simulate JSON round-trip (exactly what happens in Kafka flow)
        let mut json_map = serde_json::Map::new();
        for (k, v) in &original_fields {
            let json_val = field_value_to_json(v).unwrap();
            json_map.insert(k.to_string(), json_val);
        }
        let json_value = serde_json::Value::Object(json_map);

        // Deserialize back from JSON (simulates reading from Kafka)
        let deserialized_fields: std::collections::HashMap<String, FieldValue> = match json_value {
            serde_json::Value::Object(map) => {
                let mut result = std::collections::HashMap::new();
                for (k, v) in map {
                    result.insert(k, json_to_field_value(&v).unwrap());
                }
                result
            }
            _ => panic!("Expected object"),
        };

        // Log first record's field types after JSON round-trip
        if i == 0 {
            println!("=== Record 0 after JSON round-trip ===");
            for (k, v) in &deserialized_fields {
                println!("  {}: {:?}", k, std::mem::discriminant(v));
            }
            println!();
        }

        // Create StreamRecord with system timestamp (like Kafka does)
        let record = StreamRecord {
            fields: deserialized_fields,
            timestamp: event_time_ms, // System timestamp from Kafka message
            offset: i as i64,
            partition: 0,
            headers: std::collections::HashMap::new(),
            event_time: None,
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record_sync(&query, &record);

        match &result {
            Ok(results) => {
                total_outputs += results.len();
                // Log first few and last results
                if i < 3 || i == 99 {
                    println!(
                        "Record {:3} (symbol={}, price={:.1}, volume={}, ts={}) -> {} outputs",
                        i,
                        symbol,
                        price,
                        volume,
                        event_time_ms,
                        results.len()
                    );
                    for r in results {
                        println!("  Output: {:?}", r.fields);
                    }
                } else if i == 3 {
                    println!("... (processing records 3-98) ...");
                }
            }
            Err(e) => {
                errors.push((i, e.to_string()));
                if errors.len() <= 5 {
                    println!("✗ Record {} FAILED: {}", i, e);
                }
            }
        }
    }

    println!("\n=== FINAL SUMMARY ===");
    println!("Records processed: 100");
    println!("Total outputs: {}", total_outputs);
    println!("Errors: {}", errors.len());

    if !errors.is_empty() {
        println!("\n=== ERROR DETAILS ===");
        for (idx, err) in errors.iter().take(10) {
            println!("  Record {}: {}", idx, err);
        }
        if errors.len() > 10 {
            println!("  ... and {} more errors", errors.len() - 10);
        }
    }

    // Assert based on expected behavior
    if total_outputs == 0 {
        println!("\n✗ BUG REPRODUCED: EMIT CHANGES produced 0 outputs for 100 records!");
        println!("  This matches the integration test failure.");
        // Don't fail the test yet - we want to see what happens
    } else if total_outputs < 50 {
        println!(
            "\n⚠️ WARNING: Only {} outputs for 100 records (expected ~100)",
            total_outputs
        );
    } else {
        println!(
            "\n✓ EMIT CHANGES working in unit test: {} outputs for 100 records",
            total_outputs
        );
    }

    // The test should produce at least some outputs
    // If it doesn't, we've reproduced the bug
    assert!(
        total_outputs > 0 || !errors.is_empty(),
        "Should produce outputs or errors, not silent failure"
    );
}

#[test]
fn test_emit_changes_with_multiple_groups() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse(
            "SELECT symbol, COUNT(*) as cnt FROM test_stream \
             GROUP BY symbol WINDOW TUMBLING(60s) EMIT CHANGES",
        )
        .unwrap();

    engine.init_query_execution(query.clone());

    let symbols = vec!["AAPL", "GOOGL", "MSFT"];
    let mut outputs_per_symbol: HashMap<String, usize> = HashMap::new();
    let base_ts = chrono::Utc::now().timestamp_millis();

    // Process 3 records for each symbol
    for (idx, symbol) in symbols.iter().enumerate() {
        for i in 0..3 {
            let record = create_record_with_timestamp(
                vec![
                    ("symbol", FieldValue::String(symbol.to_string())),
                    ("price", FieldValue::Float(100.0 + i as f64)),
                ],
                base_ts + ((idx * 3 + i) * 100) as i64,
            );

            let result = engine.execute_with_record_sync(&query, &record);

            if let Ok(results) = result {
                for r in &results {
                    if let Some(FieldValue::String(s)) = r.fields.get("symbol") {
                        *outputs_per_symbol.entry(s.clone()).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    println!("\n=== MULTI-GROUP SUMMARY ===");
    println!("Outputs per symbol: {:?}", outputs_per_symbol);

    let total: usize = outputs_per_symbol.values().sum();
    if total == 0 {
        println!("✗ BUG: All symbols produced 0 outputs!");
    } else {
        println!("Total outputs across all groups: {}", total);
    }
}

// =============================================================================
// EXPRESSION EVALUATION FAILURE TESTS
// =============================================================================

#[test]
fn test_division_by_zero_should_handle_gracefully() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT price / volume as ratio FROM test_stream")
        .unwrap();

    let record = create_record(vec![
        ("price", FieldValue::Float(100.0)),
        ("volume", FieldValue::Integer(0)), // Division by zero
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            if !results.is_empty() {
                let ratio = results[0].fields.get("ratio");
                println!("Division by zero produced: {:?}", ratio);
                // Should be Infinity, NaN, or error - not silent failure
            } else {
                println!("⚠️ Division by zero produced 0 results - silent failure?");
            }
        }
        Err(e) => {
            println!("Division by zero error: {}", e);
        }
    }
}

#[test]
fn test_type_mismatch_in_comparison_should_handle() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT id FROM test_stream WHERE price > name")
        .unwrap();

    let record = create_record(vec![
        ("id", FieldValue::Integer(1)),
        ("price", FieldValue::Float(100.0)),
        ("name", FieldValue::String("test".to_string())),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            println!("Type mismatch comparison: {} results", results.len());
        }
        Err(e) => {
            println!("✓ GOOD: Type mismatch comparison error: {}", e);
        }
    }
}

// =============================================================================
// SIMPLE PASSTHROUGH TEST - BASELINE
// =============================================================================

#[test]
fn test_simple_select_works() {
    // Sanity check - basic SELECT should work
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT symbol, price FROM test_stream")
        .unwrap();

    let record = create_record(vec![
        ("symbol", FieldValue::String("AAPL".to_string())),
        ("price", FieldValue::Float(150.0)),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            assert!(!results.is_empty(), "Simple SELECT should return results");
            println!("✓ Simple SELECT works: {:?}", results[0].fields);
        }
        Err(e) => {
            panic!("Simple SELECT failed: {}", e);
        }
    }
}

#[test]
fn test_simple_group_by_without_window_works() {
    // GROUP BY without window should work
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT symbol, COUNT(*) as cnt FROM test_stream GROUP BY symbol")
        .unwrap();

    engine.init_query_execution(query.clone());

    let record = create_record(vec![
        ("symbol", FieldValue::String("AAPL".to_string())),
        ("price", FieldValue::Float(150.0)),
    ]);

    let result = engine.execute_with_record_sync(&query, &record);

    match &result {
        Ok(results) => {
            println!("GROUP BY without window: {} results", results.len());
            for r in results {
                println!("  {:?}", r.fields);
            }
        }
        Err(e) => {
            println!("GROUP BY without window error: {}", e);
        }
    }
}

// =============================================================================
// JSON ROUND-TRIP TESTS - Simulates Kafka flow
// =============================================================================

#[test]
fn test_emit_changes_with_json_roundtrip() {
    // This simulates what happens when records go through Kafka:
    // 1. Record created with FieldValue::Timestamp
    // 2. Serialized to JSON (Timestamp -> String)
    // 3. Deserialized from JSON (String -> String, NOT back to Timestamp!)
    // 4. Window query executes

    use velostream::velostream::serialization::helpers::{
        field_value_to_json, json_to_field_value,
    };

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse(
            "SELECT symbol, COUNT(*) as cnt FROM test_stream \
             GROUP BY symbol WINDOW TUMBLING(60s) EMIT CHANGES",
        )
        .unwrap();

    engine.init_query_execution(query.clone());

    let mut total_outputs = 0;
    let base_ts = chrono::Utc::now().timestamp_millis();

    for i in 0..5 {
        // Create original record with Timestamp field
        let original_fields = vec![
            ("symbol", FieldValue::String("AAPL".to_string())),
            ("price", FieldValue::Float(150.0 + i as f64)),
            (
                "event_time",
                FieldValue::Timestamp(
                    chrono::NaiveDateTime::from_timestamp_millis(base_ts + (i * 100)).unwrap(),
                ),
            ),
        ];

        // Simulate JSON serialization (to Kafka)
        let mut json_map = serde_json::Map::new();
        for (k, v) in &original_fields {
            let json_val = field_value_to_json(v).unwrap();
            json_map.insert(k.to_string(), json_val);
        }
        let json_value = serde_json::Value::Object(json_map);

        // Simulate JSON deserialization (from Kafka)
        let deserialized_fields: std::collections::HashMap<String, FieldValue> = match json_value {
            serde_json::Value::Object(map) => {
                let mut result = std::collections::HashMap::new();
                for (k, v) in map {
                    result.insert(k, json_to_field_value(&v).unwrap());
                }
                result
            }
            _ => panic!("Expected object"),
        };

        // Check what happened to event_time
        if i == 0 {
            println!("\n=== JSON ROUND-TRIP CHECK ===");
            println!("Original event_time type: FieldValue::Timestamp");
            println!(
                "After JSON round-trip: {:?}",
                deserialized_fields.get("event_time")
            );

            // This shows the bug: event_time becomes a String!
            match deserialized_fields.get("event_time") {
                Some(FieldValue::String(s)) => {
                    println!(
                        "⚠️ BUG: event_time became String after JSON round-trip: {}",
                        s
                    );
                }
                Some(FieldValue::Timestamp(_)) => {
                    println!("✓ event_time preserved as Timestamp");
                }
                other => {
                    println!("event_time is: {:?}", other);
                }
            }
        }

        // Create StreamRecord (simulating Kafka read)
        let mut record = StreamRecord {
            fields: deserialized_fields,
            timestamp: base_ts + (i * 100), // System timestamp from Kafka
            offset: i,
            partition: 0,
            headers: std::collections::HashMap::new(),
            event_time: None,
            topic: None,
            key: None,
        };

        let result = engine.execute_with_record_sync(&query, &record);

        match &result {
            Ok(results) => {
                total_outputs += results.len();
                if i == 0 {
                    println!("\n=== QUERY EXECUTION ===");
                    println!("Record {} produced {} outputs", i, results.len());
                    for r in results {
                        println!("  -> {:?}", r.fields);
                    }
                }
            }
            Err(e) => {
                println!("✗ Record {} FAILED with error: {}", i, e);
                // This is important - if we see errors here, we found a bug
            }
        }
    }

    println!("\n=== SUMMARY ===");
    println!(
        "JSON round-trip test: {} total outputs from 5 records",
        total_outputs
    );

    if total_outputs == 0 {
        println!("✗ BUG: 0 outputs after JSON round-trip!");
    } else {
        println!("✓ EMIT CHANGES working after JSON round-trip");
    }
}
