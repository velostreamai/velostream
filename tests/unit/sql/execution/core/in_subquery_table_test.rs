/*!
# IN Subquery with Table Source - Compound WHERE Predicate Test

Tests that `OptimizedTableImpl::sql_column_values` correctly handles compound
WHERE clauses (AND with IN operators) used by IN subqueries against tables.

Reproduces the `active_hours_market_data` bug where compound WHERE clauses like
`market_status = 'OPEN' AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')`
returned 0 results because `parse_simple_equality` greedily matched the entire
clause as a single equality check.
*/

use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::sql::execution::processors::QueryProcessor;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::table::unified_table::OptimizedTableImpl;

/// Create an instrument_schedules table with realistic data
fn create_instrument_schedules_table() -> OptimizedTableImpl {
    let table = OptimizedTableImpl::new();

    // AAPL - OPEN, REGULAR
    let mut record = HashMap::new();
    record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    record.insert(
        "market_status".to_string(),
        FieldValue::String("OPEN".to_string()),
    );
    record.insert(
        "session_type".to_string(),
        FieldValue::String("REGULAR".to_string()),
    );
    table.insert("AAPL".to_string(), record).unwrap();

    // GOOGL - OPEN, REGULAR
    let mut record = HashMap::new();
    record.insert(
        "symbol".to_string(),
        FieldValue::String("GOOGL".to_string()),
    );
    record.insert(
        "market_status".to_string(),
        FieldValue::String("OPEN".to_string()),
    );
    record.insert(
        "session_type".to_string(),
        FieldValue::String("REGULAR".to_string()),
    );
    table.insert("GOOGL".to_string(), record).unwrap();

    // MSFT - OPEN, REGULAR
    let mut record = HashMap::new();
    record.insert("symbol".to_string(), FieldValue::String("MSFT".to_string()));
    record.insert(
        "market_status".to_string(),
        FieldValue::String("OPEN".to_string()),
    );
    record.insert(
        "session_type".to_string(),
        FieldValue::String("REGULAR".to_string()),
    );
    table.insert("MSFT".to_string(), record).unwrap();

    // TSLA - CLOSED (should NOT match)
    let mut record = HashMap::new();
    record.insert("symbol".to_string(), FieldValue::String("TSLA".to_string()));
    record.insert(
        "market_status".to_string(),
        FieldValue::String("CLOSED".to_string()),
    );
    record.insert(
        "session_type".to_string(),
        FieldValue::String("REGULAR".to_string()),
    );
    table.insert("TSLA".to_string(), record).unwrap();

    table
}

/// Create an input StreamRecord simulating a market_data_ts record
fn create_market_data_record(symbol: &str) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert(
        "exchange".to_string(),
        FieldValue::String("NASDAQ".to_string()),
    );
    fields.insert("price".to_string(), FieldValue::Float(150.25));
    fields.insert("volume".to_string(), FieldValue::Integer(10000));
    fields.insert(
        "_event_time".to_string(),
        FieldValue::String("2024-01-15T10:30:00Z".to_string()),
    );
    fields.insert(
        "timestamp".to_string(),
        FieldValue::String("2024-01-15T10:30:00Z".to_string()),
    );

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: 1705312200000,
        offset: 0,
        partition: 0,
        event_time: None,
        topic: None,
        key: None,
    }
}

/// Test 1: Direct sql_column_values call with compound WHERE clause
///
/// This tests the root cause hypothesis: does the OptimizedTableImpl
/// correctly filter records when WHERE has AND + IN operators?
#[test]
fn test_sql_column_values_with_compound_where() {
    use velostream::velostream::table::unified_table::UnifiedTable;

    let table = create_instrument_schedules_table();

    // This is the WHERE clause that the subquery executor would generate
    // from: WHERE market_status = 'OPEN' AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')
    let where_clause =
        "market_status = 'OPEN' AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')";

    println!("Testing sql_column_values with compound WHERE clause:");
    println!("  WHERE: {}", where_clause);

    let result = table.sql_column_values("symbol", where_clause);
    match &result {
        Ok(values) => {
            println!("  Result: {} values returned", values.len());
            for (i, v) in values.iter().enumerate() {
                println!("    [{}] {:?}", i, v);
            }
        }
        Err(e) => {
            println!("  Error: {:?}", e);
        }
    }

    let values = result.expect("sql_column_values should not error");

    // We expect AAPL, GOOGL, MSFT (all OPEN + REGULAR) but NOT TSLA (CLOSED)
    println!(
        "  Expected 3 symbols (AAPL, GOOGL, MSFT), got {} symbols",
        values.len()
    );

    // Diagnostic: check if we get ALL 4 (no filtering) or 0 (over-filtering)
    if values.len() == 4 {
        println!(
            "  DIAGNOSIS: WHERE clause was NOT applied - all 4 records returned (AlwaysTrue fallback)"
        );
    } else if values.is_empty() {
        println!(
            "  DIAGNOSIS: WHERE clause filtered everything out (AlwaysFalse or parse failure)"
        );
    } else if values.len() == 3 {
        println!("  DIAGNOSIS: WHERE clause correctly filtered to 3 matching records");
    }

    // This assertion captures the expected behavior
    assert_eq!(
        values.len(),
        3,
        "Expected 3 symbols matching OPEN + REGULAR/PRE_MARKET/POST_MARKET, got {}",
        values.len()
    );
}

/// Test 2: Direct sql_column_values with simple WHERE clause (single equality)
///
/// This tests that simple WHERE clauses work correctly as a baseline.
#[test]
fn test_sql_column_values_with_simple_where() {
    use velostream::velostream::table::unified_table::UnifiedTable;

    let table = create_instrument_schedules_table();

    let where_clause = "market_status = 'OPEN'";
    println!("Testing sql_column_values with simple WHERE clause:");
    println!("  WHERE: {}", where_clause);

    let result = table.sql_column_values("symbol", where_clause);
    match &result {
        Ok(values) => {
            println!("  Result: {} values returned", values.len());
            for (i, v) in values.iter().enumerate() {
                println!("    [{}] {:?}", i, v);
            }
        }
        Err(e) => {
            println!("  Error: {:?}", e);
        }
    }

    let values = result.expect("sql_column_values should not error");
    println!(
        "  Expected 3 symbols (AAPL, GOOGL, MSFT), got {}",
        values.len()
    );

    assert_eq!(
        values.len(),
        3,
        "Expected 3 symbols matching market_status = 'OPEN', got {}",
        values.len()
    );
}

/// Test 3: Full end-to-end query execution matching the compliance app query
///
/// This is the closest reproduction of the actual active_hours_market_data query.
#[test]
fn test_compliance_active_hours_query_end_to_end() {
    let parser = StreamingSqlParser::new();

    // Simplified version of the compliance app query
    let query = r#"
        SELECT
            m.symbol,
            m.exchange,
            m.price,
            m.volume,
            m._event_time,
            m.timestamp,
            'ACTIVE_TRADING' as market_session
        FROM market_data_ts m
        WHERE m.symbol IN (
            SELECT symbol FROM instrument_schedules i
            WHERE market_status = 'OPEN'
              AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')
        )
    "#;

    println!("Parsing query...");
    let parsed = match parser.parse(query) {
        Ok(p) => {
            println!("  Parse successful: {:?}", p);
            p
        }
        Err(e) => {
            println!("  Parse FAILED: {:?}", e);
            panic!("Query should parse successfully: {:?}", e);
        }
    };

    // Set up context with real tables
    let mut context = ProcessorContext::new("test_compliance");
    context.load_reference_table(
        "instrument_schedules",
        Arc::new(create_instrument_schedules_table()),
    );

    // Create input record
    let record = create_market_data_record("AAPL");

    println!("Executing query...");
    let result = QueryProcessor::process_query(&parsed, &record, &mut context);

    match &result {
        Ok(output) => {
            println!("  Execution successful");
            println!("  Has output record: {}", output.record.is_some());
            if let Some(ref rec) = output.record {
                println!("  Output fields: {:?}", rec.fields);
            } else {
                println!("  DIAGNOSIS: Record was filtered out (WHERE clause returned false)");
            }
        }
        Err(e) => {
            println!("  Execution FAILED: {:?}", e);
            // Print error chain
            use std::error::Error;
            let mut source = e.source();
            let mut level = 1;
            while let Some(err) = source {
                println!("    Level {}: {:?}", level, err);
                source = err.source();
                level += 1;
            }
        }
    }

    let output = result.expect("Query execution should not error");
    assert!(
        output.record.is_some(),
        "AAPL record should pass through the IN subquery filter (AAPL is in instrument_schedules with OPEN + REGULAR)"
    );

    // Verify the output has the expected fields
    let rec = output.record.unwrap();
    // Fields come out with table alias prefix (m.symbol) since query uses alias
    let symbol = rec
        .fields
        .get("symbol")
        .or_else(|| rec.fields.get("m.symbol"));
    assert_eq!(
        symbol,
        Some(&FieldValue::String("AAPL".to_string())),
        "Output should have symbol=AAPL"
    );
    assert_eq!(
        rec.fields.get("market_session"),
        Some(&FieldValue::String("ACTIVE_TRADING".to_string())),
        "Output should have market_session=ACTIVE_TRADING"
    );
}

/// Test 4: Verify a symbol NOT in the table gets filtered out
#[test]
fn test_compliance_active_hours_query_filters_unknown_symbol() {
    let parser = StreamingSqlParser::new();

    let query = r#"
        SELECT
            m.symbol,
            m.exchange,
            m.price
        FROM market_data_ts m
        WHERE m.symbol IN (
            SELECT symbol FROM instrument_schedules i
            WHERE market_status = 'OPEN'
              AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')
        )
    "#;

    let parsed = parser.parse(query).expect("Query should parse");

    let mut context = ProcessorContext::new("test_compliance_filter");
    context.load_reference_table(
        "instrument_schedules",
        Arc::new(create_instrument_schedules_table()),
    );

    // Use a symbol that is NOT in the table at all
    let record = create_market_data_record("UNKNOWN_SYMBOL");

    println!("Testing that unknown symbol gets filtered out...");
    let result = QueryProcessor::process_query(&parsed, &record, &mut context);

    match &result {
        Ok(output) => {
            println!("  Has output record: {}", output.record.is_some());
            if output.record.is_some() {
                println!("  DIAGNOSIS: Unknown symbol was NOT filtered out - bug!");
            } else {
                println!("  Correctly filtered out unknown symbol");
            }
        }
        Err(e) => {
            println!("  Execution error: {:?}", e);
        }
    }

    let output = result.expect("Query execution should not error");
    assert!(
        output.record.is_none(),
        "Unknown symbol should be filtered out by IN subquery"
    );
}

/// Test 5: Verify TSLA (CLOSED market_status) gets filtered out
#[test]
fn test_compliance_active_hours_query_filters_closed_market() {
    let parser = StreamingSqlParser::new();

    let query = r#"
        SELECT
            m.symbol,
            m.exchange,
            m.price
        FROM market_data_ts m
        WHERE m.symbol IN (
            SELECT symbol FROM instrument_schedules i
            WHERE market_status = 'OPEN'
              AND session_type IN ('REGULAR', 'PRE_MARKET', 'POST_MARKET')
        )
    "#;

    let parsed = parser.parse(query).expect("Query should parse");

    let mut context = ProcessorContext::new("test_compliance_closed");
    context.load_reference_table(
        "instrument_schedules",
        Arc::new(create_instrument_schedules_table()),
    );

    // TSLA is in table but with CLOSED status
    let record = create_market_data_record("TSLA");

    println!("Testing that TSLA (CLOSED) gets filtered out...");
    let result = QueryProcessor::process_query(&parsed, &record, &mut context);

    match &result {
        Ok(output) => {
            println!("  Has output record: {}", output.record.is_some());
            if output.record.is_some() {
                println!(
                    "  DIAGNOSIS: TSLA passed through despite CLOSED status - WHERE clause not applied!"
                );
            } else {
                println!("  Correctly filtered out TSLA (CLOSED market)");
            }
        }
        Err(e) => {
            println!("  Execution error: {:?}", e);
        }
    }

    let output = result.expect("Query execution should not error");
    assert!(
        output.record.is_none(),
        "TSLA should be filtered out because market_status = 'CLOSED'"
    );
}
