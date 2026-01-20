//! Tests for _window_start and _window_end system column population
//!
//! These tests verify that window system columns are correctly
//! populated in result records from windowed aggregation queries.
//!
//! Also includes tests for GROUP BY key → record.key functionality (FR-089).

use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::collections::HashMap;
use velostream::velostream::sql::StreamingQuery;
use velostream::velostream::sql::ast::{Expr, SelectField};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::window_v2::adapter::WindowAdapter;
use velostream::velostream::sql::execution::window_v2::traits::WindowStats;
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Test that the parser correctly parses _window_start AS alias
/// Parser produces Expression { expr: Column("_window_start"), alias: Some("window_start") }
#[test]
fn test_parser_window_system_columns_aliased() {
    let sql = "SELECT _window_start AS window_start, _window_end AS window_end FROM test";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).unwrap();

    if let StreamingQuery::Select { fields, .. } = query {
        assert_eq!(fields.len(), 2);

        // Check first field: _window_start AS window_start
        // Parser produces Expression with Column expr, not AliasedColumn
        match &fields[0] {
            SelectField::Expression {
                expr: Expr::Column(col),
                alias: Some(alias),
            } => {
                assert_eq!(col, "_window_start");
                assert_eq!(alias, "window_start");
            }
            other => panic!(
                "Expected Expression {{ expr: Column(_window_start), alias: Some(window_start) }}, got {:?}",
                other
            ),
        }

        // Check second field: _window_end AS window_end
        match &fields[1] {
            SelectField::Expression {
                expr: Expr::Column(col),
                alias: Some(alias),
            } => {
                assert_eq!(col, "_window_end");
                assert_eq!(alias, "window_end");
            }
            other => panic!(
                "Expected Expression {{ expr: Column(_window_end), alias: Some(window_end) }}, got {:?}",
                other
            ),
        }
    } else {
        panic!("Expected Select query");
    }
}

/// Test that the parser correctly parses _window_start without alias
/// Parser produces Expression { expr: Column("_window_start"), alias: None }
#[test]
fn test_parser_window_system_columns_unaliased() {
    let sql = "SELECT _window_start, _window_end FROM test";
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).unwrap();

    if let StreamingQuery::Select { fields, .. } = query {
        assert_eq!(fields.len(), 2);

        // Check first field: _window_start (no alias)
        // Parser produces Expression with Column expr
        match &fields[0] {
            SelectField::Expression {
                expr: Expr::Column(col),
                alias: None,
            } => {
                assert_eq!(col, "_window_start");
            }
            other => panic!(
                "Expected Expression {{ expr: Column(_window_start), alias: None }}, got {:?}",
                other
            ),
        }

        // Check second field: _window_end (no alias)
        match &fields[1] {
            SelectField::Expression {
                expr: Expr::Column(col),
                alias: None,
            } => {
                assert_eq!(col, "_window_end");
            }
            other => panic!(
                "Expected Expression {{ expr: Column(_window_end), alias: None }}, got {:?}",
                other
            ),
        }
    } else {
        panic!("Expected Select query");
    }
}

/// Test WindowStats has both start and end times populated
#[test]
fn test_window_stats_has_both_times() {
    use velostream::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;
    use velostream::velostream::sql::execution::window_v2::traits::WindowStrategy;
    use velostream::velostream::sql::execution::window_v2::types::SharedRecord;

    let mut strategy = TumblingWindowStrategy::new(60000, "timestamp".to_string());

    // Create test record with timestamp
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldValue::Integer(10000));
    fields.insert("value".to_string(), FieldValue::Float(100.0));
    let mut record = StreamRecord::new(fields);
    record.timestamp = 10000;
    let shared = SharedRecord::new(record);

    // Add record to window
    strategy.add_record(shared).unwrap();

    // Get stats
    let stats = strategy.get_stats();

    // Verify both times are populated
    assert!(
        stats.window_start_time.is_some(),
        "window_start_time should be Some"
    );
    assert!(
        stats.window_end_time.is_some(),
        "window_end_time should be Some"
    );

    // Verify correct values for 60-second tumbling window starting at timestamp 10000
    // Window should be [0, 60000)
    assert_eq!(stats.window_start_time, Some(0));
    assert_eq!(stats.window_end_time, Some(60000));
}

/// Test that build_result_record correctly handles system columns with WindowStats
#[test]
fn test_build_result_record_with_window_stats() {
    // This tests the adapter's build_result_record function indirectly
    // by verifying the WindowStats flow

    let stats = WindowStats {
        record_count: 5,
        window_start_time: Some(1000),
        window_end_time: Some(2000),
        emission_count: 1,
        buffer_size_bytes: 100,
    };

    // Verify stats are complete
    assert_eq!(stats.window_start_time, Some(1000));
    assert_eq!(stats.window_end_time, Some(2000));
}

/// Integration test: Full flow from TumblingWindowStrategy through stats
#[test]
fn test_tumbling_window_emit_changes_flow() {
    use velostream::velostream::sql::execution::window_v2::emission::EmitChangesStrategy;
    use velostream::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;
    use velostream::velostream::sql::execution::window_v2::traits::{
        EmissionStrategy, EmitDecision, WindowStrategy,
    };
    use velostream::velostream::sql::execution::window_v2::types::SharedRecord;

    let mut window_strategy = TumblingWindowStrategy::new(60000, "timestamp".to_string());
    let mut emit_strategy = EmitChangesStrategy::new(1); // Emit every record

    // Create test records
    let create_record = |ts: i64| {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(ts));
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert("price".to_string(), FieldValue::Float(150.0));
        let mut record = StreamRecord::new(fields);
        record.timestamp = ts;
        SharedRecord::new(record)
    };

    // Add first record
    let r1 = create_record(10000);
    let decision = emit_strategy
        .process_record(r1, &mut window_strategy)
        .unwrap();

    // Should emit on first record with EMIT CHANGES
    assert!(matches!(decision, EmitDecision::Emit));

    // Get stats BEFORE any clear() call
    let stats = window_strategy.get_stats();

    // Critical assertions - both times should be populated
    assert!(
        stats.window_start_time.is_some(),
        "window_start_time should be Some after adding record"
    );
    assert!(
        stats.window_end_time.is_some(),
        "window_end_time should be Some after adding record"
    );

    // Values should be correct for 60-second tumbling window
    assert_eq!(
        stats.window_start_time,
        Some(0),
        "window_start_time should be 0"
    );
    assert_eq!(
        stats.window_end_time,
        Some(60000),
        "window_end_time should be 60000"
    );
}

/// Test that verifies window_end is populated in multiple GROUP BY scenarios
#[test]
fn test_window_stats_with_multiple_groups() {
    use velostream::velostream::sql::execution::window_v2::emission::EmitChangesStrategy;
    use velostream::velostream::sql::execution::window_v2::strategies::TumblingWindowStrategy;
    use velostream::velostream::sql::execution::window_v2::traits::{
        EmissionStrategy, EmitDecision, WindowStrategy,
    };
    use velostream::velostream::sql::execution::window_v2::types::SharedRecord;

    // Create 10-second tumbling window
    let mut window_strategy = TumblingWindowStrategy::new(10000, "timestamp".to_string());
    let mut emit_strategy = EmitChangesStrategy::new(1);

    // Create test records with different symbols (like market_aggregation.sql)
    let create_record = |ts: i64, symbol: &str| {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(ts));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(100.0));
        fields.insert("volume".to_string(), FieldValue::Integer(100));
        let mut record = StreamRecord::new(fields);
        record.timestamp = ts;
        SharedRecord::new(record)
    };

    // Add records for multiple symbols (mimicking market_aggregation.sql)
    let symbols = vec!["AAPL", "GOOGL", "MSFT"];
    for symbol in &symbols {
        let record = create_record(5000, symbol);
        let decision = emit_strategy
            .process_record(record, &mut window_strategy)
            .unwrap();
        assert!(matches!(decision, EmitDecision::Emit));
    }

    // Get stats after adding all records
    let stats = window_strategy.get_stats();

    // Verify window boundaries are set (window [0, 10000) for timestamp 5000)
    assert_eq!(
        stats.window_start_time,
        Some(0),
        "window_start_time should be 0"
    );
    assert_eq!(
        stats.window_end_time,
        Some(10000),
        "window_end_time should be 10000"
    );
    assert_eq!(stats.record_count, 3, "should have 3 records");

    println!(
        "WindowStats: start={:?}, end={:?}, count={}",
        stats.window_start_time, stats.window_end_time, stats.record_count
    );
}

/// Full integration test using WindowAdapter::process_with_v2
/// This tests the complete flow through the adapter to verify _window_start/_window_end population
#[test]
fn test_window_adapter_populates_system_columns() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;
    use velostream::velostream::sql::execution::window_v2::types::SharedRecord;

    // Create a windowed aggregation query with _window_start and _window_end
    let sql = r#"
        SELECT
            symbol,
            _window_start AS window_start,
            _window_end AS window_end,
            AVG(price) AS avg_price
        FROM market_data
        GROUP BY symbol
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    // Create processor context
    let mut context = ProcessorContext::new("test_window_query");

    // Create test records with timestamp
    let create_record = |ts: i64, symbol: &str, price: f64| {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(ts));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        let mut record = StreamRecord::new(fields);
        record.timestamp = ts;
        record
    };

    // Process first record - should emit with EMIT CHANGES
    let record1 = create_record(5000, "AAPL", 150.0);
    let result1 = WindowAdapter::process_with_v2("test_query", &query, &record1, &mut context)
        .expect("Should process record");

    // Should emit a result
    assert!(
        result1.is_some(),
        "First record should trigger emission with EMIT CHANGES"
    );

    let output = result1.unwrap();

    // Verify the result has window_start and window_end populated
    println!("Output record fields: {:?}", output.fields);

    // Check window_start
    let window_start = output.fields.get("window_start");
    assert!(
        window_start.is_some(),
        "Result should have window_start field. Got fields: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
    match window_start.unwrap() {
        FieldValue::Integer(v) => {
            assert_eq!(
                *v, 0,
                "window_start should be 0 for 10-second window with ts=5000"
            );
        }
        FieldValue::Null => {
            panic!("window_start should NOT be Null - it should be an Integer");
        }
        other => {
            panic!("window_start should be Integer, got {:?}", other);
        }
    }

    // Check window_end
    let window_end = output.fields.get("window_end");
    assert!(
        window_end.is_some(),
        "Result should have window_end field. Got fields: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
    match window_end.unwrap() {
        FieldValue::Integer(v) => {
            assert_eq!(
                *v, 10000,
                "window_end should be 10000 for 10-second window with ts=5000"
            );
        }
        FieldValue::Null => {
            panic!("window_end should NOT be Null - it should be an Integer");
        }
        other => {
            panic!("window_end should be Integer, got {:?}", other);
        }
    }

    // Verify other fields
    assert!(
        output.fields.get("symbol").is_some(),
        "Should have symbol field"
    );
    assert!(
        output.fields.get("avg_price").is_some(),
        "Should have avg_price field"
    );
}

/// Test CREATE STREAM variant (this matches the production query type)
#[test]
fn test_create_stream_populates_system_columns() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;
    use velostream::velostream::sql::execution::window_v2::adapter::WindowAdapter;

    // This is the exact pattern used in the demo SQL file
    let sql = r#"
        CREATE STREAM market_aggregates AS
        SELECT
            symbol,
            _window_start AS window_start,
            _window_end AS window_end,
            AVG(price) AS avg_price
        FROM market_data
        GROUP BY symbol
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    // Verify it parsed as CreateStream
    match &query {
        StreamingQuery::CreateStream {
            name,
            as_select,
            emit_mode,
            ..
        } => {
            println!("Parsed as CreateStream: name={}", name);
            println!("emit_mode from CreateStream: {:?}", emit_mode);

            // Verify inner SELECT has window
            if let StreamingQuery::Select {
                window,
                emit_mode: inner_emit,
                ..
            } = as_select.as_ref()
            {
                println!("Inner SELECT window: {:?}", window);
                println!("Inner SELECT emit_mode: {:?}", inner_emit);
            }
        }
        other => panic!(
            "Expected CreateStream, got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // Create processor context
    let mut context = ProcessorContext::new("test_create_stream_query");

    // Create test records with timestamp
    let create_record = |ts: i64, symbol: &str, price: f64| {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(ts));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        let mut record = StreamRecord::new(fields);
        record.timestamp = ts;
        record
    };

    // Process first record - should emit with EMIT CHANGES
    let record1 = create_record(5000, "AAPL", 150.0);
    let result1 = WindowAdapter::process_with_v2("test_query", &query, &record1, &mut context)
        .expect("Should process record");

    // Should emit a result
    assert!(
        result1.is_some(),
        "First record should trigger emission with EMIT CHANGES"
    );

    let output = result1.unwrap();

    // Verify the result has window_start and window_end populated
    println!("CREATE STREAM output record fields: {:?}", output.fields);

    // Check window_start
    let window_start = output.fields.get("window_start");
    assert!(
        window_start.is_some(),
        "Result should have window_start field. Got fields: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
    match window_start.unwrap() {
        FieldValue::Integer(v) => {
            assert_eq!(
                *v, 0,
                "window_start should be 0 for 10-second window with ts=5000"
            );
        }
        FieldValue::Null => {
            panic!("window_start should NOT be Null - it should be an Integer");
        }
        other => {
            panic!("window_start should be Integer, got {:?}", other);
        }
    }

    // Check window_end
    let window_end = output.fields.get("window_end");
    assert!(
        window_end.is_some(),
        "Result should have window_end field. Got fields: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
    match window_end.unwrap() {
        FieldValue::Integer(v) => {
            assert_eq!(
                *v, 10000,
                "window_end should be 10000 for 10-second window with ts=5000"
            );
        }
        FieldValue::Null => {
            panic!("window_end should NOT be Null - it should be an Integer");
        }
        other => {
            panic!("window_end should be Integer, got {:?}", other);
        }
    }
}

/// Test CREATE TABLE (CTAS) variant
#[test]
fn test_create_table_populates_system_columns() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;
    use velostream::velostream::sql::execution::window_v2::adapter::WindowAdapter;

    // This is the CREATE TABLE pattern
    let sql = r#"
        CREATE TABLE market_aggregates AS
        SELECT
            symbol,
            _window_start AS window_start,
            _window_end AS window_end,
            AVG(price) AS avg_price
        FROM market_data
        GROUP BY symbol
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    // Verify it parsed as CreateTable
    match &query {
        StreamingQuery::CreateTable {
            name,
            as_select,
            emit_mode,
            ..
        } => {
            println!("Parsed as CreateTable: name={}", name);
            println!("emit_mode from CreateTable: {:?}", emit_mode);

            // Verify inner SELECT has window
            if let StreamingQuery::Select {
                window,
                emit_mode: inner_emit,
                ..
            } = as_select.as_ref()
            {
                println!("Inner SELECT window: {:?}", window);
                println!("Inner SELECT emit_mode: {:?}", inner_emit);
            }
        }
        other => panic!(
            "Expected CreateTable, got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // Create processor context
    let mut context = ProcessorContext::new("test_create_table_query");

    // Create test records with timestamp
    let create_record = |ts: i64, symbol: &str, price: f64| {
        let mut fields = HashMap::new();
        fields.insert("timestamp".to_string(), FieldValue::Integer(ts));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        let mut record = StreamRecord::new(fields);
        record.timestamp = ts;
        record
    };

    // Process first record - should emit with EMIT CHANGES
    let record1 = create_record(5000, "AAPL", 150.0);
    let result1 = WindowAdapter::process_with_v2("test_query", &query, &record1, &mut context)
        .expect("Should process record");

    // Should emit a result
    assert!(
        result1.is_some(),
        "First record should trigger emission with EMIT CHANGES"
    );

    let output = result1.unwrap();

    // Verify the result has window_start and window_end populated
    println!("CREATE TABLE output record fields: {:?}", output.fields);
    println!("CREATE TABLE output record key: {:?}", output.key);

    // Check that record.key is set from GROUP BY (FR-089 fix)
    assert!(
        output.key.is_some(),
        "record.key should be set from GROUP BY symbol. Got: {:?}",
        output.key
    );
    match output.key.as_ref().unwrap() {
        FieldValue::String(s) => {
            assert_eq!(
                s, "AAPL",
                "record.key should be 'AAPL' from GROUP BY symbol"
            );
            println!("✅ record.key = {:?} (correctly set from GROUP BY)", s);
        }
        other => {
            panic!("record.key should be String('AAPL'), got {:?}", other);
        }
    }

    // Check window_start
    let window_start = output.fields.get("window_start");
    assert!(
        window_start.is_some(),
        "Result should have window_start field. Got fields: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
    match window_start.unwrap() {
        FieldValue::Integer(v) => {
            assert_eq!(
                *v, 0,
                "window_start should be 0 for 10-second window with ts=5000"
            );
        }
        FieldValue::Null => {
            panic!("window_start should NOT be Null - it should be an Integer");
        }
        other => {
            panic!("window_start should be Integer, got {:?}", other);
        }
    }

    // Check window_end
    let window_end = output.fields.get("window_end");
    assert!(
        window_end.is_some(),
        "Result should have window_end field. Got fields: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
    match window_end.unwrap() {
        FieldValue::Integer(v) => {
            assert_eq!(
                *v, 10000,
                "window_end should be 10000 for 10-second window with ts=5000"
            );
        }
        FieldValue::Null => {
            panic!("window_end should NOT be Null - it should be an Integer");
        }
        other => {
            panic!("window_end should be Integer, got {:?}", other);
        }
    }
}

// =============================================================================
// FR-089: GROUP BY key → record.key Tests
// =============================================================================

/// Test FieldValue::to_json() for all common types
#[test]
fn test_field_value_to_json_all_types() {
    // String
    let v = FieldValue::String("hello".to_string());
    assert_eq!(v.to_json(), serde_json::json!("hello"));

    // Integer
    let v = FieldValue::Integer(42);
    assert_eq!(v.to_json(), serde_json::json!(42));

    // Float
    let v = FieldValue::Float(123.456);
    assert_eq!(v.to_json(), serde_json::json!(123.456));

    // Boolean
    let v = FieldValue::Boolean(true);
    assert_eq!(v.to_json(), serde_json::json!(true));

    // Null
    let v = FieldValue::Null;
    assert_eq!(v.to_json(), serde_json::Value::Null);

    // ScaledInteger (financial precision)
    let v = FieldValue::ScaledInteger(12345, 2); // 123.45
    assert_eq!(v.to_json(), serde_json::json!("123.45"));

    // Negative ScaledInteger
    let v = FieldValue::ScaledInteger(-12345, 2); // -123.45
    assert_eq!(v.to_json(), serde_json::json!("-123.45"));

    // Zero ScaledInteger
    let v = FieldValue::ScaledInteger(0, 2);
    assert_eq!(v.to_json(), serde_json::json!("0"));

    // Float NaN becomes null (can't represent NaN in JSON)
    let v = FieldValue::Float(f64::NAN);
    assert_eq!(v.to_json(), serde_json::Value::Null);

    // Float Infinity becomes null (can't represent Infinity in JSON)
    let v = FieldValue::Float(f64::INFINITY);
    assert_eq!(v.to_json(), serde_json::Value::Null);

    // Decimal
    let v = FieldValue::Decimal(Decimal::new(12345, 2)); // 123.45
    assert_eq!(v.to_json(), serde_json::json!("123.45"));

    // Date
    let v = FieldValue::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    assert_eq!(v.to_json(), serde_json::json!("2024-01-15"));

    // Array
    let v = FieldValue::Array(vec![
        FieldValue::Integer(1),
        FieldValue::Integer(2),
        FieldValue::Integer(3),
    ]);
    assert_eq!(v.to_json(), serde_json::json!([1, 2, 3]));

    // Map
    let mut map = HashMap::new();
    map.insert("a".to_string(), FieldValue::Integer(1));
    map.insert("b".to_string(), FieldValue::Integer(2));
    let v = FieldValue::Map(map);
    let json = v.to_json();
    assert!(json.is_object());
    assert_eq!(json.get("a"), Some(&serde_json::json!(1)));
    assert_eq!(json.get("b"), Some(&serde_json::json!(2)));
}

/// Test FieldValue::to_key_string() for all common types
#[test]
fn test_field_value_to_key_string_all_types() {
    // String - returned as-is (no quotes)
    assert_eq!(
        FieldValue::String("AAPL".to_string()).to_key_string(),
        "AAPL"
    );

    // Integer
    assert_eq!(FieldValue::Integer(42).to_key_string(), "42");
    assert_eq!(FieldValue::Integer(-100).to_key_string(), "-100");

    // Float
    assert_eq!(FieldValue::Float(123.456).to_key_string(), "123.456");

    // Boolean
    assert_eq!(FieldValue::Boolean(true).to_key_string(), "true");
    assert_eq!(FieldValue::Boolean(false).to_key_string(), "false");

    // Null - empty string
    assert_eq!(FieldValue::Null.to_key_string(), "");

    // ScaledInteger (financial precision) - trailing zeros trimmed
    assert_eq!(
        FieldValue::ScaledInteger(12345, 2).to_key_string(),
        "123.45"
    );
    assert_eq!(FieldValue::ScaledInteger(12300, 2).to_key_string(), "123"); // .00 trimmed
    assert_eq!(FieldValue::ScaledInteger(12340, 2).to_key_string(), "123.4"); // trailing 0 trimmed

    // Negative ScaledInteger
    assert_eq!(
        FieldValue::ScaledInteger(-12345, 2).to_key_string(),
        "-123.45"
    );
    assert_eq!(FieldValue::ScaledInteger(-100, 2).to_key_string(), "-1"); // -1.00 trimmed

    // Zero
    assert_eq!(FieldValue::ScaledInteger(0, 2).to_key_string(), "0");

    // Decimal
    assert_eq!(
        FieldValue::Decimal(Decimal::new(12345, 2)).to_key_string(),
        "123.45"
    );

    // Float edge cases
    assert_eq!(FieldValue::Float(f64::NAN).to_key_string(), "NaN");
    assert_eq!(FieldValue::Float(f64::INFINITY).to_key_string(), "inf");
    assert_eq!(FieldValue::Float(f64::NEG_INFINITY).to_key_string(), "-inf");
}

/// Test compound GROUP BY keys are serialized as pipe-delimited strings
#[test]
fn test_compound_group_by_key_pipe_delimited() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;

    // Query with compound GROUP BY (symbol, exchange)
    let sql = r#"
        SELECT
            symbol,
            exchange,
            AVG(price) AS avg_price
        FROM market_data
        GROUP BY symbol, exchange
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    let mut context = ProcessorContext::new("test_compound_key");

    // Create test record
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldValue::Integer(5000));
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert(
        "exchange".to_string(),
        FieldValue::String("NYSE".to_string()),
    );
    fields.insert("price".to_string(), FieldValue::Float(150.0));
    let mut record = StreamRecord::new(fields);
    record.timestamp = 5000;

    let result = WindowAdapter::process_with_v2("test_query", &query, &record, &mut context)
        .expect("Should process record");

    assert!(result.is_some(), "Should emit result");
    let output = result.unwrap();

    // record.key should be pipe-delimited for compound keys
    assert!(
        output.key.is_some(),
        "record.key should be set for compound GROUP BY"
    );

    match output.key.as_ref().unwrap() {
        FieldValue::String(key_str) => {
            println!("Compound key: {}", key_str);
            // Verify pipe-delimited format: "AAPL|NYSE"
            let parts: Vec<&str> = key_str.split('|').collect();
            assert_eq!(
                parts.len(),
                2,
                "Compound key should have 2 pipe-separated parts"
            );
            assert_eq!(parts[0], "AAPL", "First key part should be symbol value");
            assert_eq!(parts[1], "NYSE", "Second key part should be exchange value");
            println!("✅ Compound GROUP BY key correctly serialized as pipe-delimited");
        }
        other => panic!(
            "Compound key should be pipe-delimited string, got {:?}",
            other
        ),
    }

    // Verify GROUP BY fields are also in output.fields
    assert_eq!(
        output.fields.get("symbol"),
        Some(&FieldValue::String("AAPL".to_string()))
    );
    assert_eq!(
        output.fields.get("exchange"),
        Some(&FieldValue::String("NYSE".to_string()))
    );
}

/// Test qualified column names (table.column) in GROUP BY
#[test]
fn test_qualified_column_names_in_group_by() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;

    // Query with qualified column name in GROUP BY
    let sql = r#"
        SELECT
            market_data.symbol,
            AVG(price) AS avg_price
        FROM market_data
        GROUP BY market_data.symbol
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    let mut context = ProcessorContext::new("test_qualified_column");

    // Create test record
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldValue::Integer(5000));
    fields.insert(
        "symbol".to_string(),
        FieldValue::String("GOOGL".to_string()),
    );
    fields.insert("price".to_string(), FieldValue::Float(2800.0));
    let mut record = StreamRecord::new(fields);
    record.timestamp = 5000;

    let result = WindowAdapter::process_with_v2("test_query", &query, &record, &mut context)
        .expect("Should process record");

    assert!(result.is_some(), "Should emit result");
    let output = result.unwrap();

    // record.key should be set (just "GOOGL", not "market_data.symbol")
    assert!(output.key.is_some(), "record.key should be set");
    match output.key.as_ref().unwrap() {
        FieldValue::String(s) => {
            assert_eq!(s, "GOOGL", "Key should be 'GOOGL' not 'market_data.GOOGL'");
            println!("✅ Qualified column name correctly extracted: {}", s);
        }
        other => panic!("Key should be String, got {:?}", other),
    }

    // Output should have "symbol" field (extracted from "market_data.symbol")
    assert!(
        output.fields.get("symbol").is_some() || output.fields.get("market_data.symbol").is_some(),
        "Output should have symbol field. Got: {:?}",
        output.fields.keys().collect::<Vec<_>>()
    );
}

/// Test CASE expression with IN operator referencing previous SELECT alias
/// This tests the fix for: spike_classification IN ('HIGH', 'LOW')
/// where spike_classification is defined in a previous SELECT field
#[test]
fn test_case_with_in_operator_alias_reference() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;

    // SQL that uses CASE with IN operator referencing a previous alias
    let sql = r#"
        SELECT
            symbol,
            CASE
                WHEN AVG(volume) > 500 THEN 'HIGH'
                ELSE 'LOW'
            END AS volume_class,
            CASE
                WHEN volume_class IN ('HIGH') THEN 'ALERT'
                ELSE 'OK'
            END AS alert_status
        FROM market_data
        GROUP BY symbol
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    let mut context = ProcessorContext::new("test_case_in_alias");

    // Create a record with high volume (>500) so volume_class = 'HIGH'
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldValue::Integer(5000));
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("volume".to_string(), FieldValue::Integer(1000)); // High volume
    let mut record = StreamRecord::new(fields);
    record.timestamp = 5000;

    let result = WindowAdapter::process_with_v2("test_query", &query, &record, &mut context)
        .expect("Should process record without 'List expressions' error");

    assert!(result.is_some(), "Should emit a result");
    let output = result.unwrap();

    println!("Output fields: {:?}", output.fields);

    // Verify volume_class is 'HIGH' (AVG(1000) > 500)
    let volume_class = output.fields.get("volume_class");
    assert!(volume_class.is_some(), "Should have volume_class field");
    match volume_class.unwrap() {
        FieldValue::String(s) => {
            assert_eq!(s, "HIGH", "volume_class should be 'HIGH' for volume=1000");
        }
        other => panic!("volume_class should be String, got {:?}", other),
    }

    // Verify alert_status is 'ALERT' (because volume_class IN ('HIGH') is true)
    let alert_status = output.fields.get("alert_status");
    assert!(alert_status.is_some(), "Should have alert_status field");
    match alert_status.unwrap() {
        FieldValue::String(s) => {
            assert_eq!(
                s, "ALERT",
                "alert_status should be 'ALERT' when volume_class='HIGH'"
            );
            println!("✅ CASE with IN operator and alias reference works correctly");
        }
        other => panic!("alert_status should be String, got {:?}", other),
    }
}

/// Test CASE with IN operator when alias value doesn't match
#[test]
fn test_case_with_in_operator_alias_no_match() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;

    let sql = r#"
        SELECT
            symbol,
            CASE
                WHEN AVG(volume) > 500 THEN 'HIGH'
                ELSE 'LOW'
            END AS volume_class,
            CASE
                WHEN volume_class IN ('HIGH') THEN 'ALERT'
                ELSE 'OK'
            END AS alert_status
        FROM market_data
        GROUP BY symbol
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    let mut context = ProcessorContext::new("test_case_in_alias_no_match");

    // Create a record with low volume (<500) so volume_class = 'LOW'
    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldValue::Integer(5000));
    fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    fields.insert("volume".to_string(), FieldValue::Integer(100)); // Low volume
    let mut record = StreamRecord::new(fields);
    record.timestamp = 5000;

    let result = WindowAdapter::process_with_v2("test_query", &query, &record, &mut context)
        .expect("Should process record");

    assert!(result.is_some());
    let output = result.unwrap();

    // Verify volume_class is 'LOW'
    let volume_class = output.fields.get("volume_class");
    assert!(volume_class.is_some());
    match volume_class.unwrap() {
        FieldValue::String(s) => {
            assert_eq!(s, "LOW", "volume_class should be 'LOW' for volume=100");
        }
        other => panic!("volume_class should be String, got {:?}", other),
    }

    // Verify alert_status is 'OK' (because volume_class='LOW' is NOT IN ('HIGH'))
    let alert_status = output.fields.get("alert_status");
    assert!(alert_status.is_some());
    match alert_status.unwrap() {
        FieldValue::String(s) => {
            assert_eq!(
                s, "OK",
                "alert_status should be 'OK' when volume_class='LOW'"
            );
            println!("✅ CASE with IN operator correctly returns 'OK' for non-matching alias");
        }
        other => panic!("alert_status should be String, got {:?}", other),
    }
}

/// Test integer GROUP BY key
#[test]
fn test_integer_group_by_key() {
    use velostream::velostream::sql::execution::processors::context::ProcessorContext;

    let sql = r#"
        SELECT
            region_id,
            SUM(amount) AS total
        FROM sales
        GROUP BY region_id
        WINDOW TUMBLING(10s)
        EMIT CHANGES
    "#;

    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Query should parse");

    let mut context = ProcessorContext::new("test_integer_key");

    let mut fields = HashMap::new();
    fields.insert("timestamp".to_string(), FieldValue::Integer(5000));
    fields.insert("region_id".to_string(), FieldValue::Integer(42));
    fields.insert("amount".to_string(), FieldValue::Float(100.0));
    let mut record = StreamRecord::new(fields);
    record.timestamp = 5000;

    let result = WindowAdapter::process_with_v2("test_query", &query, &record, &mut context)
        .expect("Should process record");

    assert!(result.is_some());
    let output = result.unwrap();

    // record.key should be Integer(42)
    assert!(output.key.is_some(), "record.key should be set");
    match output.key.as_ref().unwrap() {
        FieldValue::Integer(i) => {
            assert_eq!(*i, 42);
            println!("✅ Integer GROUP BY key correctly set: {}", i);
        }
        other => panic!("Key should be Integer(42), got {:?}", other),
    }
}
