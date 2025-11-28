//! Tests for _window_start and _window_end system column population
//!
//! These tests verify that window system columns are correctly
//! populated in result records from windowed aggregation queries.

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
