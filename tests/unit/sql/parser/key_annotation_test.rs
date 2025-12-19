/*!
# FR-089: Inline KEY Annotation Tests

Tests for the ksqlDB-style inline KEY annotation syntax that allows marking
fields as Kafka message keys directly in SQL.

## Syntax
```sql
SELECT symbol KEY, price FROM trades
SELECT region KEY, product KEY, SUM(qty) FROM orders GROUP BY region, product
```
*/

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Test single field KEY annotation
#[test]
fn test_single_key_annotation() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT symbol KEY, price FROM trades");

    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields,
            key_fields,
            from,
            ..
        } => {
            // Should have 2 fields
            assert_eq!(fields.len(), 2);

            // First field is symbol (KEY)
            if let SelectField::Expression { expr, alias } = &fields[0] {
                assert!(matches!(expr, Expr::Column(name) if name == "symbol"));
                assert!(alias.is_none());
            } else {
                panic!("Expected Expression field");
            }

            // Second field is price (no KEY)
            if let SelectField::Expression { expr, .. } = &fields[1] {
                assert!(matches!(expr, Expr::Column(name) if name == "price"));
            }

            // key_fields should contain "symbol"
            assert!(key_fields.is_some(), "key_fields should be Some");
            let keys = key_fields.unwrap();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], "symbol");

            // From should be trades
            assert!(matches!(from, StreamSource::Stream(name) if name == "trades"));
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test compound KEY annotation (multiple fields)
#[test]
fn test_compound_key_annotation() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse(
        "SELECT region KEY, product KEY, SUM(qty) as total FROM orders GROUP BY region, product",
    );

    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields,
            key_fields,
            group_by,
            ..
        } => {
            // Should have 3 fields
            assert_eq!(fields.len(), 3);

            // key_fields should contain both "region" and "product"
            assert!(key_fields.is_some(), "key_fields should be Some");
            let keys = key_fields.unwrap();
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&"region".to_string()));
            assert!(keys.contains(&"product".to_string()));

            // GROUP BY should exist
            assert!(group_by.is_some());
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test KEY annotation with alias - alias name should be used
#[test]
fn test_key_annotation_with_alias() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT stock_symbol as symbol KEY, price FROM trades");

    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields, key_fields, ..
        } => {
            assert_eq!(fields.len(), 2);

            // First field should have alias "symbol"
            if let SelectField::Expression { alias, .. } = &fields[0] {
                assert_eq!(alias.as_ref().unwrap(), "symbol");
            }

            // key_fields should contain the alias "symbol", not the original "stock_symbol"
            assert!(key_fields.is_some());
            let keys = key_fields.unwrap();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], "symbol");
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test KEY annotation in CREATE STREAM AS SELECT
#[test]
fn test_key_annotation_in_create_stream() {
    let parser = StreamingSqlParser::new();
    let sql = r#"
        CREATE STREAM flagged_symbols AS
        SELECT symbol KEY, trade_count, total_volume
        FROM trades_aggregated
        WITH ('sink.topic' = 'flagged-output')
    "#;

    let result = parser.parse(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let query = result.unwrap();

    match query {
        StreamingQuery::CreateStream {
            name, as_select, ..
        } => {
            assert_eq!(name, "flagged_symbols");

            // Extract key_fields from the inner SELECT
            match as_select.as_ref() {
                StreamingQuery::Select { key_fields, .. } => {
                    assert!(key_fields.is_some());
                    let keys = key_fields.as_ref().unwrap();
                    assert_eq!(keys.len(), 1);
                    assert_eq!(keys[0], "symbol");
                }
                _ => panic!("Expected Select query inside CreateStream"),
            }
        }
        _ => panic!("Expected CreateStream query"),
    }
}

/// Test query without KEY annotation should have None key_fields
#[test]
fn test_no_key_annotation() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT symbol, price FROM trades");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { key_fields, .. } => {
            assert!(
                key_fields.is_none(),
                "key_fields should be None when no KEY annotation"
            );
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test KEY annotation with expression (should use alias or generate name)
#[test]
fn test_key_annotation_with_expression_aliased() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT UPPER(symbol) as symbol_upper KEY, price FROM trades");

    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { key_fields, .. } => {
            assert!(key_fields.is_some());
            let keys = key_fields.unwrap();
            assert_eq!(keys.len(), 1);
            // Should use the alias name
            assert_eq!(keys[0], "symbol_upper");
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test KEY annotation case insensitivity
#[test]
fn test_key_annotation_case_insensitive() {
    let parser = StreamingSqlParser::new();

    // Test lowercase
    let result = parser.parse("SELECT symbol key, price FROM trades");
    assert!(result.is_ok(), "Failed to parse lowercase KEY");
    if let StreamingQuery::Select { key_fields, .. } = result.unwrap() {
        assert!(key_fields.is_some());
    }

    // Test uppercase
    let result = parser.parse("SELECT symbol KEY, price FROM trades");
    assert!(result.is_ok(), "Failed to parse uppercase KEY");
    if let StreamingQuery::Select { key_fields, .. } = result.unwrap() {
        assert!(key_fields.is_some());
    }

    // Test mixed case
    let result = parser.parse("SELECT symbol Key, price FROM trades");
    assert!(result.is_ok(), "Failed to parse mixed case KEY");
    if let StreamingQuery::Select { key_fields, .. } = result.unwrap() {
        assert!(key_fields.is_some());
    }
}

/// Test KEY annotation with GROUP BY and WINDOW
#[test]
fn test_key_annotation_with_group_by_and_window() {
    let parser = StreamingSqlParser::new();
    let sql = r#"
        SELECT symbol KEY, COUNT(*) as trade_count
        FROM trades
        GROUP BY symbol
        WINDOW TUMBLING(5m)
        EMIT CHANGES
    "#;

    let result = parser.parse(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        StreamingQuery::Select {
            key_fields,
            group_by,
            window,
            emit_mode,
            ..
        } => {
            // KEY annotation should be captured
            assert!(key_fields.is_some());
            assert_eq!(key_fields.unwrap()[0], "symbol");

            // GROUP BY should exist
            assert!(group_by.is_some());

            // WINDOW should exist
            assert!(window.is_some());

            // EMIT should exist
            assert!(emit_mode.is_some());
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test Display implementation outputs KEY annotation
#[test]
fn test_key_annotation_display() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT symbol KEY, price FROM trades");

    assert!(result.is_ok());
    let query = result.unwrap();

    let display = format!("{}", query);

    // Should contain "KEY" in output for the symbol field
    assert!(
        display.contains("symbol KEY") || display.contains("KEY"),
        "Display output should contain KEY annotation: {}",
        display
    );
}

/// Test KEY annotation order is preserved
#[test]
fn test_key_annotation_order_preserved() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT region KEY, product KEY, qty FROM orders");

    assert!(result.is_ok());

    match result.unwrap() {
        StreamingQuery::Select { key_fields, .. } => {
            let keys = key_fields.unwrap();
            // Order should be preserved: region, product
            assert_eq!(keys[0], "region");
            assert_eq!(keys[1], "product");
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test KEY annotation with qualified column names
#[test]
fn test_key_annotation_qualified_column() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT t.symbol KEY, t.price FROM trades t");

    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        StreamingQuery::Select { key_fields, .. } => {
            assert!(key_fields.is_some());
            let keys = key_fields.unwrap();
            assert_eq!(keys.len(), 1);
            // Should extract "t.symbol" as the key field
            assert_eq!(keys[0], "t.symbol");
        }
        _ => panic!("Expected Select query"),
    }
}

/// Test that KEY can appear after AS alias
#[test]
fn test_key_after_as_alias() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT stock_symbol AS sym KEY, price FROM trades");

    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        StreamingQuery::Select {
            fields, key_fields, ..
        } => {
            // First field should have alias
            if let SelectField::Expression { alias, .. } = &fields[0] {
                assert_eq!(alias.as_ref().unwrap(), "sym");
            }

            // key_fields should use the alias
            assert!(key_fields.is_some());
            assert_eq!(key_fields.unwrap()[0], "sym");
        }
        _ => panic!("Expected Select query"),
    }
}
