/*!
# Tests for EXTRACT Function Features

Tests the EXTRACT function with both syntaxes:
- Function call syntax: EXTRACT('YEAR', date_column)
- SQL standard syntax: EXTRACT(YEAR FROM date_column)
*/

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_extract_function_call_syntax() {
    let parser = StreamingSqlParser::new();

    // Test old function call syntax
    let query = "SELECT EXTRACT('YEAR', order_date) as order_year FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse EXTRACT function call syntax: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::Function { name, args } => {
                            assert_eq!(name, "EXTRACT");
                            assert_eq!(args.len(), 2);
                            // First arg should be a string literal
                            match &args[0] {
                                Expr::Literal(LiteralValue::String(s)) => {
                                    assert_eq!(s, "YEAR");
                                }
                                _ => panic!("Expected string literal for EXTRACT part"),
                            }
                        }
                        _ => panic!("Expected Function expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_extract_sql_standard_syntax() {
    let parser = StreamingSqlParser::new();

    // Test SQL standard syntax
    let query = "SELECT EXTRACT(YEAR FROM order_date) as order_year FROM orders";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse EXTRACT SQL standard syntax: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::Function { name, args } => {
                            assert_eq!(name, "EXTRACT");
                            assert_eq!(args.len(), 2);
                            // First arg should be a string literal (converted from identifier)
                            match &args[0] {
                                Expr::Literal(LiteralValue::String(s)) => {
                                    assert_eq!(s, "YEAR");
                                }
                                _ => panic!("Expected string literal for EXTRACT part"),
                            }
                        }
                        _ => panic!("Expected Function expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_extract_epoch_from_expression() {
    let parser = StreamingSqlParser::new();

    // Test EXTRACT with expression
    let query =
        "SELECT EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds FROM events";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse EXTRACT with expression: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::Function { name, args } => {
                            assert_eq!(name, "EXTRACT");
                            assert_eq!(args.len(), 2);
                            // First arg should be EPOCH
                            match &args[0] {
                                Expr::Literal(LiteralValue::String(s)) => {
                                    assert_eq!(s, "EPOCH");
                                }
                                _ => panic!("Expected EPOCH literal"),
                            }
                            // Second arg should be a binary expression
                            match &args[1] {
                                Expr::BinaryOp { .. } => {
                                    // Successfully parsed the expression
                                }
                                _ => panic!("Expected binary expression for time difference"),
                            }
                        }
                        _ => panic!("Expected Function expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_extract_various_parts() {
    let parser = StreamingSqlParser::new();

    let extract_parts = vec![
        "YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DOW", "DOY", "WEEK", "QUARTER",
        "EPOCH",
    ];

    for part in extract_parts {
        // Test SQL standard syntax
        let query = format!(
            "SELECT EXTRACT({} FROM timestamp_col) as extracted FROM events",
            part
        );
        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "Failed to parse EXTRACT({} FROM ...): {:?}",
            part,
            result.err()
        );

        // Test function call syntax
        let query = format!(
            "SELECT EXTRACT('{}', timestamp_col) as extracted FROM events",
            part
        );
        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "Failed to parse EXTRACT('{}', ...): {:?}",
            part,
            result.err()
        );
    }
}
