//! Tests for SELECT DISTINCT parsing

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_select_distinct_simple() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT DISTINCT value FROM input_stream");

    assert!(
        result.is_ok(),
        "Failed to parse SELECT DISTINCT: {:?}",
        result.err()
    );
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields,
            distinct,
            from,
            ..
        } => {
            assert!(distinct, "Expected distinct to be true");
            assert_eq!(fields.len(), 1);
            assert!(matches!(from, StreamSource::Stream(name) if name == "input_stream"));
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_distinct_multiple_columns() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT DISTINCT value, active FROM input_stream");

    assert!(
        result.is_ok(),
        "Failed to parse SELECT DISTINCT with multiple columns: {:?}",
        result.err()
    );
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields, distinct, ..
        } => {
            assert!(distinct, "Expected distinct to be true");
            assert_eq!(fields.len(), 2);
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_distinct_with_where() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT DISTINCT category, status FROM orders WHERE amount > 100");

    assert!(
        result.is_ok(),
        "Failed to parse SELECT DISTINCT with WHERE: {:?}",
        result.err()
    );
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields,
            distinct,
            where_clause,
            ..
        } => {
            assert!(distinct, "Expected distinct to be true");
            assert_eq!(fields.len(), 2);
            assert!(where_clause.is_some(), "Expected WHERE clause");
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_without_distinct() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT value FROM input_stream");

    assert!(result.is_ok());
    let query = result.unwrap();

    match query {
        StreamingQuery::Select { distinct, .. } => {
            assert!(
                !distinct,
                "Expected distinct to be false for regular SELECT"
            );
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_distinct_with_alias() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT DISTINCT category AS cat, status AS st FROM orders");

    assert!(
        result.is_ok(),
        "Failed to parse SELECT DISTINCT with aliases: {:?}",
        result.err()
    );
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields, distinct, ..
        } => {
            assert!(distinct, "Expected distinct to be true");
            assert_eq!(fields.len(), 2);
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_distinct_wildcard() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT DISTINCT * FROM input_stream");

    assert!(
        result.is_ok(),
        "Failed to parse SELECT DISTINCT *: {:?}",
        result.err()
    );
    let query = result.unwrap();

    match query {
        StreamingQuery::Select {
            fields, distinct, ..
        } => {
            assert!(distinct, "Expected distinct to be true");
            assert_eq!(fields.len(), 1);
            assert!(matches!(fields[0], SelectField::Wildcard));
        }
        _ => panic!("Expected Select query"),
    }
}

#[test]
fn test_select_distinct_display() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT DISTINCT value FROM input_stream");

    assert!(result.is_ok());
    let query = result.unwrap();

    // Check that Display outputs "SELECT DISTINCT"
    let display_output = format!("{}", query);
    assert!(
        display_output.starts_with("SELECT DISTINCT"),
        "Display should output 'SELECT DISTINCT', got: {}",
        display_output
    );
}

#[test]
fn test_select_non_distinct_display() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT value FROM input_stream");

    assert!(result.is_ok());
    let query = result.unwrap();

    // Check that Display outputs "SELECT " (without DISTINCT)
    let display_output = format!("{}", query);
    assert!(
        display_output.starts_with("SELECT ") && !display_output.starts_with("SELECT DISTINCT"),
        "Display should output 'SELECT ' without DISTINCT, got: {}",
        display_output
    );
}
