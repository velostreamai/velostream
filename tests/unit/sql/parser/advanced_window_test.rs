/*!
# Tests for Advanced Window Function Features

Tests the new SQL parser enhancements:
- Table aliases in PARTITION BY and ORDER BY clauses
- INTERVAL syntax in window frames (RANGE BETWEEN INTERVAL '1' DAY PRECEDING)
- Both EXTRACT syntaxes (function call and SQL standard)
*/

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_table_alias_in_partition_by() {
    let parser = StreamingSqlParser::new();

    // Test table alias in PARTITION BY
    let query =
        "SELECT LAG(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as prev_price
                 FROM market_data m
                 JOIN positions p ON m.symbol = p.symbol";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse table alias in PARTITION BY: {:?}",
        result.err()
    );

    // Verify the AST structure includes table aliases
    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::WindowFunction { over_clause, .. } => {
                            assert!(over_clause
                                .partition_by
                                .contains(&"p.trader_id".to_string()));
                            // Verify ORDER BY also has table alias
                            assert_eq!(over_clause.order_by.len(), 1);
                        }
                        _ => panic!("Expected WindowFunction expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_multiple_table_aliases_in_window() {
    let parser = StreamingSqlParser::new();

    let query = "SELECT RANK() OVER (PARTITION BY p.trader_id, m.symbol ORDER BY m.volume DESC) as volume_rank
                 FROM market_data m
                 JOIN positions p ON m.symbol = p.symbol";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse multiple table aliases: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    assert_eq!(over_clause.partition_by.len(), 2);
                    assert!(over_clause
                        .partition_by
                        .contains(&"p.trader_id".to_string()));
                    assert!(over_clause.partition_by.contains(&"m.symbol".to_string()));
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_interval_in_range_between() {
    let parser = StreamingSqlParser::new();

    // Test INTERVAL syntax in RANGE BETWEEN
    let query = "SELECT AVG(price) OVER (
                    ORDER BY event_time
                    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
                 ) as hourly_avg
                 FROM trades";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL in RANGE BETWEEN: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            match &fields[0] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::WindowFunction { over_clause, .. } => {
                            let window_frame = over_clause.window_frame.as_ref().unwrap();
                            assert_eq!(window_frame.frame_type, FrameType::Range);
                            // Check for IntervalPreceding variant
                            match &window_frame.start_bound {
                                FrameBound::IntervalPreceding { value, unit } => {
                                    assert_eq!(*value, 1);
                                    assert_eq!(*unit, TimeUnit::Hour);
                                }
                                _ => panic!(
                                    "Expected IntervalPreceding, got {:?}",
                                    window_frame.start_bound
                                ),
                            }
                            assert_eq!(
                                window_frame.end_bound.as_ref().unwrap(),
                                &FrameBound::CurrentRow
                            );
                        }
                        _ => panic!("Expected WindowFunction expression"),
                    }
                }
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_interval_day_preceding() {
    let parser = StreamingSqlParser::new();

    let query = "SELECT STDDEV(price) OVER (
                    ORDER BY event_time
                    RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
                 ) as daily_volatility
                 FROM market_data";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL DAY: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    match &window_frame.start_bound {
                        FrameBound::IntervalPreceding { value, unit } => {
                            assert_eq!(*value, 1);
                            assert_eq!(*unit, TimeUnit::Day);
                        }
                        _ => panic!("Expected IntervalPreceding with Day unit"),
                    }
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_interval_minutes_preceding() {
    let parser = StreamingSqlParser::new();

    let query = "SELECT COUNT(*) OVER (
                    ORDER BY timestamp
                    RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
                 ) as count_15min
                 FROM events";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse INTERVAL MINUTE: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => match &fields[0] {
            SelectField::Expression { expr, .. } => match expr {
                Expr::WindowFunction { over_clause, .. } => {
                    let window_frame = over_clause.window_frame.as_ref().unwrap();
                    match &window_frame.start_bound {
                        FrameBound::IntervalPreceding { value, unit } => {
                            assert_eq!(*value, 15);
                            assert_eq!(*unit, TimeUnit::Minute);
                        }
                        _ => panic!("Expected IntervalPreceding with Minute unit"),
                    }
                }
                _ => panic!("Expected WindowFunction expression"),
            },
            _ => panic!("Expected expression field"),
        },
        _ => panic!("Expected SELECT query"),
    }
}

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

// Note: Complex financial SQL test removed to focus on individual feature testing
// The comprehensive verification is done through the SQL validator and financial demo

#[test]
fn test_combined_new_features() {
    let parser = StreamingSqlParser::new();

    // Test all three new features in one query
    let query = r#"
        SELECT
            p.trader_id,
            LAG(m.price) OVER (
                PARTITION BY p.trader_id
                ORDER BY m.event_time
                RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
            ) as prev_price,
            EXTRACT(HOUR FROM m.event_time) as trade_hour
        FROM market_data m
        JOIN positions p ON m.symbol = p.symbol
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Failed to parse combined new features: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { fields, .. } => {
            assert_eq!(fields.len(), 3);

            // Check field 1: table alias
            match &fields[0] {
                SelectField::Expression { expr, .. } => match expr {
                    Expr::Column(col) => assert_eq!(col, "p.trader_id"),
                    _ => panic!("Expected column with table alias"),
                },
                _ => panic!("Expected expression field"),
            }

            // Check field 2: window function with table alias and INTERVAL
            match &fields[1] {
                SelectField::Expression { expr, .. } => {
                    match expr {
                        Expr::WindowFunction { over_clause, .. } => {
                            // Has table alias in PARTITION BY
                            assert!(over_clause
                                .partition_by
                                .contains(&"p.trader_id".to_string()));
                            // Has window frame with INTERVAL
                            assert!(over_clause.window_frame.is_some());
                        }
                        _ => panic!("Expected WindowFunction"),
                    }
                }
                _ => panic!("Expected expression field"),
            }

            // Check field 3: EXTRACT function
            match &fields[2] {
                SelectField::Expression { expr, .. } => match expr {
                    Expr::Function { name, .. } => {
                        assert_eq!(name, "EXTRACT");
                    }
                    _ => panic!("Expected EXTRACT function"),
                },
                _ => panic!("Expected expression field"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_various_interval_units() {
    let parser = StreamingSqlParser::new();

    let test_cases = vec![
        ("INTERVAL '1' SECOND", 1, TimeUnit::Second),
        ("INTERVAL '30' SECONDS", 30, TimeUnit::Second),
        ("INTERVAL '5' MINUTE", 5, TimeUnit::Minute),
        ("INTERVAL '10' MINUTES", 10, TimeUnit::Minute),
        ("INTERVAL '2' HOUR", 2, TimeUnit::Hour),
        ("INTERVAL '3' HOURS", 3, TimeUnit::Hour),
        ("INTERVAL '7' DAY", 7, TimeUnit::Day),
        ("INTERVAL '14' DAYS", 14, TimeUnit::Day),
    ];

    for (interval_str, expected_value, expected_unit) in test_cases {
        let query = format!(
            "SELECT AVG(price) OVER (
                ORDER BY event_time
                RANGE BETWEEN {} PRECEDING AND CURRENT ROW
             ) as avg FROM trades",
            interval_str
        );

        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "Failed to parse {}: {:?}",
            interval_str,
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => match &fields[0] {
                SelectField::Expression { expr, .. } => match expr {
                    Expr::WindowFunction { over_clause, .. } => {
                        let window_frame = over_clause.window_frame.as_ref().unwrap();
                        match &window_frame.start_bound {
                            FrameBound::IntervalPreceding { value, unit } => {
                                assert_eq!(
                                    *value, expected_value,
                                    "Value mismatch for {}",
                                    interval_str
                                );
                                assert_eq!(
                                    *unit, expected_unit,
                                    "Unit mismatch for {}",
                                    interval_str
                                );
                            }
                            _ => panic!("Expected IntervalPreceding for {}", interval_str),
                        }
                    }
                    _ => panic!("Expected WindowFunction"),
                },
                _ => panic!("Expected expression field"),
            },
            _ => panic!("Expected SELECT query"),
        }
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
