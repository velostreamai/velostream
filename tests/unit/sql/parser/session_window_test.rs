/*!
# SESSION Window Syntax Tests

Tests for SESSION window functionality and parsing with various syntax patterns.
Covers simple durations, INTERVAL syntax, and complex table alias patterns.
*/

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_simple_session_syntax() {
    let parser = StreamingSqlParser::new();

    // Test basic SESSION syntax as used in financial trading SQL
    let query = "SELECT COUNT(*) FROM orders WINDOW SESSION(4h)";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Simple SESSION(4h) should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Session {
                    gap,
                    time_column,
                    partition_by,
                } => {
                    assert_eq!(gap.as_secs(), 4 * 3600); // 4 hours in seconds
                    assert_eq!(time_column, None); // Simple syntax has no explicit time column
                    assert_eq!(partition_by.len(), 0); // No explicit partition key
                }
                _ => panic!("Expected Session window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_session_with_various_durations() {
    let parser = StreamingSqlParser::new();

    let test_cases = vec![
        ("30s", 30),
        ("5m", 5 * 60),
        ("2h", 2 * 3600),
        ("1d", 1 * 24 * 3600),
    ];

    for (duration_str, expected_seconds) in test_cases {
        let query = format!(
            "SELECT COUNT(*) FROM events WINDOW SESSION({})",
            duration_str
        );
        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "SESSION({}) should parse: {:?}",
            duration_str,
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { window, .. } => {
                let window = window.expect("Should have window specification");
                match window {
                    WindowSpec::Session { gap, .. } => {
                        assert_eq!(
                            gap.as_secs(),
                            expected_seconds,
                            "Duration {} should equal {} seconds",
                            duration_str,
                            expected_seconds
                        );
                    }
                    _ => panic!("Expected Session window spec"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}

#[test]
fn test_complex_session_syntax_with_table_aliases() {
    let parser = StreamingSqlParser::new();

    // Test complex SESSION syntax with table aliases and INTERVAL
    let query = r#"
        SELECT p.trader_id, COUNT(*) as trade_count
        FROM trading_positions p
        WINDOW SESSION (p.event_time, INTERVAL '4' HOUR, p.trader_id)
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Complex SESSION syntax should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Session {
                    gap,
                    time_column,
                    partition_by,
                } => {
                    assert_eq!(gap.as_secs(), 4 * 3600); // 4 hours
                    assert_eq!(time_column, Some("p.event_time".to_string()));
                    assert_eq!(partition_by, vec!["p.trader_id".to_string()]);
                }
                _ => panic!("Expected Session window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_session_with_interval_variations() {
    let parser = StreamingSqlParser::new();

    let interval_cases = vec![
        ("INTERVAL '30' SECOND", 30),
        ("INTERVAL '5' MINUTE", 5 * 60),
        ("INTERVAL '2' HOUR", 2 * 3600),
        ("INTERVAL '1' DAY", 1 * 24 * 3600),
        ("INTERVAL '15' MINUTES", 15 * 60),
        ("INTERVAL '3' HOURS", 3 * 3600),
    ];

    for (interval_str, expected_seconds) in interval_cases {
        let query = format!(
            "SELECT COUNT(*) FROM events e WINDOW SESSION (e.timestamp, {}, e.user_id)",
            interval_str
        );
        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "SESSION with {} should parse: {:?}",
            interval_str,
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { window, .. } => {
                let window = window.expect("Should have window specification");
                match window {
                    WindowSpec::Session {
                        gap,
                        time_column,
                        partition_by,
                    } => {
                        assert_eq!(gap.as_secs(), expected_seconds);
                        assert_eq!(time_column, Some("e.timestamp".to_string()));
                        assert_eq!(partition_by, vec!["e.user_id".to_string()]);
                    }
                    _ => panic!("Expected Session window spec"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}

#[test]
fn test_session_without_partition_key() {
    let parser = StreamingSqlParser::new();

    // Test SESSION with time column but no partition key
    let query = "SELECT COUNT(*) FROM events e WINDOW SESSION (e.event_time, INTERVAL '1' HOUR)";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "SESSION without partition key should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Session {
                    gap,
                    time_column,
                    partition_by,
                } => {
                    assert_eq!(gap.as_secs(), 3600); // 1 hour
                    assert_eq!(time_column, Some("e.event_time".to_string()));
                    assert_eq!(partition_by.len(), 0); // No partition key
                }
                _ => panic!("Expected Session window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_session_in_financial_context() {
    let parser = StreamingSqlParser::new();

    // Test the actual SESSION syntax pattern from financial trading demo
    let query = r#"
        SELECT
            p.trader_id,
            COUNT(*) as position_count,
            AVG(p.position_size) as avg_position
        FROM trading_positions_with_event_time p
        LEFT JOIN market_data_with_event_time m ON p.symbol = m.symbol
        WINDOW SESSION(4h)
        HAVING COUNT(*) > 5
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Financial SESSION query should parse: {:?}",
        result.err()
    );

    // Verify it's parsed as a Session window
    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            assert!(
                matches!(window, WindowSpec::Session { .. }),
                "Should be Session window"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_session_error_cases() {
    let parser = StreamingSqlParser::new();

    // Test invalid SESSION syntax patterns
    let invalid_cases = vec![
        "SELECT COUNT(*) FROM events WINDOW SESSION()", // Empty parameters
        "SELECT COUNT(*) FROM events WINDOW SESSION(,)", // Missing parameters
        "SELECT COUNT(*) FROM events WINDOW SESSION(INVALID_DURATION)", // Invalid duration
    ];

    for invalid_query in invalid_cases {
        let result = parser.parse(invalid_query);
        assert!(
            result.is_err(),
            "Invalid SESSION syntax should fail: {}",
            invalid_query
        );
    }
}
