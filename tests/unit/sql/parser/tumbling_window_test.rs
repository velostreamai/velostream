/*!
# TUMBLING Window Syntax Tests

Tests for TUMBLING window functionality and parsing with various syntax patterns.
Covers simple durations, INTERVAL syntax, and complex table alias patterns.
*/

use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_simple_tumbling_syntax() {
    let parser = StreamingSqlParser::new();

    // Test basic TUMBLING syntax
    let query = "SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Simple TUMBLING(5m) should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Tumbling { size, time_column } => {
                    assert_eq!(size.as_secs(), 5 * 60); // 5 minutes in seconds
                    assert_eq!(time_column, None); // Simple syntax has no explicit time column
                }
                _ => panic!("Expected Tumbling window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_tumbling_with_various_durations() {
    let parser = StreamingSqlParser::new();

    let test_cases = vec![
        ("30s", 30),
        ("5m", 5 * 60),
        ("2h", 2 * 3600),
        ("1d", 1 * 24 * 3600),
    ];

    for (duration_str, expected_seconds) in test_cases {
        let query = format!(
            "SELECT COUNT(*) FROM events WINDOW TUMBLING({})",
            duration_str
        );
        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "TUMBLING({}) should parse: {:?}",
            duration_str,
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { window, .. } => {
                let window = window.expect("Should have window specification");
                match window {
                    WindowSpec::Tumbling { size, .. } => {
                        assert_eq!(
                            size.as_secs(),
                            expected_seconds,
                            "Duration {} should equal {} seconds",
                            duration_str,
                            expected_seconds
                        );
                    }
                    _ => panic!("Expected Tumbling window spec"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}

#[test]
fn test_complex_tumbling_syntax_with_time_column() {
    let parser = StreamingSqlParser::new();

    // Test complex TUMBLING syntax with time column and INTERVAL
    let query = r#"
        SELECT AVG(price) as avg_price
        FROM market_data_et
        WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE)
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Complex TUMBLING syntax should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Tumbling { size, time_column } => {
                    assert_eq!(size.as_secs(), 60); // 1 minute
                    assert_eq!(time_column, Some("event_time".to_string()));
                }
                _ => panic!("Expected Tumbling window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_tumbling_with_interval_variations() {
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
            "SELECT COUNT(*) FROM events WINDOW TUMBLING (timestamp, {})",
            interval_str
        );
        let result = parser.parse(&query);
        assert!(
            result.is_ok(),
            "TUMBLING with {} should parse: {:?}",
            interval_str,
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { window, .. } => {
                let window = window.expect("Should have window specification");
                match window {
                    WindowSpec::Tumbling { size, time_column } => {
                        assert_eq!(size.as_secs(), expected_seconds);
                        assert_eq!(time_column, Some("timestamp".to_string()));
                    }
                    _ => panic!("Expected Tumbling window spec"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}

#[test]
fn test_tumbling_with_table_aliases() {
    let parser = StreamingSqlParser::new();

    // Test TUMBLING with table alias in time column
    let query = "SELECT COUNT(*) FROM events e WINDOW TUMBLING (e.event_time, INTERVAL '1' HOUR)";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "TUMBLING with table alias should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Tumbling { size, time_column } => {
                    assert_eq!(size.as_secs(), 3600); // 1 hour
                    assert_eq!(time_column, Some("e.event_time".to_string()));
                }
                _ => panic!("Expected Tumbling window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_tumbling_without_interval_keyword() {
    let parser = StreamingSqlParser::new();

    // Test TUMBLING with time column but simple duration (no INTERVAL keyword)
    let query = "SELECT COUNT(*) FROM events WINDOW TUMBLING(event_time, 5m)";
    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "TUMBLING without INTERVAL keyword should parse: {:?}",
        result.err()
    );

    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            match window {
                WindowSpec::Tumbling { size, time_column } => {
                    assert_eq!(size.as_secs(), 5 * 60); // 5 minutes
                    assert_eq!(time_column, Some("event_time".to_string()));
                }
                _ => panic!("Expected Tumbling window spec"),
            }
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_tumbling_in_financial_context() {
    let parser = StreamingSqlParser::new();

    // Test the actual TUMBLING syntax pattern from financial trading demo
    let query = r#"
        SELECT
            symbol,
            AVG(price) as avg_price,
            STDDEV(price) as price_volatility
        FROM market_data_et
        WINDOW TUMBLING (event_time, INTERVAL '1' MINUTE)
        HAVING COUNT(*) > 5
    "#;

    let result = parser.parse(query);
    assert!(
        result.is_ok(),
        "Financial TUMBLING query should parse: {:?}",
        result.err()
    );

    // Verify it's parsed as a Tumbling window
    match result.unwrap() {
        StreamingQuery::Select { window, .. } => {
            let window = window.expect("Should have window specification");
            assert!(
                matches!(window, WindowSpec::Tumbling { .. }),
                "Should be Tumbling window"
            );
        }
        _ => panic!("Expected SELECT query"),
    }
}

#[test]
fn test_tumbling_error_cases() {
    let parser = StreamingSqlParser::new();

    // Test invalid TUMBLING syntax patterns
    let invalid_cases = vec![
        "SELECT COUNT(*) FROM events WINDOW TUMBLING()", // Empty parameters
        "SELECT COUNT(*) FROM events WINDOW TUMBLING(,)", // Missing parameters
    ];

    for invalid_query in invalid_cases {
        let result = parser.parse(invalid_query);
        assert!(
            result.is_err(),
            "Invalid TUMBLING syntax should fail: {}",
            invalid_query
        );
    }
}

#[test]
fn test_tumbling_backward_compatibility() {
    let parser = StreamingSqlParser::new();

    // Ensure all existing simple TUMBLING syntax still works
    let existing_cases = vec![
        "SELECT COUNT(*) FROM events WINDOW TUMBLING(1s)",
        "SELECT COUNT(*) FROM events WINDOW TUMBLING(30s)",
        "SELECT COUNT(*) FROM events WINDOW TUMBLING(5m)",
        "SELECT COUNT(*) FROM events WINDOW TUMBLING(1h)",
        "SELECT COUNT(*) FROM events WINDOW TUMBLING(1d)",
    ];

    for query in existing_cases {
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Backward compatibility: {} should still work: {:?}",
            query,
            result.err()
        );

        // Verify it's still parsed as Tumbling with time_column = None
        match result.unwrap() {
            StreamingQuery::Select { window, .. } => {
                let window = window.expect("Should have window specification");
                match window {
                    WindowSpec::Tumbling { time_column, .. } => {
                        assert_eq!(
                            time_column, None,
                            "Simple syntax should have no time column"
                        );
                    }
                    _ => panic!("Expected Tumbling window spec"),
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }
}
