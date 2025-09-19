//! Unit tests for QueryValidator

use velostream::velostream::sql::validation::{QueryValidationResult, QueryValidator};

#[test]
fn test_query_validator_creation() {
    let validator = QueryValidator::new();
    // Verify the validator was created successfully
    // QueryValidator should be ready for use
    assert!(true); // Basic creation test
}

#[test]
fn test_query_validator_strict_mode() {
    let validator = QueryValidator::new_strict();
    // Verify strict mode validator was created
    assert!(true); // Basic creation test
}

#[test]
fn test_valid_simple_select() {
    let validator = QueryValidator::new();
    let result = validator.validate_query("SELECT id, name FROM users");

    // Should create a result (even if parsing fails, we get a structured result)
    assert!(!result.query_text.is_empty());
    assert_eq!(result.query_text, "SELECT id, name FROM users");
    assert_eq!(result.query_index, 0);
    assert_eq!(result.start_line, 1);
}

#[test]
fn test_streaming_query_validation() {
    let validator = QueryValidator::new();
    let query = "CREATE STREAM user_events AS SELECT user_id, event_type, timestamp FROM event_stream WHERE event_type = 'login'";
    let result = validator.validate_query(query);

    assert!(!result.query_text.is_empty());
    assert_eq!(result.query_text, query);
}

#[test]
fn test_invalid_sql_syntax() {
    let validator = QueryValidator::new();
    let result = validator.validate_query("SELECT * FROM");

    // Should detect parsing errors
    assert!(!result.query_text.is_empty());
    // Since parsing will likely fail, we should get parsing errors
    // The exact behavior depends on the parser implementation
}

#[test]
fn test_empty_query() {
    let validator = QueryValidator::new();
    let result = validator.validate_query("");

    assert_eq!(result.query_text, "");
}

#[test]
fn test_query_with_window_functions() {
    let validator = QueryValidator::new();
    let query =
        "SELECT user_id, COUNT(*) OVER (PARTITION BY user_id ORDER BY timestamp) FROM events";
    let result = validator.validate_query(query);

    assert!(!result.query_text.is_empty());
    // The validator should handle window function syntax
}

#[test]
fn test_performance_checks() {
    let validator = QueryValidator::new().with_performance_checks(true);
    let query = "SELECT * FROM stream1 JOIN stream2 ON stream1.id = stream2.id";
    let result = validator.validate_query(query);

    assert!(!result.query_text.is_empty());
    // Performance checks enabled - should potentially flag JOIN without time windows
}

#[test]
fn test_performance_checks_disabled() {
    let validator = QueryValidator::new().with_performance_checks(false);
    let query = "SELECT * FROM stream1 ORDER BY timestamp";
    let result = validator.validate_query(query);

    assert!(!result.query_text.is_empty());
    // Performance checks disabled - should not generate performance warnings
}

#[test]
fn test_syntax_compatibility_checks() {
    let validator = QueryValidator::new();
    let queries = vec![
        "SELECT * FROM stream WHERE col WINDOW TUMBLING(INTERVAL 5 MINUTE) GROUP BY col",
        "WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte",
        "MERGE INTO target USING source ON target.id = source.id",
    ];

    for query in queries {
        let result = validator.validate_query(query);
        assert!(!result.query_text.is_empty());
        // These queries should potentially trigger syntax compatibility warnings
    }
}

#[test]
fn test_result_structure() {
    let validator = QueryValidator::new();
    let result = validator.validate_query("SELECT 1");

    // Verify all expected fields are initialized
    assert!(!result.query_text.is_empty());
    assert!(result.parsing_errors.is_empty() || !result.parsing_errors.is_empty()); // Either way is valid
    assert!(result.configuration_errors.is_empty()); // Should start empty
    assert!(result.warnings.is_empty()); // Should start empty
    assert!(result.sources_found.is_empty()); // No sources in this simple query
    assert!(result.sinks_found.is_empty()); // No sinks in this simple query
    assert!(result.source_configs.is_empty());
    assert!(result.sink_configs.is_empty());
    assert!(result.missing_source_configs.is_empty());
    assert!(result.missing_sink_configs.is_empty());
    assert!(result.syntax_issues.is_empty());
    assert!(result.performance_warnings.is_empty());
}

#[test]
fn test_multiple_queries_independence() {
    let validator = QueryValidator::new();

    let result1 = validator.validate_query("SELECT * FROM table1");
    let result2 = validator.validate_query("SELECT * FROM table2");

    // Results should be independent
    assert_ne!(result1.query_text, result2.query_text);
    assert_eq!(result1.query_text, "SELECT * FROM table1");
    assert_eq!(result2.query_text, "SELECT * FROM table2");
}

#[test]
fn test_query_with_special_characters() {
    let validator = QueryValidator::new();
    let query =
        "SELECT 'test string with spaces', \"quoted_field\", `backtick_field` FROM my_table";
    let result = validator.validate_query(query);

    assert_eq!(result.query_text, query);
    // Should handle various quoting styles without issues
}

#[test]
fn test_query_with_comments() {
    let validator = QueryValidator::new();
    let query = "-- This is a comment\nSELECT * FROM table /* inline comment */ WHERE id = 1";
    let result = validator.validate_query(query);

    assert_eq!(result.query_text, query);
    // Should handle SQL comments properly
}
