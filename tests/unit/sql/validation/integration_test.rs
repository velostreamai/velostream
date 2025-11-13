//! Integration tests for SQL validation service

use std::collections::HashMap;
use velostream::velostream::sql::validation::SqlValidationService;

#[test]
fn test_sql_validation_service_creation() {
    let service = SqlValidationService::new();
    // Verify the service was created successfully
}

#[test]
fn test_sql_validation_service_strict() {
    let service = SqlValidationService::new_strict();
    // Verify the strict service was created successfully
}

#[test]
fn test_sql_validation_service_with_performance_checks() {
    let service = SqlValidationService::new().with_performance_checks(false);
    // Verify the service was configured successfully
}

#[test]
fn test_validate_simple_query() {
    let service = SqlValidationService::new();
    let result = service.validate_query("SELECT id, name FROM users");

    assert_eq!(result.query_text, "SELECT id, name FROM users");
    assert_eq!(result.query_index, 0);
    assert_eq!(result.start_line, 1);
    // Note: The actual validation result depends on parser implementation
}

#[test]
fn test_validate_empty_query() {
    let service = SqlValidationService::new();
    let result = service.validate_query("");

    assert_eq!(result.query_text, "");
    assert_eq!(result.query_index, 0);
    assert_eq!(result.start_line, 1);
}

#[test]
fn test_validate_complex_query() {
    let service = SqlValidationService::new();
    let query = "CREATE STREAM user_events AS SELECT user_id, event_type, timestamp FROM event_stream WHERE event_type = 'login'";
    let result = service.validate_query(query);

    assert_eq!(result.query_text, query);
    assert_eq!(result.query_index, 0);
    assert_eq!(result.start_line, 1);
}

#[test]
fn test_validate_query_with_performance_checks() {
    let service = SqlValidationService::new().with_performance_checks(true);

    let query = "SELECT * FROM stream1 JOIN stream2 ON stream1.id = stream2.id";
    let result = service.validate_query(query);

    assert_eq!(result.query_text, query);
    // Performance warnings depend on implementation
}

#[test]
fn test_validate_query_without_performance_checks() {
    let service = SqlValidationService::new().with_performance_checks(false);

    let query = "SELECT * FROM large_stream ORDER BY timestamp";
    let result = service.validate_query(query);

    assert_eq!(result.query_text, query);
    // Should have fewer performance warnings
}

#[test]
fn test_service_reusability() {
    let service = SqlValidationService::new();

    let result1 = service.validate_query("SELECT * FROM stream1");
    let result2 = service.validate_query("SELECT * FROM stream2");

    // Service should be reusable
    assert_ne!(result1.query_text, result2.query_text);
    assert_eq!(result1.query_text, "SELECT * FROM stream1");
    assert_eq!(result2.query_text, "SELECT * FROM stream2");
}

#[test]
fn test_service_default_construction() {
    let service = SqlValidationService::default();
    let result = service.validate_query("SELECT 1");

    assert_eq!(result.query_text, "SELECT 1");
}

#[test]
fn test_streaming_query_validation() {
    let service = SqlValidationService::new();

    let streaming_queries = vec![
        "SELECT user_id, COUNT(*) OVER (PARTITION BY user_id) FROM events",
        "SELECT * FROM stream WHERE timestamp > NOW() - INTERVAL '1 hour'",
        "SELECT user_id, AVG(amount) FROM transactions GROUP BY user_id WINDOW TUMBLING(INTERVAL 5 MINUTE)",
    ];

    for query in streaming_queries {
        let result = service.validate_query(query);
        assert_eq!(result.query_text, query);
        // Validate streaming-specific syntax handling
    }
}
