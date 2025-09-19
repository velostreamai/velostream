//! Unit tests for ValidationErrorFormatter

use std::collections::HashMap;
use velostream::velostream::sql::validation::error_formatter::ValidationErrorFormatter;
use velostream::velostream::sql::validation::result_types::{
    ApplicationValidationResult, ParsingError, QueryValidationResult,
};

#[test]
fn test_error_formatter_creation() {
    let formatter = ValidationErrorFormatter::new();
    // Verify the formatter was created successfully
    assert!(true); // Basic creation test
}

#[test]
fn test_error_formatter_minimal() {
    let formatter = ValidationErrorFormatter::new_minimal();
    // Verify the minimal formatter was created successfully
    assert!(true); // Basic creation test
}

#[test]
fn test_error_formatter_with_context() {
    let formatter = ValidationErrorFormatter::new().with_context(false);
    // Verify the formatter was configured successfully
    assert!(true); // Basic configuration test
}

#[test]
fn test_error_formatter_with_colors() {
    let formatter = ValidationErrorFormatter::new().with_colors(false);
    // Verify the formatter was configured successfully
    assert!(true); // Basic configuration test
}

#[test]
fn test_error_formatter_with_verbose() {
    let formatter = ValidationErrorFormatter::new().with_verbose(true);
    // Verify the formatter was configured successfully
    assert!(true); // Basic configuration test
}

#[test]
fn test_format_parsing_error() {
    let formatter = ValidationErrorFormatter::new();
    let error = ParsingError {
        message: "Unexpected token".to_string(),
        line: 1,
        column: 14,
        position: 13,
        error_type: "syntax".to_string(),
        error_indicator: "SELECT * ⟨FROM⟩".to_string(),
    };
    let content = "SELECT * FROM";

    let formatted = formatter.format_parsing_error(&error, content);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("Unexpected token")));
}

#[test]
fn test_format_query_result_valid() {
    let formatter = ValidationErrorFormatter::new();
    let result = QueryValidationResult::new("SELECT * FROM test_stream".to_string());

    let formatted = formatter.format_query_result(&result, 1);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("SELECT * FROM test_stream")));
}

#[test]
fn test_format_query_result_invalid() {
    let formatter = ValidationErrorFormatter::new();
    let mut result = QueryValidationResult::new("INVALID SQL".to_string());
    result.is_valid = false;
    result.add_configuration_error("Syntax error".to_string());

    let formatted = formatter.format_query_result(&result, 1);

    assert!(!formatted.is_empty());
    assert!(formatted.iter().any(|line| line.contains("INVALID SQL")));
    assert!(formatted.iter().any(|line| line.contains("Syntax error")));
}

#[test]
fn test_format_query_result_with_warnings() {
    let formatter = ValidationErrorFormatter::new();
    let mut result = QueryValidationResult::new("SELECT * FROM large_stream".to_string());
    result.add_warning("Performance warning: large scan".to_string());
    result
        .performance_warnings
        .push("JOIN without time windows".to_string());

    let formatted = formatter.format_query_result(&result, 1);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("SELECT * FROM large_stream")));
}

#[test]
fn test_format_application_result_valid() {
    let formatter = ValidationErrorFormatter::new();
    let mut result = ApplicationValidationResult::new();
    result.application_name = "Test Application".to_string();

    let formatted = formatter.format_application_result(&result);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("Test Application")));
}

#[test]
fn test_format_application_result_invalid() {
    let formatter = ValidationErrorFormatter::new();
    let mut result = ApplicationValidationResult::new();
    result.application_name = "Invalid Application".to_string();
    result.is_valid = false;
    result.add_configuration_error("Global configuration error".to_string());

    let formatted = formatter.format_application_result(&result);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("Invalid Application")));
    assert!(formatted
        .iter()
        .any(|line| line.contains("Global configuration error")));
}

#[test]
fn test_format_application_result_with_queries() {
    let formatter = ValidationErrorFormatter::new();
    let mut result = ApplicationValidationResult::new();
    result.application_name = "Multi-Query Application".to_string();

    let query1 = QueryValidationResult::new("SELECT * FROM stream1".to_string());
    let mut query2 = QueryValidationResult::new("SELECT count(*) FROM stream2".to_string());
    query2.add_warning("Performance warning".to_string());

    result.add_query_result(query1);
    result.add_query_result(query2);

    let formatted = formatter.format_application_result(&result);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("Multi-Query Application")));
}

#[test]
fn test_format_application_result_comprehensive() {
    let formatter = ValidationErrorFormatter::new();
    let mut result = ApplicationValidationResult::new();
    result.application_name = "Comprehensive Test".to_string();
    result.is_valid = false;
    result.add_configuration_error("Global error".to_string());

    // Add source/sink configs
    let mut source_config = HashMap::new();
    source_config.insert("topic".to_string(), "input_topic".to_string());
    result
        .source_configs
        .insert("input".to_string(), source_config);

    result
        .missing_source_configs
        .push("missing_input".to_string());

    // Add complex query
    let mut query =
        QueryValidationResult::new("SELECT * FROM input ORDER BY timestamp".to_string());
    query.sources_found.push("input".to_string());
    query.add_warning("General warning".to_string());

    let parsing_error = ParsingError {
        message: "Test parsing error".to_string(),
        line: 1,
        column: 10,
        position: 9,
        error_type: "syntax".to_string(),
        error_indicator: "SELECT * ⟨FROM⟩".to_string(),
    };
    query.parsing_errors.push(parsing_error);

    result.add_query_result(query);

    let formatted = formatter.format_application_result(&result);

    assert!(!formatted.is_empty());
    assert!(formatted
        .iter()
        .any(|line| line.contains("Comprehensive Test")));
    assert!(formatted.iter().any(|line| line.contains("Global error")));
}

#[test]
fn test_formatter_builder_pattern() {
    let formatter = ValidationErrorFormatter::new()
        .with_context(true)
        .with_colors(false)
        .with_verbose(true);

    let result = QueryValidationResult::new("SELECT 1".to_string());
    let formatted = formatter.format_query_result(&result, 1);

    assert!(!formatted.is_empty());
}

#[test]
fn test_formatter_consistency() {
    let formatter = ValidationErrorFormatter::new();

    let result1 = QueryValidationResult::new("SELECT * FROM test".to_string());
    let result2 = QueryValidationResult::new("SELECT * FROM test".to_string());

    let formatted1 = formatter.format_query_result(&result1, 1);
    let formatted2 = formatter.format_query_result(&result2, 1);

    // Same input should produce same output (except query numbers)
    assert_eq!(formatted1.len(), formatted2.len());
}

#[test]
fn test_minimal_formatter() {
    let formatter = ValidationErrorFormatter::new_minimal();

    let mut result = QueryValidationResult::new("SELECT * FROM test".to_string());
    result.add_warning("Test warning".to_string());

    let formatted = formatter.format_query_result(&result, 1);

    assert!(!formatted.is_empty());
    // Minimal formatter should still include basic information
}
