//! Unit tests for validation result types

use std::collections::HashMap;
use velostream::velostream::sql::execution::processors::BatchValidationTarget;
use velostream::velostream::sql::validation::result_types::{
    ApplicationValidationResult, ParsingError, QueryValidationResult,
};

#[test]
fn test_query_validation_result_creation() {
    let query = "SELECT * FROM test_stream".to_string();
    let result = QueryValidationResult::new(query.clone());

    assert_eq!(result.query_text, query);
    assert_eq!(result.query_index, 0);
    assert_eq!(result.start_line, 1);
    assert!(result.is_valid);
    assert!(result.parsing_errors.is_empty());
    assert!(result.configuration_errors.is_empty());
    assert!(result.warnings.is_empty());
    assert!(result.sources_found.is_empty());
    assert!(result.sinks_found.is_empty());
    assert!(result.source_configs.is_empty());
    assert!(result.sink_configs.is_empty());
    assert!(result.missing_source_configs.is_empty());
    assert!(result.missing_sink_configs.is_empty());
    assert!(result.syntax_issues.is_empty());
    assert!(result.performance_warnings.is_empty());
}

#[test]
fn test_query_validation_result_with_index() {
    let query = "SELECT * FROM test_stream".to_string();
    let mut result = QueryValidationResult::new(query.clone());
    result.query_index = 5;
    result.start_line = 10;

    assert_eq!(result.query_index, 5);
    assert_eq!(result.start_line, 10);
}

#[test]
fn test_query_validation_result_add_configuration_error() {
    let query = "SELECT * FROM test_stream".to_string();
    let mut result = QueryValidationResult::new(query);

    result.add_configuration_error("Missing source configuration".to_string());

    assert_eq!(result.configuration_errors.len(), 1);
    assert_eq!(
        result.configuration_errors[0],
        "Missing source configuration"
    );
    assert!(!result.is_valid);
}

#[test]
fn test_query_validation_result_add_warning() {
    let query = "SELECT * FROM test_stream".to_string();
    let mut result = QueryValidationResult::new(query);

    result.add_warning("Performance warning: large scan".to_string());

    assert_eq!(result.warnings.len(), 1);
    assert_eq!(result.warnings[0], "Performance warning: large scan");
    assert!(result.is_valid); // Warnings don't make query invalid
}

#[test]
fn test_query_validation_result_add_syntax_issue() {
    let query = "SELECT * FROM test_stream".to_string();
    let mut result = QueryValidationResult::new(query);

    result.add_syntax_issue("Unsupported syntax feature".to_string());

    assert_eq!(result.syntax_issues.len(), 1);
    assert_eq!(result.syntax_issues[0], "Unsupported syntax feature");
}

#[test]
fn test_query_validation_result_sources_and_sinks() {
    let query = "CREATE STREAM output AS SELECT * FROM input".to_string();
    let mut result = QueryValidationResult::new(query);

    result.sources_found.push("input".to_string());
    result.sinks_found.push("output".to_string());

    let mut source_config = HashMap::new();
    source_config.insert("topic".to_string(), "input_topic".to_string());
    result
        .source_configs
        .insert("input".to_string(), source_config);

    let mut sink_config = HashMap::new();
    sink_config.insert("topic".to_string(), "output_topic".to_string());
    result
        .sink_configs
        .insert("output".to_string(), sink_config);

    assert_eq!(result.sources_found, vec!["input"]);
    assert_eq!(result.sinks_found, vec!["output"]);
    assert!(result.source_configs.contains_key("input"));
    assert!(result.sink_configs.contains_key("output"));
}

#[test]
fn test_application_validation_result_creation() {
    let result = ApplicationValidationResult::new(
        "test_file.sql".to_string(),
        "Test Application".to_string(),
    );

    assert!(result.is_valid);
    assert!(result.query_results.is_empty());
    assert!(result.recommendations.is_empty());
    assert!(result.configuration_summary.is_empty());
    assert!(result.file_errors.is_empty());
    assert_eq!(result.file_path, "test_file.sql");
    assert_eq!(result.application_name, "Test Application");
}

#[test]
fn test_application_validation_result_with_configuration_summary() {
    let mut result = ApplicationValidationResult::new(
        "test_file.sql".to_string(),
        "Test Application".to_string(),
    );

    // ApplicationValidationResult doesn't have add_configuration_error,
    // but we can add to configuration_summary directly
    result
        .configuration_summary
        .push("Global configuration error".to_string());
    result.is_valid = false;

    assert_eq!(result.configuration_summary.len(), 1);
    assert_eq!(
        result.configuration_summary[0],
        "Global configuration error"
    );
    assert!(!result.is_valid);
}

#[test]
fn test_application_validation_result_add_query_result() {
    let mut result = ApplicationValidationResult::new(
        "test_file.sql".to_string(),
        "Test Application".to_string(),
    );
    let query = "SELECT * FROM test".to_string();
    let query_result = QueryValidationResult::new(query);

    result.query_results.push(query_result);

    assert_eq!(result.query_results.len(), 1);
    assert_eq!(result.query_results[0].query_text, "SELECT * FROM test");
}

#[test]
fn test_application_validation_result_add_invalid_query() {
    let mut result = ApplicationValidationResult::new(
        "test_file.sql".to_string(),
        "Test Application".to_string(),
    );
    let query = "INVALID SQL".to_string();
    let mut query_result = QueryValidationResult::new(query);
    query_result.is_valid = false;
    query_result.add_configuration_error("Syntax error".to_string());

    result.query_results.push(query_result);
    result.update_statistics(); // This is needed to update the is_valid field

    assert_eq!(result.query_results.len(), 1);
    assert!(!result.query_results[0].is_valid);
    assert!(!result.is_valid); // App result becomes invalid if any query is invalid
}

#[test]
fn test_application_validation_result_with_query_configurations() {
    let mut result = ApplicationValidationResult::new(
        "test_file.sql".to_string(),
        "Test Application".to_string(),
    );

    // Create a query result with configurations (this is where configs belong)
    let mut query_result = QueryValidationResult::new("SELECT * FROM test_source".to_string());

    let mut source_config = HashMap::new();
    source_config.insert("topic".to_string(), "test_topic".to_string());
    query_result
        .source_configs
        .insert("test_source".to_string(), source_config);

    let mut sink_config = HashMap::new();
    sink_config.insert("topic".to_string(), "output_topic".to_string());
    query_result
        .sink_configs
        .insert("test_sink".to_string(), sink_config);

    query_result
        .missing_source_configs
        .push("missing_source".to_string());
    query_result
        .missing_sink_configs
        .push("missing_sink".to_string());

    result.query_results.push(query_result);

    // Verify the configurations are in the query result
    assert_eq!(result.query_results.len(), 1);
    assert_eq!(result.query_results[0].source_configs.len(), 1);
    assert_eq!(result.query_results[0].sink_configs.len(), 1);
    assert_eq!(result.query_results[0].missing_source_configs.len(), 1);
    assert_eq!(result.query_results[0].missing_sink_configs.len(), 1);
}

#[test]
fn test_parsing_error_creation() {
    let error = ParsingError {
        message: "Unexpected token".to_string(),
        line: 5,
        column: 10,
        position: 123,
        error_type: "syntax".to_string(),
        error_indicator: "SELECT * ⟨FROM⟩ WHERE".to_string(),
    };

    assert_eq!(error.message, "Unexpected token");
    assert_eq!(error.line, 5);
    assert_eq!(error.column, 10);
    assert_eq!(error.position, 123);
    assert_eq!(error.error_type, "syntax");
    assert_eq!(error.error_indicator, "SELECT * ⟨FROM⟩ WHERE");
}

#[test]
fn test_parsing_error_clone() {
    let error = ParsingError {
        message: "Test error".to_string(),
        line: 1,
        column: 1,
        position: 0,
        error_type: "test".to_string(),
        error_indicator: "⟨error⟩".to_string(),
    };

    let cloned = error.clone();
    assert_eq!(error.message, cloned.message);
    assert_eq!(error.line, cloned.line);
    assert_eq!(error.column, cloned.column);
    assert_eq!(error.position, cloned.position);
    assert_eq!(error.error_type, cloned.error_type);
    assert_eq!(error.error_indicator, cloned.error_indicator);
}

#[test]
fn test_parsing_error_debug() {
    let error = ParsingError {
        message: "Debug test".to_string(),
        line: 1,
        column: 1,
        position: 0,
        error_type: "debug".to_string(),
        error_indicator: "debug".to_string(),
    };

    let debug_output = format!("{:?}", error);
    assert!(debug_output.contains("Debug test"));
    assert!(debug_output.contains("ParsingError"));
}

#[test]
fn test_query_validation_result_multiple_errors() {
    let query = "INVALID COMPLEX QUERY".to_string();
    let mut result = QueryValidationResult::new(query);

    result.add_configuration_error("Error 1".to_string());
    result.add_configuration_error("Error 2".to_string());
    result.add_warning("Warning 1".to_string());
    result.add_warning("Warning 2".to_string());
    result.add_syntax_issue("Syntax issue 1".to_string());
    result
        .performance_warnings
        .push("Performance issue 1".to_string());

    assert_eq!(result.configuration_errors.len(), 2);
    assert_eq!(result.warnings.len(), 2);
    assert_eq!(result.syntax_issues.len(), 1);
    assert_eq!(result.performance_warnings.len(), 1);
    assert!(!result.is_valid);
}

#[test]
fn test_application_validation_result_multiple_queries() {
    let mut result = ApplicationValidationResult::new(
        "test_file.sql".to_string(),
        "Test Application".to_string(),
    );

    let query1 = QueryValidationResult::new("SELECT * FROM stream1".to_string());
    let mut query2 = QueryValidationResult::new("INVALID SQL".to_string());
    query2.is_valid = false;

    result.query_results.push(query1);
    result.query_results.push(query2);
    result.update_statistics(); // This is needed to update the is_valid field

    assert_eq!(result.query_results.len(), 2);
    assert!(result.query_results[0].is_valid);
    assert!(!result.query_results[1].is_valid);
    assert!(!result.is_valid); // Overall result is invalid
}
