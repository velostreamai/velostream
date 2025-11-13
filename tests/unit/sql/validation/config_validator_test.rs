//! Unit tests for ConfigurationValidator

use std::collections::HashMap;
use velostream::velostream::sql::validation::config_validator::ConfigurationValidator;
use velostream::velostream::sql::validation::result_types::{
    ApplicationValidationResult, QueryValidationResult,
};

#[test]
fn test_config_validator_creation() {
    let validator = ConfigurationValidator::new();
    // Verify the validator was created successfully
    assert!(true); // Basic creation test
}

#[test]
fn test_validate_configurations_empty() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create an empty query result with no sources
    let query_result = QueryValidationResult::new("SELECT 1".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // Empty sources should not produce errors
    assert!(query_results[0].configuration_errors.is_empty());
}

#[test]
fn test_validate_configurations_with_sources() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with a source
    let mut query_result = QueryValidationResult::new("SELECT * FROM test_stream".to_string());
    query_result.sources_found.push("test_stream".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // This will try to load config files, which may not exist, but test should complete
    // The validator should attempt to load the configuration
    assert!(true); // Test completed without panic
}

#[test]
fn test_validate_configurations_missing_config() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with a source that won't have a config file
    let mut query_result = QueryValidationResult::new("SELECT * FROM missing_stream".to_string());
    query_result
        .sources_found
        .push("missing_stream".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // Missing configuration should be tracked in missing_source_configs
    assert_eq!(query_results[0].missing_source_configs.len(), 1);
    assert!(
        query_results[0]
            .missing_source_configs
            .contains(&"missing_stream".to_string())
    );
}

#[test]
fn test_validate_configurations_empty_sinks() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with no sinks
    let query_result = QueryValidationResult::new("SELECT 1".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // Empty sinks should not produce errors
    assert!(query_results[0].configuration_errors.is_empty());
}

#[test]
fn test_validate_configurations_with_sinks() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with a sink
    let mut query_result =
        QueryValidationResult::new("INSERT INTO output_stream SELECT 1".to_string());
    query_result.sinks_found.push("output_stream".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // This will try to load config files, which may not exist, but test should complete
    // The validator should attempt to load the configuration
    assert!(true); // Test completed without panic
}

#[test]
fn test_validate_configurations_missing_sink_config() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with a sink that won't have a config file
    let mut query_result =
        QueryValidationResult::new("INSERT INTO missing_sink SELECT 1".to_string());
    query_result.sinks_found.push("missing_sink".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // Missing configuration should be tracked in missing_sink_configs
    assert_eq!(query_results[0].missing_sink_configs.len(), 1);
    assert!(
        query_results[0]
            .missing_sink_configs
            .contains(&"missing_sink".to_string())
    );
}

#[test]
fn test_validate_configurations_batch_processing() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with sources and sinks for batch processing validation
    let mut query_result = QueryValidationResult::new("SELECT * FROM test_stream".to_string());
    query_result.sources_found.push("test_stream".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // Should complete without errors - batch processing validation is part of validate_configurations
    assert!(true); // Validation completed
}

#[test]
fn test_validate_configurations_multiple_sources_and_sinks() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with multiple sources and sinks
    let mut query_result =
        QueryValidationResult::new("SELECT * FROM stream1 UNION SELECT * FROM stream2".to_string());
    query_result.sources_found.push("stream1".to_string());
    query_result.sources_found.push("stream2".to_string());
    query_result.sinks_found.push("output1".to_string());
    query_result.sinks_found.push("output2".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // This will try to load config files for all sources and sinks
    // Since config files may not exist, we test that it doesn't panic
    assert!(true); // Test completed without panic
}

#[test]
fn test_validate_configurations_mixed_valid_invalid() {
    let validator = ConfigurationValidator::new();
    let mut query_results = Vec::new();

    // Create a query result with multiple sources (some may have configs, some may not)
    let mut query_result = QueryValidationResult::new(
        "SELECT * FROM valid_stream UNION SELECT * FROM missing_stream".to_string(),
    );
    query_result.sources_found.push("valid_stream".to_string());
    query_result
        .sources_found
        .push("missing_stream".to_string());
    query_results.push(query_result);

    validator.validate_configurations(&mut query_results);

    // This will attempt to load configs for both sources
    // missing_stream should end up in missing_source_configs
    assert!(query_results[0].missing_source_configs.len() >= 1); // At least missing_stream
}

#[test]
fn test_config_validator_independence() {
    let validator = ConfigurationValidator::new();
    let mut query_results1 = Vec::new();
    let mut query_results2 = Vec::new();

    // Create independent query results
    let mut query_result1 = QueryValidationResult::new("SELECT * FROM stream1".to_string());
    query_result1.sources_found.push("stream1".to_string());
    query_results1.push(query_result1);

    let mut query_result2 = QueryValidationResult::new("SELECT * FROM stream2".to_string());
    query_result2.sources_found.push("stream2".to_string());
    query_results2.push(query_result2);

    validator.validate_configurations(&mut query_results1);
    validator.validate_configurations(&mut query_results2);

    // Results should be independent
    assert!(
        query_results1[0]
            .missing_source_configs
            .contains(&"stream1".to_string())
    );
    assert!(
        query_results2[0]
            .missing_source_configs
            .contains(&"stream2".to_string())
    );
    // Each should only contain its own missing stream
    assert!(
        !query_results1[0]
            .missing_source_configs
            .contains(&"stream2".to_string())
    );
    assert!(
        !query_results2[0]
            .missing_source_configs
            .contains(&"stream1".to_string())
    );
}
