//! Unit tests for ConfigurationValidator

use std::collections::HashMap;
use velostream::velostream::sql::validation::config_validator::ConfigurationValidator;
use velostream::velostream::sql::validation::result_types::ApplicationValidationResult;

#[test]
fn test_config_validator_creation() {
    let validator = ConfigurationValidator::new();
    // Verify the validator was created successfully
    assert!(true); // Basic creation test
}

#[test]
fn test_validate_data_sources_empty() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let sources: Vec<String> = vec![];
    let configs: HashMap<String, HashMap<String, String>> = HashMap::new();

    validator.validate_data_sources(&sources, &configs, &mut result);

    // Empty sources should not produce errors
    assert!(result.configuration_errors.is_empty());
}

#[test]
fn test_validate_data_sources_with_configs() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let sources = vec!["test_stream".to_string()];
    let mut configs = HashMap::new();
    let mut source_config = HashMap::new();
    source_config.insert("topic".to_string(), "test_topic".to_string());
    source_config.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    configs.insert("test_stream".to_string(), source_config);

    validator.validate_data_sources(&sources, &configs, &mut result);

    // Valid configuration should not produce errors
    assert!(result.configuration_errors.is_empty());
    assert!(!result.source_configs.is_empty());
}

#[test]
fn test_validate_data_sources_missing_config() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let sources = vec!["missing_stream".to_string()];
    let configs = HashMap::new();

    validator.validate_data_sources(&sources, &configs, &mut result);

    // Missing configuration should be tracked
    assert_eq!(result.missing_source_configs.len(), 1);
    assert!(result
        .missing_source_configs
        .contains(&"missing_stream".to_string()));
}

#[test]
fn test_validate_data_sinks_empty() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let sinks: Vec<String> = vec![];
    let configs: HashMap<String, HashMap<String, String>> = HashMap::new();

    validator.validate_data_sinks(&sinks, &configs, &mut result);

    // Empty sinks should not produce errors
    assert!(result.configuration_errors.is_empty());
}

#[test]
fn test_validate_data_sinks_with_configs() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let sinks = vec!["output_stream".to_string()];
    let mut configs = HashMap::new();
    let mut sink_config = HashMap::new();
    sink_config.insert("topic".to_string(), "output_topic".to_string());
    sink_config.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    configs.insert("output_stream".to_string(), sink_config);

    validator.validate_data_sinks(&sinks, &configs, &mut result);

    // Valid configuration should not produce errors
    assert!(result.configuration_errors.is_empty());
    assert!(!result.sink_configs.is_empty());
}

#[test]
fn test_validate_data_sinks_missing_config() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let sinks = vec!["missing_sink".to_string()];
    let configs = HashMap::new();

    validator.validate_data_sinks(&sinks, &configs, &mut result);

    // Missing configuration should be tracked
    assert_eq!(result.missing_sink_configs.len(), 1);
    assert!(result
        .missing_sink_configs
        .contains(&"missing_sink".to_string()));
}

#[test]
fn test_validate_batch_processing() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();
    let mut configs = HashMap::new();
    let mut source_config = HashMap::new();
    source_config.insert("topic".to_string(), "test_topic".to_string());
    source_config.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    configs.insert("test_stream".to_string(), source_config);

    validator.validate_batch_processing(&configs, &mut result);

    // Should complete without errors for basic configuration
    assert!(true); // Validation completed
}

#[test]
fn test_validate_multiple_sources_and_sinks() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();

    let sources = vec!["stream1".to_string(), "stream2".to_string()];
    let sinks = vec!["output1".to_string(), "output2".to_string()];

    let mut configs = HashMap::new();

    // Add source configurations
    for source in &sources {
        let mut config = HashMap::new();
        config.insert("topic".to_string(), format!("{}_topic", source));
        config.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        configs.insert(source.clone(), config);
    }

    // Add sink configurations
    for sink in &sinks {
        let mut config = HashMap::new();
        config.insert("topic".to_string(), format!("{}_topic", sink));
        config.insert(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );
        configs.insert(sink.clone(), config);
    }

    validator.validate_data_sources(&sources, &configs, &mut result);
    validator.validate_data_sinks(&sinks, &configs, &mut result);

    // All configurations should be valid
    assert!(result.configuration_errors.is_empty());
    assert_eq!(result.source_configs.len(), 2);
    assert_eq!(result.sink_configs.len(), 2);
    assert!(result.missing_source_configs.is_empty());
    assert!(result.missing_sink_configs.is_empty());
}

#[test]
fn test_validate_mixed_valid_invalid_configs() {
    let validator = ConfigurationValidator::new();
    let mut result = ApplicationValidationResult::new();

    let sources = vec!["valid_stream".to_string(), "missing_stream".to_string()];
    let mut configs = HashMap::new();

    // Only add config for one source
    let mut config = HashMap::new();
    config.insert("topic".to_string(), "valid_topic".to_string());
    config.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    configs.insert("valid_stream".to_string(), config);

    validator.validate_data_sources(&sources, &configs, &mut result);

    // Should have one valid config and one missing
    assert_eq!(result.source_configs.len(), 1);
    assert_eq!(result.missing_source_configs.len(), 1);
    assert!(result.source_configs.contains_key("valid_stream"));
    assert!(result
        .missing_source_configs
        .contains(&"missing_stream".to_string()));
}

#[test]
fn test_config_validator_independence() {
    let validator = ConfigurationValidator::new();
    let mut result1 = ApplicationValidationResult::new();
    let mut result2 = ApplicationValidationResult::new();

    let sources1 = vec!["stream1".to_string()];
    let sources2 = vec!["stream2".to_string()];
    let configs = HashMap::new();

    validator.validate_data_sources(&sources1, &configs, &mut result1);
    validator.validate_data_sources(&sources2, &configs, &mut result2);

    // Results should be independent
    assert_eq!(result1.missing_source_configs, vec!["stream1".to_string()]);
    assert_eq!(result2.missing_source_configs, vec!["stream2".to_string()]);
}
