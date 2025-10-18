//! Configuration Validation Module
//!
//! Handles validation of data source and sink configurations.

use super::result_types::QueryValidationResult;
use crate::velostream::{
    datasource::{
        file::{FileDataSink, FileDataSource},
        kafka::{data_sink::KafkaDataSink, data_source::KafkaDataSource},
    },
    sql::{
        config::yaml_loader::load_yaml_config,
        execution::processors::{BatchProcessingValidator, BatchValidationTarget},
        query_analyzer::QueryAnalyzer,
    },
};
use std::collections::HashMap;
use std::path::Path;

/// Handles configuration validation for data sources and sinks
pub struct ConfigurationValidator {
    batch_validator: BatchProcessingValidator,
    analyzer: QueryAnalyzer,
    strict_mode: bool,
}

impl ConfigurationValidator {
    /// Create a new configuration validator
    pub fn new() -> Self {
        Self {
            batch_validator: BatchProcessingValidator::new(),
            analyzer: QueryAnalyzer::new("config-validator".to_string()),
            strict_mode: false,
        }
    }

    /// Create validator in strict mode
    pub fn new_strict() -> Self {
        Self {
            batch_validator: BatchProcessingValidator::new(),
            analyzer: QueryAnalyzer::new("config-validator".to_string()),
            strict_mode: true,
        }
    }

    /// Validate configurations for all queries
    pub fn validate_configurations(&self, query_results: &mut Vec<QueryValidationResult>) {
        // Load and validate source/sink configurations
        self.load_source_configurations(query_results);
        self.load_sink_configurations(query_results);

        // Validate source and sink specific configurations
        self.validate_source_configurations(query_results);
        self.validate_sink_configurations(query_results);

        // Validate batch processing configurations
        let mut recommendations = Vec::new();
        self.batch_validator
            .validate_batch_processing(query_results, &mut recommendations);

        // Add recommendations to the first query result (they'll be moved to application level later)
        if let Some(first_result) = query_results.first_mut() {
            for recommendation in recommendations {
                first_result.performance_warnings.push(recommendation);
            }
        }
    }

    // Private implementation methods
    fn load_source_configurations(&self, query_results: &mut Vec<QueryValidationResult>) {
        for result in query_results.iter_mut() {
            for source_name in result.sources_found.clone() {
                // Check if configuration is already loaded by query analyzer
                if result.source_configs.contains_key(&source_name) {
                    // Configuration already loaded from config_file - skip default loading
                    continue;
                }

                match self.load_source_config(&source_name) {
                    Ok(config) => {
                        result.source_configs.insert(source_name, config);
                    }
                    Err(error) => {
                        result.missing_source_configs.push(source_name.clone());
                        result.add_configuration_error(format!(
                            "Failed to load config for source '{}': {}",
                            source_name, error
                        ));
                    }
                }
            }
        }
    }

    fn load_sink_configurations(&self, query_results: &mut Vec<QueryValidationResult>) {
        for result in query_results.iter_mut() {
            for sink_name in result.sinks_found.clone() {
                // Check if configuration is already loaded by query analyzer
                if result.sink_configs.contains_key(&sink_name) {
                    // Configuration already loaded from config_file - skip default loading
                    continue;
                }

                match self.load_sink_config(&sink_name) {
                    Ok(config) => {
                        result.sink_configs.insert(sink_name, config);
                    }
                    Err(error) => {
                        result.missing_sink_configs.push(sink_name.clone());
                        result.add_configuration_error(format!(
                            "Failed to load config for sink '{}': {}",
                            sink_name, error
                        ));
                    }
                }
            }
        }
    }

    fn load_source_config(&self, source_name: &str) -> Result<HashMap<String, String>, String> {
        let config_path = format!("configs/{}_source.yaml", source_name);
        self.load_yaml_config_file(&config_path)
    }

    fn load_sink_config(&self, sink_name: &str) -> Result<HashMap<String, String>, String> {
        let config_path = format!("configs/{}_sink.yaml", sink_name);
        self.load_yaml_config_file(&config_path)
    }

    fn load_yaml_config_file(&self, config_path: &str) -> Result<HashMap<String, String>, String> {
        let path = Path::new(config_path);

        if !path.exists() {
            return Err(format!("File not found: ./{}", config_path));
        }

        match load_yaml_config(path) {
            Ok(resolved_config) => {
                // Flatten the configuration
                let mut flattened = HashMap::new();
                self.flatten_yaml_config(&resolved_config.config, "", &mut flattened);
                Ok(flattened)
            }
            Err(e) => Err(format!("YAML parsing error: {}", e)),
        }
    }

    fn flatten_yaml_config(
        &self,
        value: &serde_yaml::Value,
        prefix: &str,
        flattened: &mut HashMap<String, String>,
    ) {
        match value {
            serde_yaml::Value::Mapping(map) => {
                for (key, val) in map {
                    if let Some(key_str) = key.as_str() {
                        let new_prefix = if prefix.is_empty() {
                            key_str.to_string()
                        } else {
                            format!("{}.{}", prefix, key_str)
                        };
                        self.flatten_yaml_config(val, &new_prefix, flattened);
                    }
                }
            }
            serde_yaml::Value::Sequence(seq) => {
                for (i, val) in seq.iter().enumerate() {
                    let new_prefix = format!("{}[{}]", prefix, i);
                    self.flatten_yaml_config(val, &new_prefix, flattened);
                }
            }
            _ => {
                let value_str = match value {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    serde_yaml::Value::Null => "null".to_string(),
                    _ => format!("{:?}", value),
                };
                flattened.insert(prefix.to_string(), value_str);
            }
        }
    }

    fn validate_source_configurations(&self, query_results: &mut Vec<QueryValidationResult>) {
        for result in query_results.iter_mut() {
            for (source_name, config) in &result.source_configs.clone() {
                // Determine source type and delegate to appropriate validator
                if self.is_kafka_config(config) {
                    self.validate_kafka_source_config(source_name, config, result);
                } else if self.is_file_config(config) {
                    self.validate_file_source_config(source_name, config, result);
                }
            }
        }
    }

    fn validate_sink_configurations(&self, query_results: &mut Vec<QueryValidationResult>) {
        for result in query_results.iter_mut() {
            for (sink_name, config) in &result.sink_configs.clone() {
                // Determine sink type and delegate to appropriate validator
                if self.is_kafka_config(config) {
                    self.validate_kafka_sink_config(sink_name, config, result);
                } else if self.is_file_config(config) {
                    self.validate_file_sink_config(sink_name, config, result);
                }
            }
        }
    }

    fn validate_kafka_source_config(
        &self,
        source_name: &str,
        config: &HashMap<String, String>,
        result: &mut QueryValidationResult,
    ) {
        let (errors, warnings, recommendations) =
            KafkaDataSource::validate_source_config(config, source_name);

        for error in errors {
            result.add_configuration_error(format!("Kafka source '{}': {}", source_name, error));
        }

        for warning in warnings {
            result.add_warning(format!("Kafka source '{}': {}", source_name, warning));
        }

        for recommendation in recommendations {
            result.performance_warnings.push(format!(
                "Kafka source '{}': {}",
                source_name, recommendation
            ));
        }
    }

    fn validate_kafka_sink_config(
        &self,
        sink_name: &str,
        config: &HashMap<String, String>,
        result: &mut QueryValidationResult,
    ) {
        let (errors, warnings, recommendations) =
            KafkaDataSink::validate_sink_config(config, sink_name);

        for error in errors {
            result.add_configuration_error(format!("Kafka sink '{}': {}", sink_name, error));
        }

        for warning in warnings {
            result.add_warning(format!("Kafka sink '{}': {}", sink_name, warning));
        }

        for recommendation in recommendations {
            result
                .performance_warnings
                .push(format!("Kafka sink '{}': {}", sink_name, recommendation));
        }
    }

    fn validate_file_source_config(
        &self,
        source_name: &str,
        config: &HashMap<String, String>,
        result: &mut QueryValidationResult,
    ) {
        let (errors, warnings, recommendations) =
            FileDataSource::validate_source_config(config, source_name);

        for error in errors {
            result.add_configuration_error(format!("File source '{}': {}", source_name, error));
        }

        for warning in warnings {
            result.add_warning(format!("File source '{}': {}", source_name, warning));
        }

        for recommendation in recommendations {
            result
                .performance_warnings
                .push(format!("File source '{}': {}", source_name, recommendation));
        }
    }

    fn validate_file_sink_config(
        &self,
        sink_name: &str,
        config: &HashMap<String, String>,
        result: &mut QueryValidationResult,
    ) {
        let (errors, warnings, recommendations) =
            FileDataSink::validate_sink_config(config, sink_name);

        for error in errors {
            result.add_configuration_error(format!("File sink '{}': {}", sink_name, error));
        }

        for warning in warnings {
            result.add_warning(format!("File sink '{}': {}", sink_name, warning));
        }

        for recommendation in recommendations {
            result
                .performance_warnings
                .push(format!("File sink '{}': {}", sink_name, recommendation));
        }
    }

    // Helper methods to determine config type
    fn is_kafka_config(&self, config: &HashMap<String, String>) -> bool {
        config.keys().any(|k| {
            k.contains("bootstrap.servers")
                || k.contains("kafka")
                || k.contains("topic.name")
                || k.contains("producer_config")
                || k.contains("consumer_config")
        })
    }

    fn is_file_config(&self, config: &HashMap<String, String>) -> bool {
        config.keys().any(|k| {
            k == "path"
                || k.contains("source.path")
                || k.contains("sink.path")
                || k.contains("file.")
                || (k == "format"
                    && config.get("format").map_or(false, |v| {
                        ["csv", "csv_no_header", "json", "jsonlines", "jsonl"].contains(&v.as_str())
                    }))
        })
    }
}

impl Default for ConfigurationValidator {
    fn default() -> Self {
        Self::new()
    }
}
