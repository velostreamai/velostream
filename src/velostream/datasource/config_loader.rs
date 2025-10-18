//! Common configuration file loading utilities for data sources and sinks
//!
//! This module provides shared functionality for loading YAML configuration files
//! and merging them with provided properties. Used by KafkaDataSource, KafkaDataSink,
//! and other configurable components.

use crate::velostream::sql::config::yaml_loader::load_yaml_config;
use std::collections::HashMap;

/// Load a YAML config file and flatten it to key-value properties
///
/// # Arguments
/// * `file_path` - Path to the YAML config file (can be relative or absolute)
///
/// # Returns
/// * `Ok(HashMap)` - Flattened properties from the config file
/// * `Err` - If the file cannot be loaded or parsed
///
/// # Example
/// ```ignore
/// let props = load_config_file_to_properties("configs/source.yaml")?;
/// assert!(props.contains_key("bootstrap.servers"));
/// ```
pub fn load_config_file_to_properties(
    file_path: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error + Send + Sync>> {
    // Load YAML config with extends support
    let yaml_config = load_yaml_config(file_path)?;

    // Flatten YAML to key-value pairs
    let mut flattened = HashMap::new();
    flatten_yaml_value(&yaml_config.config, "", &mut flattened);

    Ok(flattened)
}

/// Merge config file properties with provided properties
///
/// This function:
/// 1. Loads config file if `config_file` property is present
/// 2. Merges file properties with provided properties
/// 3. Provided properties override file properties
/// 4. Logs the process for debugging
///
/// # Arguments
/// * `props` - HashMap of provided properties (may include `config_file` key)
/// * `context_name` - Name for logging (e.g., "source", "sink", "kafka_source")
///
/// # Returns
/// HashMap with merged properties (file props + provided props, provided takes precedence)
///
/// # Panics
/// Panics with detailed error message if config file is specified but cannot be loaded.
/// This is intentional FAIL-FAST behavior to prevent silent configuration errors.
///
/// # Example
/// ```ignore
/// let mut props = HashMap::new();
/// props.insert("config_file".to_string(), "configs/source.yaml".to_string());
/// props.insert("topic".to_string(), "override_topic".to_string());
///
/// let merged = merge_config_file_properties(&props, "kafka_source");
/// // merged will have all properties from source.yaml, but topic will be "override_topic"
/// ```
pub fn merge_config_file_properties(
    props: &HashMap<String, String>,
    context_name: &str,
) -> HashMap<String, String> {
    // DEBUG: Log all incoming properties
    log::info!(
        "merge_config_file_properties called for context: {}",
        context_name
    );
    log::info!("  Received {} properties:", props.len());
    for (k, v) in props.iter() {
        log::info!("    {} = {}", k, v);
    }

    // Check if there's a config file to load
    let config_file = extract_config_file_path(props);

    let mut merged_props = HashMap::new();

    if let Some(config_file_path) = config_file {
        log::info!(
            "Loading config file '{}' for {}",
            config_file_path,
            context_name
        );

        match load_config_file_to_properties(&config_file_path) {
            Ok(file_props) => {
                log::info!(
                    "Loaded {} properties from config file '{}' for {}",
                    file_props.len(),
                    config_file_path,
                    context_name
                );
                // File properties go in first
                merged_props.extend(file_props);
            }
            Err(e) => {
                // FAIL FAST: If a config file is specified but can't be loaded, this is a configuration error
                panic!(
                    "CONFIGURATION ERROR: Failed to load config file '{}' for {}: {}\n\
                     \n\
                     When a config file is specified in SQL (e.g., '<name>.config_file = ...'), \
                     the file MUST exist and be readable. This prevents silent failures from misconfigured sources.\n\
                     \n\
                     Please verify:\n\
                     1. The file path is correct and relative to the working directory\n\
                     2. The file exists and is readable\n\
                     3. The file contains valid YAML\n\
                     4. The 'extends' references (if any) are resolvable",
                    config_file_path, context_name, e
                );
            }
        }
    }

    // Provided properties override file properties
    merged_props.extend(props.clone());

    merged_props
}

/// Extract config file path from properties
///
/// Checks for config file in various common property name patterns:
/// - `config_file`
/// - `source.config_file`
/// - `sink.config_file`
/// - `*.config_file` (any property ending with `.config_file`)
///
/// Returns the first match found.
fn extract_config_file_path(props: &HashMap<String, String>) -> Option<String> {
    log::debug!(
        "extract_config_file_path: Checking {} properties",
        props.len()
    );

    // Check each pattern and log
    if let Some(v) = props.get("config_file") {
        log::info!("  Found 'config_file' = {}", v);
        return Some(v.clone());
    }

    if let Some(v) = props.get("source.config_file") {
        log::info!("  Found 'source.config_file' = {}", v);
        return Some(v.clone());
    }

    if let Some(v) = props.get("sink.config_file") {
        log::info!("  Found 'sink.config_file' = {}", v);
        return Some(v.clone());
    }

    // Check for {topic}.config_file pattern
    for (k, v) in props.iter() {
        if k.ends_with(".config_file") {
            log::info!("  Found pattern '*.config_file': {} = {}", k, v);
            return Some(v.clone());
        }
    }

    log::warn!("  No config_file found in properties");
    None
}

/// Flatten a YAML value into dot-notation key-value pairs
///
/// Converts nested YAML structures into flat key-value pairs using dot notation.
///
/// # Example
/// ```ignore
/// datasource:
///   consumer_config:
///     bootstrap.servers: "localhost:9092"
/// ```
/// Becomes:
/// ```ignore
/// "datasource.consumer_config.bootstrap.servers" => "localhost:9092"
/// ```
fn flatten_yaml_value(
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
                    flatten_yaml_value(val, &new_prefix, flattened);
                }
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for (i, val) in seq.iter().enumerate() {
                let new_prefix = format!("{}[{}]", prefix, i);
                flatten_yaml_value(val, &new_prefix, flattened);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_config_file_to_properties() {
        // Create temporary config file
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let config_content = r#"
datasource:
  consumer_config:
    bootstrap.servers: "test-broker:9092"
  schema:
    value.serializer: "avro"
"#;
        temp_file
            .write_all(config_content.as_bytes())
            .expect("Failed to write");
        let path = temp_file.path().to_str().unwrap();

        // Load and flatten
        let props = load_config_file_to_properties(path).expect("Should load successfully");

        // Verify flattened properties
        assert!(props.contains_key("datasource.consumer_config.bootstrap.servers"));
        assert_eq!(
            props.get("datasource.consumer_config.bootstrap.servers"),
            Some(&"test-broker:9092".to_string())
        );
        assert_eq!(
            props.get("datasource.schema.value.serializer"),
            Some(&"avro".to_string())
        );
    }

    #[test]
    fn test_merge_config_file_properties_with_override() {
        // Create temporary config file
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(b"topic: file_topic\nbrokers: file_brokers")
            .expect("Failed to write");
        let path = temp_file.path().to_str().unwrap().to_string();

        // Properties with config file and override
        let mut props = HashMap::new();
        props.insert("config_file".to_string(), path);
        props.insert("topic".to_string(), "override_topic".to_string());

        // Merge
        let merged = merge_config_file_properties(&props, "test");

        // Verify: provided props override file props
        assert_eq!(merged.get("topic"), Some(&"override_topic".to_string()));
        assert_eq!(merged.get("brokers"), Some(&"file_brokers".to_string()));
    }

    #[test]
    fn test_extract_config_file_path_patterns() {
        // Test different property name patterns
        let test_cases = vec![
            ("config_file", "path1.yaml"),
            ("source.config_file", "path2.yaml"),
            ("sink.config_file", "path3.yaml"),
            ("my_topic.config_file", "path4.yaml"),
        ];

        for (prop_name, expected_path) in test_cases {
            let mut props = HashMap::new();
            props.insert(prop_name.to_string(), expected_path.to_string());

            let result = extract_config_file_path(&props);
            assert_eq!(
                result,
                Some(expected_path.to_string()),
                "Should extract from '{}'",
                prop_name
            );
        }
    }

    #[test]
    #[should_panic(expected = "CONFIGURATION ERROR")]
    fn test_merge_fails_fast_on_missing_file() {
        let mut props = HashMap::new();
        props.insert(
            "config_file".to_string(),
            "nonexistent_file.yaml".to_string(),
        );

        // Should panic with detailed error message
        merge_config_file_properties(&props, "test");
    }
}
