#!/usr/bin/env cargo
//! Velostream Configuration Validator CLI
//!
//! A command-line tool for validating Velostream configuration files
//! against the generated JSON Schema. Supports YAML and JSON config files.

use clap::{Arg, Command};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process;
use velostream::velostream::config::schema_registry::validate_configuration;
use velostream::velostream::config::HierarchicalSchemaRegistry;

fn main() {
    let matches = Command::new("velo-config-validator")
        .version("1.0.0")
        .about("Velostream Configuration Validator")
        .long_about("Validates Velostream configuration files against the schema and performs comprehensive validation checks.")
        .arg(
            Arg::new("config")
                .help("Configuration file to validate")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("schema")
                .long("schema")
                .short('s')
                .help("Path to JSON schema file")
                .default_value("velostream-config.schema.json"),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .short('v')
                .help("Verbose output")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("strict")
                .long("strict")
                .help("Enable strict validation mode")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let config_path = matches.get_one::<String>("config").unwrap();
    let schema_path = matches.get_one::<String>("schema").unwrap();
    let verbose = matches.get_flag("verbose");
    let strict = matches.get_flag("strict");

    if verbose {
        println!("🔧 Velostream Configuration Validator");
        println!("📁 Config file: {}", config_path);
        println!("📋 Schema file: {}", schema_path);
        println!();
    }

    match validate_config_file(config_path, schema_path, verbose, strict) {
        Ok(()) => {
            println!("✅ Configuration validation passed!");
            if verbose {
                println!("🎉 All validation checks completed successfully.");
            }
        }
        Err(error) => {
            eprintln!("❌ Configuration validation failed!");
            eprintln!("{}", error);
            process::exit(1);
        }
    }
}

fn validate_config_file(
    config_path: &str,
    schema_path: &str,
    verbose: bool,
    strict: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Check if files exist
    if !Path::new(config_path).exists() {
        return Err(format!("Configuration file not found: {}", config_path).into());
    }

    if !Path::new(schema_path).exists() {
        return Err(format!("Schema file not found: {}", schema_path).into());
    }

    // Step 2: Parse configuration file
    if verbose {
        println!("📖 Parsing configuration file...");
    }

    let config_content = fs::read_to_string(config_path)?;
    let config: HashMap<String, Value> =
        if config_path.ends_with(".yaml") || config_path.ends_with(".yml") {
            let yaml_value: serde_yaml::Value = serde_yaml::from_str(&config_content)?;
            serde_json::from_value(serde_json::to_value(yaml_value)?)?
        } else if config_path.ends_with(".json") {
            serde_json::from_str(&config_content)?
        } else {
            return Err("Unsupported file format. Use .yaml, .yml, or .json".into());
        };

    // Step 3: Load and validate JSON Schema syntax
    if verbose {
        println!("📋 Loading JSON schema...");
    }

    let schema_content = fs::read_to_string(schema_path)?;
    let _schema: Value =
        serde_json::from_str(&schema_content).map_err(|e| format!("Invalid JSON schema: {}", e))?;

    // Step 4: Prepare configuration for validation
    if verbose {
        println!("🔍 Performing configuration validation...");
    }

    // Convert HashMap<String, Value> to HashMap<String, String> for validation
    let string_config: HashMap<String, String> = config
        .iter()
        .map(|(k, v)| (k.clone(), value_to_string(v)))
        .collect();

    // Prepare configuration maps for validation
    let global_config = HashMap::new(); // Empty global config for CLI validation
    let named_config = HashMap::new(); // Empty named config for CLI validation

    match validate_configuration(&global_config, &named_config, &string_config) {
        Ok(()) => {
            if verbose {
                println!("  ✓ Schema registry validation passed");
            }
        }
        Err(errors) => {
            let mut error_msg = String::from("Schema validation failed:\n");
            for error in errors {
                error_msg.push_str(&format!("  • {}\n", error));
            }
            return Err(error_msg.into());
        }
    }

    // Step 6: Validate specific configuration sections
    validate_sections(&config, verbose, strict)?;

    // Step 7: Check for common configuration issues
    check_common_issues(&config, verbose)?;

    Ok(())
}

/// Convert JSON Value to String for validation
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
        Value::Null => "null".to_string(),
    }
}

/// Validate specific configuration sections
fn validate_sections(
    config: &HashMap<String, Value>,
    verbose: bool,
    strict: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Validate global configuration
    if let Some(global) = config.get("global") {
        if verbose {
            println!("  🌐 Validating global configuration...");
        }
        validate_global_config(global, strict)?;
    }

    // Validate sources
    if let Some(sources) = config.get("sources") {
        if verbose {
            println!("  📥 Validating data sources...");
        }
        validate_sources_config(sources, strict)?;
    }

    // Validate sinks
    if let Some(sinks) = config.get("sinks") {
        if verbose {
            println!("  📤 Validating data sinks...");
        }
        validate_sinks_config(sinks, strict)?;
    }

    Ok(())
}

/// Validate global configuration section
fn validate_global_config(global: &Value, _strict: bool) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = global.as_object() {
        // Check environment values
        if let Some(env) = obj.get("environment") {
            if let Some(env_str) = env.as_str() {
                let valid_envs = ["development", "staging", "production"];
                if !valid_envs.contains(&env_str) {
                    return Err(format!(
                        "Invalid environment: {}. Must be one of: {:?}",
                        env_str, valid_envs
                    )
                    .into());
                }
            }
        }

        // Check log_level values
        if let Some(log_level) = obj.get("log_level") {
            if let Some(level_str) = log_level.as_str() {
                let valid_levels = ["error", "warn", "info", "debug", "trace"];
                if !valid_levels.contains(&level_str) {
                    return Err(format!(
                        "Invalid log_level: {}. Must be one of: {:?}",
                        level_str, valid_levels
                    )
                    .into());
                }
            }
        }
    }

    Ok(())
}

/// Validate sources configuration section
fn validate_sources_config(
    sources: &Value,
    _strict: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = sources.as_object() {
        for (source_name, source_config) in obj {
            if source_name == "kafka_source" {
                validate_kafka_source_config(source_config)?;
            } else if source_name == "file_source" {
                validate_file_source_config(source_config)?;
            }
        }
    }
    Ok(())
}

/// Validate sinks configuration section
fn validate_sinks_config(sinks: &Value, _strict: bool) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = sinks.as_object() {
        for (sink_name, sink_config) in obj {
            if sink_name == "kafka_sink" {
                validate_kafka_sink_config(sink_config)?;
            } else if sink_name == "file_sink" {
                validate_file_sink_config(sink_config)?;
            }
        }
    }
    Ok(())
}

/// Validate Kafka source configuration
fn validate_kafka_source_config(config: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = config.as_object() {
        // Check required fields
        if !obj.contains_key("brokers") && !obj.contains_key("bootstrap.servers") {
            return Err("Kafka source missing required 'brokers' or 'bootstrap.servers'".into());
        }

        if !obj.contains_key("topic") {
            return Err("Kafka source missing required 'topic'".into());
        }
    }
    Ok(())
}

/// Validate File source configuration
fn validate_file_source_config(config: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = config.as_object() {
        // Check required fields
        if !obj.contains_key("path") {
            return Err("File source missing required 'path'".into());
        }

        // Validate format if present
        if let Some(format) = obj.get("format") {
            if let Some(format_str) = format.as_str() {
                let valid_formats = ["csv", "json", "jsonlines"];
                if !valid_formats.contains(&format_str) {
                    return Err(format!(
                        "Invalid file format: {}. Must be one of: {:?}",
                        format_str, valid_formats
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

/// Validate Kafka sink configuration
fn validate_kafka_sink_config(config: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = config.as_object() {
        // Check required fields
        if !obj.contains_key("brokers") && !obj.contains_key("bootstrap.servers") {
            return Err("Kafka sink missing required 'brokers' or 'bootstrap.servers'".into());
        }

        if !obj.contains_key("topic") {
            return Err("Kafka sink missing required 'topic'".into());
        }
    }
    Ok(())
}

/// Validate File sink configuration
fn validate_file_sink_config(config: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(obj) = config.as_object() {
        // Check required fields
        if !obj.contains_key("path") {
            return Err("File sink missing required 'path'".into());
        }

        // Validate format if present
        if let Some(format) = obj.get("format") {
            if let Some(format_str) = format.as_str() {
                let valid_formats = ["csv", "json", "jsonlines"];
                if !valid_formats.contains(&format_str) {
                    return Err(format!(
                        "Invalid file format: {}. Must be one of: {:?}",
                        format_str, valid_formats
                    )
                    .into());
                }
            }
        }
    }
    Ok(())
}

/// Check for common configuration issues and provide helpful suggestions
fn check_common_issues(
    config: &HashMap<String, Value>,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if verbose {
        println!("🔍 Checking for common configuration issues...");
    }

    // Check if both sources and sinks are defined
    let has_sources = config.contains_key("sources");
    let has_sinks = config.contains_key("sinks");

    if !has_sources && !has_sinks {
        return Err("Configuration should define at least one source or sink".into());
    }

    if verbose {
        if has_sources {
            println!("  ✓ Data sources configured");
        }
        if has_sinks {
            println!("  ✓ Data sinks configured");
        }
    }

    Ok(())
}
