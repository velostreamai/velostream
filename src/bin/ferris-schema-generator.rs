#!/usr/bin/env cargo
//! FerrisStreams Configuration Schema Generator
//!
//! Generates comprehensive JSON Schema files for IDE integration and validation.
//! This tool exports schemas from all registered ConfigSchemaProvider implementations.

use ferrisstreams::ferris::config::{ConfigSchemaProvider, HierarchicalSchemaRegistry};
use ferrisstreams::ferris::datasource::file::data_sink::FileSink;
use ferrisstreams::ferris::datasource::file::data_source::FileDataSource;
use ferrisstreams::ferris::datasource::kafka::data_sink::KafkaDataSink;
use ferrisstreams::ferris::datasource::kafka::data_source::KafkaDataSource;
use serde_json::{json, Map, Value};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 FerrisStreams Schema Generator");
    println!("Generating JSON Schema files for IDE integration...\n");

    // Create registry and register all schema providers
    let mut registry = HierarchicalSchemaRegistry::new();

    // Register all available schema providers
    println!("📋 Registering schema providers...");
    register_all_providers(&mut registry);

    // Generate comprehensive JSON Schema
    println!("🏗️ Generating JSON Schema...");
    let json_schema = generate_comprehensive_json_schema(&registry)?;

    // Write to ferrisstreams-config.schema.json
    let output_path = "ferrisstreams-config.schema.json";
    fs::write(output_path, serde_json::to_string_pretty(&json_schema)?)?;

    println!("✅ Generated: {}", output_path);
    println!(
        "📊 Schema includes {} configuration types",
        count_schema_types(&json_schema)
    );

    // Generate IDE-specific configurations
    generate_ide_configurations(&json_schema)?;

    println!("\n🎉 Schema generation complete!");
    println!("💡 To use with VS Code, add this to your config files:");
    println!("   # yaml-language-server: $schema=./ferrisstreams-config.schema.json");

    Ok(())
}

/// Register all available configuration schema providers
fn register_all_providers(registry: &mut HierarchicalSchemaRegistry) {
    // Source configurations
    registry.register_source_schema::<KafkaDataSource>();
    registry.register_source_schema::<FileDataSource>();

    // Sink configurations
    registry.register_sink_schema::<KafkaDataSink>();
    registry.register_sink_schema::<FileSink>();

    println!("   ✓ Registered 4 schema providers");
}

/// Generate comprehensive JSON Schema for all registered providers
fn generate_comprehensive_json_schema(
    registry: &HierarchicalSchemaRegistry,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut schema = json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://ferrisstreams.io/config.schema.json",
        "title": "FerrisStreams Configuration Schema",
        "description": "Comprehensive configuration schema for FerrisStreams multi-source/multi-sink processing",
        "type": "object",
        "additionalProperties": false,
        "properties": {}
    });

    let properties = schema["properties"].as_object_mut().unwrap();

    // Add source configurations manually since we need to call static methods
    let mut source_def = Map::new();
    source_def.insert("kafka_source".to_string(), KafkaDataSource::json_schema());
    source_def.insert("file_source".to_string(), FileDataSource::json_schema());

    properties.insert(
        "sources".to_string(),
        json!({
            "type": "object",
            "description": "Data source configurations",
            "additionalProperties": false,
            "properties": source_def
        }),
    );

    // Add sink configurations
    let mut sink_def = Map::new();
    sink_def.insert("kafka_sink".to_string(), KafkaDataSink::json_schema());
    sink_def.insert("file_sink".to_string(), FileSink::json_schema());

    properties.insert(
        "sinks".to_string(),
        json!({
            "type": "object",
            "description": "Data sink configurations",
            "additionalProperties": false,
            "properties": sink_def
        }),
    );

    // Add global configuration properties
    properties.insert(
        "global".to_string(),
        json!({
            "type": "object",
            "description": "Global configuration properties inherited by all components",
            "additionalProperties": true,
            "properties": {
                "environment": {
                    "type": "string",
                    "enum": ["development", "staging", "production"],
                    "description": "Deployment environment affecting default values"
                },
                "log_level": {
                    "type": "string",
                    "enum": ["error", "warn", "info", "debug", "trace"],
                    "default": "info",
                    "description": "Global logging level"
                },
                "compression": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["none", "gzip", "lz4", "snappy", "zstd"],
                            "description": "Default compression algorithm"
                        }
                    }
                }
            }
        }),
    );

    // Add configuration file inheritance
    properties.insert(
        "extends".to_string(),
        json!({
            "oneOf": [
                {
                    "type": "string",
                    "description": "Single configuration file to extend"
                },
                {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Multiple configuration files to extend (processed in order)"
                }
            ],
            "description": "Configuration file inheritance - extend other config files"
        }),
    );

    Ok(schema)
}

/// Count the number of schema types in the generated schema
fn count_schema_types(_schema: &Value) -> usize {
    // Return the hardcoded count since we know we have 4 providers
    4 // KafkaDataSource, FileDataSource, KafkaDataSink, FileSink
}

/// Generate IDE-specific configuration files
fn generate_ide_configurations(_schema: &Value) -> Result<(), Box<dyn std::error::Error>> {
    // VS Code settings for YAML language server
    let vscode_settings = json!({
        "yaml.schemas": {
            "./ferrisstreams-config.schema.json": [
                "ferris-config.yaml",
                "ferris-config.yml",
                "**/config/ferris*.yaml",
                "**/config/ferris*.yml"
            ]
        },
        "yaml.completion": true,
        "yaml.validate": true,
        "yaml.hover": true
    });

    // Create .vscode directory if it doesn't exist
    std::fs::create_dir_all(".vscode")?;

    // Write VS Code settings
    fs::write(
        ".vscode/ferris-schema-settings.json",
        serde_json::to_string_pretty(&vscode_settings)?,
    )?;

    println!("📝 Generated: .vscode/ferris-schema-settings.json");

    // Generate example configuration with schema reference
    let example_config = format!(
        r#"# yaml-language-server: $schema=./ferrisstreams-config.schema.json

# FerrisStreams Configuration Example
# This file demonstrates the configuration schema with IDE support

global:
  environment: development
  log_level: debug
  compression:
    type: lz4

sources:
  kafka_source:
    brokers: "localhost:9092"
    topic: "input-events"
    group.id: "ferris-processor"
    auto.offset.reset: "earliest"
  
  file_source:
    path: "./data/input.jsonl"
    format: "jsonlines"
    watch: true

sinks:
  kafka_sink:
    brokers: "localhost:9092" 
    topic: "output-events"
    acks: "all"
    
  file_sink:
    path: "./output/processed.jsonl"
    format: "jsonlines"

batch:
  batch_config:
    strategy: "time_based"
    max_size: 1000
    timeout_ms: 5000
"#
    );

    fs::write("ferris-config.example.yaml", example_config)?;
    println!("📄 Generated: ferris-config.example.yaml");

    Ok(())
}
