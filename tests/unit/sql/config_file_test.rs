//! Tests for named source/sink config_file functionality

use ferrisstreams::ferris::sql::query_analyzer::QueryAnalyzer;
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_named_source_config_file() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("kafka_source_config.yaml");

    // Create a simple YAML config file
    fs::write(
        &config_path,
        r#"
type: "kafka_source"
bootstrap.servers: "kafka-prod:9092"
topic: "orders"
group.id: "analytics"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new();
    let mut config = HashMap::new();

    // Configure named source with config_file
    config.insert(
        "orders_source.type".to_string(),
        "kafka_source".to_string(),
    );
    config.insert(
        "orders_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Test source analysis with config_file
    let result = analyzer.analyze_source(
        "orders_source",
        &config,
        &ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default(),
        &mut analysis,
    );

    match result {
        Ok(_) => {
            println!("✅ Successfully analyzed source with config_file");
            assert_eq!(analysis.required_sources.len(), 1);

            let source = &analysis.required_sources[0];
            assert_eq!(source.name, "orders_source");
            assert_eq!(
                source.source_type,
                ferrisstreams::ferris::sql::query_analyzer::DataSourceType::Kafka
            );

            // Verify properties were loaded from YAML
            assert_eq!(
                source.properties.get("bootstrap.servers"),
                Some(&"kafka-prod:9092".to_string())
            );
            assert_eq!(source.properties.get("topic"), Some(&"orders".to_string()));
            assert_eq!(
                source.properties.get("group.id"),
                Some(&"analytics".to_string())
            );
            assert_eq!(
                source.properties.get("value.format"),
                Some(&"avro".to_string())
            );
            assert_eq!(
                source.properties.get("schema.registry.url"),
                Some(&"http://schema-registry:8081".to_string())
            );
        }
        Err(e) => {
            panic!("Failed to analyze source with config_file: {}", e);
        }
    }
}

#[test]
fn test_named_sink_config_file() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("file_sink_config.yaml");

    // Create a simple YAML config file for file sink
    fs::write(
        &config_path,
        r#"
type: "file_sink"
path: "/var/log/processed_orders.json"
format: "jsonlines"
append: true
compression: "gzip"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new();
    let mut config = HashMap::new();

    // Configure named sink with config_file
    config.insert("output_sink.type".to_string(), "file_sink".to_string());
    config.insert(
        "output_sink.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Test sink analysis with config_file
    let result = analyzer.analyze_sink(
        "output_sink",
        &config,
        &ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default(),
        &mut analysis,
    );

    match result {
        Ok(_) => {
            println!("✅ Successfully analyzed sink with config_file");
            assert_eq!(analysis.required_sinks.len(), 1);

            let sink = &analysis.required_sinks[0];
            assert_eq!(sink.name, "output_sink");
            assert_eq!(
                sink.sink_type,
                ferrisstreams::ferris::sql::query_analyzer::DataSinkType::File
            );

            // Verify properties were loaded from YAML
            assert_eq!(
                sink.properties.get("path"),
                Some(&"/var/log/processed_orders.json".to_string())
            );
            assert_eq!(
                sink.properties.get("format"),
                Some(&"jsonlines".to_string())
            );
            assert_eq!(sink.properties.get("append"), Some(&"true".to_string()));
            assert_eq!(
                sink.properties.get("compression"),
                Some(&"gzip".to_string())
            );
        }
        Err(e) => {
            panic!("Failed to analyze sink with config_file: {}", e);
        }
    }
}

#[test]
fn test_config_file_with_inline_override() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("kafka_config.yaml");

    // Create YAML config with base settings
    fs::write(
        &config_path,
        r#"
type: "kafka_source"
bootstrap.servers: "kafka-dev:9092"
topic: "dev-orders"
group.id: "dev-analytics"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new();
    let mut config = HashMap::new();

    // Configure with config_file and inline overrides
    config.insert("kafka_src.type".to_string(), "kafka_source".to_string());
    config.insert(
        "kafka_src.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );
    // Inline properties should override YAML
    config.insert(
        "kafka_src.bootstrap.servers".to_string(),
        "kafka-prod:9092".to_string(),
    );
    config.insert("kafka_src.topic".to_string(), "prod-orders".to_string());

    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "kafka_src",
        &config,
        &ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default(),
        &mut analysis,
    );

    match result {
        Ok(_) => {
            let source = &analysis.required_sources[0];

            // Inline properties should override YAML values
            assert_eq!(
                source.properties.get("bootstrap.servers"),
                Some(&"kafka-prod:9092".to_string())
            );
            assert_eq!(
                source.properties.get("topic"),
                Some(&"prod-orders".to_string())
            );
            // YAML-only values should still be present
            assert_eq!(
                source.properties.get("group.id"),
                Some(&"dev-analytics".to_string())
            );
        }
        Err(e) => {
            panic!("Failed to analyze source with config_file override: {}", e);
        }
    }
}

#[test]
fn test_invalid_config_file() {
    let analyzer = QueryAnalyzer::new();
    let mut config = HashMap::new();

    // Configure with non-existent config_file
    config.insert("bad_src.type".to_string(), "kafka_source".to_string());
    config.insert(
        "bad_src.config_file".to_string(),
        "nonexistent_config.yaml".to_string(),
    );

    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "bad_src",
        &config,
        &ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default(),
        &mut analysis,
    );

    // Should return an error for missing config file
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Failed to load config file"));
    assert!(error_msg.contains("nonexistent_config.yaml"));
}

#[test]
fn test_config_file_without_type_in_yaml() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("incomplete_config.yaml");

    // Create YAML config without type field (should be fine since type is specified in SQL)
    fs::write(
        &config_path,
        r#"
bootstrap.servers: "localhost:9092"
topic: "test-topic"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new();
    let mut config = HashMap::new();

    config.insert("test_src.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_src.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = ferrisstreams::ferris::sql::query_analyzer::QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_src",
        &config,
        &ferrisstreams::ferris::kafka::serialization_format::SerializationConfig::default(),
        &mut analysis,
    );

    // Should succeed - type comes from SQL, config_file provides other properties
    assert!(result.is_ok());
    let source = &analysis.required_sources[0];
    assert_eq!(
        source.source_type,
        ferrisstreams::ferris::sql::query_analyzer::DataSourceType::Kafka
    );
    assert_eq!(
        source.properties.get("bootstrap.servers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(
        source.properties.get("topic"),
        Some(&"test-topic".to_string())
    );
}