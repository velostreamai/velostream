//! Comprehensive tests for config_file functionality to ensure exhaustive coverage

use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;
use velostream::velostream::kafka::serialization_format::SerializationConfig;
use velostream::velostream::sql::query_analyzer::{QueryAnalysis, QueryAnalyzer};

#[test]
fn test_extends_functionality() {
    let temp_dir = TempDir::new().unwrap();

    // Create base config
    let base_config_path = temp_dir.path().join("base_kafka.yaml");
    fs::write(
        &base_config_path,
        r#"
bootstrap.servers: "base-kafka:9092"
compression.type: "snappy"
failure_strategy: "RetryWithBackoff"
max_retries: 3
"#,
    )
    .unwrap();

    // Create extending config
    let source_config_path = temp_dir.path().join("orders_source.yaml");
    fs::write(
        &source_config_path,
        r#"
extends: "base_kafka.yaml"
topic: "orders"
group.id: "analytics"
value.format: "avro"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("orders_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "orders_source.config_file".to_string(),
        source_config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "orders_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_ok());
    let source = &analysis.required_sources[0];

    // Should have base config properties
    assert_eq!(
        source.properties.get("bootstrap.servers"),
        Some(&"base-kafka:9092".to_string())
    );
    assert_eq!(
        source.properties.get("compression.type"),
        Some(&"snappy".to_string())
    );
    assert_eq!(source.properties.get("max_retries"), Some(&"3".to_string()));

    // Should have extending config properties
    assert_eq!(source.properties.get("topic"), Some(&"orders".to_string()));
    assert_eq!(
        source.properties.get("group.id"),
        Some(&"analytics".to_string())
    );
    assert_eq!(
        source.properties.get("value.format"),
        Some(&"avro".to_string())
    );
}

#[test]
fn test_circular_dependency_detection() {
    let temp_dir = TempDir::new().unwrap();

    let config_a_path = temp_dir.path().join("config_a.yaml");
    fs::write(
        &config_a_path,
        r#"
extends: "config_b.yaml"
setting_a: "value_a"
"#,
    )
    .unwrap();

    let config_b_path = temp_dir.path().join("config_b.yaml");
    fs::write(
        &config_b_path,
        r#"
extends: "config_a.yaml"
setting_b: "value_b"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_a_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    // Should detect circular dependency and return error
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Failed to load config file") || error_msg.contains("circular"));
}

#[test]
fn test_malformed_yaml() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("malformed.yaml");

    fs::write(
        &config_path,
        r#"
invalid: yaml: content:
  - missing
    - proper
  - indentation
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Failed to load config file"));
}

#[test]
fn test_all_data_types_in_yaml() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("all_types.yaml");

    fs::write(
        &config_path,
        r#"
string_value: "hello"
integer_value: 42
float_value: 3.14
boolean_true: true
boolean_false: false
null_value: null
array_value: [1, 2, 3]
nested_object:
  key: "nested_value"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_ok());
    let source = &analysis.required_sources[0];

    // Check all data types are properly converted to strings
    assert_eq!(
        source.properties.get("string_value"),
        Some(&"hello".to_string())
    );
    assert_eq!(
        source.properties.get("integer_value"),
        Some(&"42".to_string())
    );
    assert_eq!(
        source.properties.get("float_value"),
        Some(&"3.14".to_string())
    );
    assert_eq!(
        source.properties.get("boolean_true"),
        Some(&"true".to_string())
    );
    assert_eq!(
        source.properties.get("boolean_false"),
        Some(&"false".to_string())
    );
    assert_eq!(
        source.properties.get("null_value"),
        Some(&"null".to_string())
    );

    // Complex types are flattened with indices/dot notation
    // Arrays become array_value[0], array_value[1], array_value[2]
    assert_eq!(
        source.properties.get("array_value[0]"),
        Some(&"1".to_string())
    );
    assert_eq!(
        source.properties.get("array_value[1]"),
        Some(&"2".to_string())
    );
    assert_eq!(
        source.properties.get("array_value[2]"),
        Some(&"3".to_string())
    );
    // Nested objects become nested_object.key
    assert_eq!(
        source.properties.get("nested_object.key"),
        Some(&"nested_value".to_string())
    );
}

#[test]
fn test_multiple_sources_different_config_files() {
    let temp_dir = TempDir::new().unwrap();

    // Create first source config
    let source1_path = temp_dir.path().join("source1.yaml");
    fs::write(
        &source1_path,
        r#"
bootstrap.servers: "kafka1:9092"
topic: "topic1"
group.id: "group1"
"#,
    )
    .unwrap();

    // Create second source config
    let source2_path = temp_dir.path().join("source2.yaml");
    fs::write(
        &source2_path,
        r#"
bootstrap.servers: "kafka2:9092"
topic: "topic2"
group.id: "group2"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Configure first source
    config.insert("source1.type".to_string(), "kafka_source".to_string());
    config.insert(
        "source1.config_file".to_string(),
        source1_path.to_string_lossy().to_string(),
    );

    // Configure second source
    config.insert("source2.type".to_string(), "kafka_source".to_string());
    config.insert(
        "source2.config_file".to_string(),
        source2_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Analyze first source
    let result1 = analyzer.analyze_source(
        "source1",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );
    assert!(result1.is_ok());

    // Analyze second source
    let result2 = analyzer.analyze_source(
        "source2",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );
    assert!(result2.is_ok());

    // Should have two different sources with different configs
    assert_eq!(analysis.required_sources.len(), 2);

    let source1 = &analysis.required_sources[0];
    let source2 = &analysis.required_sources[1];

    assert_eq!(
        source1.properties.get("bootstrap.servers"),
        Some(&"kafka1:9092".to_string())
    );
    assert_eq!(source1.properties.get("topic"), Some(&"topic1".to_string()));

    assert_eq!(
        source2.properties.get("bootstrap.servers"),
        Some(&"kafka2:9092".to_string())
    );
    assert_eq!(source2.properties.get("topic"), Some(&"topic2".to_string()));
}

#[test]
fn test_missing_type_field_error() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("test.yaml");
    fs::write(&config_path, "topic: test").unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    // Missing .type field
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    // Should fail because type is required
    assert!(result.is_err());
}

#[test]
fn test_config_file_only_no_inline_properties() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("complete.yaml");

    fs::write(
        &config_path,
        r#"
bootstrap.servers: "yaml-kafka:9092"
topic: "yaml-topic"
group.id: "yaml-group"
value.format: "json"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_ok());
    let source = &analysis.required_sources[0];

    // All properties should come from YAML file
    assert_eq!(
        source.properties.get("bootstrap.servers"),
        Some(&"yaml-kafka:9092".to_string())
    );
    assert_eq!(
        source.properties.get("topic"),
        Some(&"yaml-topic".to_string())
    );
    assert_eq!(
        source.properties.get("group.id"),
        Some(&"yaml-group".to_string())
    );
    assert_eq!(
        source.properties.get("value.format"),
        Some(&"json".to_string())
    );
}

#[test]
fn test_environment_variable_substitution_in_config_file() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("env_config.yaml");

    // Note: This test would require actual environment variable support in the YAML loader
    // For now, we test that the YAML loads the literal strings
    fs::write(
        &config_path,
        r#"
bootstrap.servers: "${KAFKA_BROKERS:-localhost:9092}"
topic: "${KAFKA_TOPIC:-default-topic}"
group.id: "${KAFKA_GROUP:-default-group}"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_ok());
    let source = &analysis.required_sources[0];

    // Properties should contain the literal environment variable strings
    // (actual substitution would happen at runtime)
    assert!(source.properties.get("bootstrap.servers").is_some());
    assert!(source.properties.get("topic").is_some());
    assert!(source.properties.get("group.id").is_some());
}

#[test]
fn test_sink_and_source_same_config_structure() {
    let temp_dir = TempDir::new().unwrap();

    let source_config_path = temp_dir.path().join("source.yaml");
    fs::write(
        &source_config_path,
        r#"
bootstrap.servers: "source-kafka:9092"
topic: "source-topic"
"#,
    )
    .unwrap();

    let sink_config_path = temp_dir.path().join("sink.yaml");
    fs::write(
        &sink_config_path,
        r#"
bootstrap.servers: "sink-kafka:9092"
topic: "sink-topic"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        source_config_path.to_string_lossy().to_string(),
    );
    config.insert("test_sink.type".to_string(), "kafka_sink".to_string());
    config.insert(
        "test_sink.config_file".to_string(),
        sink_config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Test source
    let source_result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );
    assert!(source_result.is_ok());

    // Test sink
    let sink_result = analyzer.analyze_sink(
        "test_sink",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );
    assert!(sink_result.is_ok());

    // Verify both loaded correctly
    assert_eq!(analysis.required_sources.len(), 1);
    assert_eq!(analysis.required_sinks.len(), 1);

    let source = &analysis.required_sources[0];
    let sink = &analysis.required_sinks[0];

    assert_eq!(
        source.properties.get("bootstrap.servers"),
        Some(&"source-kafka:9092".to_string())
    );
    assert_eq!(
        sink.properties.get("bootstrap.servers"),
        Some(&"sink-kafka:9092".to_string())
    );
}

#[test]
fn test_nested_topic_name_is_normalized() {
    // Tests that YAML files using `topic.name` nested structure are properly
    // normalized to a flat `topic` key for consistent access.
    // This is the structure used in demo/trading configs like market_data_ts_sink.yaml:
    //   topic:
    //     name: "market_data_ts"

    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("nested_topic.yaml");

    // Use nested topic.name structure (as in production configs)
    fs::write(
        &config_path,
        r#"
bootstrap.servers: "kafka:9092"
topic:
  name: "my_custom_topic"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_sink.type".to_string(), "kafka_sink".to_string());
    config.insert(
        "test_sink.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_sink(
        "test_sink",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_ok());
    let sink = &analysis.required_sinks[0];

    // The nested "topic.name" should be normalized to "topic"
    assert_eq!(
        sink.properties.get("topic"),
        Some(&"my_custom_topic".to_string()),
        "Expected topic.name to be normalized to topic key"
    );

    // The original "topic.name" key should also be present from YAML flattening
    assert_eq!(
        sink.properties.get("topic.name"),
        Some(&"my_custom_topic".to_string()),
        "Original topic.name key should still be available"
    );
}

#[test]
fn test_nested_topic_name_for_source() {
    // Same test for source (e.g., market_data_ts_source.yaml)
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("nested_source_topic.yaml");

    fs::write(
        &config_path,
        r#"
bootstrap.servers: "kafka:9092"
topic:
  name: "source_topic_name"
group.id: "test-consumers"
"#,
    )
    .unwrap();

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();
    config.insert("test_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "test_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    let result = analyzer.analyze_source(
        "test_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    );

    assert!(result.is_ok());
    let source = &analysis.required_sources[0];

    // The nested "topic.name" should be normalized to "topic"
    assert_eq!(
        source.properties.get("topic"),
        Some(&"source_topic_name".to_string()),
        "Expected topic.name to be normalized to topic key for source"
    );
}
