//! Tests for YAML configuration loader with extends support

use std::fs;
use tempfile::TempDir;
use velostream::velostream::sql::config::yaml_loader::{YamlConfigError, YamlConfigLoader};

#[test]
fn test_simple_config_loading() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("simple.yaml");

    fs::write(
        &config_path,
        r#"
name: "test_config"
value: 42
nested:
  key: "nested_value"
  array: [1, 2, 3]
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&config_path).unwrap();

    assert_eq!(result.metadata.inheritance_chain.len(), 1);
    assert_eq!(result.metadata.merge_count, 1);
    assert!(!result.metadata.has_circular_dependency);

    // Verify config content
    let config = &result.config;
    assert_eq!(config["name"], "test_config");
    assert_eq!(config["value"], 42);
    assert_eq!(config["nested"]["key"], "nested_value");
}

#[test]
fn test_extends_inheritance() {
    let temp_dir = TempDir::new().unwrap();

    // Create base configuration
    let base_path = temp_dir.path().join("base_kafka.yaml");
    fs::write(
        &base_path,
        r#"
datasource:
  type: "kafka"
  bootstrap.servers: "localhost:9092"
  consumer_config:
    auto.offset.reset: "latest"
    session_timeout_ms: 6000
schema:
  format: "avro"
  registry_url: "http://schema-registry:8081"
performance:
  buffer_size: 65536
"#,
    )
    .unwrap();

    // Create derived configuration
    let derived_path = temp_dir.path().join("market_data.yaml");
    fs::write(
        &derived_path,
        r#"
extends: base_kafka.yaml
topic:
  name: "market_data"
  partitions: 12
schema:
  key.field: "symbol"
performance:
  fetch.max.wait.ms: 10
metadata:
  description: "Market data feed"
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&derived_path).unwrap();

    assert_eq!(result.metadata.inheritance_chain.len(), 2);
    assert_eq!(result.metadata.merge_count, 2);

    // Verify merged configuration
    let config = &result.config;

    // Base values should be present
    assert_eq!(config["datasource"]["type"], "kafka");
    assert_eq!(config["datasource"]["bootstrap.servers"], "localhost:9092");
    assert_eq!(
        config["datasource"]["consumer_config"]["auto.offset.reset"],
        "latest"
    );

    // Derived values should be present
    assert_eq!(config["topic"]["name"], "market_data");
    assert_eq!(config["topic"]["partitions"], 12);
    assert_eq!(config["metadata"]["description"], "Market data feed");

    // Schema should be merged (base + derived)
    assert_eq!(config["schema"]["format"], "avro");
    assert_eq!(
        config["schema"]["registry_url"],
        "http://schema-registry:8081"
    );
    assert_eq!(config["schema"]["key.field"], "symbol");

    // Performance should be merged (base buffer_size + derived fetch.max.wait.ms)
    assert_eq!(config["performance"]["buffer_size"], 65536);
    assert_eq!(config["performance"]["fetch.max.wait.ms"], 10);
}

#[test]
fn test_multi_level_inheritance() {
    let temp_dir = TempDir::new().unwrap();

    // Create base configuration
    let base_path = temp_dir.path().join("common_kafka.yaml");
    fs::write(
        &base_path,
        r#"
datasource:
  type: "kafka"
  bootstrap.servers: "localhost:9092"
common_setting: "base_value"
"#,
    )
    .unwrap();

    // Create intermediate configuration
    let intermediate_path = temp_dir.path().join("kafka_source.yaml");
    fs::write(
        &intermediate_path,
        r#"
extends: common_kafka.yaml
datasource:
  consumer_config:
    auto.offset.reset: "latest"
intermediate_setting: "intermediate_value"
"#,
    )
    .unwrap();

    // Create final configuration
    let final_path = temp_dir.path().join("specific_topic.yaml");
    fs::write(
        &final_path,
        r#"
extends: kafka_source.yaml
topic:
  name: "specific_topic"
final_setting: "final_value"
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&final_path).unwrap();

    assert_eq!(result.metadata.inheritance_chain.len(), 3);
    assert_eq!(result.metadata.merge_count, 3);

    // Verify all levels are merged correctly
    let config = &result.config;
    assert_eq!(config["common_setting"], "base_value");
    assert_eq!(config["intermediate_setting"], "intermediate_value");
    assert_eq!(config["final_setting"], "final_value");
    assert_eq!(config["datasource"]["type"], "kafka");
    assert_eq!(
        config["datasource"]["consumer_config"]["auto.offset.reset"],
        "latest"
    );
    assert_eq!(config["topic"]["name"], "specific_topic");
}

#[test]
fn test_circular_dependency_detection() {
    let temp_dir = TempDir::new().unwrap();

    let a_path = temp_dir.path().join("config_a.yaml");
    fs::write(
        &a_path,
        r#"
extends: config_b.yaml
setting_a: "value_a"
"#,
    )
    .unwrap();

    let b_path = temp_dir.path().join("config_b.yaml");
    fs::write(
        &b_path,
        r#"
extends: config_a.yaml
setting_b: "value_b"
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&a_path);

    assert!(matches!(
        result,
        Err(YamlConfigError::CircularDependency { .. })
    ));
}

#[test]
fn test_file_not_found_error() {
    let temp_dir = TempDir::new().unwrap();
    let mut loader = YamlConfigLoader::new(temp_dir.path());

    let result = loader.load_config("nonexistent.yaml");
    assert!(matches!(result, Err(YamlConfigError::FileNotFound { .. })));
}

#[test]
fn test_invalid_yaml_error() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("invalid.yaml");

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

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&config_path);

    assert!(matches!(result, Err(YamlConfigError::ParseError { .. })));
}

#[test]
fn test_extends_path_resolution() {
    let temp_dir = TempDir::new().unwrap();
    let subdir = temp_dir.path().join("configs");
    fs::create_dir(&subdir).unwrap();

    // Create base config in subdirectory
    let base_path = subdir.join("base.yaml");
    fs::write(
        &base_path,
        r#"
base_setting: "base_value"
"#,
    )
    .unwrap();

    // Create derived config that extends relative path
    let derived_path = temp_dir.path().join("derived.yaml");
    fs::write(
        &derived_path,
        r#"
extends: configs/base.yaml
derived_setting: "derived_value"
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&derived_path).unwrap();

    assert_eq!(result.metadata.inheritance_chain.len(), 2);

    let config = &result.config;
    assert_eq!(config["base_setting"], "base_value");
    assert_eq!(config["derived_setting"], "derived_value");
}

#[test]
fn test_array_replacement_not_merge() {
    let temp_dir = TempDir::new().unwrap();

    // Base config with array
    let base_path = temp_dir.path().join("base.yaml");
    fs::write(
        &base_path,
        r#"
servers: ["server1", "server2"]
other_setting: "base"
"#,
    )
    .unwrap();

    // Derived config with different array
    let derived_path = temp_dir.path().join("derived.yaml");
    fs::write(
        &derived_path,
        r#"
extends: base.yaml
servers: ["new_server1", "new_server2", "new_server3"]
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&derived_path).unwrap();

    let config = &result.config;

    // Array should be completely replaced, not merged
    assert_eq!(config["servers"][0], "new_server1");
    assert_eq!(config["servers"][1], "new_server2");
    assert_eq!(config["servers"][2], "new_server3");
    assert_eq!(config["servers"].as_sequence().unwrap().len(), 3);

    // Other settings should still be inherited
    assert_eq!(config["other_setting"], "base");
}

#[test]
fn test_config_cache() {
    let temp_dir = TempDir::new().unwrap();

    let config_path = temp_dir.path().join("cached.yaml");
    fs::write(
        &config_path,
        r#"
cached_setting: "cached_value"
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());

    // Load config first time
    let result1 = loader.load_config(&config_path).unwrap();
    let (cache_size_1, _) = loader.cache_stats();

    // Load same config second time (should use cache)
    let result2 = loader.load_config(&config_path).unwrap();
    let (cache_size_2, _) = loader.cache_stats();

    // Results should be identical
    assert_eq!(result1.config, result2.config);

    // Cache size should not increase (same file cached)
    assert_eq!(cache_size_1, cache_size_2);
    assert_eq!(cache_size_1, 1);
}

#[test]
fn test_trading_system_config_structure() {
    let temp_dir = TempDir::new().unwrap();

    // Create common Kafka source config
    let common_source_path = temp_dir.path().join("common_kafka_source.yaml");
    fs::write(
        &common_source_path,
        r#"
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "broker:9092"
    auto.offset.reset: "latest"
    enable_auto_commit: true
    session_timeout_ms: 6000
  schema:
    key_format: string
    value.format: avro
    schema_registry_url: "http://schema-registry:8081"
  stream.config:
    replication_factor: 3
    cleanup_policy: "delete"

performance_profiles:
  ultra_low_latency:
    fetch.max.wait.ms: 10
    max_poll_records: 1000
    buffer_size: 65536
"#,
    )
    .unwrap();

    // Create market data topic config
    let market_data_path = temp_dir.path().join("market_data_ts_source.yaml");
    fs::write(
        &market_data_path,
        r#"
extends: common_kafka_source.yaml
topic:
  name: "market_data"
consumer.group: "trading_analytics_group"
performance_profile: ultra_low_latency
datasource:
  schema:
    key.field: symbol
    schema_file: "schemas/market_data.avsc"
topic_config:
  partitions: 12
  retention_ms: 86400000
metadata:
  description: "High-frequency trading market data feed"
  data_classification: "real_time_financial"
  update_frequency: "microseconds"
"#,
    )
    .unwrap();

    let mut loader = YamlConfigLoader::new(temp_dir.path());
    let result = loader.load_config(&market_data_path).unwrap();

    let config = &result.config;

    // Verify inheritance worked correctly
    assert_eq!(config["datasource"]["type"], "kafka");
    assert_eq!(
        config["datasource"]["consumer_config"]["bootstrap.servers"],
        "broker:9092"
    );
    assert_eq!(config["topic"]["name"], "market_data");
    assert_eq!(config["consumer.group"], "trading_analytics_group");
    assert_eq!(config["datasource"]["schema"]["key.field"], "symbol");
    assert_eq!(config["datasource"]["schema"]["value.format"], "avro"); // inherited
    assert_eq!(config["topic_config"]["partitions"], 12);
    assert_eq!(
        config["performance_profiles"]["ultra_low_latency"]["fetch.max.wait.ms"],
        10
    );
}
