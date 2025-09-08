use ferrisstreams::ferris::kafka::serialization_format::SerializationConfig;
use ferrisstreams::ferris::sql::query_analyzer::{
    DataSinkType, DataSourceType, QueryAnalysis, QueryAnalyzer,
};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing named source/sink config_file functionality...\n");

    // Test 1: Kafka Source with config_file
    test_kafka_source_config_file()?;

    // Test 2: File Sink with config_file
    test_file_sink_config_file()?;

    // Test 3: Config override test
    test_config_override()?;

    println!("\n✅ All config_file tests passed!");
    Ok(())
}

fn test_kafka_source_config_file() -> Result<(), Box<dyn std::error::Error>> {
    println!("Test 1: Kafka Source with config_file");

    let config_path = PathBuf::from("/tmp/kafka_source_test.yaml");

    // Create a YAML config file
    fs::write(
        &config_path,
        r#"
bootstrap.servers: "kafka-prod:9092"
topic: "orders"
group.id: "analytics"
value.format: "avro"
schema.registry.url: "http://schema-registry:8081"
"#,
    )?;

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Configure named source with config_file
    config.insert("orders_source.type".to_string(), "kafka_source".to_string());
    config.insert(
        "orders_source.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Analyze source
    analyzer.analyze_source(
        "orders_source",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    )?;

    // Verify results
    assert_eq!(analysis.required_sources.len(), 1);
    let source = &analysis.required_sources[0];
    assert_eq!(source.name, "orders_source");
    assert_eq!(source.source_type, DataSourceType::Kafka);
    assert_eq!(
        source.properties.get("bootstrap.servers"),
        Some(&"kafka-prod:9092".to_string())
    );
    assert_eq!(source.properties.get("topic"), Some(&"orders".to_string()));

    println!("   ✓ Successfully loaded Kafka source config from YAML file");
    Ok(())
}

fn test_file_sink_config_file() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 2: File Sink with config_file");

    let config_path = PathBuf::from("/tmp/file_sink_test.yaml");

    // Create a YAML config file
    fs::write(
        &config_path,
        r#"
path: "/var/log/processed_orders.json"
format: "jsonlines"
append: true
compression: "gzip"
"#,
    )?;

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Configure named sink with config_file
    config.insert("output_sink.type".to_string(), "file_sink".to_string());
    config.insert(
        "output_sink.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    // Analyze sink
    analyzer.analyze_sink(
        "output_sink",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    )?;

    // Verify results
    assert_eq!(analysis.required_sinks.len(), 1);
    let sink = &analysis.required_sinks[0];
    assert_eq!(sink.name, "output_sink");
    assert_eq!(sink.sink_type, DataSinkType::File);
    assert_eq!(
        sink.properties.get("path"),
        Some(&"/var/log/processed_orders.json".to_string())
    );
    assert_eq!(
        sink.properties.get("format"),
        Some(&"jsonlines".to_string())
    );

    println!("   ✓ Successfully loaded File sink config from YAML file");
    Ok(())
}

fn test_config_override() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nTest 3: Config file with inline overrides");

    let config_path = PathBuf::from("/tmp/base_kafka_test.yaml");

    // Create base YAML config
    fs::write(
        &config_path,
        r#"
bootstrap.servers: "kafka-dev:9092"
topic: "dev-orders"
group.id: "dev-analytics"
value.format: "json"
"#,
    )?;

    let analyzer = QueryAnalyzer::new("test_group".to_string());
    let mut config = HashMap::new();

    // Configure with config_file and inline overrides
    config.insert("kafka_src.type".to_string(), "kafka_source".to_string());
    config.insert(
        "kafka_src.config_file".to_string(),
        config_path.to_string_lossy().to_string(),
    );
    // These inline properties should override YAML values
    config.insert(
        "kafka_src.bootstrap.servers".to_string(),
        "kafka-prod:9092".to_string(),
    );
    config.insert("kafka_src.topic".to_string(), "prod-orders".to_string());

    let mut analysis = QueryAnalysis {
        required_sources: vec![],
        required_sinks: vec![],
        configuration: config.clone(),
    };

    analyzer.analyze_source(
        "kafka_src",
        &config,
        &SerializationConfig::default(),
        &mut analysis,
    )?;

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
    assert_eq!(
        source.properties.get("value.format"),
        Some(&"json".to_string())
    );

    println!("   ✓ Successfully verified config override behavior");
    Ok(())
}
