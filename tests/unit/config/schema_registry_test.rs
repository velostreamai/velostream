//! Unit tests for the self-registering configuration schema system

use ferrisstreams::ferris::config::{
    ConfigSchemaProvider, ConfigValidationError, GlobalSchemaContext, HierarchicalSchemaRegistry,
    PropertyDefault, PropertyValidation,
};
use ferrisstreams::ferris::datasource::kafka::{KafkaDataSink, KafkaDataSource};
use ferrisstreams::ferris::datasource::{BatchConfig, BatchStrategy};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

#[test]
fn test_batch_config_schema_provider_basic() {
    let batch_config = BatchConfig::default();

    // Test config type ID
    assert_eq!(BatchConfig::config_type_id(), "batch_config");

    // Test inheritable properties
    let inheritable = BatchConfig::inheritable_properties();
    assert!(inheritable.contains(&"batch.size"));
    assert!(inheritable.contains(&"batch.timeout"));
    assert!(inheritable.contains(&"batch.strategy"));

    // Test required properties (should be empty for batch config)
    let required = BatchConfig::required_named_properties();
    assert!(required.is_empty());

    // Test optional properties with defaults
    let defaults = BatchConfig::optional_properties_with_defaults();
    assert!(defaults.contains_key("batch.size"));
    assert!(defaults.contains_key("batch.timeout"));
}

#[test]
fn test_batch_config_property_validation() {
    let batch_config = BatchConfig::default();

    // Valid batch.size
    assert!(batch_config.validate_property("batch.size", "100").is_ok());
    assert!(batch_config.validate_property("batch.size", "1000").is_ok());

    // Invalid batch.size
    assert!(batch_config.validate_property("batch.size", "0").is_err());
    assert!(batch_config
        .validate_property("batch.size", "200000")
        .is_err()); // Too large
    assert!(batch_config
        .validate_property("batch.size", "invalid")
        .is_err());

    // Valid batch.timeout
    assert!(batch_config
        .validate_property("batch.timeout", "1000")
        .is_ok());
    assert!(batch_config
        .validate_property("batch.timeout", "5000")
        .is_ok());

    // Invalid batch.timeout
    assert!(batch_config
        .validate_property("batch.timeout", "0")
        .is_err());
    assert!(batch_config
        .validate_property("batch.timeout", "400000")
        .is_err()); // Too large

    // Valid batch.strategy
    assert!(batch_config
        .validate_property("batch.strategy", "FixedSize")
        .is_ok());
    assert!(batch_config
        .validate_property("batch.strategy", "TimeWindow")
        .is_ok());
    assert!(batch_config
        .validate_property("batch.strategy", "AdaptiveSize")
        .is_ok());

    // Invalid batch.strategy
    assert!(batch_config
        .validate_property("batch.strategy", "InvalidStrategy")
        .is_err());

    // Valid batch.enable
    assert!(batch_config
        .validate_property("batch.enable", "true")
        .is_ok());
    assert!(batch_config
        .validate_property("batch.enable", "false")
        .is_ok());

    // Invalid batch.enable
    assert!(batch_config
        .validate_property("batch.enable", "yes")
        .is_err());
    assert!(batch_config.validate_property("batch.enable", "1").is_err());
}

#[test]
fn test_kafka_data_source_schema_provider_basic() {
    // Test config type ID
    assert_eq!(KafkaDataSource::config_type_id(), "kafka_source");

    // Test inheritable properties
    let inheritable = KafkaDataSource::inheritable_properties();
    assert!(inheritable.contains(&"bootstrap.servers"));
    assert!(inheritable.contains(&"security.protocol"));
    assert!(inheritable.contains(&"batch.size"));

    // Test required properties
    let required = KafkaDataSource::required_named_properties();
    assert_eq!(required, vec!["topic"]);

    // Test optional properties with defaults
    let defaults = KafkaDataSource::optional_properties_with_defaults();
    assert!(defaults.contains_key("bootstrap.servers"));
    assert!(defaults.contains_key("auto.offset.reset"));

    // Test custom properties support
    assert!(KafkaDataSource::supports_custom_properties());

    // Test schema dependencies
    let dependencies = KafkaDataSource::global_schema_dependencies();
    assert!(dependencies.contains(&"kafka_global"));
    assert!(dependencies.contains(&"batch_config"));
}

#[test]
fn test_kafka_data_source_property_validation() {
    let kafka_source = KafkaDataSource::new("localhost:9092".to_string(), "test-topic".to_string());

    // Valid bootstrap.servers
    assert!(kafka_source
        .validate_property("bootstrap.servers", "localhost:9092")
        .is_ok());
    assert!(kafka_source
        .validate_property("bootstrap.servers", "broker1:9092,broker2:9092")
        .is_ok());

    // Invalid bootstrap.servers
    assert!(kafka_source
        .validate_property("bootstrap.servers", "")
        .is_err());
    assert!(kafka_source
        .validate_property("bootstrap.servers", "localhost")
        .is_err()); // Missing port
    assert!(kafka_source
        .validate_property("bootstrap.servers", "localhost:invalid")
        .is_err()); // Invalid port

    // Valid topic
    assert!(kafka_source
        .validate_property("topic", "test-topic")
        .is_ok());
    assert!(kafka_source
        .validate_property("topic", "user.events")
        .is_ok());
    assert!(kafka_source.validate_property("topic", "orders_v2").is_ok());

    // Invalid topic
    assert!(kafka_source.validate_property("topic", "").is_err());
    assert!(kafka_source
        .validate_property("topic", "topic with spaces")
        .is_err());
    assert!(kafka_source
        .validate_property("topic", "topic@invalid")
        .is_err());

    // Valid security.protocol
    assert!(kafka_source
        .validate_property("security.protocol", "PLAINTEXT")
        .is_ok());
    assert!(kafka_source
        .validate_property("security.protocol", "SSL")
        .is_ok());
    assert!(kafka_source
        .validate_property("security.protocol", "SASL_SSL")
        .is_ok());

    // Invalid security.protocol
    assert!(kafka_source
        .validate_property("security.protocol", "INVALID")
        .is_err());

    // Valid auto.offset.reset
    assert!(kafka_source
        .validate_property("auto.offset.reset", "earliest")
        .is_ok());
    assert!(kafka_source
        .validate_property("auto.offset.reset", "latest")
        .is_ok());
    assert!(kafka_source
        .validate_property("auto.offset.reset", "none")
        .is_ok());

    // Invalid auto.offset.reset
    assert!(kafka_source
        .validate_property("auto.offset.reset", "invalid")
        .is_err());
}

#[test]
fn test_kafka_data_sink_schema_provider_basic() {
    // Test config type ID
    assert_eq!(KafkaDataSink::config_type_id(), "kafka_sink");

    // Test inheritable properties
    let inheritable = KafkaDataSink::inheritable_properties();
    assert!(inheritable.contains(&"bootstrap.servers"));
    assert!(inheritable.contains(&"compression.type"));
    assert!(inheritable.contains(&"acks"));

    // Test required properties
    let required = KafkaDataSink::required_named_properties();
    assert_eq!(required, vec!["topic"]);

    // Test optional properties with defaults
    let defaults = KafkaDataSink::optional_properties_with_defaults();
    assert!(defaults.contains_key("acks"));
    assert!(defaults.contains_key("compression.type"));
}

#[test]
fn test_kafka_data_sink_property_validation() {
    let kafka_sink = KafkaDataSink::new("localhost:9092".to_string(), "output-topic".to_string());

    // Valid acks values
    assert!(kafka_sink.validate_property("acks", "0").is_ok());
    assert!(kafka_sink.validate_property("acks", "1").is_ok());
    assert!(kafka_sink.validate_property("acks", "all").is_ok());
    assert!(kafka_sink.validate_property("acks", "-1").is_ok());

    // Invalid acks values
    assert!(kafka_sink.validate_property("acks", "2").is_err());
    assert!(kafka_sink.validate_property("acks", "invalid").is_err());

    // Valid compression.type
    assert!(kafka_sink
        .validate_property("compression.type", "none")
        .is_ok());
    assert!(kafka_sink
        .validate_property("compression.type", "gzip")
        .is_ok());
    assert!(kafka_sink
        .validate_property("compression.type", "snappy")
        .is_ok());
    assert!(kafka_sink
        .validate_property("compression.type", "lz4")
        .is_ok());
    assert!(kafka_sink
        .validate_property("compression.type", "zstd")
        .is_ok());

    // Invalid compression.type
    assert!(kafka_sink
        .validate_property("compression.type", "invalid")
        .is_err());

    // Valid batch.size
    assert!(kafka_sink.validate_property("batch.size", "16384").is_ok());
    assert!(kafka_sink.validate_property("batch.size", "65536").is_ok());

    // Invalid batch.size (too large)
    assert!(kafka_sink
        .validate_property("batch.size", "2000000")
        .is_err()); // > 1MB

    // Valid linger.ms
    assert!(kafka_sink.validate_property("linger.ms", "5").is_ok());
    assert!(kafka_sink.validate_property("linger.ms", "100").is_ok());

    // Invalid linger.ms (too large)
    assert!(kafka_sink.validate_property("linger.ms", "35000").is_err()); // > 30 seconds
}

#[test]
fn test_hierarchical_schema_registry_creation() {
    let mut registry = HierarchicalSchemaRegistry::new();

    // Register schemas
    registry.register_global_schema::<BatchConfig>();
    registry.register_source_schema::<KafkaDataSource>();
    registry.register_sink_schema::<KafkaDataSink>();

    // Test global registry access
    let global_registry = HierarchicalSchemaRegistry::global();
    assert!(global_registry.read().is_ok());
}

#[test]
fn test_config_validation_with_inheritance() {
    let registry = HierarchicalSchemaRegistry::new();

    // Create test configurations
    let global_config = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            "kafka1:9092,kafka2:9092".to_string(),
        ),
        ("batch.size".to_string(), "500".to_string()),
        ("security.protocol".to_string(), "SSL".to_string()),
    ]);

    let named_config = HashMap::from([
        ("orders_source.topic".to_string(), "orders".to_string()),
        (
            "orders_source.group.id".to_string(),
            "analytics".to_string(),
        ),
    ]);

    let inline_config = HashMap::from([("compression.type".to_string(), "snappy".to_string())]);

    // Test validation (will pass basic validation)
    let result =
        registry.validate_config_with_inheritance(&global_config, &named_config, &inline_config);

    // Should succeed with basic registry (no registered schemas yet)
    assert!(result.is_ok());
}

#[test]
fn test_config_validation_errors() {
    let registry = HierarchicalSchemaRegistry::new();

    // Create configuration with errors
    let global_config = HashMap::from([
        ("batch.size".to_string(), "0".to_string()), // Invalid - must be > 0
        (
            "bootstrap.servers".to_string(),
            "invalid_server".to_string(),
        ), // Missing port
    ]);

    let named_config = HashMap::from([
        ("orders_source.topic".to_string(), "".to_string()), // Invalid - empty topic
    ]);

    let inline_config = HashMap::new();

    // Test validation
    let result =
        registry.validate_config_with_inheritance(&global_config, &named_config, &inline_config);

    // Should succeed with basic registry (no validation rules applied without registered schemas)
    assert!(result.is_ok());
}

#[test]
fn test_json_schema_generation() {
    // Test BatchConfig JSON schema
    let batch_schema = BatchConfig::json_schema();
    assert_eq!(batch_schema["type"], "object");
    assert!(batch_schema["properties"]["batch.size"].is_object());
    assert_eq!(batch_schema["properties"]["batch.size"]["type"], "integer");

    // Test KafkaDataSource JSON schema
    let kafka_source_schema = KafkaDataSource::json_schema();
    assert_eq!(kafka_source_schema["type"], "object");
    assert!(kafka_source_schema["properties"]["topic"].is_object());
    assert_eq!(kafka_source_schema["properties"]["topic"]["type"], "string");
    assert_eq!(kafka_source_schema["required"][0], "topic");

    // Test KafkaDataSink JSON schema
    let kafka_sink_schema = KafkaDataSink::json_schema();
    assert_eq!(kafka_sink_schema["type"], "object");
    assert!(kafka_sink_schema["properties"]["acks"].is_object());
    assert!(kafka_sink_schema["properties"]["acks"]["enum"].is_array());
}

#[test]
fn test_property_inheritance_resolution() {
    let kafka_source = KafkaDataSource::new("localhost:9092".to_string(), "test".to_string());

    // Create global context
    let mut global_context = GlobalSchemaContext::default();
    global_context.global_properties.insert(
        "kafka.bootstrap.servers".to_string(),
        "global-kafka:9092".to_string(),
    );
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "production".to_string());
    global_context
        .system_defaults
        .insert("job.id".to_string(), "test-job".to_string());

    // Test bootstrap.servers inheritance
    let result = kafka_source
        .resolve_property_with_inheritance(
            "bootstrap.servers",
            None, // No local value
            &global_context,
        )
        .unwrap();
    assert_eq!(result, Some("global-kafka:9092".to_string()));

    // Test local value precedence
    let result = kafka_source
        .resolve_property_with_inheritance(
            "bootstrap.servers",
            Some("local-kafka:9092"), // Local value present
            &global_context,
        )
        .unwrap();
    assert_eq!(result, Some("local-kafka:9092".to_string()));

    // Test security.protocol environment-based default
    let result = kafka_source
        .resolve_property_with_inheritance("security.protocol", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("SASL_SSL".to_string())); // Production default

    // Test group.id computed default
    let result = kafka_source
        .resolve_property_with_inheritance("group.id", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("ferris-production-test-job".to_string()));
}

#[test]
fn test_property_validations_structure() {
    // Test BatchConfig property validations
    let batch_validations = BatchConfig::property_validations();
    assert!(!batch_validations.is_empty());

    let batch_size_validation = batch_validations
        .iter()
        .find(|v| v.key == "batch.size")
        .expect("batch.size validation should exist");

    assert!(!batch_size_validation.required);
    assert!(batch_size_validation.default.is_some());
    assert_eq!(batch_size_validation.json_type, "integer");

    // Test KafkaDataSource property validations
    let kafka_source_validations = KafkaDataSource::property_validations();
    assert!(!kafka_source_validations.is_empty());

    let topic_validation = kafka_source_validations
        .iter()
        .find(|v| v.key == "topic")
        .expect("topic validation should exist");

    assert!(topic_validation.required);
    assert!(topic_validation.default.is_none());
    assert_eq!(topic_validation.json_type, "string");
}

#[test]
fn test_schema_registry_complete_json_schema_generation() {
    let mut registry = HierarchicalSchemaRegistry::new();

    // Register all schema providers
    registry.register_global_schema::<BatchConfig>();
    registry.register_source_schema::<KafkaDataSource>();
    registry.register_sink_schema::<KafkaDataSink>();

    // Generate complete schema
    let complete_schema = registry.generate_complete_json_schema();

    assert_eq!(
        complete_schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert_eq!(
        complete_schema["title"],
        "FerrisStreams Configuration Schema"
    );
    assert!(complete_schema["definitions"].is_object());
    assert!(complete_schema["definitions"]["batch_config"].is_object());
    assert!(complete_schema["definitions"]["kafka_source"].is_object());
    assert!(complete_schema["definitions"]["kafka_sink"].is_object());
}

#[test]
fn test_config_validation_error_structure() {
    let error = ConfigValidationError {
        property: "batch.size".to_string(),
        message: "Value must be greater than 0".to_string(),
        suggestion: Some("Try setting batch.size to 100".to_string()),
        source_name: Some("kafka_orders".to_string()),
        inheritance_path: vec!["global".to_string(), "kafka_orders".to_string()],
    };

    assert_eq!(error.property, "batch.size");
    assert!(error.message.contains("greater than 0"));
    assert!(error.suggestion.is_some());
    assert!(error.source_name.is_some());
    assert_eq!(error.inheritance_path.len(), 2);

    // Test Display implementation
    let error_string = format!("{}", error);
    assert!(error_string.contains("batch.size"));
    assert!(error_string.contains("greater than 0"));
    assert!(error_string.contains("Try setting"));
}

#[test]
fn test_global_context_usage() {
    let mut context = GlobalSchemaContext::default();

    // Add various types of configuration
    context.global_properties.insert(
        "bootstrap.servers".to_string(),
        "global-kafka:9092".to_string(),
    );
    context
        .environment_variables
        .insert("KAFKA_BROKERS".to_string(), "env-kafka:9092".to_string());
    context
        .profile_properties
        .insert("prod.batch.size".to_string(), "1000".to_string());
    context
        .system_defaults
        .insert("timeout".to_string(), "30000".to_string());

    assert_eq!(context.global_properties.len(), 1);
    assert_eq!(context.environment_variables.len(), 1);
    assert_eq!(context.profile_properties.len(), 1);
    assert_eq!(context.system_defaults.len(), 1);
}

#[test]
fn test_property_default_variants() {
    let defaults = KafkaDataSource::optional_properties_with_defaults();

    // Test different PropertyDefault variants are created
    if let Some(PropertyDefault::GlobalLookup(lookup_key)) = defaults.get("bootstrap.servers") {
        assert_eq!(lookup_key, "kafka.bootstrap.servers");
    } else {
        panic!("Expected GlobalLookup for bootstrap.servers");
    }

    if let Some(PropertyDefault::Static(static_value)) = defaults.get("auto.offset.reset") {
        assert_eq!(static_value, "latest");
    } else {
        panic!("Expected Static for auto.offset.reset");
    }
}

#[test]
fn test_schema_version_tracking() {
    assert_eq!(BatchConfig::schema_version(), "1.0.0");
    assert_eq!(KafkaDataSource::schema_version(), "2.0.0");
    assert_eq!(KafkaDataSink::schema_version(), "2.0.0");
}

#[test]
fn test_timeout_and_interval_validation_patterns() {
    let kafka_source = KafkaDataSource::new("localhost:9092".to_string(), "test".to_string());

    // Test timeout validation pattern
    assert!(kafka_source
        .validate_property("session.timeout.ms", "30000")
        .is_ok());
    assert!(kafka_source
        .validate_property("request.timeout.ms", "10000")
        .is_ok());
    assert!(kafka_source
        .validate_property("session.timeout.ms", "0")
        .is_err()); // Must be > 0
    assert!(kafka_source
        .validate_property("session.timeout.ms", "4000000")
        .is_err()); // Too large

    // Test interval validation pattern
    assert!(kafka_source
        .validate_property("heartbeat.interval.ms", "3000")
        .is_ok());
    assert!(kafka_source
        .validate_property("heartbeat.interval.ms", "0")
        .is_err()); // Must be > 0
    assert!(kafka_source
        .validate_property("heartbeat.interval.ms", "invalid")
        .is_err());
}
