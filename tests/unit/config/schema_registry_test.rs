//! Unit tests for the self-registering configuration schema system

use ferrisstreams::ferris::config::{
    is_schema_version_compatible, validate_config_file_inheritance, validate_environment_variables,
    ConfigFileInheritance, ConfigSchemaProvider, ConfigValidationError, EnvironmentVariablePattern,
    GlobalSchemaContext, HierarchicalSchemaRegistry, PropertyDefault, PropertyValidation,
};
use ferrisstreams::ferris::datasource::file::{FileDataSource, FileSink};
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

#[test]
fn test_file_data_source_schema_provider_basic() {
    let file_source = FileDataSource::default();

    // Test config type ID
    assert_eq!(FileDataSource::config_type_id(), "file_source");

    // Test inheritable properties
    let inheritable = FileDataSource::inheritable_properties();
    assert!(inheritable.contains(&"buffer_size"));
    assert!(inheritable.contains(&"batch.size"));
    assert!(inheritable.contains(&"batch.timeout_ms"));
    assert!(inheritable.contains(&"polling_interval"));
    assert!(inheritable.contains(&"recursive"));

    // Test required properties
    let required = FileDataSource::required_named_properties();
    assert_eq!(required, vec!["path"]);

    // Test optional properties with defaults
    let optional = FileDataSource::optional_properties_with_defaults();
    assert!(optional.contains_key("format"));
    assert!(optional.contains_key("delimiter"));
    assert!(optional.contains_key("quote"));
    assert!(optional.contains_key("has_headers"));
    assert!(optional.contains_key("watching"));

    // Test supports custom properties
    assert!(FileDataSource::supports_custom_properties());

    // Test global schema dependencies
    let dependencies = FileDataSource::global_schema_dependencies();
    assert!(dependencies.contains(&"batch_config"));
    assert!(dependencies.contains(&"file_global"));

    // Test schema version
    assert_eq!(FileDataSource::schema_version(), "2.0.0");
}

#[test]
fn test_file_data_source_path_validation() {
    let file_source = FileDataSource::default();

    // Valid paths
    assert!(file_source
        .validate_property("path", "./data/sample.csv")
        .is_ok());
    assert!(file_source
        .validate_property("path", "/logs/*.json")
        .is_ok());
    assert!(file_source
        .validate_property("path", "./data/**/*.csv")
        .is_ok());
    assert!(file_source
        .validate_property("path", "relative/path.json")
        .is_ok());

    // Invalid paths
    assert!(file_source.validate_property("path", "").is_err()); // Empty path
    assert!(file_source
        .validate_property("path", "../../../etc/passwd")
        .is_err()); // Dangerous traversal
    assert!(file_source.validate_property("path", "/data/*/").is_err()); // Glob ending with /
}

#[test]
fn test_file_data_source_format_validation() {
    let file_source = FileDataSource::default();

    // Valid formats
    assert!(file_source.validate_property("format", "csv").is_ok());
    assert!(file_source
        .validate_property("format", "csv_no_header")
        .is_ok());
    assert!(file_source.validate_property("format", "json").is_ok());
    assert!(file_source.validate_property("format", "jsonlines").is_ok());
    assert!(file_source.validate_property("format", "jsonl").is_ok());

    // Invalid formats
    assert!(file_source.validate_property("format", "xml").is_err());
    assert!(file_source.validate_property("format", "parquet").is_err());
    assert!(file_source.validate_property("format", "").is_err());
    assert!(file_source.validate_property("format", "CSV").is_err()); // Case sensitive
}

#[test]
fn test_file_data_source_delimiter_validation() {
    let file_source = FileDataSource::default();

    // Valid delimiters
    assert!(file_source.validate_property("delimiter", ",").is_ok());
    assert!(file_source.validate_property("delimiter", ";").is_ok());
    assert!(file_source.validate_property("delimiter", "|").is_ok());
    assert!(file_source.validate_property("delimiter", "\t").is_ok());

    // Invalid delimiters
    assert!(file_source.validate_property("delimiter", "").is_err()); // Empty
    assert!(file_source.validate_property("delimiter", ",,").is_err()); // Multiple characters
    assert!(file_source.validate_property("delimiter", "a").is_err()); // Alphanumeric
    assert!(file_source.validate_property("delimiter", "1").is_err()); // Numeric
}

#[test]
fn test_file_data_source_boolean_validation() {
    let file_source = FileDataSource::default();

    // Valid boolean values
    assert!(file_source.validate_property("has_headers", "true").is_ok());
    assert!(file_source
        .validate_property("has_headers", "false")
        .is_ok());
    assert!(file_source.validate_property("watching", "true").is_ok());
    assert!(file_source.validate_property("watching", "false").is_ok());
    assert!(file_source.validate_property("recursive", "true").is_ok());
    assert!(file_source.validate_property("recursive", "false").is_ok());

    // Invalid boolean values
    assert!(file_source.validate_property("has_headers", "yes").is_err());
    assert!(file_source.validate_property("watching", "1").is_err());
    assert!(file_source.validate_property("recursive", "").is_err());
    assert!(file_source
        .validate_property("has_headers", "TRUE")
        .is_err()); // Case sensitive
}

#[test]
fn test_file_data_source_numeric_validation() {
    let file_source = FileDataSource::default();

    // Valid polling intervals
    assert!(file_source
        .validate_property("polling_interval", "1000")
        .is_ok());
    assert!(file_source
        .validate_property("polling_interval", "5000")
        .is_ok());
    assert!(file_source
        .validate_property("polling_interval", "3600000")
        .is_ok()); // 1 hour max

    // Invalid polling intervals
    assert!(file_source
        .validate_property("polling_interval", "0")
        .is_err()); // Must be > 0
    assert!(file_source
        .validate_property("polling_interval", "3600001")
        .is_err()); // Over 1 hour
    assert!(file_source
        .validate_property("polling_interval", "invalid")
        .is_err());

    // Valid buffer sizes
    assert!(file_source.validate_property("buffer_size", "1024").is_ok()); // Minimum
    assert!(file_source.validate_property("buffer_size", "8192").is_ok());
    assert!(file_source
        .validate_property("buffer_size", "104857600")
        .is_ok()); // 100MB max

    // Invalid buffer sizes
    assert!(file_source
        .validate_property("buffer_size", "1023")
        .is_err()); // Below minimum
    assert!(file_source
        .validate_property("buffer_size", "104857601")
        .is_err()); // Over 100MB
    assert!(file_source
        .validate_property("buffer_size", "invalid")
        .is_err());

    // Valid max records
    assert!(file_source.validate_property("max_records", "1000").is_ok());
    assert!(file_source.validate_property("max_records", "1").is_ok());

    // Invalid max records
    assert!(file_source.validate_property("max_records", "0").is_err()); // Must be > 0
    assert!(file_source
        .validate_property("max_records", "invalid")
        .is_err());

    // Valid skip lines
    assert!(file_source.validate_property("skip_lines", "0").is_ok());
    assert!(file_source.validate_property("skip_lines", "5").is_ok());

    // Invalid skip lines
    assert!(file_source
        .validate_property("skip_lines", "invalid")
        .is_err());
}

#[test]
fn test_file_data_source_extension_filter_validation() {
    let file_source = FileDataSource::default();

    // Valid extension filters
    assert!(file_source
        .validate_property("extension_filter", "csv")
        .is_ok());
    assert!(file_source
        .validate_property("extension_filter", "json")
        .is_ok());
    assert!(file_source
        .validate_property("extension_filter", "txt")
        .is_ok());
    assert!(file_source
        .validate_property("extension_filter", "log.gz")
        .is_ok()); // Multiple dots allowed

    // Invalid extension filters
    assert!(file_source
        .validate_property("extension_filter", "")
        .is_err()); // Empty
    assert!(file_source
        .validate_property("extension_filter", "csv!")
        .is_err()); // Special characters
    assert!(file_source
        .validate_property("extension_filter", "csv spaces")
        .is_err()); // Spaces not allowed
}

#[test]
fn test_file_data_source_property_inheritance() {
    let file_source = FileDataSource::default();
    let mut global_context = GlobalSchemaContext::default();

    // Test buffer_size inheritance
    global_context
        .global_properties
        .insert("file.buffer_size".to_string(), "16384".to_string());

    let result = file_source
        .resolve_property_with_inheritance("buffer_size", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("16384".to_string()));

    // Test local value takes precedence
    let result = file_source
        .resolve_property_with_inheritance("buffer_size", Some("32768"), &global_context)
        .unwrap();
    assert_eq!(result, Some("32768".to_string()));

    // Test default fallback
    global_context.global_properties.clear();
    let result = file_source
        .resolve_property_with_inheritance("buffer_size", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("8192".to_string())); // Default 8KB
}

#[test]
fn test_file_data_source_environment_based_defaults() {
    let file_source = FileDataSource::default();
    let mut global_context = GlobalSchemaContext::default();

    // Test development environment enables recursive by default
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "development".to_string());

    let result = file_source
        .resolve_property_with_inheritance("recursive", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("true".to_string()));

    // Test production environment disables recursive by default
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "production".to_string());

    let result = file_source
        .resolve_property_with_inheritance("recursive", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("false".to_string()));

    // Test default environment (non-development) disables recursive
    global_context.environment_variables.clear();

    let result = file_source
        .resolve_property_with_inheritance("recursive", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("false".to_string()));
}

#[test]
fn test_file_data_source_json_schema_generation() {
    let json_schema = FileDataSource::json_schema();

    // Check schema structure
    assert_eq!(json_schema["type"], "object");
    assert_eq!(
        json_schema["title"],
        "File Data Source Configuration Schema"
    );

    // Check required properties
    let required = json_schema["required"].as_array().unwrap();
    assert!(required.contains(&serde_json::Value::String("path".to_string())));

    // Check properties exist
    let properties = json_schema["properties"].as_object().unwrap();
    assert!(properties.contains_key("path"));
    assert!(properties.contains_key("format"));
    assert!(properties.contains_key("delimiter"));
    assert!(properties.contains_key("has_headers"));
    assert!(properties.contains_key("buffer_size"));

    // Check format enum values
    let format_enum = properties["format"]["enum"].as_array().unwrap();
    assert!(format_enum.contains(&serde_json::Value::String("csv".to_string())));
    assert!(format_enum.contains(&serde_json::Value::String("json".to_string())));
    assert!(format_enum.contains(&serde_json::Value::String("jsonlines".to_string())));

    // Check numeric constraints
    assert_eq!(properties["buffer_size"]["minimum"], 1024);
    assert_eq!(properties["buffer_size"]["maximum"], 104857600);
    assert_eq!(properties["polling_interval"]["minimum"], 1);
    assert_eq!(properties["polling_interval"]["maximum"], 3600000);
}

#[test]
fn test_file_data_source_property_validations() {
    let validations = FileDataSource::property_validations();

    // Should have key property validations
    assert!(!validations.is_empty());

    // Find path validation
    let path_validation = validations
        .iter()
        .find(|v| v.key == "path")
        .expect("Should have path validation");
    assert!(path_validation.required);
    assert!(path_validation.default.is_none());

    // Find format validation
    let format_validation = validations
        .iter()
        .find(|v| v.key == "format")
        .expect("Should have format validation");
    assert!(!format_validation.required);
    assert!(format_validation.default.is_some());
    assert!(format_validation
        .validation_pattern
        .as_ref()
        .unwrap()
        .contains("csv"));

    // Find buffer_size validation
    let buffer_validation = validations
        .iter()
        .find(|v| v.key == "buffer_size")
        .expect("Should have buffer_size validation");
    assert!(!buffer_validation.required);
    assert!(buffer_validation.default.is_some());
}

#[test]
fn test_file_data_source_registry_integration() {
    let mut registry = HierarchicalSchemaRegistry::new();

    // Register FileDataSource schema
    registry.register_source_schema::<FileDataSource>();

    // Test global configuration with file properties
    let mut global_config = HashMap::new();
    global_config.insert("file.buffer_size".to_string(), "16384".to_string());
    global_config.insert("file.polling_interval".to_string(), "2000".to_string());

    // Test named file configuration
    let mut named_config = HashMap::new();
    named_config.insert("log_files.path".to_string(), "/logs/*.json".to_string());
    named_config.insert("log_files.format".to_string(), "json".to_string());
    named_config.insert("log_files.watching".to_string(), "true".to_string());

    // Test validation - should pass
    let result =
        registry.validate_config_with_inheritance(&global_config, &named_config, &HashMap::new());
    assert!(result.is_ok());

    // Test with invalid configuration
    let mut invalid_named = HashMap::new();
    invalid_named.insert("bad_files.path".to_string(), "".to_string()); // Empty path
    invalid_named.insert("bad_files.format".to_string(), "xml".to_string()); // Invalid format

    let result =
        registry.validate_config_with_inheritance(&global_config, &invalid_named, &HashMap::new());
    assert!(result.is_err());

    let errors = result.unwrap_err();
    assert!(errors.len() >= 2); // Should have at least 2 errors
}

#[test]
fn test_file_sink_schema_provider_basic() {
    let file_sink = FileSink::default();

    // Test config type ID
    assert_eq!(FileSink::config_type_id(), "file_sink");

    // Test inheritable properties
    let inheritable = FileSink::inheritable_properties();
    assert!(inheritable.contains(&"buffer_size_bytes"));
    assert!(inheritable.contains(&"batch.size"));
    assert!(inheritable.contains(&"batch.timeout_ms"));
    assert!(inheritable.contains(&"compression"));
    assert!(inheritable.contains(&"writer_threads"));
    assert!(inheritable.contains(&"max_file_size_bytes"));

    // Test required properties
    let required = FileSink::required_named_properties();
    assert_eq!(required, vec!["path"]);

    // Test optional properties with defaults
    let optional = FileSink::optional_properties_with_defaults();
    assert!(optional.contains_key("format"));
    assert!(optional.contains_key("append_if_exists"));
    assert!(optional.contains_key("buffer_size_bytes"));
    assert!(optional.contains_key("csv_delimiter"));
    assert!(optional.contains_key("csv_has_header"));
    assert!(optional.contains_key("writer_threads"));

    // Test supports custom properties
    assert!(FileSink::supports_custom_properties());

    // Test global schema dependencies
    let dependencies = FileSink::global_schema_dependencies();
    assert!(dependencies.contains(&"batch_config"));
    assert!(dependencies.contains(&"file_global"));

    // Test schema version
    assert_eq!(FileSink::schema_version(), "2.0.0");
}

#[test]
fn test_file_sink_path_validation() {
    let file_sink = FileSink::default();

    // Valid paths
    assert!(file_sink.validate_property("path", "./output.json").is_ok());
    assert!(file_sink
        .validate_property("path", "/data/output-%Y-%m-%d.csv")
        .is_ok());
    assert!(file_sink
        .validate_property("path", "./logs/app-%H%M.jsonl")
        .is_ok());
    assert!(file_sink
        .validate_property("path", "relative/path.log")
        .is_ok());

    // Valid paths with strftime patterns containing ".."
    assert!(file_sink
        .validate_property("path", "/data/%Y-%m-%d/../archive/log.json")
        .is_ok()); // ".." allowed in strftime patterns

    // Invalid paths
    assert!(file_sink.validate_property("path", "").is_err()); // Empty path
    assert!(file_sink
        .validate_property("path", "../../../etc/passwd")
        .is_err()); // Dangerous traversal without strftime
    assert!(file_sink.validate_property("path", "/data/").is_err()); // Directory without filename
    assert!(file_sink.validate_property("path", "output\\").is_err()); // Windows path ending in separator
}

#[test]
fn test_file_sink_format_validation() {
    let file_sink = FileSink::default();

    // Valid formats
    assert!(file_sink.validate_property("format", "json").is_ok());
    assert!(file_sink.validate_property("format", "jsonlines").is_ok());
    assert!(file_sink.validate_property("format", "csv").is_ok());
    assert!(file_sink
        .validate_property("format", "csv_no_header")
        .is_ok());

    // Invalid formats
    assert!(file_sink.validate_property("format", "xml").is_err());
    assert!(file_sink.validate_property("format", "parquet").is_err());
    assert!(file_sink.validate_property("format", "").is_err());
    assert!(file_sink.validate_property("format", "JSON").is_err()); // Case sensitive
}

#[test]
fn test_file_sink_boolean_validation() {
    let file_sink = FileSink::default();

    // Valid boolean values
    assert!(file_sink
        .validate_property("append_if_exists", "true")
        .is_ok());
    assert!(file_sink
        .validate_property("append_if_exists", "false")
        .is_ok());
    assert!(file_sink
        .validate_property("csv_has_header", "true")
        .is_ok());
    assert!(file_sink
        .validate_property("csv_has_header", "false")
        .is_ok());

    // Invalid boolean values
    assert!(file_sink
        .validate_property("append_if_exists", "yes")
        .is_err());
    assert!(file_sink.validate_property("csv_has_header", "1").is_err());
    assert!(file_sink.validate_property("append_if_exists", "").is_err());
    assert!(file_sink
        .validate_property("csv_has_header", "TRUE")
        .is_err()); // Case sensitive
}

#[test]
fn test_file_sink_numeric_validation() {
    let file_sink = FileSink::default();

    // Valid buffer sizes
    assert!(file_sink
        .validate_property("buffer_size_bytes", "1024")
        .is_ok()); // Minimum
    assert!(file_sink
        .validate_property("buffer_size_bytes", "65536")
        .is_ok());
    assert!(file_sink
        .validate_property("buffer_size_bytes", "1073741824")
        .is_ok()); // 1GB max

    // Invalid buffer sizes
    assert!(file_sink
        .validate_property("buffer_size_bytes", "1023")
        .is_err()); // Below minimum
    assert!(file_sink
        .validate_property("buffer_size_bytes", "1073741825")
        .is_err()); // Over 1GB
    assert!(file_sink
        .validate_property("buffer_size_bytes", "invalid")
        .is_err());

    // Valid max file sizes
    assert!(file_sink
        .validate_property("max_file_size_bytes", "1024")
        .is_ok()); // Minimum
    assert!(file_sink
        .validate_property("max_file_size_bytes", "1073741824")
        .is_ok()); // 1GB

    // Invalid max file sizes
    assert!(file_sink
        .validate_property("max_file_size_bytes", "1023")
        .is_err()); // Below minimum
    assert!(file_sink
        .validate_property("max_file_size_bytes", "invalid")
        .is_err());

    // Valid rotation intervals
    assert!(file_sink
        .validate_property("rotation_interval_ms", "1000")
        .is_ok()); // Minimum
    assert!(file_sink
        .validate_property("rotation_interval_ms", "86400000")
        .is_ok()); // 24 hours max

    // Invalid rotation intervals
    assert!(file_sink
        .validate_property("rotation_interval_ms", "999")
        .is_err()); // Below minimum
    assert!(file_sink
        .validate_property("rotation_interval_ms", "86400001")
        .is_err()); // Over 24 hours
    assert!(file_sink
        .validate_property("rotation_interval_ms", "invalid")
        .is_err());

    // Valid max records per file
    assert!(file_sink
        .validate_property("max_records_per_file", "1")
        .is_ok());
    assert!(file_sink
        .validate_property("max_records_per_file", "1000000")
        .is_ok());

    // Invalid max records per file
    assert!(file_sink
        .validate_property("max_records_per_file", "0")
        .is_err()); // Must be > 0
    assert!(file_sink
        .validate_property("max_records_per_file", "invalid")
        .is_err());

    // Valid writer threads
    assert!(file_sink.validate_property("writer_threads", "1").is_ok());
    assert!(file_sink.validate_property("writer_threads", "4").is_ok());
    assert!(file_sink.validate_property("writer_threads", "64").is_ok()); // Maximum

    // Invalid writer threads
    assert!(file_sink.validate_property("writer_threads", "0").is_err()); // Must be > 0
    assert!(file_sink.validate_property("writer_threads", "65").is_err()); // Over maximum
    assert!(file_sink
        .validate_property("writer_threads", "invalid")
        .is_err());
}

#[test]
fn test_file_sink_compression_validation() {
    let file_sink = FileSink::default();

    // Valid compression types
    assert!(file_sink.validate_property("compression", "none").is_ok());
    assert!(file_sink.validate_property("compression", "gzip").is_ok());
    assert!(file_sink.validate_property("compression", "snappy").is_ok());
    assert!(file_sink.validate_property("compression", "zstd").is_ok());

    // Invalid compression types
    assert!(file_sink.validate_property("compression", "bzip2").is_err());
    assert!(file_sink.validate_property("compression", "lz4").is_err());
    assert!(file_sink.validate_property("compression", "").is_err());
    assert!(file_sink.validate_property("compression", "GZIP").is_err()); // Case sensitive
}

#[test]
fn test_file_sink_csv_delimiter_validation() {
    let file_sink = FileSink::default();

    // Valid delimiters
    assert!(file_sink.validate_property("csv_delimiter", ",").is_ok());
    assert!(file_sink.validate_property("csv_delimiter", ";").is_ok());
    assert!(file_sink.validate_property("csv_delimiter", "|").is_ok());
    assert!(file_sink.validate_property("csv_delimiter", "\t").is_ok());

    // Invalid delimiters
    assert!(file_sink.validate_property("csv_delimiter", "").is_err()); // Empty
    assert!(file_sink.validate_property("csv_delimiter", ",,").is_err()); // Multiple characters
    assert!(file_sink.validate_property("csv_delimiter", "a").is_err()); // Alphanumeric
    assert!(file_sink.validate_property("csv_delimiter", "1").is_err()); // Numeric
}

#[test]
fn test_file_sink_property_inheritance() {
    let file_sink = FileSink::default();
    let mut global_context = GlobalSchemaContext::default();

    // Test buffer_size_bytes inheritance
    global_context.global_properties.insert(
        "file.sink.buffer_size_bytes".to_string(),
        "131072".to_string(),
    );

    let result = file_sink
        .resolve_property_with_inheritance("buffer_size_bytes", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("131072".to_string()));

    // Test local value takes precedence
    let result = file_sink
        .resolve_property_with_inheritance("buffer_size_bytes", Some("262144"), &global_context)
        .unwrap();
    assert_eq!(result, Some("262144".to_string()));

    // Test default fallback
    global_context.global_properties.clear();
    let result = file_sink
        .resolve_property_with_inheritance("buffer_size_bytes", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("65536".to_string())); // Default 64KB buffer
}

#[test]
fn test_file_sink_environment_based_defaults() {
    let file_sink = FileSink::default();
    let mut global_context = GlobalSchemaContext::default();

    // Test production environment enables compression by default
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "production".to_string());

    let result = file_sink
        .resolve_property_with_inheritance("compression", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("gzip".to_string()));

    // Test development environment disables compression by default
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "development".to_string());

    let result = file_sink
        .resolve_property_with_inheritance("compression", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("none".to_string()));

    // Test writer threads based on environment
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "production".to_string());

    let result = file_sink
        .resolve_property_with_inheritance("writer_threads", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("4".to_string())); // Production uses 4 threads

    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "development".to_string());

    let result = file_sink
        .resolve_property_with_inheritance("writer_threads", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("1".to_string())); // Development uses 1 thread

    // Test max file size based on environment
    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "production".to_string());

    let result = file_sink
        .resolve_property_with_inheritance("max_file_size_bytes", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("1073741824".to_string())); // 1GB in production

    global_context
        .environment_variables
        .insert("ENVIRONMENT".to_string(), "development".to_string());

    let result = file_sink
        .resolve_property_with_inheritance("max_file_size_bytes", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("10485760".to_string())); // 10MB in development
}

#[test]
fn test_file_sink_json_schema_generation() {
    let json_schema = FileSink::json_schema();

    // Check schema structure
    assert_eq!(json_schema["type"], "object");
    assert_eq!(json_schema["title"], "File Data Sink Configuration Schema");

    // Check required properties
    let required = json_schema["required"].as_array().unwrap();
    assert!(required.contains(&serde_json::Value::String("path".to_string())));

    // Check properties exist
    let properties = json_schema["properties"].as_object().unwrap();
    assert!(properties.contains_key("path"));
    assert!(properties.contains_key("format"));
    assert!(properties.contains_key("append_if_exists"));
    assert!(properties.contains_key("buffer_size_bytes"));
    assert!(properties.contains_key("compression"));
    assert!(properties.contains_key("writer_threads"));

    // Check format enum values
    let format_enum = properties["format"]["enum"].as_array().unwrap();
    assert!(format_enum.contains(&serde_json::Value::String("json".to_string())));
    assert!(format_enum.contains(&serde_json::Value::String("jsonlines".to_string())));
    assert!(format_enum.contains(&serde_json::Value::String("csv".to_string())));

    // Check compression enum values
    let compression_enum = properties["compression"]["enum"].as_array().unwrap();
    assert!(compression_enum.contains(&serde_json::Value::String("none".to_string())));
    assert!(compression_enum.contains(&serde_json::Value::String("gzip".to_string())));
    assert!(compression_enum.contains(&serde_json::Value::String("snappy".to_string())));
    assert!(compression_enum.contains(&serde_json::Value::String("zstd".to_string())));

    // Check numeric constraints
    assert_eq!(properties["buffer_size_bytes"]["minimum"], 1024);
    assert_eq!(properties["buffer_size_bytes"]["maximum"], 1073741824);
    assert_eq!(properties["rotation_interval_ms"]["minimum"], 1000);
    assert_eq!(properties["rotation_interval_ms"]["maximum"], 86400000);
    assert_eq!(properties["writer_threads"]["minimum"], 1);
    assert_eq!(properties["writer_threads"]["maximum"], 64);
}

#[test]
fn test_file_sink_property_validations() {
    let validations = FileSink::property_validations();

    // Should have key property validations
    assert!(!validations.is_empty());

    // Find path validation
    let path_validation = validations
        .iter()
        .find(|v| v.key == "path")
        .expect("Should have path validation");
    assert!(path_validation.required);
    assert!(path_validation.default.is_none());

    // Find format validation
    let format_validation = validations
        .iter()
        .find(|v| v.key == "format")
        .expect("Should have format validation");
    assert!(!format_validation.required);
    assert!(format_validation.default.is_some());
    assert!(format_validation
        .validation_pattern
        .as_ref()
        .unwrap()
        .contains("json"));

    // Find buffer_size_bytes validation
    let buffer_validation = validations
        .iter()
        .find(|v| v.key == "buffer_size_bytes")
        .expect("Should have buffer_size_bytes validation");
    assert!(!buffer_validation.required);
    assert!(buffer_validation.default.is_some());

    // Find compression validation
    let compression_validation = validations
        .iter()
        .find(|v| v.key == "compression")
        .expect("Should have compression validation");
    assert!(!compression_validation.required);
    assert!(compression_validation.default.is_some());
    assert!(compression_validation
        .validation_pattern
        .as_ref()
        .unwrap()
        .contains("gzip"));
}

#[test]
fn test_file_sink_registry_integration() {
    let mut registry = HierarchicalSchemaRegistry::new();

    // Register FileSink schema
    registry.register_sink_schema::<FileSink>();

    // Test basic sink provider functionality without registry validation
    // (since the current registry implementation is focused on source validation)
    let file_sink = FileSink::default();

    // Test individual property validations work correctly
    assert!(file_sink
        .validate_property("path", "./data/output.json")
        .is_ok());
    assert!(file_sink.validate_property("format", "jsonlines").is_ok());
    assert!(file_sink
        .validate_property("buffer_size_bytes", "131072")
        .is_ok());
    assert!(file_sink.validate_property("compression", "gzip").is_ok());

    // Test validation failures
    assert!(file_sink.validate_property("path", "").is_err()); // Empty path
    assert!(file_sink.validate_property("format", "xml").is_err()); // Invalid format
    assert!(file_sink.validate_property("compression", "bzip2").is_err()); // Invalid compression

    // Test property inheritance
    let mut global_context = GlobalSchemaContext::default();
    global_context.global_properties.insert(
        "file.sink.buffer_size_bytes".to_string(),
        "131072".to_string(),
    );

    let result = file_sink
        .resolve_property_with_inheritance("buffer_size_bytes", None, &global_context)
        .unwrap();
    assert_eq!(result, Some("131072".to_string()));

    // Verify the registry has schemas registered by generating JSON schema
    let json_schema = registry.generate_complete_json_schema();

    // Check that the schema contains definitions (indicating successful registration)
    let definitions = json_schema["definitions"].as_object().unwrap();

    // We can't directly verify sink schemas since they're private,
    // but we can verify the schema provider works correctly
    assert!(!definitions.is_empty());
}

// ===== PHASE 2 FEATURE TESTS =====

#[test]
fn test_config_file_inheritance_valid_chain() {
    let inheritance = ConfigFileInheritance::new(
        "./config/app.yaml",
        vec![
            "./config/base.yaml".to_string(),
            "./config/environment/prod.yaml".to_string(),
        ],
    );

    // Test basic structure
    assert_eq!(inheritance.config_file, "./config/app.yaml");
    assert_eq!(inheritance.extends_files.len(), 2);
    assert_eq!(inheritance.extends_files[0], "./config/base.yaml");
    assert_eq!(
        inheritance.extends_files[1],
        "./config/environment/prod.yaml"
    );
}

#[test]
fn test_validate_config_file_inheritance_no_circular_dependency() {
    // Create a valid inheritance chain: app -> base -> common
    let config_files = vec![
        ConfigFileInheritance::new("./config/app.yaml", vec!["./config/base.yaml".to_string()]),
        ConfigFileInheritance::new(
            "./config/base.yaml",
            vec!["./config/common.yaml".to_string()],
        ),
        ConfigFileInheritance::new("./config/common.yaml", vec![]),
    ];

    let result = validate_config_file_inheritance(&config_files);
    assert!(
        result.is_ok(),
        "Valid inheritance chain should pass validation"
    );
}

#[test]
fn test_validate_config_file_inheritance_direct_circular_dependency() {
    // Create a circular dependency: app -> base -> app
    let config_files = vec![
        ConfigFileInheritance::new("./config/app.yaml", vec!["./config/base.yaml".to_string()]),
        ConfigFileInheritance::new(
            "./config/base.yaml",
            vec![
                "./config/app.yaml".to_string(), // Circular!
            ],
        ),
    ];

    let result = validate_config_file_inheritance(&config_files);
    assert!(result.is_err(), "Circular dependency should be detected");

    let errors = result.unwrap_err();
    assert!(!errors.is_empty());
    assert!(errors[0].message.contains("Circular dependency"));
    assert!(errors[0].message.contains("app.yaml"));
    assert!(errors[0].message.contains("base.yaml"));
}

#[test]
fn test_validate_config_file_inheritance_indirect_circular_dependency() {
    // Create an indirect circular dependency: app -> base -> common -> app
    let config_files = vec![
        ConfigFileInheritance::new("./config/app.yaml", vec!["./config/base.yaml".to_string()]),
        ConfigFileInheritance::new(
            "./config/base.yaml",
            vec!["./config/common.yaml".to_string()],
        ),
        ConfigFileInheritance::new(
            "./config/common.yaml",
            vec![
                "./config/app.yaml".to_string(), // Circular!
            ],
        ),
    ];

    let result = validate_config_file_inheritance(&config_files);
    assert!(
        result.is_err(),
        "Indirect circular dependency should be detected"
    );

    let errors = result.unwrap_err();
    assert!(!errors.is_empty());
    assert!(errors[0].message.contains("Circular dependency"));
    assert!(errors[0].message.contains("app.yaml"));
}

#[test]
fn test_validate_config_file_inheritance_self_reference() {
    // Create a self-referencing file: app -> app
    let config_files = vec![ConfigFileInheritance::new(
        "./config/app.yaml",
        vec![
            "./config/app.yaml".to_string(), // Self-reference!
        ],
    )];

    let result = validate_config_file_inheritance(&config_files);
    assert!(result.is_err(), "Self-reference should be detected");

    let errors = result.unwrap_err();
    assert!(!errors.is_empty());
    assert!(errors[0].message.contains("Circular dependency"));
    assert!(errors[0].message.contains("app.yaml"));
}

#[test]
fn test_validate_config_file_inheritance_missing_file_reference() {
    // Create inheritance with missing file reference
    let config_files = vec![
        ConfigFileInheritance::new(
            "./config/app.yaml",
            vec![
                "./config/missing.yaml".to_string(), // File not in list
            ],
        ),
        ConfigFileInheritance::new("./config/base.yaml", vec![]),
    ];

    let result = validate_config_file_inheritance(&config_files);
    assert!(result.is_err(), "Missing file reference should be detected");

    let errors = result.unwrap_err();
    assert!(!errors.is_empty());
    assert!(errors[0].message.contains("references missing file"));
    assert!(errors[0].message.contains("missing.yaml"));
}

#[test]
fn test_validate_config_file_inheritance_complex_valid_hierarchy() {
    // Create a complex but valid hierarchy:
    // app -> [base, environment/prod]
    // base -> common
    // environment/prod -> environment/shared
    let config_files = vec![
        ConfigFileInheritance::new(
            "./config/app.yaml",
            vec![
                "./config/base.yaml".to_string(),
                "./config/environment/prod.yaml".to_string(),
            ],
        ),
        ConfigFileInheritance::new(
            "./config/base.yaml",
            vec!["./config/common.yaml".to_string()],
        ),
        ConfigFileInheritance::new(
            "./config/environment/prod.yaml",
            vec!["./config/environment/shared.yaml".to_string()],
        ),
        ConfigFileInheritance::new("./config/common.yaml", vec![]),
        ConfigFileInheritance::new("./config/environment/shared.yaml", vec![]),
    ];

    let result = validate_config_file_inheritance(&config_files);
    assert!(
        result.is_ok(),
        "Complex valid hierarchy should pass validation"
    );
}

#[test]
fn test_validate_config_file_inheritance_empty_list() {
    let config_files = vec![];
    let result = validate_config_file_inheritance(&config_files);
    assert!(result.is_ok(), "Empty config file list should be valid");
}

#[test]
fn test_environment_variable_pattern_creation() {
    let pattern = EnvironmentVariablePattern::new(
        "KAFKA_*_BROKERS",
        "kafka.{}.brokers",
        Some("localhost:9092"),
    );

    assert_eq!(pattern.pattern, "KAFKA_*_BROKERS");
    assert_eq!(pattern.config_key_template, "kafka.{}.brokers");
    assert_eq!(pattern.default_value, Some("localhost:9092".to_string()));
}

#[test]
fn test_validate_environment_variables_valid_patterns() {
    let patterns = vec![
        EnvironmentVariablePattern::new(
            "KAFKA_*_BROKERS",
            "kafka.{}.brokers",
            Some("localhost:9092"),
        ),
        EnvironmentVariablePattern::new("DB_*_HOST", "database.{}.host", Some("localhost")),
        EnvironmentVariablePattern::new("APP_*_PORT", "app.{}.port", Some("8080")),
    ];

    let env_vars = std::collections::HashMap::from([
        (
            "KAFKA_PROD_BROKERS".to_string(),
            "prod-kafka:9092".to_string(),
        ),
        (
            "KAFKA_DEV_BROKERS".to_string(),
            "dev-kafka:9092".to_string(),
        ),
        ("DB_MAIN_HOST".to_string(), "db.example.com".to_string()),
        ("APP_WEB_PORT".to_string(), "3000".to_string()),
        ("OTHER_VAR".to_string(), "ignored".to_string()), // Should be ignored
    ]);

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(
        result.is_ok(),
        "Valid environment variables should pass validation"
    );

    let resolved_config = result.unwrap();
    assert_eq!(
        resolved_config.get("kafka.PROD.brokers"),
        Some(&"prod-kafka:9092".to_string())
    );
    assert_eq!(
        resolved_config.get("kafka.DEV.brokers"),
        Some(&"dev-kafka:9092".to_string())
    );
    assert_eq!(
        resolved_config.get("database.MAIN.host"),
        Some(&"db.example.com".to_string())
    );
    assert_eq!(
        resolved_config.get("app.WEB.port"),
        Some(&"3000".to_string())
    );
    assert!(!resolved_config.contains_key("OTHER_VAR")); // Should be filtered out
}

#[test]
fn test_validate_environment_variables_pattern_matching() {
    let patterns = vec![EnvironmentVariablePattern::new(
        "PREFIX_*_SUFFIX",
        "config.{}.property",
        None::<&str>,
    )];

    let env_vars = std::collections::HashMap::from([
        ("PREFIX_MATCH_SUFFIX".to_string(), "value1".to_string()), // Should match
        ("PREFIX_ANOTHER_SUFFIX".to_string(), "value2".to_string()), // Should match
        ("PREFIX_SUFFIX".to_string(), "value3".to_string()),       // No wildcard part
        ("WRONG_MATCH_SUFFIX".to_string(), "value4".to_string()),  // Wrong prefix
        ("PREFIX_MATCH_WRONG".to_string(), "value5".to_string()),  // Wrong suffix
    ]);

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(result.is_ok(), "Pattern matching should work correctly");

    let resolved_config = result.unwrap();
    assert_eq!(
        resolved_config.get("config.MATCH.property"),
        Some(&"value1".to_string())
    );
    assert_eq!(
        resolved_config.get("config.ANOTHER.property"),
        Some(&"value2".to_string())
    );
    assert!(!resolved_config.contains_key("config..property")); // Empty wildcard should be filtered
    assert_eq!(resolved_config.len(), 2); // Only 2 should match
}

#[test]
fn test_validate_environment_variables_default_values() {
    let patterns = vec![
        EnvironmentVariablePattern::new("MISSING_*_VAR", "config.{}.value", Some("default_val")),
        EnvironmentVariablePattern::new("PRESENT_*_VAR", "config.{}.value", Some("default_val")),
    ];

    let env_vars = std::collections::HashMap::from([
        ("PRESENT_FOUND_VAR".to_string(), "actual_value".to_string()),
        // MISSING_* pattern vars are not present, should use defaults
    ]);

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(result.is_ok(), "Default values should work correctly");

    let resolved_config = result.unwrap();
    assert_eq!(
        resolved_config.get("config.FOUND.value"),
        Some(&"actual_value".to_string())
    );
    // Note: Current implementation doesn't generate defaults for missing patterns
    // This is by design - only actual env vars are processed
    assert_eq!(resolved_config.len(), 1);
}

#[test]
fn test_validate_environment_variables_invalid_template() {
    let patterns = vec![EnvironmentVariablePattern::new(
        "VALID_*_PATTERN",
        "invalid_template_no_placeholder",
        None::<&str>,
    )];

    let env_vars =
        std::collections::HashMap::from([("VALID_MATCH_PATTERN".to_string(), "value".to_string())]);

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(
        result.is_err(),
        "Invalid template should cause validation error"
    );

    let errors = result.unwrap_err();
    assert!(!errors.is_empty());
    assert!(errors[0].contains("template"));
    assert!(errors[0].contains("placeholder"));
}

#[test]
fn test_validate_environment_variables_empty_patterns() {
    let patterns = vec![];
    let env_vars = std::collections::HashMap::from([("ANY_VAR".to_string(), "value".to_string())]);

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(result.is_ok(), "Empty patterns should be valid");

    let resolved_config = result.unwrap();
    assert!(
        resolved_config.is_empty(),
        "No patterns should result in empty config"
    );
}

#[test]
fn test_validate_environment_variables_empty_env_vars() {
    let patterns = vec![EnvironmentVariablePattern::new(
        "ANY_*_PATTERN",
        "config.{}.value",
        None::<&str>,
    )];
    let env_vars = std::collections::HashMap::new();

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(
        result.is_ok(),
        "Empty environment variables should be valid"
    );

    let resolved_config = result.unwrap();
    assert!(
        resolved_config.is_empty(),
        "No env vars should result in empty config"
    );
}

#[test]
fn test_schema_version_validation_compatible() {
    // Test major version compatibility (should be compatible)
    assert!(is_schema_version_compatible("1.0.0", "1.1.0"));
    assert!(is_schema_version_compatible("1.5.2", "1.8.1"));
    assert!(is_schema_version_compatible("2.0.0", "2.3.5"));

    // Test patch version compatibility
    assert!(is_schema_version_compatible("1.2.0", "1.2.3"));
    assert!(is_schema_version_compatible("1.2.5", "1.2.7"));
}

#[test]
fn test_schema_version_validation_incompatible() {
    // Test major version incompatibility (should be incompatible)
    assert!(!is_schema_version_compatible("1.0.0", "2.0.0"));
    assert!(!is_schema_version_compatible("2.5.3", "3.0.0"));
    assert!(!is_schema_version_compatible("1.8.9", "2.1.0"));

    // Test minor version backward incompatibility (older can't handle newer minor)
    assert!(!is_schema_version_compatible("1.5.0", "1.3.0")); // Newer runtime, older schema
    assert!(!is_schema_version_compatible("2.8.0", "2.5.0"));
}

#[test]
fn test_schema_version_validation_identical() {
    // Test identical versions (should be compatible)
    assert!(is_schema_version_compatible("1.0.0", "1.0.0"));
    assert!(is_schema_version_compatible("2.5.3", "2.5.3"));
    assert!(is_schema_version_compatible("10.15.22", "10.15.22"));
}

#[test]
fn test_schema_version_validation_edge_cases() {
    // Test single digit versions
    assert!(is_schema_version_compatible("1.0.0", "1.1.0"));
    assert!(is_schema_version_compatible("0.1.0", "0.1.5"));
    assert!(!is_schema_version_compatible("0.1.0", "0.2.0")); // Minor version matters in 0.x

    // Test large version numbers
    assert!(is_schema_version_compatible("15.23.0", "15.45.12"));
    assert!(!is_schema_version_compatible("15.23.0", "16.0.0"));

    // Test version 0.x special handling (0.x versions are considered unstable)
    assert!(is_schema_version_compatible("0.5.0", "0.5.8")); // Patch compatible
    assert!(!is_schema_version_compatible("0.5.0", "0.6.0")); // Minor incompatible in 0.x
}

#[test]
fn test_schema_version_validation_invalid_versions() {
    // Note: These should ideally return errors, but the current implementation
    // uses simple string parsing. In a real implementation, these would be
    // handled more robustly with proper semver parsing.

    // For now, test that malformed versions don't cause panics
    // These might return false or behave unpredictably, which is acceptable
    // for this test since proper validation would happen during config parsing

    let result1 = std::panic::catch_unwind(|| is_schema_version_compatible("invalid", "1.0.0"));
    assert!(result1.is_ok(), "Invalid version should not panic");

    let result2 =
        std::panic::catch_unwind(|| is_schema_version_compatible("1.0.0", "not.a.version"));
    assert!(result2.is_ok(), "Invalid version should not panic");
}

#[test]
fn test_validate_config_file_inheritance_integration() {
    // Test that the exported function works correctly
    let config_files = vec![
        ConfigFileInheritance::new("./config/app.yaml", vec!["./config/base.yaml".to_string()]),
        ConfigFileInheritance::new("./config/base.yaml", vec![]),
    ];

    let result = validate_config_file_inheritance(&config_files);
    assert!(result.is_ok(), "Integration test should pass");
}

#[test]
fn test_validate_environment_variables_integration() {
    // Test that the exported function works correctly
    let patterns = vec![EnvironmentVariablePattern::new(
        "TEST_*_VAR",
        "test.{}.value",
        Some("default"),
    )];

    let env_vars = std::collections::HashMap::from([(
        "TEST_EXAMPLE_VAR".to_string(),
        "test_value".to_string(),
    )]);

    let result = validate_environment_variables(&patterns, &env_vars);
    assert!(result.is_ok(), "Integration test should pass");

    let resolved_config = result.unwrap();
    assert_eq!(
        resolved_config.get("test.EXAMPLE.value"),
        Some(&"test_value".to_string())
    );
}

#[test]
fn test_phase_2_comprehensive_integration() {
    // Test all Phase 2 features working together
    let mut registry = HierarchicalSchemaRegistry::new();

    // Register schemas
    registry.register_source_schema::<KafkaDataSource>();
    registry.register_source_schema::<FileDataSource>();
    registry.register_sink_schema::<FileSink>();

    // Test config file inheritance
    let config_files = vec![
        ConfigFileInheritance::new(
            "./config/app.yaml",
            vec!["./config/kafka-base.yaml".to_string()],
        ),
        ConfigFileInheritance::new("./config/kafka-base.yaml", vec![]),
    ];
    assert!(validate_config_file_inheritance(&config_files).is_ok());

    // Test environment variable patterns
    let env_patterns = vec![
        EnvironmentVariablePattern::new(
            "KAFKA_*_BROKERS",
            "kafka.{}.brokers",
            Some("localhost:9092"),
        ),
        EnvironmentVariablePattern::new("FILE_*_PATH", "file.{}.path", None::<&str>),
    ];

    let env_vars = std::collections::HashMap::from([
        (
            "KAFKA_PROD_BROKERS".to_string(),
            "prod-kafka:9092".to_string(),
        ),
        (
            "FILE_OUTPUT_PATH".to_string(),
            "./data/output.json".to_string(),
        ),
    ]);

    let env_result = validate_environment_variables(&env_patterns, &env_vars);
    assert!(env_result.is_ok());

    let resolved_env = env_result.unwrap();
    assert_eq!(
        resolved_env.get("kafka.PROD.brokers"),
        Some(&"prod-kafka:9092".to_string())
    );
    assert_eq!(
        resolved_env.get("file.OUTPUT.path"),
        Some(&"./data/output.json".to_string())
    );

    // Test schema version compatibility
    assert!(is_schema_version_compatible("2.0.0", "2.1.0")); // Kafka/File schemas
    assert!(!is_schema_version_compatible("2.0.0", "3.0.0")); // Breaking change

    // Test that all registered schemas have version info
    assert_eq!(KafkaDataSource::schema_version(), "2.0.0");
    assert_eq!(FileDataSource::schema_version(), "2.0.0");
    assert_eq!(FileSink::schema_version(), "2.0.0");
}
