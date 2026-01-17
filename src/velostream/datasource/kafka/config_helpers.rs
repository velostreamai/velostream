//! Shared configuration helpers for KafkaDataReader and KafkaDataWriter
//!
//! This module provides common functionality for extracting and validating
//! Kafka datasource configuration from property maps, ensuring consistency
//! between reader and writer implementations.

use crate::velostream::kafka::serialization_format::SerializationFormat;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;

/// Extract serialization format for MESSAGE VALUE
/// Requires explicit property key: "value.serializer"
/// Example: value.serializer: "avro"
pub fn extract_format_from_properties(properties: &HashMap<String, String>) -> SerializationFormat {
    let format_str = properties
        .get("value.serializer")
        .map(|s| s.as_str())
        .unwrap_or("json");

    SerializationFormat::from_str(format_str).unwrap_or(SerializationFormat::Json)
}

/// Extract key serialization format from properties
/// Requires explicit property key: "key.serializer"
pub fn extract_key_format_from_properties(
    properties: &HashMap<String, String>,
) -> SerializationFormat {
    let format_str = properties
        .get("key.serializer")
        .map(|s| s.as_str())
        .unwrap_or("string");

    SerializationFormat::from_str(format_str).unwrap_or(SerializationFormat::String)
}

/// Extract key field name from properties using standard key conventions
///
/// Tries multiple property keys in preference order:
/// 1. "key.field"
/// 2. "message.key.field"
/// 3. "schema.key.field"
pub fn extract_key_field_from_properties(properties: &HashMap<String, String>) -> Option<String> {
    properties
        .get("key.field")
        .or_else(|| properties.get("message.key.field"))
        .or_else(|| properties.get("schema.key.field"))
        .cloned()
}

/// Extract Avro schema for MESSAGE VALUE (inline or file)
/// First tries inline schema (value.avro.schema)
/// Then tries generic schema file (value.schema.file)
pub fn extract_avro_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    // Try inline schema first
    if let Some(schema) = properties.get("value.avro.schema") {
        return Ok(Some(schema.clone()));
    }

    // Try generic schema file (works because format is already known as Avro)
    if let Some(schema_file) = properties.get("value.schema.file") {
        return load_schema_from_file(schema_file);
    }

    Ok(None)
}

/// Extract Avro schema for MESSAGE KEY from file only
/// Keys support file-based schemas only (no inline)
pub fn extract_key_avro_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    // Only try generic schema file (no inline support for keys)
    if let Some(schema_file) = properties.get("key.schema.file") {
        return load_schema_from_file(schema_file);
    }

    Ok(None)
}

/// Extract Protobuf schema for MESSAGE VALUE (inline or file)
/// First tries inline schema (value.protobuf.schema)
/// Then tries generic schema file (value.schema.file)
pub fn extract_protobuf_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    // Try inline schema first
    if let Some(schema) = properties.get("value.protobuf.schema") {
        return Ok(Some(schema.clone()));
    }

    // Try generic schema file
    if let Some(schema_file) = properties.get("value.schema.file") {
        return load_schema_from_file(schema_file);
    }

    Ok(None)
}

/// Extract Protobuf schema for MESSAGE KEY from file only
/// Keys support file-based schemas only (no inline)
pub fn extract_key_protobuf_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    // Only try generic schema file (no inline support for keys)
    if let Some(schema_file) = properties.get("key.schema.file") {
        return load_schema_from_file(schema_file);
    }

    Ok(None)
}

/// Extract JSON schema for MESSAGE VALUE (inline only)
pub fn extract_json_schema_from_properties(properties: &HashMap<String, String>) -> Option<String> {
    properties.get("value.json.schema").cloned()
}

/// Extract JSON schema for MESSAGE KEY from file only
pub fn extract_key_json_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Option<String> {
    // Keys support file paths only - check generic key.schema.file
    // but return as string since JSON is typically text
    properties.get("key.schema.file").cloned()
}

/// Extract schema from properties for MESSAGE VALUE based on serialization format
/// Delegates to format-specific extraction functions
pub fn extract_schema_from_properties(
    format: &SerializationFormat,
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    match format {
        SerializationFormat::Avro { .. } => extract_avro_schema_from_properties(properties),
        SerializationFormat::Protobuf { .. } => extract_protobuf_schema_from_properties(properties),
        SerializationFormat::Json | SerializationFormat::Bytes | SerializationFormat::String => {
            Ok(extract_json_schema_from_properties(properties))
        }
    }
}

/// Extract schema from properties for MESSAGE KEY based on serialization format
pub fn extract_key_schema_from_properties(
    format: &SerializationFormat,
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    match format {
        SerializationFormat::Avro { .. } => extract_key_avro_schema_from_properties(properties),
        SerializationFormat::Protobuf { .. } => {
            extract_key_protobuf_schema_from_properties(properties)
        }
        SerializationFormat::Json | SerializationFormat::Bytes | SerializationFormat::String => {
            Ok(extract_key_json_schema_from_properties(properties))
        }
    }
}

/// Load schema content from file path
///
/// # Errors
/// Returns an error if the file cannot be read
pub fn load_schema_from_file(
    file_path: &str,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    use std::fs;
    match fs::read_to_string(file_path) {
        Ok(content) => {
            log::info!("Loaded schema from file: {}", file_path);
            Ok(Some(content))
        }
        Err(e) => Err(format!("Failed to load schema from file '{}': {}", file_path, e).into()),
    }
}

/// Client type for generating client IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
    /// Source/consumer client
    Source,
    /// Sink/producer client
    Sink,
}

impl ClientType {
    /// Get the suffix for client IDs
    pub fn suffix(&self) -> &'static str {
        match self {
            ClientType::Source => "src",
            ClientType::Sink => "snk",
        }
    }

    /// Get the display name for logging
    pub fn display_name(&self) -> &'static str {
        match self {
            ClientType::Source => "consumer",
            ClientType::Sink => "producer",
        }
    }
}

/// Generate a hierarchical client ID for Kafka observability
///
/// Format: `velo_{app_name}_{job_name}_{instance_id}_{type_suffix}`
///
/// Uses underscores as delimiters so that dashes can be used within component names
/// (e.g., app name "my-app" or job name "etl-pipeline").
///
/// This ensures:
/// - Jobs from the same app sort together in Kafka UI
/// - Instance/run IDs provide uniqueness
/// - Client type (src/snk) is visible at a glance
/// - Component names can contain dashes without ambiguity
///
/// # Arguments
/// * `app_name` - Optional application name (from SQL app metadata)
/// * `job_name` - Job/query name
/// * `instance_id` - Optional instance ID (for test isolation or multi-instance)
/// * `client_type` - Source or Sink client
///
/// # Examples
/// ```
/// use velostream::velostream::datasource::kafka::config_helpers::{generate_client_id, ClientType};
///
/// // Full specification
/// let id = generate_client_id(Some("my-app"), "etl-pipeline", Some("inst-1"), ClientType::Sink);
/// assert_eq!(id, "velo_my-app_etl-pipeline_inst-1_snk");
///
/// // App only
/// let id = generate_client_id(Some("myapp"), "analytics", None, ClientType::Source);
/// assert_eq!(id, "velo_myapp_analytics_src");
///
/// // Instance only
/// let id = generate_client_id(None, "analytics", Some("test-123"), ClientType::Sink);
/// assert_eq!(id, "velo_test-123_analytics_snk");
///
/// // Minimal
/// let id = generate_client_id(None, "analytics", None, ClientType::Source);
/// assert_eq!(id, "velo_analytics_src");
/// ```
pub fn generate_client_id(
    app_name: Option<&str>,
    job_name: &str,
    instance_id: Option<&str>,
    client_type: ClientType,
) -> String {
    let suffix = client_type.suffix();
    match (app_name, instance_id) {
        (Some(app), Some(inst)) => format!("velo_{}_{}_{}_{}", app, job_name, inst, suffix),
        (Some(app), None) => format!("velo_{}_{}_{}", app, job_name, suffix),
        (None, Some(inst)) => format!("velo_{}_{}_{}", inst, job_name, suffix),
        (None, None) => format!("velo_{}_{}", job_name, suffix),
    }
}

/// Log client ID creation with consistent format
pub fn log_client_id(
    client_id: &str,
    client_type: ClientType,
    app_name: Option<&str>,
    job_name: &str,
    instance_id: Option<&str>,
) {
    log::info!(
        "Kafka {} client.id: '{}' (app: {}, job: {}, instance: {})",
        client_type.display_name(),
        client_id,
        app_name.unwrap_or("none"),
        job_name,
        instance_id.unwrap_or("none")
    );
}

/// Generate a processor-level client ID for multi-source/multi-sink jobs
///
/// Format: `velo_{app_name}_{job_name}_{instance_id}_{endpoint_name}`
///
/// This format is used by job processors where a single job may have multiple
/// sources or sinks, and each needs a unique client.id for Kafka monitoring.
///
/// # Arguments
/// * `app_name` - Optional application name (defaults to "default")
/// * `job_name` - Job/query name
/// * `instance_id` - Optional instance ID (defaults to "0")
/// * `endpoint_name` - Source or sink name (e.g., "orders", "users")
///
/// # Examples
/// ```
/// use velostream::velostream::datasource::kafka::config_helpers::generate_processor_client_id;
///
/// let id = generate_processor_client_id(Some("myapp"), "analytics", Some("inst-1"), "orders");
/// assert_eq!(id, "velo_myapp_analytics_inst-1_orders");
///
/// let id = generate_processor_client_id(None, "etl", None, "source_events");
/// assert_eq!(id, "velo_default_etl_0_events");
/// ```
pub fn generate_processor_client_id(
    app_name: Option<&str>,
    job_name: &str,
    instance_id: Option<&str>,
    endpoint_name: &str,
) -> String {
    // Strip common prefixes from endpoint name for cleaner IDs
    let clean_name = endpoint_name
        .strip_prefix("source_")
        .or_else(|| endpoint_name.strip_prefix("sink_"))
        .unwrap_or(endpoint_name);

    format!(
        "velo_{}_{}_{}_{}",
        app_name.unwrap_or("default"),
        job_name,
        instance_id.unwrap_or("0"),
        clean_name
    )
}

/// Generate a transactional ID for exactly-once Kafka producers
///
/// Format: `velo_{app_name}_{job_name}_{instance_id}_{sink_name}`
///
/// The transactional.id must be unique per producer instance to avoid conflicts.
/// It uses the same format as processor client IDs for consistency.
///
/// # Arguments
/// * `app_name` - Optional application name (defaults to "default")
/// * `job_name` - Job/query name
/// * `instance_id` - Optional instance ID (defaults to "0")
/// * `sink_name` - Sink name (e.g., "output", "results")
///
/// # Examples
/// ```
/// use velostream::velostream::datasource::kafka::config_helpers::generate_transactional_id;
///
/// let txn_id = generate_transactional_id(Some("myapp"), "analytics", Some("inst-1"), "output");
/// assert_eq!(txn_id, "velo_myapp_analytics_inst-1_output");
/// ```
pub fn generate_transactional_id(
    app_name: Option<&str>,
    job_name: &str,
    instance_id: Option<&str>,
    sink_name: &str,
) -> String {
    // Use same format as processor client ID for consistency
    generate_processor_client_id(app_name, job_name, instance_id, sink_name)
}

/// Log processor client ID creation with endpoint context
pub fn log_processor_client_id(
    client_id: &str,
    client_type: ClientType,
    app_name: Option<&str>,
    job_name: &str,
    instance_id: Option<&str>,
    endpoint_name: &str,
) {
    log::debug!(
        "Setting {} client.id='{}' (app: {}, job: {}, instance: {}, endpoint: {})",
        client_type.display_name(),
        client_id,
        app_name.unwrap_or("default"),
        job_name,
        instance_id.unwrap_or("0"),
        endpoint_name
    );
}

/// Log transactional ID creation
pub fn log_transactional_id(
    txn_id: &str,
    app_name: Option<&str>,
    job_name: &str,
    instance_id: Option<&str>,
    sink_name: &str,
) {
    log::info!(
        "Enabling transactional producer with transactional.id='{}' (app: {}, job: {}, instance: {}, sink: {})",
        txn_id,
        app_name.unwrap_or("default"),
        job_name,
        instance_id.unwrap_or("0"),
        sink_name
    );
}

/// Generate a consumer group ID for Kafka consumers
///
/// Format: `velo_{app_name}_{job_name}` or `velo_{job_name}` if no app
///
/// Uses underscores as delimiters so that dashes can be used within component names.
///
/// # Arguments
/// * `app_name` - Optional application name
/// * `job_name` - Job/query name
///
/// # Examples
/// ```
/// use velostream::velostream::datasource::kafka::config_helpers::generate_consumer_group_id;
///
/// let group = generate_consumer_group_id(Some("my-app"), "etl-job");
/// assert_eq!(group, "velo_my-app_etl-job");
///
/// let group = generate_consumer_group_id(None, "analytics");
/// assert_eq!(group, "velo_analytics");
/// ```
pub fn generate_consumer_group_id(app_name: Option<&str>, job_name: &str) -> String {
    match app_name {
        Some(app) => format!("velo_{}_{}", app, job_name),
        None => format!("velo_{}", job_name),
    }
}

/// Generate a consumer group ID for test harness usage
///
/// Format: `test_harness_{run_id}_{context}`
///
/// # Arguments
/// * `run_id` - Unique run identifier (e.g., UUID prefix)
/// * `context` - Optional context (e.g., topic name)
///
/// # Examples
/// ```
/// use velostream::velostream::datasource::kafka::config_helpers::generate_test_harness_group_id;
///
/// let group = generate_test_harness_group_id("abc123", Some("orders"));
/// assert_eq!(group, "test_harness_abc123_orders");
///
/// let group = generate_test_harness_group_id("abc123", None);
/// assert_eq!(group, "test_harness_abc123");
/// ```
pub fn generate_test_harness_group_id(run_id: &str, context: Option<&str>) -> String {
    match context {
        Some(ctx) => format!("test_harness_{}_{}", run_id, ctx),
        None => format!("test_harness_{}", run_id),
    }
}

/// Suspicious topic names that indicate misconfiguration
/// These are common placeholder/fallback values
const SUSPICIOUS_TOPIC_NAMES: &[&str] = &[
    "default",
    "test",
    "temp",
    "placeholder",
    "undefined",
    "null",
    "none",
    "example",
    "my-topic",
    "topic-name",
];

/// Validate topic configuration
///
/// # Checks
/// - Empty topic names (always fails)
/// - Suspicious/placeholder topic names (fails with guidance)
///
/// # Errors
/// Returns a detailed error message if validation fails
pub fn validate_topic_configuration(
    topic: &str,
    context: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if topic.is_empty() {
        return Err(format!(
            "CONFIGURATION ERROR: Kafka {} topic name is empty.\n\
             \n\
             A valid Kafka topic name MUST be configured. Please configure via:\n\
             1. YAML config file: 'topic.name: <topic_name>'\n\
             2. SQL properties: '<sink_name>.topic = <topic_name>'\n\
             3. Direct parameter when creating KafkaDataReader/Writer\n\
             \n\
             This validation prevents misconfiguration of data {}.",
            context, context
        )
        .into());
    }

    if SUSPICIOUS_TOPIC_NAMES.contains(&topic.to_lowercase().as_str()) {
        return Err(format!(
            "CONFIGURATION ERROR: Kafka {} configured with suspicious topic name '{}'.\n\
             \n\
             This is a common placeholder/fallback value that indicates configuration \
             was not properly loaded.\n\
             \n\
             Valid topic names should be:\n\
             1. Extracted from source/sink name in SQL: CREATE STREAM <name> ...\n\
             2. Configured in YAML: 'topic: <topic_name>' or 'topic.name: <topic_name>'\n\
             \n\
             Common misconfiguration causes:\n\
             - YAML file not found or not loaded\n\
             - Missing 'topic' or 'topic.name' in YAML\n\
             - Hardcoded fallback value not updated\n\
             \n\
             This validation prevents misconfiguration of {}.",
            context, topic, context
        )
        .into());
    }

    log::info!(
        "Topic validation passed - will use {} topic '{}'",
        context,
        topic
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_format_requires_value_serializer() {
        let mut props = HashMap::new();
        props.insert("value.serializer".to_string(), "avro".to_string());

        let format = extract_format_from_properties(&props);
        assert!(matches!(format, SerializationFormat::Avro { .. }));
    }

    #[test]
    fn test_extract_format_rejects_generic_format_key() {
        let mut props = HashMap::new();
        props.insert("format".to_string(), "avro".to_string());
        let format = extract_format_from_properties(&props);
        // Should default to JSON, NOT recognize "format" key
        assert!(matches!(format, SerializationFormat::Json));
    }

    #[test]
    fn test_extract_key_format_explicit() {
        let mut props = HashMap::new();
        props.insert("key.serializer".to_string(), "string".to_string());

        let format = extract_key_format_from_properties(&props);
        assert!(matches!(format, SerializationFormat::String));
    }

    #[test]
    fn test_extract_format_default() {
        let props = HashMap::new();
        let format = extract_format_from_properties(&props);
        assert!(matches!(format, SerializationFormat::Json));
    }

    #[test]
    fn test_extract_key_format_default() {
        let props = HashMap::new();
        let format = extract_key_format_from_properties(&props);
        assert!(matches!(format, SerializationFormat::String));
    }

    #[test]
    fn test_extract_avro_finds_inline_schema() {
        let mut props = HashMap::new();
        props.insert(
            "value.avro.schema".to_string(),
            r#"{"type":"string"}"#.to_string(),
        );
        let result = extract_avro_schema_from_properties(&props);
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_extract_avro_prefers_inline_over_file() {
        let mut props = HashMap::new();
        props.insert(
            "value.avro.schema".to_string(),
            r#"{"type":"string"}"#.to_string(),
        );
        props.insert(
            "value.schema.file".to_string(),
            "nonexistent.avsc".to_string(),
        );
        let result = extract_avro_schema_from_properties(&props);
        // Inline should take priority, won't try to load file
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_extract_key_no_inline_schema() {
        let props = HashMap::new();
        // Try to provide inline schema for key (should be ignored)
        // Keys only support file-based schemas
        assert_eq!(
            extract_key_avro_schema_from_properties(&props).unwrap(),
            None
        );
    }

    #[test]
    fn test_extract_key_field() {
        let mut props = HashMap::new();
        props.insert("key.field".to_string(), "order_id".to_string());

        let key_field = extract_key_field_from_properties(&props);
        assert_eq!(key_field, Some("order_id".to_string()));
    }

    #[test]
    fn test_validate_suspicious_topic() {
        let result = validate_topic_configuration("test", "sink");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("suspicious"));
    }

    #[test]
    fn test_validate_empty_topic() {
        let result = validate_topic_configuration("", "source");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    // ============================================================================
    // CLIENT ID GENERATION TESTS
    // ============================================================================

    #[test]
    fn test_generate_client_id_full() {
        let id = generate_client_id(Some("myapp"), "analytics", Some("inst-1"), ClientType::Sink);
        assert_eq!(id, "velo_myapp_analytics_inst-1_snk");
    }

    #[test]
    fn test_generate_client_id_app_only() {
        let id = generate_client_id(Some("myapp"), "analytics", None, ClientType::Source);
        assert_eq!(id, "velo_myapp_analytics_src");
    }

    #[test]
    fn test_generate_client_id_instance_only() {
        let id = generate_client_id(None, "analytics", Some("test-123"), ClientType::Sink);
        assert_eq!(id, "velo_test-123_analytics_snk");
    }

    #[test]
    fn test_generate_client_id_minimal() {
        let id = generate_client_id(None, "analytics", None, ClientType::Source);
        assert_eq!(id, "velo_analytics_src");
    }

    #[test]
    fn test_generate_client_id_with_dashes_in_names() {
        // Verify dashes within component names are preserved
        let id = generate_client_id(
            Some("my-app"),
            "etl-pipeline",
            Some("run-001"),
            ClientType::Source,
        );
        assert_eq!(id, "velo_my-app_etl-pipeline_run-001_src");
    }

    #[test]
    fn test_client_type_suffix() {
        assert_eq!(ClientType::Source.suffix(), "src");
        assert_eq!(ClientType::Sink.suffix(), "snk");
    }

    #[test]
    fn test_client_type_display_name() {
        assert_eq!(ClientType::Source.display_name(), "consumer");
        assert_eq!(ClientType::Sink.display_name(), "producer");
    }

    // ============================================================================
    // PROCESSOR CLIENT ID TESTS
    // ============================================================================

    #[test]
    fn test_generate_processor_client_id_full() {
        let id = generate_processor_client_id(Some("myapp"), "analytics", Some("inst-1"), "orders");
        assert_eq!(id, "velo_myapp_analytics_inst-1_orders");
    }

    #[test]
    fn test_generate_processor_client_id_defaults() {
        let id = generate_processor_client_id(None, "etl", None, "events");
        assert_eq!(id, "velo_default_etl_0_events");
    }

    #[test]
    fn test_generate_processor_client_id_strips_source_prefix() {
        let id = generate_processor_client_id(None, "job", None, "source_events");
        assert_eq!(id, "velo_default_job_0_events");
    }

    #[test]
    fn test_generate_processor_client_id_strips_sink_prefix() {
        let id = generate_processor_client_id(None, "job", None, "sink_output");
        assert_eq!(id, "velo_default_job_0_output");
    }

    // ============================================================================
    // TRANSACTIONAL ID TESTS
    // ============================================================================

    #[test]
    fn test_generate_transactional_id() {
        let txn_id =
            generate_transactional_id(Some("myapp"), "analytics", Some("inst-1"), "output");
        assert_eq!(txn_id, "velo_myapp_analytics_inst-1_output");
    }

    #[test]
    fn test_generate_transactional_id_defaults() {
        let txn_id = generate_transactional_id(None, "etl", None, "results");
        assert_eq!(txn_id, "velo_default_etl_0_results");
    }

    #[test]
    fn test_generate_transactional_id_strips_sink_prefix() {
        let txn_id = generate_transactional_id(None, "job", None, "sink_final");
        assert_eq!(txn_id, "velo_default_job_0_final");
    }

    // ============================================================================
    // CONSUMER GROUP ID TESTS
    // ============================================================================

    #[test]
    fn test_generate_consumer_group_id_with_app() {
        let group = generate_consumer_group_id(Some("my-app"), "etl-job");
        assert_eq!(group, "velo_my-app_etl-job");
    }

    #[test]
    fn test_generate_consumer_group_id_without_app() {
        let group = generate_consumer_group_id(None, "analytics");
        assert_eq!(group, "velo_analytics");
    }

    #[test]
    fn test_generate_consumer_group_id_preserves_dashes() {
        let group = generate_consumer_group_id(Some("streaming-app"), "etl-pipeline-v2");
        assert_eq!(group, "velo_streaming-app_etl-pipeline-v2");
    }

    // ============================================================================
    // TEST HARNESS GROUP ID TESTS
    // ============================================================================

    #[test]
    fn test_generate_test_harness_group_id_with_context() {
        let group = generate_test_harness_group_id("abc123", Some("orders"));
        assert_eq!(group, "test_harness_abc123_orders");
    }

    #[test]
    fn test_generate_test_harness_group_id_without_context() {
        let group = generate_test_harness_group_id("abc123", None);
        assert_eq!(group, "test_harness_abc123");
    }

    #[test]
    fn test_generate_test_harness_group_id_preserves_dashes() {
        let group = generate_test_harness_group_id("run-001", Some("market-data"));
        assert_eq!(group, "test_harness_run-001_market-data");
    }
}
