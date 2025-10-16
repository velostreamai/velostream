//! Shared configuration helpers for KafkaDataReader and KafkaDataWriter
//!
//! This module provides common functionality for extracting and validating
//! Kafka datasource configuration from property maps, ensuring consistency
//! between reader and writer implementations.

use crate::velostream::kafka::serialization_format::SerializationFormat;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;

/// Extract serialization format from properties using standard key conventions
///
/// Tries multiple property keys in preference order:
/// 1. "value.serializer" (Kafka convention)
/// 2. "schema.value.serializer"
/// 3. "serializer.format"
/// 4. "format" (fallback)
pub fn extract_format_from_properties(properties: &HashMap<String, String>) -> SerializationFormat {
    let format_str = properties
        .get("value.serializer")
        .or_else(|| properties.get("schema.value.serializer"))
        .or_else(|| properties.get("serializer.format"))
        .or_else(|| properties.get("format"))
        .map(|s| s.as_str())
        .unwrap_or("json");

    SerializationFormat::from_str(format_str).unwrap_or(SerializationFormat::Json)
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

/// Extract Avro schema from properties (inline or from file)
///
/// Tries multiple property keys for inline schema:
/// - "avro.schema"
/// - "value.avro.schema"
/// - "schema.avro"
/// - "avro_schema" (legacy)
///
/// If no inline schema found, tries file paths:
/// - "avro.schema.file"
/// - "schema.value.schema.file"
/// - "value.schema.file"
/// - "schema.file"
/// - "avro_schema_file" (legacy)
/// - "schema_file" (legacy)
pub fn extract_avro_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    // Try inline schema keys first
    let schema = properties
        .get("avro.schema")
        .or_else(|| properties.get("value.avro.schema"))
        .or_else(|| properties.get("schema.avro"))
        .or_else(|| properties.get("avro_schema"))
        .cloned();

    if schema.is_some() {
        return Ok(schema);
    }

    // Try schema file keys
    if let Some(schema_file) = properties
        .get("avro.schema.file")
        .or_else(|| properties.get("schema.value.schema.file"))
        .or_else(|| properties.get("value.schema.file"))
        .or_else(|| properties.get("schema.file"))
        .or_else(|| properties.get("avro_schema_file"))
        .or_else(|| properties.get("schema_file"))
    {
        return load_schema_from_file(schema_file);
    }

    Ok(None)
}

/// Extract Protobuf schema from properties (inline or from file)
///
/// Tries multiple property keys for inline schema:
/// - "protobuf.schema"
/// - "value.protobuf.schema"
/// - "schema.protobuf"
/// - "protobuf_schema" (legacy)
/// - "proto.schema"
///
/// If no inline schema found, tries file paths:
/// - "protobuf.schema.file"
/// - "proto.schema.file"
/// - "schema.value.schema.file"
/// - "value.schema.file"
/// - "schema.file"
/// - "protobuf_schema_file" (legacy)
/// - "schema_file" (legacy)
pub fn extract_protobuf_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    // Try inline schema keys first
    let schema = properties
        .get("protobuf.schema")
        .or_else(|| properties.get("value.protobuf.schema"))
        .or_else(|| properties.get("schema.protobuf"))
        .or_else(|| properties.get("protobuf_schema"))
        .or_else(|| properties.get("proto.schema"))
        .cloned();

    if schema.is_some() {
        return Ok(schema);
    }

    // Try schema file keys
    if let Some(schema_file) = properties
        .get("protobuf.schema.file")
        .or_else(|| properties.get("proto.schema.file"))
        .or_else(|| properties.get("schema.value.schema.file"))
        .or_else(|| properties.get("value.schema.file"))
        .or_else(|| properties.get("schema.file"))
        .or_else(|| properties.get("protobuf_schema_file"))
        .or_else(|| properties.get("schema_file"))
    {
        return load_schema_from_file(schema_file);
    }

    Ok(None)
}

/// Extract JSON schema from properties (inline only)
///
/// Tries multiple property keys:
/// - "json.schema"
/// - "schema.json"
pub fn extract_json_schema_from_properties(
    properties: &HashMap<String, String>,
) -> Option<String> {
    properties
        .get("json.schema")
        .or_else(|| properties.get("schema.json"))
        .cloned()
}

/// Extract schema from properties based on serialization format
///
/// Delegates to format-specific extraction functions for comprehensive coverage
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
    fn test_extract_format_from_properties() {
        let mut props = HashMap::new();
        props.insert("format".to_string(), "avro".to_string());

        let format = extract_format_from_properties(&props);
        assert!(matches!(format, SerializationFormat::Avro { .. }));
    }

    #[test]
    fn test_extract_format_with_priority() {
        let mut props = HashMap::new();
        props.insert("format".to_string(), "json".to_string());
        props.insert("value.serializer".to_string(), "avro".to_string());

        let format = extract_format_from_properties(&props);
        assert!(matches!(format, SerializationFormat::Avro { .. }));
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
}
