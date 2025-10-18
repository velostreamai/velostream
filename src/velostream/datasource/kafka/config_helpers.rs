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
        assert!(matches!(format, SerializationFormat::Json { .. }));
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
        assert!(matches!(format, SerializationFormat::Json { .. }));
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
        let mut props = HashMap::new();
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
}
