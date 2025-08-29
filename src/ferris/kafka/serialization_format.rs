//! Configurable serialization format system for Phase 2 Enhanced Kafka Integration
//!
//! This module implements runtime-configurable serialization formats that can be
//! selected via SQL WITH clauses or programmatic configuration. It replaces the
//! hardcoded JsonSerializer approach with a flexible factory pattern.

use crate::ferris::kafka::serialization::{JsonSerializer, SerializationError, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;



/// Enumeration of supported serialization formats for Kafka messages
///
/// This enum enables runtime selection of serialization formats, supporting
/// different use cases:
/// - JSON: Development, debugging, cross-language compatibility
/// - Avro: Production, schema evolution, compact binary format
/// - Protobuf: High performance, strict typing, cross-platform
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerializationFormat {
    /// JSON serialization for maximum compatibility and readability
    Json,

    /// Apache Avro with Schema Registry integration
    #[cfg(feature = "avro")]
    Avro {
        /// Schema Registry URL (e.g., "http://localhost:8081")
        schema_registry_url: String,
        /// Subject name for schema lookup (e.g., "customer-spending-value")
        subject: String,
    },

    /// Protocol Buffers for high-performance binary serialization
    #[cfg(feature = "protobuf")]
    Protobuf {
        /// Fully qualified message type name
        message_type: String,
    },

    /// Raw bytes (no serialization/deserialization)
    Bytes,

    /// UTF-8 string serialization
    String,
}

impl fmt::Display for SerializationFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerializationFormat::Json => write!(f, "json"),
            #[cfg(feature = "avro")]
            SerializationFormat::Avro { .. } => write!(f, "avro"),
            #[cfg(feature = "protobuf")]
            SerializationFormat::Protobuf { .. } => write!(f, "protobuf"),
            SerializationFormat::Bytes => write!(f, "bytes"),
            SerializationFormat::String => write!(f, "string"),
        }
    }
}

impl std::str::FromStr for SerializationFormat {
    type Err = SerializationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(SerializationFormat::Json),
            #[cfg(feature = "avro")]
            "avro" => Ok(SerializationFormat::Avro {
                schema_registry_url: String::new(), // Will be populated later
                subject: String::new(),
            }),
            #[cfg(feature = "protobuf")]
            "protobuf" | "proto" => Ok(SerializationFormat::Protobuf {
                message_type: String::new(), // Will be populated later
            }),
            "bytes" | "raw" => Ok(SerializationFormat::Bytes),
            "string" | "text" => Ok(SerializationFormat::String),
            _ => Err(SerializationError::Schema(format!(
                "Unsupported serialization format: '{}'. Supported formats: json, avro, protobuf, bytes, string",
                s
            ))),
        }
    }
}

/// Configuration container for serialization parameters extracted from SQL WITH clauses
#[derive(Debug, Clone, Default)]
pub struct SerializationConfig {
    /// Key serialization format
    pub key_format: Option<SerializationFormat>,
    /// Value serialization format  
    pub value_format: Option<SerializationFormat>,
    /// Schema Registry URL for Avro serialization
    pub schema_registry_url: Option<String>,
    /// Subject name for key schema (Avro)
    pub key_subject: Option<String>,
    /// Subject name for value schema (Avro)
    pub value_subject: Option<String>,
    /// Custom properties for advanced configuration
    pub custom_properties: HashMap<String, String>,
}

impl SerializationConfig {
    /// Create a new empty serialization configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse serialization configuration from SQL WITH clause parameters
    ///
    /// Expected parameters:
    /// - `key.serializer` or `key_serializer`: Key serialization format
    /// - `value.serializer` or `value_serializer`: Value serialization format  
    /// - `schema.registry.url`: Schema Registry URL for Avro
    /// - `key.subject`: Avro subject for key schema
    /// - `value.subject`: Avro subject for value schema
    pub fn from_sql_params(params: &HashMap<String, String>) -> Result<Self, SerializationError> {
        let mut config = SerializationConfig::new();

        // Parse key serializer
        if let Some(key_serializer) = params
            .get("key.serializer")
            .or_else(|| params.get("key_serializer"))
        {
            let mut format: SerializationFormat = key_serializer.parse()?;

            // Configure Avro-specific parameters
            #[cfg(feature = "avro")]
            if let SerializationFormat::Avro {
                schema_registry_url,
                subject,
            } = &mut format
            {
                if let Some(registry_url) = params.get("schema.registry.url") {
                    *schema_registry_url = registry_url.clone();
                }
                if let Some(key_subject) = params.get("key.subject") {
                    *subject = key_subject.clone();
                }
            }

            config.key_format = Some(format);
        }

        // Parse value serializer
        if let Some(value_serializer) = params
            .get("value.serializer")
            .or_else(|| params.get("value_serializer"))
        {
            let mut format: SerializationFormat = value_serializer.parse()?;

            // Configure Avro-specific parameters
            #[cfg(feature = "avro")]
            if let SerializationFormat::Avro {
                schema_registry_url,
                subject,
            } = &mut format
            {
                if let Some(registry_url) = params.get("schema.registry.url") {
                    *schema_registry_url = registry_url.clone();
                }
                if let Some(value_subject) = params.get("value.subject") {
                    *subject = value_subject.clone();
                }
            }

            config.value_format = Some(format);
        }

        // Store additional parameters
        config.schema_registry_url = params.get("schema.registry.url").cloned();
        config.key_subject = params.get("key.subject").cloned();
        config.value_subject = params.get("value.subject").cloned();

        // Store any custom properties
        for (key, value) in params {
            if !key.contains("serializer") && !key.contains("schema") && !key.contains("subject") {
                config.custom_properties.insert(key.clone(), value.clone());
            }
        }

        Ok(config)
    }

    /// Get the key serialization format, defaulting to JSON
    pub fn key_format(&self) -> SerializationFormat {
        self.key_format.clone().unwrap_or(SerializationFormat::Json)
    }

    /// Get the value serialization format, defaulting to JSON
    pub fn value_format(&self) -> SerializationFormat {
        self.value_format
            .clone()
            .unwrap_or(SerializationFormat::Json)
    }

    /// Validate the serialization configuration
    pub fn validate(&self) -> Result<(), SerializationError> {
        // Validate Avro configuration
        #[cfg(feature = "avro")]
        {
            if matches!(self.key_format, Some(SerializationFormat::Avro { .. })) {
                if self.schema_registry_url.is_none() {
                    return Err(SerializationError::Schema(
                        "Schema Registry URL required for Avro key serialization".to_string(),
                    ));
                }
                if self.key_subject.is_none() {
                    return Err(SerializationError::Schema(
                        "Key subject required for Avro key serialization".to_string(),
                    ));
                }
            }

            if matches!(self.value_format, Some(SerializationFormat::Avro { .. })) {
                if self.schema_registry_url.is_none() {
                    return Err(SerializationError::Schema(
                        "Schema Registry URL required for Avro value serialization".to_string(),
                    ));
                }
                if self.value_subject.is_none() {
                    return Err(SerializationError::Schema(
                        "Value subject required for Avro value serialization".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Factory for creating serializers based on configuration
///
/// This factory enables runtime creation of appropriate serializers based on
/// the SerializationFormat, supporting dependency injection and configuration-driven
/// setup.
pub struct SerializationFactory;

impl SerializationFactory {
    /// Create a boxed serializer for the given format and type
    ///
    /// This is the main factory method that returns a trait object, allowing
    /// runtime polymorphism over different serialization formats.
    pub fn create_boxed_serializer<T>(
        format: &SerializationFormat,
    ) -> Result<Box<dyn Serializer<T>>, SerializationError>
    where
        T: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        match format {
            SerializationFormat::Json => Ok(Box::new(JsonSerializer)),

            #[cfg(feature = "avro")]
            SerializationFormat::Avro {
                schema_registry_url,
                subject,
            } => {
                // For now, return an error - we'll implement proper Avro support
                // with Schema Registry integration in the next step
                Err(SerializationError::FeatureNotEnabled(
                    format!("Avro serialization with Schema Registry not yet implemented. URL: {}, Subject: {}", 
                           schema_registry_url, subject)
                ))
            }

            #[cfg(feature = "protobuf")]
            SerializationFormat::Protobuf { message_type } => {
                // For now, return an error - we'll implement proper Protobuf support
                // in a later step
                Err(SerializationError::FeatureNotEnabled(format!(
                    "Protobuf serialization not yet implemented. Message type: {}",
                    message_type
                )))
            }

            SerializationFormat::Bytes => {
                // Bytes serialization only works with Vec<u8>
                Err(SerializationError::Schema(
                    "Bytes serialization only supported for Vec<u8> type".to_string(),
                ))
            }

            SerializationFormat::String => {
                // String serialization only works with String type
                Err(SerializationError::Schema(
                    "String serialization only supported for String type".to_string(),
                ))
            }
        }
    }

    /// Create a JSON serializer (convenience method)
    pub fn json_serializer<T>() -> JsonSerializer
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        JsonSerializer
    }

    /// Validate that the given format is supported and properly configured
    pub fn validate_format(format: &SerializationFormat) -> Result<(), SerializationError> {
        match format {
            SerializationFormat::Json => Ok(()),

            #[cfg(feature = "avro")]
            SerializationFormat::Avro {
                schema_registry_url,
                subject,
            } => {
                if schema_registry_url.is_empty() {
                    return Err(SerializationError::Schema(
                        "Schema Registry URL cannot be empty for Avro serialization".to_string(),
                    ));
                }
                if subject.is_empty() {
                    return Err(SerializationError::Schema(
                        "Subject cannot be empty for Avro serialization".to_string(),
                    ));
                }
                Ok(())
            }

            #[cfg(feature = "protobuf")]
            SerializationFormat::Protobuf { message_type } => {
                if message_type.is_empty() {
                    return Err(SerializationError::Schema(
                        "Message type cannot be empty for Protobuf serialization".to_string(),
                    ));
                }
                Ok(())
            }

            SerializationFormat::Bytes | SerializationFormat::String => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_format_parsing() {
        assert_eq!(
            "json".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );
        assert_eq!(
            "bytes".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Bytes
        );
        assert_eq!(
            "string".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::String
        );

        // Test case insensitive
        assert_eq!(
            "JSON".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );

        // Test invalid format
        assert!("invalid".parse::<SerializationFormat>().is_err());
    }

    #[test]
    fn test_serialization_config_from_sql_params() {
        let mut params = HashMap::new();
        params.insert("key.serializer".to_string(), "string".to_string());
        params.insert("value.serializer".to_string(), "json".to_string());
        params.insert("custom.param".to_string(), "custom_value".to_string());

        let config = SerializationConfig::from_sql_params(&params).unwrap();

        assert_eq!(config.key_format(), SerializationFormat::String);
        assert_eq!(config.value_format(), SerializationFormat::Json);
        assert_eq!(
            config.custom_properties.get("custom.param"),
            Some(&"custom_value".to_string())
        );
    }

    #[test]
    fn test_serialization_factory_json() {
        let _serializer = SerializationFactory::json_serializer::<serde_json::Value>();
        assert!(SerializationFactory::validate_format(&SerializationFormat::Json).is_ok());
    }
}
