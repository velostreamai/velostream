//! Serialization traits and core types

use crate::ferris::sql::execution::types::FieldValue;
use std::collections::HashMap;

/// Core trait for serialization formats
pub trait SerializationFormat: Send + Sync {
    /// Serialize a single field value
    fn serialize_field(&self, value: &FieldValue) -> Result<Vec<u8>, SerializationError>;
    
    /// Deserialize a single field value
    fn deserialize_field(&self, data: &[u8]) -> Result<FieldValue, SerializationError>;
    
    /// Serialize a complete record
    fn serialize_record(
        &self,
        fields: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError>;
    
    /// Deserialize a complete record
    fn deserialize_record(
        &self,
        data: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError>;
    
    /// Get the format identifier
    fn format_name(&self) -> &str;
    
    /// Check if format supports schema
    fn supports_schema(&self) -> bool;
}

/// Internal value representation for cross-format compatibility
#[derive(Debug, Clone, PartialEq)]
pub enum InternalValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<InternalValue>),
    Map(HashMap<String, InternalValue>),
    Timestamp(i64),
    ScaledNumber(i64, u8), // For financial precision
}

/// Serialization error types
#[derive(Debug)]
pub enum SerializationError {
    UnsupportedType(String),
    InvalidData(String),
    SchemaError(String),
    IoError(std::io::Error),
    JsonError(serde_json::Error),
    #[cfg(feature = "avro")]
    AvroError(apache_avro::Error),
    #[cfg(feature = "protobuf")]
    ProtobufError(prost::DecodeError),
}

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializationError::UnsupportedType(msg) => write!(f, "Unsupported type: {}", msg),
            SerializationError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            SerializationError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            SerializationError::IoError(err) => write!(f, "IO error: {}", err),
            SerializationError::JsonError(err) => write!(f, "JSON error: {}", err),
            #[cfg(feature = "avro")]
            SerializationError::AvroError(err) => write!(f, "Avro error: {}", err),
            #[cfg(feature = "protobuf")]
            SerializationError::ProtobufError(err) => write!(f, "Protobuf error: {}", err),
        }
    }
}

impl std::error::Error for SerializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SerializationError::IoError(err) => Some(err),
            SerializationError::JsonError(err) => Some(err),
            #[cfg(feature = "avro")]
            SerializationError::AvroError(err) => Some(err),
            #[cfg(feature = "protobuf")]
            SerializationError::ProtobufError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SerializationError {
    fn from(err: std::io::Error) -> Self {
        SerializationError::IoError(err)
    }
}

impl From<serde_json::Error> for SerializationError {
    fn from(err: serde_json::Error) -> Self {
        SerializationError::JsonError(err)
    }
}

#[cfg(feature = "avro")]
impl From<apache_avro::Error> for SerializationError {
    fn from(err: apache_avro::Error) -> Self {
        SerializationError::AvroError(err)
    }
}

#[cfg(feature = "protobuf")]
impl From<prost::DecodeError> for SerializationError {
    fn from(err: prost::DecodeError) -> Self {
        SerializationError::ProtobufError(err)
    }
}