//! Error types for serialization

use crate::ferris::sql::SqlError;

/// Serialization error type
#[derive(Debug)]
pub enum SerializationError {
    SerializationFailed(String),
    DeserializationFailed(String),
    FormatConversionFailed(String),
    UnsupportedType(String),
    SchemaError(String),
    // New variant for preserving JSON error source chain
    JsonSerializationFailed(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializationError::SerializationFailed(msg) => {
                write!(f, "Serialization failed: {}", msg)
            }
            SerializationError::DeserializationFailed(msg) => {
                write!(f, "Deserialization failed: {}", msg)
            }
            SerializationError::FormatConversionFailed(msg) => {
                write!(f, "Format conversion failed: {}", msg)
            }
            SerializationError::UnsupportedType(msg) => {
                write!(f, "Unsupported type: {}", msg)
            },
            SerializationError::SchemaError(msg) => {
                write!(f, "Schema error: {}", msg)
            },
            SerializationError::JsonSerializationFailed(err) => {
                write!(f, "JSON serialization error: {}", err)
            }
        }
    }
}

impl std::error::Error for SerializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SerializationError::JsonSerializationFailed(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

/// Convert SerializationError to SqlError
impl From<SerializationError> for SqlError {
    fn from(err: SerializationError) -> Self {
        SqlError::ExecutionError {
            message: format!("Serialization error: {}", err),
            query: None,
        }
    }
}
