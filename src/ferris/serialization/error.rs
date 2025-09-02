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
            // The corrected line is here
            SerializationError::SchemaError(msg) => {
                write!(f, "Schema error: {}", msg)
            }
        }
    }
}

impl std::error::Error for SerializationError {}

/// Convert SerializationError to SqlError
impl From<SerializationError> for SqlError {
    fn from(err: SerializationError) -> Self {
        SqlError::ExecutionError {
            message: format!("Serialization error: {}", err),
            query: None,
        }
    }
}
