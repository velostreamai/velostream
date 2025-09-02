//! Error types for serialization

use crate::ferris::sql::SqlError;

/// Enhanced serialization error type with proper error chaining
#[derive(Debug)]
pub enum SerializationError {
    // Legacy string-only variants (kept for backward compatibility)
    SerializationFailed(String),
    DeserializationFailed(String), 
    FormatConversionFailed(String),
    UnsupportedType(String),
    SchemaError(String),

    // Enhanced variants with error source preservation
    /// JSON serialization/deserialization errors with source chain
    JsonError {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Avro serialization/deserialization errors with source chain
    AvroError {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Protobuf serialization/deserialization errors with source chain
    ProtobufError {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// UTF-8 conversion errors with source chain
    EncodingError {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Schema validation errors with source chain
    SchemaValidationError {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// Type conversion errors with source chain
    TypeConversionError {
        message: String,
        from_type: String,
        to_type: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    // Backward compatibility - kept for existing JsonSerializationFailed usage
    JsonSerializationFailed(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Legacy variants
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
            }
            SerializationError::SchemaError(msg) => {
                write!(f, "Schema error: {}", msg)
            }

            // Enhanced variants with error chaining
            SerializationError::JsonError { message, .. } => {
                write!(f, "JSON error: {}", message)
            }
            SerializationError::AvroError { message, .. } => {
                write!(f, "Avro error: {}", message)
            }
            SerializationError::ProtobufError { message, .. } => {
                write!(f, "Protobuf error: {}", message)
            }
            SerializationError::EncodingError { message, .. } => {
                write!(f, "Encoding error: {}", message)
            }
            SerializationError::SchemaValidationError { message, .. } => {
                write!(f, "Schema validation error: {}", message)
            }
            SerializationError::TypeConversionError { 
                message, from_type, to_type, .. 
            } => {
                write!(f, "Type conversion error: {} (from {} to {})", message, from_type, to_type)
            }

            // Backward compatibility
            SerializationError::JsonSerializationFailed(err) => {
                write!(f, "JSON serialization error: {}", err)
            }
        }
    }
}

impl std::error::Error for SerializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Legacy variants have no source chain
            SerializationError::SerializationFailed(_) |
            SerializationError::DeserializationFailed(_) |
            SerializationError::FormatConversionFailed(_) |
            SerializationError::UnsupportedType(_) |
            SerializationError::SchemaError(_) => None,

            // Enhanced variants with error chaining
            SerializationError::JsonError { source, .. } => Some(source.as_ref()),
            SerializationError::AvroError { source, .. } => Some(source.as_ref()),
            SerializationError::ProtobufError { source, .. } => Some(source.as_ref()),
            SerializationError::EncodingError { source, .. } => Some(source.as_ref()),
            SerializationError::SchemaValidationError { source, .. } => source.as_ref().map(|s| s.as_ref() as &dyn std::error::Error),
            SerializationError::TypeConversionError { source, .. } => source.as_ref().map(|s| s.as_ref() as &dyn std::error::Error),

            // Backward compatibility
            SerializationError::JsonSerializationFailed(err) => Some(err.as_ref()),
        }
    }
}

impl SerializationError {
    /// Create a JSON error with source chain preservation
    pub fn json_error(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        SerializationError::JsonError {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Create an Avro error with source chain preservation
    pub fn avro_error(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        SerializationError::AvroError {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Create a Protobuf error with source chain preservation
    pub fn protobuf_error(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        SerializationError::ProtobufError {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Create an encoding error with source chain preservation
    pub fn encoding_error(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        SerializationError::EncodingError {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// Create a schema validation error with optional source chain preservation
    pub fn schema_validation_error(message: impl Into<String>, source: Option<impl std::error::Error + Send + Sync + 'static>) -> Self {
        SerializationError::SchemaValidationError {
            message: message.into(),
            source: source.map(|s| Box::new(s) as Box<dyn std::error::Error + Send + Sync>),
        }
    }

    /// Create a type conversion error with optional source chain preservation
    pub fn type_conversion_error(
        message: impl Into<String>,
        from_type: impl Into<String>,
        to_type: impl Into<String>,
        source: Option<impl std::error::Error + Send + Sync + 'static>,
    ) -> Self {
        SerializationError::TypeConversionError {
            message: message.into(),
            from_type: from_type.into(),
            to_type: to_type.into(),
            source: source.map(|s| Box::new(s) as Box<dyn std::error::Error + Send + Sync>),
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
