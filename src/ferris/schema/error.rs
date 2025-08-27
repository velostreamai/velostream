//! Schema Error Types
//!
//! This module defines comprehensive error types for schema management operations.

/// Error types for schema management operations
#[derive(Debug)]
pub enum SchemaError {
    NotFound {
        source: String,
    },
    Incompatible {
        reason: String,
    },
    Provider {
        source: String,
        message: String,
    },
    Cache {
        message: String,
    },
    Evolution {
        from: String,
        to: String,
        reason: String,
    },
    Validation {
        message: String,
    },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::NotFound { source } => write!(f, "Schema not found: {}", source),
            SchemaError::Incompatible { reason } => write!(f, "Schema incompatible: {}", reason),
            SchemaError::Provider { source, message } => {
                write!(f, "Schema provider error: {} - {}", source, message)
            }
            SchemaError::Cache { message } => write!(f, "Schema cache error: {}", message),
            SchemaError::Evolution { from, to, reason } => {
                write!(f, "Schema evolution error: {} -> {}: {}", from, to, reason)
            }
            SchemaError::Validation { message } => {
                write!(f, "Schema validation error: {}", message)
            }
        }
    }
}

impl std::error::Error for SchemaError {}

pub type SchemaResult<T> = Result<T, SchemaError>;