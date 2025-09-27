//! CTAS and Table-specific error types with proper context preservation
//!
//! This module provides comprehensive error types for table operations,
//! ensuring proper error context is preserved throughout the error chain.

use crate::velostream::datasource::DataSourceError;
use crate::velostream::sql::SqlError;
use std::fmt;

/// Main error type for CTAS (CREATE TABLE AS SELECT) operations
#[derive(Debug, thiserror::Error)]
pub enum CtasError {
    /// Table creation failed
    #[error("Table '{table_name}' creation failed: {message}")]
    TableCreationFailed {
        table_name: String,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Source configuration is invalid
    #[error("Invalid source configuration for '{source_name}': {reason}")]
    InvalidSourceConfig { source_name: String, reason: String },

    /// Property combination is invalid
    #[error("Invalid property combination: {details}")]
    InvalidProperties {
        details: String,
        conflicting_properties: Vec<String>,
    },

    /// Feature not yet implemented
    #[error("Feature not implemented: {feature}")]
    NotImplemented {
        feature: String,
        workaround: Option<String>,
    },

    /// Connection to data source failed
    #[error("Failed to connect to {source_type} source '{source_name}': {reason}")]
    SourceConnectionFailed {
        source_type: String,
        source_name: String,
        reason: String,
        #[source]
        source: Option<DataSourceError>,
    },

    /// Table already exists
    #[error("Table '{table_name}' already exists")]
    TableAlreadyExists {
        table_name: String,
        drop_hint: String,
    },

    /// Table not found
    #[error("Table '{table_name}' not found. Available tables: {available:?}")]
    TableNotFound {
        table_name: String,
        available: Vec<String>,
    },

    /// Schema validation failed
    #[error("Schema validation failed for table '{table_name}': {reason}")]
    SchemaValidationFailed {
        table_name: String,
        reason: String,
        expected_schema: Option<String>,
        actual_schema: Option<String>,
    },

    /// Query parsing failed
    #[error("Failed to parse CTAS query: {reason}")]
    QueryParseFailed {
        reason: String,
        query_snippet: String,
        #[source]
        source: SqlError,
    },

    /// Background job failed
    #[error("Background population job for table '{table_name}' failed: {reason}")]
    BackgroundJobFailed {
        table_name: String,
        reason: String,
        records_processed: usize,
    },

    /// Memory limit exceeded
    #[error(
        "Memory limit exceeded for table '{table_name}': used {used_mb}MB, limit {limit_mb}MB"
    )]
    MemoryLimitExceeded {
        table_name: String,
        used_mb: usize,
        limit_mb: usize,
        suggestion: String,
    },

    /// Configuration conflict
    #[error("Configuration conflict in table '{table_name}': {description}")]
    ConfigurationConflict {
        table_name: String,
        description: String,
        resolution: String,
    },
}

impl CtasError {
    /// Create a table creation failed error with source
    pub fn table_creation<E>(
        table_name: impl Into<String>,
        message: impl Into<String>,
        source: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::TableCreationFailed {
            table_name: table_name.into(),
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a not implemented error with optional workaround
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Self::NotImplemented {
            feature: feature.into(),
            workaround: None,
        }
    }

    /// Create a not implemented error with workaround suggestion
    pub fn not_implemented_with_workaround(
        feature: impl Into<String>,
        workaround: impl Into<String>,
    ) -> Self {
        Self::NotImplemented {
            feature: feature.into(),
            workaround: Some(workaround.into()),
        }
    }

    /// Create a memory limit exceeded error
    pub fn memory_limit(table_name: impl Into<String>, used_mb: usize, limit_mb: usize) -> Self {
        Self::MemoryLimitExceeded {
            table_name: table_name.into(),
            used_mb,
            limit_mb,
            suggestion: format!(
                "Consider using CompactTable format or increasing memory limit to {}MB",
                used_mb + (used_mb / 4) // Suggest 25% more than current usage
            ),
        }
    }
}

/// Convert CtasError to SqlError for backward compatibility
impl From<CtasError> for SqlError {
    fn from(err: CtasError) -> Self {
        match err {
            CtasError::QueryParseFailed { source, .. } => source,
            _ => SqlError::ExecutionError {
                message: err.to_string(),
                query: None,
            },
        }
    }
}

/// Table-specific error type for runtime operations
#[derive(Debug, thiserror::Error)]
pub enum TableError {
    /// Query execution failed
    #[error("Query execution failed on table '{table_name}': {reason}")]
    QueryExecutionFailed {
        table_name: String,
        reason: String,
        query: String,
    },

    /// Field not found in table
    #[error(
        "Field '{field_name}' not found in table '{table_name}'. Available fields: {available:?}"
    )]
    FieldNotFound {
        table_name: String,
        field_name: String,
        available: Vec<String>,
    },

    /// Type conversion failed
    #[error(
        "Type conversion failed for field '{field_name}': cannot convert {from_type} to {to_type}"
    )]
    TypeConversionFailed {
        field_name: String,
        from_type: String,
        to_type: String,
        value: String,
    },

    /// Index out of bounds
    #[error("Index {index} out of bounds for table '{table_name}' with {size} records")]
    IndexOutOfBounds {
        table_name: String,
        index: usize,
        size: usize,
    },

    /// Concurrent modification
    #[error("Concurrent modification detected on table '{table_name}'")]
    ConcurrentModification {
        table_name: String,
        operation: String,
    },
}

/// Result type alias for CTAS operations
pub type CtasResult<T> = Result<T, CtasError>;

/// Result type alias for table operations
pub type TableResult<T> = Result<T, TableError>;

/// Helper trait for adding context to errors
pub trait ErrorContext<T> {
    /// Add context to an error
    fn context(self, msg: impl fmt::Display) -> Result<T, CtasError>;

    /// Add table context to an error
    fn table_context(self, table_name: impl Into<String>) -> Result<T, CtasError>;
}

impl<T, E> ErrorContext<T> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn context(self, msg: impl fmt::Display) -> Result<T, CtasError> {
        self.map_err(|e| CtasError::TableCreationFailed {
            table_name: String::new(),
            message: msg.to_string(),
            source: Some(Box::new(e)),
        })
    }

    fn table_context(self, table_name: impl Into<String>) -> Result<T, CtasError> {
        let table_name = table_name.into();
        self.map_err(|e| CtasError::TableCreationFailed {
            table_name: table_name.clone(),
            message: format!("Operation failed on table '{}'", table_name),
            source: Some(Box::new(e)),
        })
    }
}
