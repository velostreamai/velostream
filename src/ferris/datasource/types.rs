//! Generic data source type definitions
//!
//! These types are independent of SQL and can be used by any data processing system.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Source-specific offset/position information
#[derive(Debug, Clone, PartialEq)]
pub enum SourceOffset {
    /// Kafka offset (partition, offset)
    Kafka { partition: i32, offset: i64 },
    /// File position (file path, byte offset, line number)
    File {
        path: String,
        byte_offset: u64,
        line_number: u64,
    },
    /// S3 object position (bucket, key, byte offset)
    S3 {
        bucket: String,
        key: String,
        byte_offset: u64,
    },
    /// Database cursor position (table, primary key values)
    Database {
        table: String,
        cursor: HashMap<String, String>,
    },
    /// Generic string-based offset
    Generic(String),
}

/// Metadata about a data source
#[derive(Debug, Clone)]
pub struct SourceMetadata {
    pub source_type: String,
    pub version: String,
    pub supports_streaming: bool,
    pub supports_batch: bool,
    pub supports_schema_evolution: bool,
    pub capabilities: Vec<String>,
}

/// Metadata about a data sink
#[derive(Debug, Clone)]
pub struct SinkMetadata {
    pub sink_type: String,
    pub version: String,
    pub supports_transactions: bool,
    pub supports_upsert: bool,
    pub supports_schema_evolution: bool,
    pub capabilities: Vec<String>,
}

/// Error types for data source operations
#[derive(Debug)]
pub enum DataSourceError {
    /// Configuration error
    Configuration(String),
    /// Connection/initialization error
    Connection(String),
    /// Schema-related error
    Schema(String),
    /// IO error during read/write
    Io(String),
    /// Unsupported operation
    Unsupported(String),
    /// Source-specific error
    SourceSpecific(Box<dyn Error + Send + Sync>),
    /// Sink-specific error
    SinkSpecific(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for DataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataSourceError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            DataSourceError::Connection(msg) => write!(f, "Connection error: {}", msg),
            DataSourceError::Schema(msg) => write!(f, "Schema error: {}", msg),
            DataSourceError::Io(msg) => write!(f, "IO error: {}", msg),
            DataSourceError::Unsupported(msg) => write!(f, "Unsupported operation: {}", msg),
            DataSourceError::SourceSpecific(err) => write!(f, "Source error: {}", err),
            DataSourceError::SinkSpecific(err) => write!(f, "Sink error: {}", err),
        }
    }
}

impl Error for DataSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataSourceError::SourceSpecific(err) => Some(err.as_ref()),
            DataSourceError::SinkSpecific(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}
