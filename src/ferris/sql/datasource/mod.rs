//! Data Source Abstraction Layer
//!
//! This module provides the core abstractions for pluggable data sources in FerrisStreams.
//! It enables reading from any source (Kafka, S3, files, databases) and writing to any sink
//! (Kafka, Iceberg, Parquet, etc.) using a unified interface.
//!
//! ## Architecture
//!
//! - **DataSource**: Input source that can provide readers
//! - **DataSink**: Output destination that can provide writers  
//! - **DataReader**: Reads records from any source type
//! - **DataWriter**: Writes records to any sink type
//! - **SourceConfig/SinkConfig**: Type-specific configuration
//!
//! ## Examples
//!
//! ```rust
//! use ferrisstreams::ferris::sql::datasource::*;
//!
//! // Create a Kafka source
//! let source = create_source("kafka://localhost:9092/orders")?;
//! let reader = source.create_reader().await?;
//!
//! // Create an S3 sink  
//! let sink = create_sink("s3://bucket/path/*.parquet")?;
//! let writer = sink.create_writer().await?;
//!
//! // Process: Kafka -> S3
//! while let Some(record) = reader.read().await? {
//!     writer.write(record).await?;
//! }
//! ```

use crate::ferris::sql::error::SqlError;
use crate::ferris::sql::execution::types::StreamRecord;
use crate::ferris::sql::schema::Schema;
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

pub mod config;
pub mod kafka;
pub mod registry;

// Re-export key types
pub use config::{ConnectionString, SinkConfig, SourceConfig};
pub use registry::{create_sink, create_source, DataSourceRegistry};

/// Core trait for data input sources
///
/// This trait abstracts any data source that can provide streaming or batch data.
/// Implementations can be for Kafka topics, S3 buckets, local files, database tables, etc.
#[async_trait]
pub trait DataSource: Send + Sync + 'static {
    /// Initialize the data source with configuration
    async fn initialize(
        &mut self,
        config: SourceConfig,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Get schema information for this source
    async fn fetch_schema(&self) -> Result<Schema, Box<dyn Error + Send + Sync>>;

    /// Create a reader for this source
    /// Multiple readers can be created for parallel processing
    async fn create_reader(&self) -> Result<Box<dyn DataReader>, Box<dyn Error + Send + Sync>>;

    /// Check if this source supports real-time streaming
    fn supports_streaming(&self) -> bool;

    /// Check if this source supports batch reading  
    fn supports_batch(&self) -> bool;

    /// Get source metadata (type, version, capabilities)
    fn metadata(&self) -> SourceMetadata;
}

/// Core trait for data output sinks
///
/// This trait abstracts any data destination that can accept streaming or batch data.
/// Implementations can be for Kafka topics, S3 buckets, Iceberg tables, databases, etc.
#[async_trait]
pub trait DataSink: Send + Sync + 'static {
    /// Initialize the data sink with configuration
    async fn initialize(&mut self, config: SinkConfig) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Validate that the provided schema is compatible with this sink
    async fn validate_schema(&self, schema: &Schema) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Create a writer for this sink
    /// Multiple writers can be created for parallel processing
    async fn create_writer(&self) -> Result<Box<dyn DataWriter>, Box<dyn Error + Send + Sync>>;

    /// Check if this sink supports transactional writes
    fn supports_transactions(&self) -> bool;

    /// Check if this sink supports updates/deletes (vs append-only)
    fn supports_upsert(&self) -> bool;

    /// Get sink metadata (type, version, capabilities)
    fn metadata(&self) -> SinkMetadata;
}

/// Reader trait for consuming data from any source
#[async_trait]
pub trait DataReader: Send + Sync {
    /// Read a single record from the source
    /// Returns None when no more data is available (for batch sources)
    /// May block waiting for data (for streaming sources)
    async fn read(&mut self) -> Result<Option<StreamRecord>, Box<dyn Error + Send + Sync>>;

    /// Read multiple records in a batch (more efficient for some sources)
    /// Returns empty vector when no more data is available
    async fn read_batch(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>>;

    /// Commit the current reading position (for sources that support it)
    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Seek to a specific position/offset (for sources that support it)
    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Check if more data is available (non-blocking)
    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>>;
}

/// Writer trait for publishing data to any sink
#[async_trait]
pub trait DataWriter: Send + Sync {
    /// Write a single record to the sink
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Write multiple records in a batch (more efficient for some sinks)
    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Update an existing record (for sinks that support it)
    async fn update(
        &mut self,
        key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Delete a record (for sinks that support it)
    async fn delete(&mut self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Flush any buffered writes
    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Commit the current transaction (for transactional sinks)
    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Rollback the current transaction (for transactional sinks)
    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

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
        }
    }
}

impl Error for DataSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DataSourceError::SourceSpecific(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

/// Convert DataSourceError to SqlError for compatibility
impl From<DataSourceError> for SqlError {
    fn from(err: DataSourceError) -> Self {
        SqlError::ExecutionError {
            message: err.to_string(),
            query: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_types() {
        let kafka_offset = SourceOffset::Kafka {
            partition: 0,
            offset: 100,
        };
        let file_offset = SourceOffset::File {
            path: "/data/file.json".to_string(),
            byte_offset: 1024,
            line_number: 42,
        };

        assert!(matches!(kafka_offset, SourceOffset::Kafka { .. }));
        assert!(matches!(file_offset, SourceOffset::File { .. }));
    }

    #[test]
    fn test_metadata() {
        let metadata = SourceMetadata {
            source_type: "kafka".to_string(),
            version: "1.0.0".to_string(),
            supports_streaming: true,
            supports_batch: false,
            supports_schema_evolution: true,
            capabilities: vec!["transactions".to_string()],
        };

        assert_eq!(metadata.source_type, "kafka");
        assert!(metadata.supports_streaming);
    }
}
