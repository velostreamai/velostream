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
//! ```rust,no_run
//! use ferrisstreams::ferris::sql::datasource::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Create a Kafka source
//!     let source = create_source("kafka://localhost:9092/orders").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut reader = source.create_reader().await?;
//!
//!     // Create an S3 sink  
//!     let sink = create_sink("s3://bucket/path/*.parquet").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut writer = sink.create_writer().await?;
//!
//!     // Process: Kafka -> S3
//!     while let Some(record) = reader.read().await? {
//!         writer.write(record).await?;
//!     }
//!
//!     Ok(())
//! }
//! ```

use crate::ferris::sql::execution::types::StreamRecord;
use crate::ferris::sql::schema::Schema;
use async_trait::async_trait;
use std::error::Error;

pub mod config;
pub mod kafka;
pub mod registry;
pub mod types;

// Re-export key types
pub use config::{ConnectionString, SinkConfig, SourceConfig};
pub use registry::{create_sink, create_source, DataSourceRegistry};
pub use types::{DataSourceError, SinkMetadata, SourceMetadata, SourceOffset};

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

