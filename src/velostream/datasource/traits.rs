//! Generic data source abstraction traits
//!
//! These traits provide a generic interface for any data source or sink,
//! independent of the SQL execution engine. They can be used by SQL, streaming,
//! ETL, or any other data processing system.

use crate::velostream::schema::Schema;
use crate::velostream::sql::execution::types::StreamRecord; // TODO: Move this to generic module
use async_trait::async_trait;
use std::error::Error;

use super::config::{BatchConfig, SinkConfig, SourceConfig};
use super::types::{SinkMetadata, SourceMetadata, SourceOffset};

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

    /// Create a reader for this source with specific batch configuration
    /// Default implementation calls create_reader() for backward compatibility
    async fn create_reader_with_batch_config(
        &self,
        _batch_config: BatchConfig,
    ) -> Result<Box<dyn DataReader>, Box<dyn Error + Send + Sync>> {
        self.create_reader().await
    }

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

    /// Create a writer for this sink with specific batch configuration
    /// Default implementation calls create_writer() for backward compatibility
    async fn create_writer_with_batch_config(
        &self,
        _batch_config: BatchConfig,
    ) -> Result<Box<dyn DataWriter>, Box<dyn Error + Send + Sync>> {
        self.create_writer().await
    }

    /// Check if this sink supports transactional writes
    fn supports_transactions(&self) -> bool;

    /// Check if this sink supports updates/deletes (vs append-only)
    fn supports_upsert(&self) -> bool;

    /// Get sink metadata (type, version, capabilities)
    fn metadata(&self) -> SinkMetadata;
}

/// Reader trait for consuming data from any source
#[async_trait]
pub trait DataReader: Send + Sync + 'static {
    /// Read records from the source
    /// Returns a vector of records (size determined by batch configuration during initialization)
    /// Returns empty vector when no more data is available
    /// For single-record sources, batch_size=1; for batched sources, uses configured size
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn Error + Send + Sync>>;

    /// Commit the current reading position (for sources that support it)
    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Seek to a specific position/offset (for sources that support it)
    async fn seek(&mut self, offset: SourceOffset) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Check if more data is available (non-blocking)
    async fn has_more(&self) -> Result<bool, Box<dyn Error + Send + Sync>>;

    /// Begin a transaction (for sources that support exactly-once processing)
    /// Returns true if transactions are supported, false otherwise
    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // Default implementation: transactions not supported
        Ok(false)
    }

    /// Commit the current transaction (for sources that support exactly-once processing)
    /// Should only be called if begin_transaction() returned true
    async fn commit_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default implementation: no-op (fallback to regular commit)
        self.commit().await
    }

    /// Abort/rollback the current transaction (for sources that support exactly-once processing)
    /// Should only be called if begin_transaction() returned true
    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default implementation: not supported
        Err("Transaction abort not supported by this datasource".into())
    }

    /// Check if this datasource supports transactional processing
    fn supports_transactions(&self) -> bool {
        false
    }
}

/// Writer trait for publishing data to any sink
#[async_trait]
pub trait DataWriter: Send + Sync + 'static {
    /// Write a single record to the sink
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Write multiple records in a batch (more efficient for some sinks)
    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Write multiple records from a shared slice (zero-copy for multi-sink scenarios)
    ///
    /// This method is used when the same batch needs to be written to multiple sinks.
    /// Instead of cloning the entire Vec for each sink, we pass a slice reference.
    ///
    /// Default implementation clones the slice into a Vec and calls write_batch.
    /// Implementations can override this for better performance.
    async fn write_batch_shared(
        &mut self,
        records: &[StreamRecord],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default: clone the slice and call write_batch
        self.write_batch(records.to_vec()).await
    }

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

    /// Begin a transaction (for sinks that support exactly-once processing)
    /// Returns true if transactions are supported, false otherwise
    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        // Default implementation: transactions not supported
        Ok(false)
    }

    /// Commit the current transaction (for sources that support exactly-once processing)
    /// Should only be called if begin_transaction() returned true
    async fn commit_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default implementation: no-op (fallback to regular commit)
        self.commit().await
    }

    /// Abort/rollback the current transaction (for sources that support exactly-once processing)
    /// Should only be called if begin_transaction() returned true
    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default implementation: not supported
        Err("Transaction abort not supported by this datasource".into())
    }

    /// Commit the current transaction (for transactional sinks)
    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Rollback the current transaction (for transactional sinks)
    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>>;

    /// Check if this datasink supports transactional processing
    fn supports_transactions(&self) -> bool {
        false
    }
}
