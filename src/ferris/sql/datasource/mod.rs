//! SQL Data Source Layer
//!
//! This module provides SQL-specific extensions and compatibility layer for the generic
//! datasource implementations. It handles connection string parsing and maintains backward
//! compatibility with the SQL engine's existing APIs.
//!
//! This module delegates to the generic datasource implementations in `crate::ferris::datasource`
//! while providing SQL-specific conveniences like connection string parsing.
//!
//! ## Examples
//!
//! ```rust,no_run
//! use ferrisstreams::ferris::sql::datasource::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Create a Kafka source using connection string
//!     let source = create_source("kafka://localhost:9092/orders").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut reader = source.create_reader().await?;
//!
//!     // Create a File sink using connection string  
//!     let sink = create_sink("file:///tmp/output.jsonl?format=jsonl").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut writer = sink.create_writer().await?;
//!
//!     // Process: Kafka -> File
//!     while let Some(record) = reader.read().await? {
//!         writer.write(record).await?;
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod registry;

// Re-export old module structure for backward compatibility
// These delegate to the generic implementations
pub mod file {
    pub use crate::ferris::datasource::file::*;
}

pub mod kafka {
    pub use crate::ferris::datasource::kafka::*;
}

// Re-export key types - delegating to generic implementations
pub use config::ConnectionString;
pub use registry::{create_sink, create_source};

// Re-export generic datasource types for compatibility
pub use crate::ferris::datasource::{
    DataReader, DataSink, DataSource, DataWriter,
    DataSourceError, SinkConfig, SinkMetadata, SourceConfig, SourceMetadata, SourceOffset,
};

// Re-export specific implementations for compatibility
pub use crate::ferris::datasource::file::{FileDataSource, FileFormat, FileSourceConfig};
pub use crate::ferris::datasource::kafka::{KafkaDataSink, KafkaDataSource};
