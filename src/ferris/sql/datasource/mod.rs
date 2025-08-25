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

pub mod config;
pub mod kafka;
pub mod registry;
pub mod traits;
pub mod types;

// Re-export key types
pub use config::{ConnectionString, SinkConfig, SourceConfig};
pub use registry::{create_sink, create_source, DataSourceRegistry};
pub use traits::{DataReader, DataSink, DataSource, DataWriter};
pub use types::{DataSourceError, SinkMetadata, SourceMetadata, SourceOffset};
