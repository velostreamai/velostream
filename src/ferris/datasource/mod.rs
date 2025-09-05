//! Generic Data Source Abstraction Layer
//!
//! This module provides generic data source and sink abstractions that are independent
//! of SQL or any specific query engine. The implementations here can be used by:
//!
//! - SQL query engines  
//! - Streaming processors
//! - ETL pipelines
//! - Data ingestion systems
//! - Analytics engines
//!
//! ## Architecture
//!
//! - **DataSource/DataSink**: Core traits for sources and sinks
//! - **DataReader/DataWriter**: Streaming interfaces for read/write operations
//! - **Implementations**: Specific adapters for Kafka, Files, S3, databases, etc.
//! - **Configuration**: Generic configuration traits with source-specific implementations
//!
//! ## Examples
//!
//! ```rust,no_run
//! use ferrisstreams::ferris::datasource::{DataSource, DataReader};
//!
//! async fn process_data(source: Box<dyn DataSource>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let mut reader = source.create_reader().await?;
//!     
//!     let records = reader.read().await?;
//!     for record in records {
//!         // Process record independent of source type
//!         println!("Processing: {:?}", record);
//!     }
//!     
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod file;
pub mod kafka;
pub mod registry;
pub mod stdout_writer;
pub mod traits;
pub mod types;

// Re-export core types from config.rs
pub use config::{
    BatchConfig, BatchStrategy, CdcFormat, CompressionType, ConnectionString, FileFormat,
    SinkConfig, SourceConfig, WriteMode,
};
pub use traits::{DataReader, DataSink, DataSource, DataWriter};
pub use types::{DataSourceError, SinkMetadata, SourceMetadata, SourceOffset};

// Re-export specific implementations
pub use file::{FileDataSource, FileSink};
pub use kafka::{KafkaDataSink, KafkaDataSource};
pub use stdout_writer::StdoutWriter;

// Re-export registry functions
pub use registry::{create_sink, create_source, DataSourceRegistry};
