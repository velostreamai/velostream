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
//!     while let Some(record) = reader.read().await? {
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
pub mod traits;
pub mod types;

// Re-export core types
pub use config::{CdcFormat, FileFormat, SinkConfig, SourceConfig};
pub use traits::{DataReader, DataSink, DataSource, DataWriter};
pub use types::{DataSourceError, SinkMetadata, SourceMetadata, SourceOffset};

// Re-export specific implementations
pub use file::FileDataSource;
pub use kafka::{KafkaDataSink, KafkaDataSource};
