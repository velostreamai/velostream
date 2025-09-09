//! File Data Source Implementation
//!
//! This module provides file-based data sources for FerrisStreams, enabling:
//!
//! - **CSV File Reading**: Parse CSV files with schema inference
//! - **JSON File Reading**: Parse JSONL (newline-delimited JSON) files  
//! - **File Watching**: Real-time monitoring for new data
//! - **Streaming Ingestion**: Convert file data to streaming records
//! - **Path Pattern Support**: Glob patterns for multiple files
//!
//! ## Usage Examples
//!
//! ### CSV File Source
//! ```rust,no_run
//! use ferrisstreams::ferris::datasource::file::*;
//! use ferrisstreams::ferris::datasource::traits::DataSource;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let config = FileSourceConfig {
//!         path: "/data/orders.csv".to_string(),
//!         format: FileFormat::Csv,
//!         watch_for_changes: false,
//!         ..Default::default()
//!     };
//!
//!     let mut source = FileDataSource::new();
//!     source.initialize(config.into()).await?;
//!     let mut reader = source.create_reader().await?;
//!
//!     let records = reader.read().await?;
//!     for record in records {
//!         println!("Record: {:?}", record);
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ### Real-time File Watching
//! ```rust,no_run
//! use ferrisstreams::ferris::datasource::file::*;
//! use ferrisstreams::ferris::datasource::traits::DataSource;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let config = FileSourceConfig {
//!         path: "/data/live/*.csv".to_string(),
//!         format: FileFormat::Csv,
//!         watch_for_changes: true,
//!         polling_interval_ms: Some(1000),
//!         ..Default::default()
//!     };
//!
//!     let mut source = FileDataSource::new();
//!     source.initialize(config.into()).await?;
//!     let mut reader = source.create_reader().await?;
//!
//!     // This will continuously watch for new files and data
//!     let records = reader.read().await?;
//!     for record in records {
//!         println!("New record: {:?}", record);
//!     }
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod data_source;
pub mod error;
pub mod reader;
pub mod sink;
pub mod watcher;

// Re-export key types for convenient access
pub use data_source::FileDataSource;
pub use sink::FileSink;
