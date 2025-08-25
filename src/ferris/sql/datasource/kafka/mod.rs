//! Kafka DataSource Implementation
//!
//! This module provides Kafka adapter implementations for the pluggable data source traits.
//! It wraps the existing Kafka consumer/producer implementations with the new trait interface
//! while maintaining full backward compatibility.
//!
//! ## Architecture
//!
//! - `KafkaDataSource` - Implements `DataSource` trait for Kafka topics
//! - `KafkaDataSink` - Implements `DataSink` trait for Kafka topics  
//! - `KafkaDataReader` - Implements `DataReader` trait, wraps `KafkaConsumer`
//! - `KafkaDataWriter` - Implements `DataWriter` trait, wraps `KafkaProducer`
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use ferrisstreams::ferris::sql::datasource::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Create Kafka source
//!     let source = create_source("kafka://localhost:9092/orders?group_id=processor").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut reader = source.create_reader().await?;
//!
//!     // Create Kafka sink
//!     let sink = create_sink("kafka://localhost:9092/processed_orders").map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
//!     let mut writer = sink.create_writer().await?;
//!
//!     // Process records
//!     while let Some(record) = reader.read().await? {
//!         // Transform record...
//!         writer.write(record).await?;
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod data_source;
pub mod data_sink;
pub mod reader;
pub mod writer;

// Re-export key types for backward compatibility
pub use error::KafkaDataSourceError;
pub use data_source::KafkaDataSource;
pub use data_sink::KafkaDataSink;
pub use reader::KafkaDataReader;
pub use writer::KafkaDataWriter;




