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
//! use velostream::velostream::datasource::{DataSource, DataSink, SourceConfig};
//! use velostream::velostream::datasource::kafka::{KafkaDataSource, KafkaDataSink};
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     // Create Kafka source using current constructor
//!     let mut source = KafkaDataSource::new("localhost:9092".to_string(), "orders".to_string());
//!     let source_config = SourceConfig::Kafka {
//!         brokers: "localhost:9092".to_string(),
//!         topic: "orders".to_string(),
//!         group_id: Some("processor".to_string()),
//!         properties: HashMap::new(),
//!         batch_config: Default::default(),
//!         event_time_config: None,
//!     };
//!     source.initialize(source_config).await?;
//!     let mut reader = source.create_reader().await?;
//!
//!     // Create Kafka sink using current constructor
//!     let mut sink = KafkaDataSink::new("localhost:9092".to_string(), "processed_orders".to_string());
//!     // Processing would happen here...
//!
//!     Ok(())
//! }
//! ```

pub mod data_sink;
pub mod data_source;
pub mod error;
pub mod reader;
pub mod writer;

// Re-export key types for backward compatibility
pub use data_sink::KafkaDataSink;
pub use data_source::KafkaDataSource;
pub use error::{KafkaDataSinkError, KafkaDataSourceError};
