//! Configuration & URI Parsing Module
//!
//! This module provides comprehensive configuration management for VeloStream,
//! including URI parsing, configuration validation, and environment-based configuration.
//!
//! ## Features
//!
//! - **URI Parsing**: Parse connection strings like `kafka://broker:port/topic?param=value`
//! - **Configuration Validation**: Validate configuration parameters for different data sources
//! - **Environment Support**: Load configuration from environment variables and files
//! - **Builder Pattern**: Fluent API for constructing configurations
//! - **Schema Integration**: Automatic schema discovery based on URI schemes
//!
//! ## Usage
//!
//! ```rust
//! use velostream::velostream::sql::config::*;
//! use velostream::velostream::sql::config::builder::DataSourceConfigBuilder;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Parse URI
//! let conn = ConnectionString::parse("kafka://localhost:9092/orders?group_id=analytics")?;
//!
//! // Build configuration
//! let config = DataSourceConfigBuilder::new()
//!     .scheme("kafka")
//!     .host("localhost")
//!     .port(9092)
//!     .path("orders")
//!     .parameter("group_id", "analytics")
//!     .build()?;
//! # Ok(())
//! # }
//! ```

pub mod builder;
pub mod connection_string;
pub mod environment;
pub mod types;
pub mod validation;
pub mod with_clause_parser;
pub mod yaml_loader;

// Re-export main types for convenience
pub use connection_string::ConnectionString;
pub use environment::ConfigSource;
pub use types::{ConfigError, DataSourceConfig};
pub use validation::ValidationError;
pub use yaml_loader::load_yaml_config;
