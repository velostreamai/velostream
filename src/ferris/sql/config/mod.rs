//! Configuration & URI Parsing Module
//!
//! This module provides comprehensive configuration management for FerrisStreams,
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
//! use ferrisstreams::ferris::sql::config::*;
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

// Re-export main types for convenience
pub use builder::DataSourceConfigBuilder;
pub use connection_string::{ConnectionString, ParseError};
pub use environment::{ConfigSource, EnvironmentConfig};
pub use types::{ConfigError, DataSourceConfig, ValidationStats};
pub use validation::{ValidationError, ValidationStats as ValidationStatsValidation};
pub use with_clause_parser::{
    WithClauseParser, WithClauseConfig, WithClauseError, ConfigValue, ConfigValueType,
    ConfigKeySchema,
};
