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
//! ```

pub mod builder;
pub mod connection_string;
pub mod environment;
pub mod types;
pub mod validation;

// Re-export main types for convenience
pub use builder::{ConfigurationError, DataSourceConfigBuilder};
pub use connection_string::{ConnectionString, ParseError, UriComponents};
pub use environment::{ConfigSource, EnvironmentConfig};
pub use types::{ConfigError, ConfigMetadata, ConfigResult, DataSourceConfig, ValidationStats};
pub use validation::{ConfigValidator, ValidationError, ValidationResult};

