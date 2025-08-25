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
pub mod validation;

// Re-export main types for convenience
pub use builder::{ConfigurationError, DataSourceConfigBuilder};
pub use connection_string::{ConnectionString, ParseError, UriComponents};
pub use environment::{ConfigSource, EnvironmentConfig};
pub use validation::{ConfigValidator, ValidationError, ValidationResult};

use std::collections::HashMap;
use std::fmt;

/// Complete data source configuration
#[derive(Debug, Clone, PartialEq)]
pub struct DataSourceConfig {
    /// URI scheme (kafka, s3, file, etc.)
    pub scheme: String,

    /// Host address (for network-based sources)
    pub host: Option<String>,

    /// Port number (for network-based sources)
    pub port: Option<u16>,

    /// Path component (topic, file path, S3 prefix, etc.)
    pub path: Option<String>,

    /// Query parameters
    pub parameters: HashMap<String, String>,

    /// Connection timeout in milliseconds
    pub timeout_ms: Option<u64>,

    /// Maximum retry attempts
    pub max_retries: Option<u32>,

    /// Configuration source metadata
    pub source: ConfigSource,

    /// Validation status
    pub validated: bool,
}

impl DataSourceConfig {
    /// Create new configuration with scheme
    pub fn new(scheme: &str) -> Self {
        Self {
            scheme: scheme.to_string(),
            host: None,
            port: None,
            path: None,
            parameters: HashMap::new(),
            timeout_ms: None,
            max_retries: None,
            source: ConfigSource::Code,
            validated: false,
        }
    }

    /// Create configuration from URI
    pub fn from_uri(uri: &str) -> Result<Self, ParseError> {
        let conn = ConnectionString::parse(uri)?;
        Ok(conn.into())
    }

    /// Convert to URI string
    pub fn to_uri(&self) -> String {
        let mut uri = format!("{}://", self.scheme);

        if let (Some(host), Some(port)) = (&self.host, &self.port) {
            uri.push_str(&format!("{}:{}", host, port));
        } else if let Some(host) = &self.host {
            uri.push_str(host);
        }

        if let Some(path) = &self.path {
            if !path.starts_with('/') {
                uri.push('/');
            }
            uri.push_str(path);
        }

        if !self.parameters.is_empty() {
            uri.push('?');
            let params: Vec<String> = self
                .parameters
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            uri.push_str(&params.join("&"));
        }

        uri
    }

    /// Get parameter value
    pub fn get_parameter(&self, key: &str) -> Option<&String> {
        self.parameters.get(key)
    }

    /// Get parameter value with default
    pub fn get_parameter_or(&self, key: &str, default: &str) -> String {
        self.parameters
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    /// Get boolean parameter
    pub fn get_bool_parameter(&self, key: &str) -> Option<bool> {
        self.parameters
            .get(key)
            .and_then(|v| match v.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Some(true),
                "false" | "0" | "no" | "off" => Some(false),
                _ => None,
            })
    }

    /// Get integer parameter
    pub fn get_int_parameter(&self, key: &str) -> Option<i64> {
        self.parameters.get(key).and_then(|v| v.parse().ok())
    }

    /// Set parameter
    pub fn set_parameter<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
        self.parameters.insert(key.into(), value.into());
        self.validated = false; // Require re-validation
    }

    /// Remove parameter
    pub fn remove_parameter(&mut self, key: &str) -> Option<String> {
        let result = self.parameters.remove(key);
        if result.is_some() {
            self.validated = false; // Require re-validation
        }
        result
    }

    /// Check if configuration is valid
    pub fn is_valid(&self) -> bool {
        self.validated
    }

    /// Mark as validated
    pub fn mark_validated(&mut self) {
        self.validated = true;
    }

    /// Clear validation status
    pub fn clear_validation(&mut self) {
        self.validated = false;
    }
}

impl fmt::Display for DataSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uri())
    }
}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self::new("unknown")
    }
}

/// Configuration metadata and statistics
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigMetadata {
    /// Total configurations loaded
    pub total_configs: usize,

    /// Configurations by source type
    pub configs_by_source: HashMap<ConfigSource, usize>,

    /// Validation statistics
    pub validation_stats: ValidationStats,

    /// Schema schemes in use
    pub schemes_in_use: HashMap<String, usize>,
}

/// Configuration validation statistics
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationStats {
    /// Total validations performed
    pub total_validations: usize,

    /// Successful validations
    pub successful_validations: usize,

    /// Failed validations
    pub failed_validations: usize,

    /// Average validation time in microseconds
    pub avg_validation_time_us: f64,
}

impl ValidationStats {
    pub fn new() -> Self {
        Self {
            total_validations: 0,
            successful_validations: 0,
            failed_validations: 0,
            avg_validation_time_us: 0.0,
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_validations == 0 {
            0.0
        } else {
            (self.successful_validations as f64) / (self.total_validations as f64) * 100.0
        }
    }
}

impl Default for ValidationStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

/// Configuration errors
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigError {
    /// URI parsing error
    ParseError(ParseError),

    /// Configuration validation error
    ValidationError(ValidationError),

    /// Environment configuration error
    EnvironmentError(String),

    /// Invalid parameter value
    InvalidParameter {
        parameter: String,
        value: String,
        expected: String,
    },

    /// Missing required parameter
    MissingParameter { parameter: String, context: String },

    /// Unsupported scheme
    UnsupportedScheme {
        scheme: String,
        supported: Vec<String>,
    },

    /// Configuration conflict
    ConfigConflict {
        parameter1: String,
        parameter2: String,
        reason: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::ParseError(err) => write!(f, "URI parsing error: {}", err),
            ConfigError::ValidationError(err) => {
                write!(f, "Configuration validation error: {}", err)
            }
            ConfigError::EnvironmentError(msg) => {
                write!(f, "Environment configuration error: {}", msg)
            }
            ConfigError::InvalidParameter {
                parameter,
                value,
                expected,
            } => {
                write!(
                    f,
                    "Invalid parameter '{}': got '{}', expected {}",
                    parameter, value, expected
                )
            }
            ConfigError::MissingParameter { parameter, context } => {
                write!(
                    f,
                    "Missing required parameter '{}' in context: {}",
                    parameter, context
                )
            }
            ConfigError::UnsupportedScheme { scheme, supported } => {
                write!(
                    f,
                    "Unsupported scheme '{}', supported: {:?}",
                    scheme, supported
                )
            }
            ConfigError::ConfigConflict {
                parameter1,
                parameter2,
                reason,
            } => {
                write!(
                    f,
                    "Configuration conflict between '{}' and '{}': {}",
                    parameter1, parameter2, reason
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::ParseError(err) => Some(err),
            ConfigError::ValidationError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ParseError> for ConfigError {
    fn from(err: ParseError) -> Self {
        ConfigError::ParseError(err)
    }
}

impl From<ValidationError> for ConfigError {
    fn from(err: ValidationError) -> Self {
        ConfigError::ValidationError(err)
    }
}

