/*!
# Modern Error Types for FerrisStreams

Centralized error handling using Rust 2024 edition features.
*/

use crate::ferris::kafka::kafka_error::KafkaClientError;
use crate::ferris::sql::SqlError;
use std::time::Duration;
use thiserror::Error;

/// Main application error type using modern Rust error handling patterns
#[derive(Debug, Error)]
pub enum FerrisError {
    /// Kafka-related errors with enhanced context
    #[error("Kafka operation failed: {message}")]
    Kafka {
        #[source]
        source: KafkaClientError,
        message: String,
    },

    /// SQL execution errors
    #[error("SQL execution failed")]
    Sql(#[from] SqlError),

    /// I/O errors with additional context
    #[error("I/O operation failed: {operation}")]
    Io {
        #[source]
        source: std::io::Error,
        operation: String,
    },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Config { message: String },

    /// Network timeout errors with duration context
    #[error("Operation timed out after {timeout:?}")]
    Timeout { timeout: Duration },

    /// Server startup errors
    #[error("Server failed to start on {address}: {reason}")]
    ServerStartup { address: String, reason: String },

    /// JSON parsing errors
    #[error("JSON parsing failed")]
    Json(#[from] serde_json::Error),

    /// Generic application errors
    #[error("Application error: {message}")]
    Application { message: String },
}

impl FerrisError {
    /// Helper to create Kafka errors with context
    pub fn kafka(source: KafkaClientError, message: impl Into<String>) -> Self {
        Self::Kafka {
            source,
            message: message.into(),
        }
    }

    /// Helper to create I/O errors with context
    pub fn io(source: std::io::Error, operation: impl Into<String>) -> Self {
        Self::Io {
            source,
            operation: operation.into(),
        }
    }

    /// Helper to create configuration errors
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Helper to create timeout errors
    pub fn timeout(timeout: Duration) -> Self {
        Self::Timeout { timeout }
    }

    /// Helper to create server startup errors
    pub fn server_startup(address: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ServerStartup {
            address: address.into(),
            reason: reason.into(),
        }
    }

    /// Helper to create application errors
    pub fn application(message: impl Into<String>) -> Self {
        Self::Application {
            message: message.into(),
        }
    }
}

/// Type alias for Results using FerrisError
pub type FerrisResult<T> = Result<T, FerrisError>;

/// Convert from standard Box<dyn std::error::Error> to FerrisError
impl From<Box<dyn std::error::Error + Send + Sync>> for FerrisError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        FerrisError::Application {
            message: err.to_string(),
        }
    }
}

impl From<Box<dyn std::error::Error>> for FerrisError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        FerrisError::Application {
            message: err.to_string(),
        }
    }
}
