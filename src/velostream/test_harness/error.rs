//! Error types for the test harness
//!
//! Provides comprehensive error handling for all test harness operations.

use std::fmt;
use std::io;

/// Main error type for test harness operations
#[derive(Debug, Clone)]
pub enum TestHarnessError {
    /// Failed to parse SQL file
    SqlParseError {
        message: String,
        file: String,
        line: Option<usize>,
    },

    /// Failed to parse test specification
    SpecParseError { message: String, file: String },

    /// Failed to parse schema definition
    SchemaParseError { message: String, file: String },

    /// Infrastructure startup failure (testcontainers)
    InfraError {
        message: String,
        source: Option<String>,
    },

    /// Data generation failure
    GeneratorError { message: String, schema: String },

    /// Query execution failure
    ExecutionError {
        message: String,
        query_name: String,
        source: Option<String>,
    },

    /// Sink capture failure
    CaptureError {
        message: String,
        sink_name: String,
        source: Option<String>,
    },

    /// Assertion failure (test failure, not error)
    AssertionFailed {
        assertion_type: String,
        expected: String,
        actual: String,
        message: String,
    },

    /// Configuration error
    ConfigError { message: String },

    /// IO error (file operations)
    IoError { message: String, path: String },

    /// Timeout error
    TimeoutError {
        message: String,
        operation: String,
        timeout_ms: u64,
    },

    /// AI integration error
    AiError {
        message: String,
        source: Option<String>,
    },
}

impl fmt::Display for TestHarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestHarnessError::SqlParseError {
                message,
                file,
                line,
            } => {
                if let Some(l) = line {
                    write!(f, "SQL parse error in {} at line {}: {}", file, l, message)
                } else {
                    write!(f, "SQL parse error in {}: {}", file, message)
                }
            }
            TestHarnessError::SpecParseError { message, file } => {
                write!(f, "Test spec parse error in {}: {}", file, message)
            }
            TestHarnessError::SchemaParseError { message, file } => {
                write!(f, "Schema parse error in {}: {}", file, message)
            }
            TestHarnessError::InfraError { message, source } => {
                if let Some(s) = source {
                    write!(f, "Infrastructure error: {} ({})", message, s)
                } else {
                    write!(f, "Infrastructure error: {}", message)
                }
            }
            TestHarnessError::GeneratorError { message, schema } => {
                write!(
                    f,
                    "Data generation error for schema '{}': {}",
                    schema, message
                )
            }
            TestHarnessError::ExecutionError {
                message,
                query_name,
                source,
            } => {
                if let Some(s) = source {
                    write!(
                        f,
                        "Execution error in query '{}': {} ({})",
                        query_name, message, s
                    )
                } else {
                    write!(f, "Execution error in query '{}': {}", query_name, message)
                }
            }
            TestHarnessError::CaptureError {
                message,
                sink_name,
                source,
            } => {
                if let Some(s) = source {
                    write!(
                        f,
                        "Capture error for sink '{}': {} ({})",
                        sink_name, message, s
                    )
                } else {
                    write!(f, "Capture error for sink '{}': {}", sink_name, message)
                }
            }
            TestHarnessError::AssertionFailed {
                assertion_type,
                expected,
                actual,
                message,
            } => {
                write!(
                    f,
                    "Assertion '{}' failed: {} (expected: {}, actual: {})",
                    assertion_type, message, expected, actual
                )
            }
            TestHarnessError::ConfigError { message } => {
                write!(f, "Configuration error: {}", message)
            }
            TestHarnessError::IoError { message, path } => {
                write!(f, "IO error for '{}': {}", path, message)
            }
            TestHarnessError::TimeoutError {
                message,
                operation,
                timeout_ms,
            } => {
                write!(
                    f,
                    "Timeout after {}ms during '{}': {}",
                    timeout_ms, operation, message
                )
            }
            TestHarnessError::AiError { message, source } => {
                if let Some(s) = source {
                    write!(f, "AI integration error: {} ({})", message, s)
                } else {
                    write!(f, "AI integration error: {}", message)
                }
            }
        }
    }
}

impl std::error::Error for TestHarnessError {}

impl From<io::Error> for TestHarnessError {
    fn from(err: io::Error) -> Self {
        TestHarnessError::IoError {
            message: err.to_string(),
            path: String::new(),
        }
    }
}

impl From<serde_yaml::Error> for TestHarnessError {
    fn from(err: serde_yaml::Error) -> Self {
        TestHarnessError::SpecParseError {
            message: err.to_string(),
            file: String::new(),
        }
    }
}

/// Result type alias for test harness operations
pub type TestHarnessResult<T> = Result<T, TestHarnessError>;
