/*!
# Streaming-Specific Error Types

Enhanced error handling for streaming operations with circuit breakers,
retry logic, and resource management. These errors complement the existing
SqlError system and are designed for streaming-specific failure scenarios.

## Design Philosophy

This error system follows Phase 2 requirements:
- **Optional Component**: Only activated when enhanced error handling is enabled
- **Backward Compatible**: Existing SqlError continues to work unchanged
- **Progressive Enhancement**: Can be enabled via StreamingConfig
- **Production Ready**: Comprehensive error context and recovery strategies
*/

use std::error::Error;
use std::fmt;
use std::time::Duration;

use crate::velostream::sql::SqlError;

/// Enhanced streaming-specific errors with circuit breaker and retry support
///
/// This error type is designed for streaming operations that require advanced
/// error handling beyond basic SQL errors. It integrates with the ResourceManager
/// and circuit breaker system for automated recovery.
#[derive(Debug, Clone)]
pub enum StreamingError {
    /// Resource exhaustion errors
    ResourceExhausted {
        resource_type: String,
        current_usage: u64,
        limit: u64,
        message: String,
    },

    /// Circuit breaker is open due to repeated failures
    CircuitBreakerOpen {
        service: String,
        failure_count: u32,
        last_failure_time: std::time::SystemTime,
        next_retry_time: std::time::SystemTime,
    },

    /// Retry attempts exceeded
    RetryExhausted {
        operation: String,
        attempt_count: u32,
        max_attempts: u32,
        last_error_message: String, // Store error message instead of boxed error for Clone
    },

    /// Watermark processing errors
    WatermarkError {
        source_id: String,
        message: String,
        recoverable: bool,
    },

    /// Late data handling errors
    LateDataError {
        strategy: String,
        lateness: Duration,
        message: String,
    },

    /// Message passing errors in enhanced mode
    MessagePassingError {
        operation: String,
        message: String,
        retry_possible: bool,
    },

    /// Resource monitoring threshold violations
    ResourceThresholdViolation {
        resource_type: String,
        current_value: f64,
        threshold: f64,
        severity: ThresholdSeverity,
    },

    /// Wrapped SqlError for compatibility
    SqlError(SqlError),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ThresholdSeverity {
    Warning,
    Critical,
    Fatal,
}

impl StreamingError {
    /// Check if the error is recoverable through retry
    pub fn is_recoverable(&self) -> bool {
        match self {
            StreamingError::ResourceExhausted { .. } => false, // Need resource cleanup
            StreamingError::CircuitBreakerOpen { .. } => false, // Wait for circuit breaker
            StreamingError::RetryExhausted { .. } => false,    // Already exhausted retries
            StreamingError::WatermarkError { recoverable, .. } => *recoverable,
            StreamingError::LateDataError { .. } => true, // Can often retry late data
            StreamingError::MessagePassingError { retry_possible, .. } => *retry_possible,
            StreamingError::ResourceThresholdViolation { severity, .. } => {
                matches!(severity, ThresholdSeverity::Warning)
            }
            StreamingError::SqlError(sql_err) => {
                // Basic heuristic: parse errors are not recoverable, others might be
                !sql_err.to_string().contains("parse error")
            }
        }
    }

    /// Get suggested retry delay based on error type
    pub fn suggested_retry_delay(&self) -> Option<Duration> {
        match self {
            StreamingError::ResourceExhausted { .. } => Some(Duration::from_secs(5)),
            StreamingError::CircuitBreakerOpen {
                next_retry_time, ..
            } => {
                let now = std::time::SystemTime::now();
                next_retry_time.duration_since(now).ok()
            }
            StreamingError::WatermarkError {
                recoverable: true, ..
            } => Some(Duration::from_millis(100)),
            StreamingError::LateDataError { .. } => Some(Duration::from_millis(50)),
            StreamingError::MessagePassingError {
                retry_possible: true,
                ..
            } => Some(Duration::from_millis(10)),
            StreamingError::ResourceThresholdViolation { severity, .. } => match severity {
                ThresholdSeverity::Warning => Some(Duration::from_millis(100)),
                ThresholdSeverity::Critical => Some(Duration::from_secs(1)),
                ThresholdSeverity::Fatal => None,
            },
            _ => None,
        }
    }

    /// Get error severity for monitoring and alerting
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            StreamingError::ResourceExhausted { .. } => ErrorSeverity::High,
            StreamingError::CircuitBreakerOpen { .. } => ErrorSeverity::Medium,
            StreamingError::RetryExhausted { .. } => ErrorSeverity::High,
            StreamingError::WatermarkError { recoverable, .. } => {
                if *recoverable {
                    ErrorSeverity::Low
                } else {
                    ErrorSeverity::Medium
                }
            }
            StreamingError::LateDataError { .. } => ErrorSeverity::Low,
            StreamingError::MessagePassingError { .. } => ErrorSeverity::Medium,
            StreamingError::ResourceThresholdViolation { severity, .. } => match severity {
                ThresholdSeverity::Warning => ErrorSeverity::Low,
                ThresholdSeverity::Critical => ErrorSeverity::High,
                ThresholdSeverity::Fatal => ErrorSeverity::Critical,
            },
            StreamingError::SqlError(_) => ErrorSeverity::Medium,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl fmt::Display for StreamingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamingError::ResourceExhausted {
                resource_type,
                current_usage,
                limit,
                message,
            } => {
                write!(
                    f,
                    "Resource exhausted: {} ({}/{}) - {}",
                    resource_type, current_usage, limit, message
                )
            }
            StreamingError::CircuitBreakerOpen {
                service,
                failure_count,
                ..
            } => {
                write!(
                    f,
                    "Circuit breaker open for {}: {} failures",
                    service, failure_count
                )
            }
            StreamingError::RetryExhausted {
                operation,
                attempt_count,
                max_attempts,
                last_error_message,
            } => {
                write!(
                    f,
                    "Retry exhausted for {}: {}/{} attempts, last error: {}",
                    operation, attempt_count, max_attempts, last_error_message
                )
            }
            StreamingError::WatermarkError {
                source_id, message, ..
            } => {
                write!(f, "Watermark error for {}: {}", source_id, message)
            }
            StreamingError::LateDataError {
                strategy,
                lateness,
                message,
            } => {
                write!(
                    f,
                    "Late data error (strategy: {}, lateness: {:?}): {}",
                    strategy, lateness, message
                )
            }
            StreamingError::MessagePassingError {
                operation, message, ..
            } => {
                write!(f, "Message passing error in {}: {}", operation, message)
            }
            StreamingError::ResourceThresholdViolation {
                resource_type,
                current_value,
                threshold,
                severity,
            } => {
                write!(
                    f,
                    "Resource threshold violation ({:?}): {} {} > {}",
                    severity, resource_type, current_value, threshold
                )
            }
            StreamingError::SqlError(err) => write!(f, "SQL error: {}", err),
        }
    }
}

impl Error for StreamingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StreamingError::SqlError(err) => Some(err),
            _ => None,
        }
    }
}

// Conversion from SqlError for compatibility
impl From<SqlError> for StreamingError {
    fn from(err: SqlError) -> Self {
        StreamingError::SqlError(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_error_recoverability() {
        let resource_error = StreamingError::ResourceExhausted {
            resource_type: "memory".to_string(),
            current_usage: 1024,
            limit: 1000,
            message: "Out of memory".to_string(),
        };
        assert!(!resource_error.is_recoverable());

        let watermark_error = StreamingError::WatermarkError {
            source_id: "source1".to_string(),
            message: "Late watermark".to_string(),
            recoverable: true,
        };
        assert!(watermark_error.is_recoverable());
    }

    #[test]
    fn test_error_severity_classification() {
        let threshold_warning = StreamingError::ResourceThresholdViolation {
            resource_type: "cpu".to_string(),
            current_value: 80.0,
            threshold: 70.0,
            severity: ThresholdSeverity::Warning,
        };
        assert_eq!(threshold_warning.severity(), ErrorSeverity::Low);

        let threshold_critical = StreamingError::ResourceThresholdViolation {
            resource_type: "memory".to_string(),
            current_value: 95.0,
            threshold: 90.0,
            severity: ThresholdSeverity::Critical,
        };
        assert_eq!(threshold_critical.severity(), ErrorSeverity::High);
    }

    #[test]
    fn test_suggested_retry_delays() {
        let resource_error = StreamingError::ResourceExhausted {
            resource_type: "memory".to_string(),
            current_usage: 1024,
            limit: 1000,
            message: "Out of memory".to_string(),
        };
        assert_eq!(
            resource_error.suggested_retry_delay(),
            Some(Duration::from_secs(5))
        );

        let late_data_error = StreamingError::LateDataError {
            strategy: "Drop".to_string(),
            lateness: Duration::from_secs(10),
            message: "Data too late".to_string(),
        };
        assert_eq!(
            late_data_error.suggested_retry_delay(),
            Some(Duration::from_millis(50))
        );
    }

    #[test]
    fn test_error_display() {
        let error = StreamingError::ResourceExhausted {
            resource_type: "memory".to_string(),
            current_usage: 1024,
            limit: 1000,
            message: "Heap exhausted".to_string(),
        };
        let display = format!("{}", error);
        assert!(display.contains("Resource exhausted"));
        assert!(display.contains("memory"));
        assert!(display.contains("1024/1000"));
    }

    #[test]
    fn test_sql_error_conversion() {
        let sql_err = SqlError::ParseError {
            message: "Invalid query".to_string(),
            position: Some(10),
        };
        let streaming_err: StreamingError = sql_err.into();

        match streaming_err {
            StreamingError::SqlError(_) => {} // Expected
            _ => panic!("Should convert to SqlError variant"),
        }

        assert!(!streaming_err.is_recoverable()); // Parse errors not recoverable
    }
}
