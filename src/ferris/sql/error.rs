/*!
# SQL Error Handling & Recovery

This module provides comprehensive error handling and recovery mechanisms for the
streaming SQL engine, including resilience patterns, retry strategies, and
automatic recovery capabilities.

## Error Categories

The SQL engine defines several categories of errors:

- **Parse Errors**: Syntax errors in SQL text with position information
- **Schema Errors**: Type mismatches and column validation errors
- **Execution Errors**: Runtime errors during query execution
- **Type Errors**: Data type conversion and validation errors
- **Stream Errors**: Issues with stream registration and access
- **Window Errors**: Problems with windowing operations
- **Resource Errors**: Memory and resource exhaustion issues
- **Recovery Errors**: Circuit breaker, retry, and resilience failures

## Recovery Features

- **Circuit Breaker Pattern**: Prevents cascading failures in distributed systems
- **Retry Mechanisms**: Configurable retry strategies with exponential backoff
- **Dead Letter Queue**: Routes failed messages to separate processing pipelines
- **Health Monitoring**: Tracks system health and recovery metrics

## Error Context

All errors include relevant context information:
- Position in SQL text for parse errors
- Column names for schema errors
- Query text for execution errors
- Expected vs actual types for type errors
- Stream names for stream errors
- Window types for windowing errors
- Resource names for resource errors
- Recovery state for resilience errors

## Examples

```rust,no_run
use ferrisstreams::ferris::sql::error::{SqlError, recovery::*};
use std::time::Duration;

fn main() {
    // Parse error with position
    let error = SqlError::parse_error("Expected FROM clause", Some(42));
    println!("{}", error); // "SQL parse error at position 42: Expected FROM clause"

    // Type error with context
    let error = SqlError::type_error("Integer", "String", Some("hello".to_string()));
    println!("{}", error); // "Type error: expected Integer, got String for value 'hello'"

    // Stream error
    let error = SqlError::stream_error("orders", "Stream not found");
    println!("{}", error); // "Stream error for 'orders': Stream not found"
}
```

## Recovery Usage

```rust,no_run
use ferrisstreams::ferris::sql::error::recovery::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create circuit breaker for external service calls
    let mut circuit_breaker = CircuitBreaker::builder()
        .failure_threshold(5)
        .recovery_timeout(Duration::from_secs(30))
        .build();

    // Configure retry policy with exponential backoff
    let retry_policy = RetryPolicy::exponential_backoff()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(100))
        .max_delay(Duration::from_secs(5))
        .build();

    // Setup dead letter queue for failed messages
    let dlq = DeadLetterQueue::new("failed_events").await?;

    Ok(())
}
```

## Error Propagation

The module provides convenient type aliases for error handling:
- `SqlResult<T>` for operations that may fail
- `ParseResult<T>` for parsing operations with remaining input
- `RecoveryResult<T>` for recovery operations

Errors implement standard Rust error traits (`std::error::Error`, `Display`, `Debug`)
for seamless integration with error handling libraries and frameworks.
*/

pub mod recovery;

// Re-export recovery types for easy access

use std::fmt;

/// Comprehensive error types for SQL parsing and execution operations.
///
/// Each error variant includes specific context information relevant to the
/// error type, enabling detailed error reporting and debugging support.
///
/// # Error Categories
///
/// - **Parse Errors**: Syntax errors during SQL text parsing
/// - **Schema Errors**: Column and type validation errors
/// - **Execution Errors**: Runtime query execution failures
/// - **Type Errors**: Data type conversion and validation issues
/// - **Stream Errors**: Stream registration and access problems
/// - **Window Errors**: Windowing operation failures
/// - **Resource Errors**: Memory and resource constraints
///
/// # Examples
///
/// ```rust,no_run
/// use ferrisstreams::ferris::sql::error::SqlError;
///
/// // Create different error types
/// let parse_err = SqlError::parse_error("Unexpected token", Some(15));
/// let type_err = SqlError::type_error("Number", "String", Some("abc".to_string()));
/// let stream_err = SqlError::stream_error("orders", "Stream not registered");
/// ```
#[derive(Debug, Clone)]
pub enum SqlError {
    /// SQL parsing errors with optional position information.
    ///
    /// Occurs during tokenization and parsing of SQL text. Includes the
    /// character position where the error occurred for precise error reporting.
    ParseError {
        /// Human-readable error message
        message: String,
        /// Character position in SQL text where error occurred
        position: Option<usize>,
    },

    /// Schema validation errors for column and type mismatches.
    ///
    /// Occurs when SQL queries reference non-existent columns or when
    /// type validation fails during query planning.
    SchemaError {
        /// Description of the schema validation failure
        message: String,
        /// Name of the column that caused the error, if applicable
        column: Option<String>,
    },

    /// Runtime errors during SQL query execution.
    ///
    /// Occurs when a parsed query fails during execution due to runtime
    /// conditions like missing data, constraint violations, or system failures.
    ExecutionError {
        /// Description of the execution failure
        message: String,
        /// SQL query text that caused the error, if available
        query: Option<String>,
    },

    /// Data type conversion and validation errors.
    ///
    /// Occurs when attempting to convert values between incompatible types
    /// or when type constraints are violated during expression evaluation.
    TypeError {
        /// Expected data type
        expected: String,
        /// Actual data type encountered
        actual: String,
        /// The value that caused the type error, if available
        value: Option<String>,
    },

    /// Stream registration and access errors.
    ///
    /// Occurs when SQL queries reference streams that haven't been registered
    /// or when stream operations fail due to connectivity or permission issues.
    StreamError {
        /// Name of the stream that caused the error
        stream_name: String,
        /// Description of the stream-related failure
        message: String,
    },

    /// Window operation errors for time-based processing.
    ///
    /// Occurs when windowing operations fail due to invalid specifications,
    /// missing time columns, or window state management issues.
    WindowError {
        /// Description of the windowing failure
        message: String,
        /// Type of window operation that failed (TUMBLING, SLIDING, SESSION)
        window_type: Option<String>,
    },

    /// Resource exhaustion and system constraint errors.
    ///
    /// Occurs when operations fail due to memory limits, CPU constraints,
    /// or other system resource limitations.
    ResourceError {
        /// Name of the constrained resource (memory, CPU, disk, etc.)
        resource: String,
        /// Description of the resource constraint or exhaustion
        message: String,
    },

    /// Table or stream not found errors.
    ///
    /// Occurs when DML operations (INSERT, UPDATE, DELETE) reference
    /// tables or streams that haven't been registered in the context.
    TableNotFound {
        /// Name of the table that was not found
        table_name: String,
    },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::ParseError { message, position } => {
                if let Some(pos) = position {
                    write!(f, "SQL parse error at position {}: {}", pos, message)
                } else {
                    write!(f, "SQL parse error: {}", message)
                }
            }
            SqlError::SchemaError { message, column } => {
                if let Some(col) = column {
                    write!(f, "Schema error for column '{}': {}", col, message)
                } else {
                    write!(f, "Schema error: {}", message)
                }
            }
            SqlError::ExecutionError { message, query } => {
                if let Some(q) = query {
                    write!(f, "Query execution error in '{}': {}", q, message)
                } else {
                    write!(f, "Query execution error: {}", message)
                }
            }
            SqlError::TypeError {
                expected,
                actual,
                value,
            } => {
                if let Some(val) = value {
                    write!(
                        f,
                        "Type error: expected {}, got {} for value '{}'",
                        expected, actual, val
                    )
                } else {
                    write!(f, "Type error: expected {}, got {}", expected, actual)
                }
            }
            SqlError::StreamError {
                stream_name,
                message,
            } => {
                write!(f, "Stream error for '{}': {}", stream_name, message)
            }
            SqlError::WindowError {
                message,
                window_type,
            } => {
                if let Some(wtype) = window_type {
                    write!(f, "Window error for {} window: {}", wtype, message)
                } else {
                    write!(f, "Window error: {}", message)
                }
            }
            SqlError::ResourceError { resource, message } => {
                write!(f, "Resource error for {}: {}", resource, message)
            }
            SqlError::TableNotFound { table_name } => {
                write!(f, "Table '{}' not found", table_name)
            }
        }
    }
}

impl std::error::Error for SqlError {}

impl SqlError {
    /// Create a parse error with position and optional SQL text for enhanced reporting
    pub fn parse_error(message: impl Into<String>, position: Option<usize>) -> Self {
        SqlError::ParseError {
            message: message.into(),
            position,
        }
    }

    /// Create a parse error with enhanced context information
    pub fn parse_error_with_context(
        message: impl Into<String>,
        position: Option<usize>,
        sql_text: Option<&str>,
    ) -> Self {
        let mut error = SqlError::ParseError {
            message: message.into(),
            position,
        };

        if let (Some(pos), Some(sql)) = (position, sql_text) {
            // Enhance the error message with line/column and snippet
            if let SqlError::ParseError {
                ref mut message, ..
            } = error
            {
                let enhanced_msg = Self::enhance_parse_error_message(message, pos, sql);
                *message = enhanced_msg;
            }
        }

        error
    }

    /// Calculate line and column numbers from position
    fn calculate_line_column(text: &str, position: usize) -> (usize, usize) {
        let mut line = 1;
        let mut column = 1;

        for (i, ch) in text.char_indices() {
            if i >= position {
                break;
            }
            if ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }

        (line, column)
    }

    /// Extract a text snippet around the error position
    fn extract_snippet(text: &str, position: usize, context_chars: usize) -> String {
        let text_len = text.len();
        if position >= text_len {
            return text.to_string();
        }

        // Find snippet bounds
        let start = position.saturating_sub(context_chars);
        let end = std::cmp::min(position + context_chars, text_len);

        // Extract the snippet
        let snippet = &text[start..end];
        let relative_pos = position - start;

        // Create the snippet with cursor indicator
        let lines: Vec<&str> = snippet.lines().collect();
        if lines.is_empty() {
            return format!("{}\n{:width$}^", snippet, "", width = relative_pos);
        }

        // Find which line contains the error
        let mut current_pos = 0;
        for (line_idx, line) in lines.iter().enumerate() {
            let line_end = current_pos + line.len();
            if relative_pos <= line_end {
                let col_in_line = relative_pos - current_pos;
                let mut result = String::new();

                // Add context lines before
                if line_idx > 0 {
                    result.push_str(lines[line_idx - 1]);
                    result.push('\n');
                }

                // Add the error line
                result.push_str(line);
                result.push('\n');

                // Add cursor indicator
                result.push_str(&" ".repeat(col_in_line));
                result.push('^');

                // Add context lines after
                if line_idx + 1 < lines.len() {
                    result.push('\n');
                    result.push_str(lines[line_idx + 1]);
                }

                return result;
            }
            current_pos = line_end + 1; // +1 for newline
        }

        // Fallback
        format!("{}\n{:width$}^", snippet, "", width = relative_pos)
    }

    /// Enhance parse error message with line/column and snippet
    fn enhance_parse_error_message(
        original_message: &str,
        position: usize,
        sql_text: &str,
    ) -> String {
        let (line, column) = Self::calculate_line_column(sql_text, position);
        let snippet = Self::extract_snippet(sql_text, position, 50);

        format!(
            "{}\n  at line {}, column {}\n  |\n{}\n  |",
            original_message,
            line,
            column,
            snippet
                .lines()
                .map(|line| format!("  | {}", line))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }

    /// Create a schema error
    pub fn schema_error(message: impl Into<String>, column: Option<String>) -> Self {
        SqlError::SchemaError {
            message: message.into(),
            column,
        }
    }

    /// Create an execution error
    pub fn execution_error(message: impl Into<String>, query: Option<String>) -> Self {
        SqlError::ExecutionError {
            message: message.into(),
            query,
        }
    }

    /// Create a type error
    pub fn type_error(
        expected: impl Into<String>,
        actual: impl Into<String>,
        value: Option<String>,
    ) -> Self {
        SqlError::TypeError {
            expected: expected.into(),
            actual: actual.into(),
            value,
        }
    }

    /// Create a stream error
    pub fn stream_error(stream_name: impl Into<String>, message: impl Into<String>) -> Self {
        SqlError::StreamError {
            stream_name: stream_name.into(),
            message: message.into(),
        }
    }

    /// Create a window error
    pub fn window_error(message: impl Into<String>, window_type: Option<String>) -> Self {
        SqlError::WindowError {
            message: message.into(),
            window_type,
        }
    }

    /// Create a resource error
    pub fn resource_error(resource: impl Into<String>, message: impl Into<String>) -> Self {
        SqlError::ResourceError {
            resource: resource.into(),
            message: message.into(),
        }
    }

    /// Create an execution error with function suggestions for unknown functions
    pub fn unknown_function_error(function_name: impl Into<String>) -> Self {
        let func_name = function_name.into();
        let suggestions = Self::suggest_similar_functions(&func_name);

        let mut message = format!("Unknown function: {}", func_name);
        if !suggestions.is_empty() {
            message.push_str(&format!(
                "\n\nDid you mean one of these?\n{}",
                suggestions.join(", ")
            ));
        }

        SqlError::ExecutionError {
            message,
            query: None,
        }
    }

    /// Suggest similar function names using fuzzy string matching
    fn suggest_similar_functions(unknown_func: &str) -> Vec<String> {
        let available_functions = vec![
            // Aggregate functions
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "APPROX_COUNT_DISTINCT",
            "FIRST_VALUE",
            "LAST_VALUE",
            "LISTAGG",
            "STRING_AGG",
            "COUNT_DISTINCT",
            "MEDIAN",
            "STDDEV",
            "STDDEV_SAMP",
            "STDDEV_POP",
            "VARIANCE",
            "VAR_SAMP",
            "VAR_POP",
            // Header functions
            "HEADER",
            "HEADER_KEYS",
            "HAS_HEADER",
            "SET_HEADER",
            "REMOVE_HEADER",
            // Math functions
            "ABS",
            "ROUND",
            "CEIL",
            "CEILING",
            "FLOOR",
            "SQRT",
            "POWER",
            "POW",
            "MOD",
            // String functions
            "UPPER",
            "LOWER",
            "SUBSTRING",
            "REPLACE",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "LENGTH",
            "LEN",
            "SPLIT",
            "JOIN",
            "LEFT",
            "RIGHT",
            "POSITION",
            "CONCAT",
            // JSON functions
            "JSON_EXTRACT",
            "JSON_VALUE",
            // Conversion functions
            "CAST",
            "COALESCE",
            "NULLIF",
            // System functions
            "TIMESTAMP",
            "NOW",
            "CURRENT_TIMESTAMP",
            // Date/Time functions
            "DATE_FORMAT",
            "DATEDIFF",
            "EXTRACT",
            // Comparison functions
            "LEAST",
            "GREATEST",
            // Advanced type functions
            "ARRAY",
            "STRUCT",
            "MAP",
            "ARRAY_LENGTH",
            "ARRAY_CONTAINS",
            "MAP_KEYS",
            "MAP_VALUES",
        ];

        let unknown_lower = unknown_func.to_lowercase();
        let mut suggestions = Vec::new();

        // Find functions with similar names using multiple strategies
        for &func in &available_functions {
            let func_lower = func.to_lowercase();

            // Strategy 1: Check for exact prefix/suffix matches
            if func_lower.starts_with(&unknown_lower) || func_lower.ends_with(&unknown_lower) {
                suggestions.push(func.to_string());
                continue;
            }

            // Strategy 2: Check if unknown function is contained in available function
            if func_lower.contains(&unknown_lower) {
                suggestions.push(func.to_string());
                continue;
            }

            // Strategy 3: Check for simple edit distance (Levenshtein-like)
            if Self::is_similar_string(&unknown_lower, &func_lower) {
                suggestions.push(func.to_string());
            }
        }

        // Limit suggestions to avoid overwhelming output
        suggestions.truncate(5);
        suggestions
    }

    /// Simple similarity check based on character overlap and length difference
    fn is_similar_string(s1: &str, s2: &str) -> bool {
        // Only suggest if strings are reasonably similar in length
        let len_diff = (s1.len() as i32 - s2.len() as i32).abs();
        if len_diff > 3 {
            return false;
        }

        // Count matching characters (simple overlap check)
        let mut matches = 0;
        let chars1: Vec<char> = s1.chars().collect();
        let chars2: Vec<char> = s2.chars().collect();

        for &c1 in &chars1 {
            if chars2.contains(&c1) {
                matches += 1;
            }
        }

        // Require at least 60% character overlap
        let overlap_ratio = matches as f64 / s1.len().max(s2.len()) as f64;
        overlap_ratio >= 0.6
    }
}

/// Result type for SQL operations
pub type SqlResult<T> = Result<T, SqlError>;

/// Parse result with remaining input
pub type ParseResult<'a, T> = Result<(&'a str, T), SqlError>;
