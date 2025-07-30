/*!
# SQL Error Handling

This module provides comprehensive error handling for the streaming SQL engine.
All SQL operations return well-structured errors with detailed context information
to help with debugging and user feedback.

## Error Categories

The SQL engine defines several categories of errors:

- **Parse Errors**: Syntax errors in SQL text with position information
- **Schema Errors**: Type mismatches and column validation errors  
- **Execution Errors**: Runtime errors during query execution
- **Type Errors**: Data type conversion and validation errors
- **Stream Errors**: Issues with stream registration and access
- **Window Errors**: Problems with windowing operations
- **Resource Errors**: Memory and resource exhaustion issues

## Error Context

All errors include relevant context information:
- Position in SQL text for parse errors
- Column names for schema errors
- Query text for execution errors
- Expected vs actual types for type errors
- Stream names for stream errors
- Window types for windowing errors
- Resource names for resource errors

## Examples

```rust
use ferrisstreams::ferris::sql::error::SqlError;

// Parse error with position
let error = SqlError::parse_error("Expected FROM clause", Some(42));
println!("{}", error); // "SQL parse error at position 42: Expected FROM clause"

// Type error with context
let error = SqlError::type_error("Integer", "String", Some("hello"));
println!("{}", error); // "Type error: expected Integer, got String for value 'hello'"

// Stream error
let error = SqlError::stream_error("orders", "Stream not found");
println!("{}", error); // "Stream error for 'orders': Stream not found"
```

## Error Propagation

The module provides convenient type aliases for error handling:
- `SqlResult<T>` for operations that may fail
- `ParseResult<T>` for parsing operations with remaining input

Errors implement standard Rust error traits (`std::error::Error`, `Display`, `Debug`)
for seamless integration with error handling libraries and frameworks.
*/

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
/// ```rust
/// use ferrisstreams::ferris::sql::error::SqlError;
/// 
/// // Create different error types
/// let parse_err = SqlError::parse_error("Unexpected token", Some(15));
/// let type_err = SqlError::type_error("Number", "String", Some("abc"));
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
            SqlError::TypeError { expected, actual, value } => {
                if let Some(val) = value {
                    write!(f, "Type error: expected {}, got {} for value '{}'", expected, actual, val)
                } else {
                    write!(f, "Type error: expected {}, got {}", expected, actual)
                }
            }
            SqlError::StreamError { stream_name, message } => {
                write!(f, "Stream error for '{}': {}", stream_name, message)
            }
            SqlError::WindowError { message, window_type } => {
                if let Some(wtype) = window_type {
                    write!(f, "Window error for {} window: {}", wtype, message)
                } else {
                    write!(f, "Window error: {}", message)
                }
            }
            SqlError::ResourceError { resource, message } => {
                write!(f, "Resource error for {}: {}", resource, message)
            }
        }
    }
}

impl std::error::Error for SqlError {}

impl SqlError {
    /// Create a parse error with position
    pub fn parse_error(message: impl Into<String>, position: Option<usize>) -> Self {
        SqlError::ParseError {
            message: message.into(),
            position,
        }
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
    pub fn type_error(expected: impl Into<String>, actual: impl Into<String>, value: Option<String>) -> Self {
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
}

/// Result type for SQL operations
pub type SqlResult<T> = Result<T, SqlError>;

/// Parse result with remaining input
pub type ParseResult<'a, T> = Result<(&'a str, T), SqlError>;