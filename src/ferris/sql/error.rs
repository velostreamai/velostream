use std::fmt;

/// Errors that can occur during SQL parsing and execution
#[derive(Debug, Clone)]
pub enum SqlError {
    /// SQL parsing errors
    ParseError {
        message: String,
        position: Option<usize>,
    },
    
    /// Schema validation errors
    SchemaError {
        message: String,
        column: Option<String>,
    },
    
    /// Stream execution errors
    ExecutionError {
        message: String,
        query: Option<String>,
    },
    
    /// Type conversion errors
    TypeError {
        expected: String,
        actual: String,
        value: Option<String>,
    },
    
    /// Stream not found or registration errors
    StreamError {
        stream_name: String,
        message: String,
    },
    
    /// Window operation errors
    WindowError {
        message: String,
        window_type: Option<String>,
    },
    
    /// Resource exhaustion errors
    ResourceError {
        resource: String,
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