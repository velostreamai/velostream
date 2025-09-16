//! File Data Source Error Types

use std::fmt;

/// File data source specific errors
#[derive(Debug)]
pub enum FileDataSourceError {
    /// File not found or inaccessible
    FileNotFound(String),

    /// Permission denied accessing file or directory
    PermissionDenied(String),

    /// Unsupported file format
    UnsupportedFormat(String),

    /// CSV parsing error
    CsvParseError(String),

    /// JSON parsing error  
    JsonParseError(String),

    /// File watching error
    WatchError(String),

    /// Schema inference error
    SchemaInferenceError(String),

    /// Invalid file path or pattern
    InvalidPath(String),

    /// IO error
    IoError(String),
}

impl fmt::Display for FileDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileDataSourceError::FileNotFound(path) => {
                write!(f, "File not found: {}", path)
            }
            FileDataSourceError::PermissionDenied(path) => {
                write!(f, "Permission denied accessing: {}", path)
            }
            FileDataSourceError::UnsupportedFormat(format) => {
                write!(f, "Unsupported file format: {}", format)
            }
            FileDataSourceError::CsvParseError(msg) => {
                write!(f, "CSV parsing error: {}", msg)
            }
            FileDataSourceError::JsonParseError(msg) => {
                write!(f, "JSON parsing error: {}", msg)
            }
            FileDataSourceError::WatchError(msg) => {
                write!(f, "File watching error: {}", msg)
            }
            FileDataSourceError::SchemaInferenceError(msg) => {
                write!(f, "Schema inference error: {}", msg)
            }
            FileDataSourceError::InvalidPath(path) => {
                write!(f, "Invalid file path or pattern: {}", path)
            }
            FileDataSourceError::IoError(msg) => {
                write!(f, "IO error: {}", msg)
            }
        }
    }
}

impl std::error::Error for FileDataSourceError {}

impl From<std::io::Error> for FileDataSourceError {
    fn from(err: std::io::Error) -> Self {
        FileDataSourceError::IoError(err.to_string())
    }
}

// CSV parsing errors can be handled by converting string errors
// Remove CSV crate dependency for now

impl From<serde_json::Error> for FileDataSourceError {
    fn from(err: serde_json::Error) -> Self {
        FileDataSourceError::JsonParseError(err.to_string())
    }
}
