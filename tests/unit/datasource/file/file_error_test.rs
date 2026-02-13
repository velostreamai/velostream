//! Tests for file data source error types
//!
//! Covers: Display formatting, Debug output, Error trait, From conversions,
//! variant coverage, and error source chains.

use std::io;
use velostream::velostream::datasource::file::error::FileDataSourceError;

#[test]
fn test_error_display() {
    let cases = vec![
        (
            FileDataSourceError::FileNotFound("/nonexistent/file.csv".to_string()),
            "File not found: /nonexistent/file.csv",
        ),
        (
            FileDataSourceError::PermissionDenied("/restricted/file.csv".to_string()),
            "Permission denied accessing: /restricted/file.csv",
        ),
        (
            FileDataSourceError::UnsupportedFormat("parquet".to_string()),
            "Unsupported file format: parquet",
        ),
        (
            FileDataSourceError::CsvParseError("Invalid CSV format".to_string()),
            "CSV parsing error: Invalid CSV format",
        ),
        (
            FileDataSourceError::JsonParseError("Invalid JSON syntax".to_string()),
            "JSON parsing error: Invalid JSON syntax",
        ),
        (
            FileDataSourceError::WatchError("File watcher failed".to_string()),
            "File watching error: File watcher failed",
        ),
        (
            FileDataSourceError::SchemaInferenceError("Cannot infer schema".to_string()),
            "Schema inference error: Cannot infer schema",
        ),
        (
            FileDataSourceError::InvalidPath("/invalid/*/path".to_string()),
            "Invalid file path or pattern: /invalid/*/path",
        ),
        (
            FileDataSourceError::IoError("Permission denied".to_string()),
            "IO error: Permission denied",
        ),
    ];

    for (error, expected_message) in cases {
        assert_eq!(error.to_string(), expected_message);
    }
}

#[test]
fn test_error_debug() {
    let error = FileDataSourceError::FileNotFound("/test/file.csv".to_string());
    let debug_output = format!("{:?}", error);
    assert!(debug_output.contains("FileNotFound"));
    assert!(debug_output.contains("/test/file.csv"));
}

#[test]
fn test_error_is_error_trait() {
    let error = FileDataSourceError::IoError("Test error".to_string());

    // Verify it implements std::error::Error
    let error_ref: &dyn std::error::Error = &error;
    assert_eq!(error_ref.to_string(), "IO error: Test error");
}

#[test]
fn test_from_io_error() {
    let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
    let file_error: FileDataSourceError = io_error.into();

    match file_error {
        FileDataSourceError::IoError(msg) => {
            assert!(msg.contains("File not found"));
        }
        _ => panic!("Expected IoError variant"),
    }
}

#[test]
fn test_from_json_error() {
    let json_str = "{ invalid json }";
    let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let file_error: FileDataSourceError = json_error.into();

    match file_error {
        FileDataSourceError::JsonParseError(msg) => {
            assert!(!msg.is_empty());
        }
        _ => panic!("Expected JsonParseError variant"),
    }
}

#[test]
fn test_error_variants_coverage() {
    // Every variant can be created and displayed
    let errors = vec![
        FileDataSourceError::FileNotFound("test".to_string()),
        FileDataSourceError::PermissionDenied("test".to_string()),
        FileDataSourceError::UnsupportedFormat("test".to_string()),
        FileDataSourceError::CsvParseError("test".to_string()),
        FileDataSourceError::JsonParseError("test".to_string()),
        FileDataSourceError::WatchError("test".to_string()),
        FileDataSourceError::SchemaInferenceError("test".to_string()),
        FileDataSourceError::InvalidPath("test".to_string()),
        FileDataSourceError::IoError("test".to_string()),
    ];

    for error in errors {
        assert!(!error.to_string().is_empty());
        assert!(!format!("{:?}", error).is_empty());
        let _: &dyn std::error::Error = &error;
    }
}

#[test]
fn test_error_source_chain() {
    let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "Access denied");
    let file_error: FileDataSourceError = io_error.into();

    match file_error {
        FileDataSourceError::IoError(msg) => {
            assert!(msg.contains("Access denied"));
        }
        _ => panic!("Expected IoError variant"),
    }
}

#[test]
fn test_common_io_error_conversions() {
    let test_cases = vec![
        (io::ErrorKind::NotFound, "file not found"),
        (io::ErrorKind::PermissionDenied, "permission denied"),
        (io::ErrorKind::InvalidInput, "invalid input"),
        (io::ErrorKind::UnexpectedEof, "unexpected eof"),
    ];

    for (kind, description) in test_cases {
        let io_error = io::Error::new(kind, description);
        let file_error: FileDataSourceError = io_error.into();

        match file_error {
            FileDataSourceError::IoError(msg) => {
                assert!(
                    msg.contains(description),
                    "Error message should contain '{}': {}",
                    description,
                    msg
                );
            }
            _ => panic!("Expected IoError variant"),
        }
    }
}
