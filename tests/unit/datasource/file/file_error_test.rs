//! Tests for file data source error handling

use ferrisstreams::ferris::datasource::file::error::FileDataSourceError;
use std::io;

#[cfg(test)]
mod file_error_tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let errors = vec![
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

        for (error, expected_message) in errors {
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

        // Test that it implements std::error::Error
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
        // Test that all error variants can be created and displayed
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
            // Each error should have a non-empty display message
            assert!(!error.to_string().is_empty());

            // Each error should have a debug representation
            assert!(!format!("{:?}", error).is_empty());

            // Each error should implement the Error trait
            let _: &dyn std::error::Error = &error;
        }
    }

    #[test]
    fn test_error_source_chain() {
        // Test error chaining with IO errors
        let io_error = io::Error::new(io::ErrorKind::PermissionDenied, "Access denied");
        let file_error: FileDataSourceError = io_error.into();

        // Should maintain error information
        match file_error {
            FileDataSourceError::IoError(msg) => {
                assert!(msg.contains("Access denied"));
            }
            _ => panic!("Expected IoError variant"),
        }
    }

    #[test]
    fn test_error_equality() {
        // Test that identical errors are equal
        let error1 = FileDataSourceError::FileNotFound("test.csv".to_string());
        let error2 = FileDataSourceError::FileNotFound("test.csv".to_string());
        let error3 = FileDataSourceError::FileNotFound("other.csv".to_string());

        // Note: FileDataSourceError doesn't derive PartialEq, so we test by string representation
        assert_eq!(error1.to_string(), error2.to_string());
        assert_ne!(error1.to_string(), error3.to_string());
    }

    #[test]
    fn test_common_io_error_conversions() {
        let test_cases = vec![
            (io::ErrorKind::NotFound, "should contain 'not found'"),
            (
                io::ErrorKind::PermissionDenied,
                "should contain 'permission'",
            ),
            (io::ErrorKind::InvalidInput, "should contain 'invalid'"),
            (io::ErrorKind::UnexpectedEof, "should contain 'eof'"),
        ];

        for (kind, description) in test_cases {
            let io_error = io::Error::new(kind, description);
            let file_error: FileDataSourceError = io_error.into();

            match file_error {
                FileDataSourceError::IoError(msg) => {
                    assert!(
                        msg.contains(description),
                        "Error message should contain expected text: {}",
                        msg
                    );
                }
                _ => panic!("Expected IoError variant"),
            }
        }
    }
}
