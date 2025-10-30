//! Tests for enhanced SerializationError with proper error chaining
//!
//! These tests validate that the enhanced error system preserves
//! error source chains and provides proper error context.

use std::error::Error;
use velostream::velostream::serialization::SerializationError;

#[cfg(test)]
mod enhanced_error_tests {
    use super::*;

    #[test]
    fn test_json_error_with_source_chain() {
        // Create a JSON parsing error
        let invalid_json = r#"{"invalid": json, syntax}"#;
        let json_error = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();

        // Create enhanced JSON error with source chain
        let enhanced_error =
            SerializationError::json_error("Failed to parse user input as JSON", json_error);

        // Verify display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("JSON error"));
        assert!(display_msg.contains("Failed to parse user input as JSON"));

        // Verify error source chain is preserved
        assert!(enhanced_error.source().is_some());

        // Verify we can downcast to the original JSON error
        let source_error = enhanced_error.source().unwrap();
        assert!(source_error.downcast_ref::<serde_json::Error>().is_some());
    }

    #[test]
    fn test_encoding_error_with_source_chain() {
        // Create invalid UTF-8 bytes
        let invalid_utf8 = vec![0xFF, 0xFE, 0xFD];
        let utf8_error = String::from_utf8(invalid_utf8).unwrap_err();

        // Create enhanced encoding error with source chain
        let enhanced_error =
            SerializationError::encoding_error("Invalid UTF-8 sequence in input data", utf8_error);

        // Verify display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Encoding error"));
        assert!(display_msg.contains("Invalid UTF-8 sequence"));

        // Verify error source chain is preserved
        assert!(enhanced_error.source().is_some());

        // Verify we can downcast to the original UTF-8 error
        let source_error = enhanced_error.source().unwrap();
        assert!(
            source_error
                .downcast_ref::<std::string::FromUtf8Error>()
                .is_some()
        );
    }

    #[test]
    fn test_schema_validation_error_without_source() {
        let enhanced_error = SerializationError::schema_validation_error(
            "Schema field 'age' must be integer, found string",
            None::<std::io::Error>,
        );

        // Verify display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Schema validation error"));
        assert!(display_msg.contains("must be integer"));

        // Verify no source chain when None provided
        assert!(enhanced_error.source().is_none());
    }

    #[test]
    fn test_schema_validation_error_with_source() {
        let io_error =
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid schema format");

        let enhanced_error = SerializationError::schema_validation_error(
            "Failed to validate Avro schema structure",
            Some(io_error),
        );

        // Verify display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Schema validation error"));

        // Verify error source chain is preserved
        assert!(enhanced_error.source().is_some());

        // Verify we can downcast to the original IO error
        let source_error = enhanced_error.source().unwrap();
        assert!(source_error.downcast_ref::<std::io::Error>().is_some());
    }

    #[test]
    fn test_type_conversion_error_with_detailed_info() {
        let parse_error = "invalid float".parse::<f64>().unwrap_err();

        let enhanced_error = SerializationError::type_conversion_error(
            "Cannot convert string value to floating point number",
            "String",
            "f64",
            Some(parse_error),
        );

        // Verify detailed display message with type information
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Type conversion error"));
        assert!(display_msg.contains("(from String to f64)"));
        assert!(display_msg.contains("floating point number"));

        // Verify error source chain is preserved
        assert!(enhanced_error.source().is_some());

        // Verify we can downcast to the original parse error
        let source_error = enhanced_error.source().unwrap();
        assert!(
            source_error
                .downcast_ref::<std::num::ParseFloatError>()
                .is_some()
        );
    }

    #[test]
    fn test_type_conversion_error_without_source() {
        let enhanced_error = SerializationError::type_conversion_error(
            "Unsupported conversion from complex nested structure",
            "HashMap<String, Vec<CustomStruct>>",
            "PrimitiveType",
            None::<std::io::Error>,
        );

        // Verify detailed display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Type conversion error"));
        assert!(display_msg.contains("(from HashMap<String, Vec<CustomStruct>> to PrimitiveType)"));
        assert!(display_msg.contains("Unsupported conversion"));

        // Verify no source chain when None provided
        assert!(enhanced_error.source().is_none());
    }

    #[test]
    fn test_avro_error_with_source_chain() {
        // This would typically be an actual apache_avro::Error in real usage
        // For testing purposes, we'll use a generic error
        let mock_avro_error =
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Avro schema mismatch");

        let enhanced_error = SerializationError::avro_error(
            "Failed to serialize record with Avro schema",
            mock_avro_error,
        );

        // Verify display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Avro error"));
        assert!(display_msg.contains("serialize record"));

        // Verify error source chain is preserved
        assert!(enhanced_error.source().is_some());
    }

    #[test]
    fn test_protobuf_error_with_source_chain() {
        // Mock protobuf error for testing
        let mock_protobuf_error = std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Protobuf message truncated",
        );

        let enhanced_error = SerializationError::protobuf_error(
            "Protobuf deserialization failed due to malformed message",
            mock_protobuf_error,
        );

        // Verify display message
        let display_msg = enhanced_error.to_string();
        assert!(display_msg.contains("Protobuf error"));
        assert!(display_msg.contains("deserialization failed"));

        // Verify error source chain is preserved
        assert!(enhanced_error.source().is_some());
    }

    #[test]
    fn test_backward_compatibility_with_json_serialization_failed() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();

        // Test that the old JsonSerializationFailed variant still works
        let legacy_error = SerializationError::JsonSerializationFailed(Box::new(json_error));

        // Verify it still displays correctly
        let display_msg = legacy_error.to_string();
        assert!(display_msg.contains("JSON serialization error"));

        // Verify source chain still works
        assert!(legacy_error.source().is_some());
    }

    #[test]
    fn test_error_chain_traversal_deep() {
        // Create a deep error chain: ParseFloatError -> TypeConversionError -> JsonError
        let parse_error = "not_a_number".parse::<f64>().unwrap_err();

        let type_conversion_error = SerializationError::type_conversion_error(
            "Failed to convert field to number",
            "String",
            "f64",
            Some(parse_error),
        );

        let json_error = SerializationError::json_error(
            "JSON object contains invalid numeric field",
            type_conversion_error,
        );

        // Verify the top-level error
        assert!(json_error.to_string().contains("JSON error"));

        // Traverse the error chain
        let mut current_error: &dyn Error = &json_error;
        let mut chain_depth = 0;

        while let Some(source) = current_error.source() {
            chain_depth += 1;
            current_error = source;

            // Prevent infinite loops in test
            if chain_depth > 10 {
                break;
            }
        }

        // Should have at least 2 levels: JsonError -> TypeConversionError -> ParseFloatError
        assert!(
            chain_depth >= 2,
            "Error chain should have at least 2 levels, found {}",
            chain_depth
        );
    }

    #[test]
    fn test_legacy_variants_still_work() {
        // Test that old string-based variants still work for backward compatibility
        let legacy_error = SerializationError::SerializationFailed("Old style error".to_string());
        assert!(legacy_error.to_string().contains("Serialization failed"));
        assert!(legacy_error.source().is_none()); // Legacy variants have no source

        let legacy_error2 =
            SerializationError::UnsupportedType("Complex type not supported".to_string());
        assert!(legacy_error2.to_string().contains("Unsupported type"));
        assert!(legacy_error2.source().is_none());
    }
}
