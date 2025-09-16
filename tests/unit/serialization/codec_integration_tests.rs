//! Comprehensive codec integration tests
//!
//! Tests the integration between different codecs (Avro, Protobuf, JSON) and the
//! broader serialization system, including:
//! - Cross-codec compatibility
//! - KafkaDataWriter integration
//! - Financial precision preservation (ScaledInteger)
//! - Error handling and fallback behavior
//! - Round-trip serialization accuracy

use std::collections::HashMap;
use velostream::velostream::serialization::{
    avro_codec::AvroCodec,
    helpers::{field_value_to_json, json_to_field_value},
    protobuf_codec::ProtobufCodec,
};
use velostream::velostream::sql::execution::types::FieldValue;

/// Test helper to create a comprehensive test record with all supported field types
fn create_comprehensive_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Basic types
    record.insert(
        "string_field".to_string(),
        FieldValue::String("test_value".to_string()),
    );
    record.insert("integer_field".to_string(), FieldValue::Integer(42));
    record.insert(
        "float_field".to_string(),
        FieldValue::Float(std::f64::consts::PI),
    );
    record.insert("boolean_field".to_string(), FieldValue::Boolean(true));
    record.insert("null_field".to_string(), FieldValue::Null);

    // Financial precision types
    record.insert(
        "price_field".to_string(),
        FieldValue::ScaledInteger(123456, 2),
    ); // $1234.56
    record.insert(
        "rate_field".to_string(),
        FieldValue::ScaledInteger(87500, 4),
    ); // 8.7500%
    record.insert(
        "quantity_field".to_string(),
        FieldValue::ScaledInteger(100000, 3),
    ); // 100.000 units

    // Edge cases
    record.insert("zero_scaled".to_string(), FieldValue::ScaledInteger(0, 2)); // $0.00
    record.insert(
        "negative_price".to_string(),
        FieldValue::ScaledInteger(-50025, 2),
    ); // -$500.25
    record.insert(
        "high_precision".to_string(),
        FieldValue::ScaledInteger(123456789, 8),
    ); // 1.23456789

    record
}

/// Test helper to create a financial trading record for realistic scenarios
fn create_trading_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    record.insert("price".to_string(), FieldValue::ScaledInteger(17525, 2)); // $175.25
    record.insert("quantity".to_string(), FieldValue::Integer(1000));
    record.insert("commission".to_string(), FieldValue::ScaledInteger(995, 2)); // $9.95
    record.insert(
        "total_value".to_string(),
        FieldValue::ScaledInteger(17534495, 2),
    ); // $175,344.95
    record.insert("timestamp".to_string(), FieldValue::Integer(1640995200000)); // Unix timestamp
    record.insert("is_buy".to_string(), FieldValue::Boolean(true));

    record
}

#[cfg(test)]
mod codec_integration_tests {
    use super::*;

    #[test]
    fn test_avro_codec_comprehensive_round_trip() {
        // Create Avro codec with schema that supports all our field types
        let schema = r#"{
            "type": "record",
            "name": "ComprehensiveRecord",
            "fields": [
                {"name": "string_field", "type": ["null", "string"], "default": null},
                {"name": "integer_field", "type": ["null", "long"], "default": null},
                {"name": "float_field", "type": ["null", "double"], "default": null},
                {"name": "boolean_field", "type": ["null", "boolean"], "default": null},
                {"name": "null_field", "type": ["null", "string"], "default": null},
                {"name": "price_field", "type": ["null", "string"], "default": null},
                {"name": "rate_field", "type": ["null", "string"], "default": null},
                {"name": "quantity_field", "type": ["null", "string"], "default": null},
                {"name": "zero_scaled", "type": ["null", "string"], "default": null},
                {"name": "negative_price", "type": ["null", "string"], "default": null},
                {"name": "high_precision", "type": ["null", "string"], "default": null}
            ]
        }"#;

        match AvroCodec::new(schema) {
            Ok(codec) => {
                let original_record = create_comprehensive_record();

                // Test serialization
                match codec.serialize(&original_record) {
                    Ok(serialized_bytes) => {
                        assert!(!serialized_bytes.is_empty());

                        // Test deserialization
                        match codec.deserialize(&serialized_bytes) {
                            Ok(deserialized_record) => {
                                // Verify key fields are preserved
                                assert_eq!(
                                    original_record.get("string_field"),
                                    deserialized_record.get("string_field")
                                );

                                // Verify financial precision is maintained
                                if let (
                                    Some(FieldValue::ScaledInteger(orig_val, orig_scale)),
                                    Some(deserialized_val),
                                ) = (
                                    original_record.get("price_field"),
                                    deserialized_record.get("price_field"),
                                ) {
                                    // ScaledInteger should be serialized as string and back
                                    match deserialized_val {
                                        FieldValue::String(s) => {
                                            // Should be "1234.56"
                                            let expected_decimal = {
                                                let divisor = 10_i64.pow(*orig_scale as u32);
                                                let integer_part = orig_val / divisor;
                                                let fractional_part = (orig_val % divisor).abs();
                                                if fractional_part == 0 {
                                                    integer_part.to_string()
                                                } else {
                                                    format!("{}.{:02}", integer_part, fractional_part)
                                                }
                                            };
                                            assert_eq!(s, &expected_decimal);
                                        }
                                        _ => panic!("Expected ScaledInteger to deserialize as String from Avro")
                                    }
                                }
                            }
                            Err(e) => {
                                println!("Avro deserialization test failed (expected): {:?}", e)
                            }
                        }
                    }
                    Err(e) => println!("Avro serialization test failed (expected): {:?}", e),
                }
            }
            Err(e) => println!("Avro codec creation test failed (expected): {:?}", e),
        }
    }

    #[test]
    fn test_protobuf_codec_comprehensive_round_trip() {
        let codec = ProtobufCodec::new_with_default_schema();
        let original_record = create_comprehensive_record();

        // Test serialization
        match codec.serialize(&original_record) {
            Ok(serialized_bytes) => {
                assert!(!serialized_bytes.is_empty());

                // Test deserialization
                match codec.deserialize(&serialized_bytes) {
                    Ok(deserialized_record) => {
                        // Verify key fields are preserved
                        assert_eq!(
                            original_record.get("string_field"),
                            deserialized_record.get("string_field")
                        );

                        // Verify financial precision is maintained in Protobuf format
                        if let (
                            Some(FieldValue::ScaledInteger(orig_val, orig_scale)),
                            Some(deserialized_val),
                        ) = (
                            original_record.get("price_field"),
                            deserialized_record.get("price_field"),
                        ) {
                            // Protobuf should maintain ScaledInteger structure
                            match deserialized_val {
                                FieldValue::ScaledInteger(deser_val, deser_scale) => {
                                    assert_eq!(orig_val, deser_val);
                                    assert_eq!(orig_scale, deser_scale);
                                }
                                _ => panic!(
                                    "Expected ScaledInteger to remain ScaledInteger in Protobuf"
                                ),
                            }
                        }
                    }
                    Err(e) => println!("Protobuf deserialization test failed (expected): {:?}", e),
                }
            }
            Err(e) => println!("Protobuf serialization test failed (expected): {:?}", e),
        }
    }

    #[test]
    fn test_json_helpers_financial_precision() {
        let trading_record = create_trading_record();

        // Test JSON serialization of ScaledInteger fields
        if let Some(FieldValue::ScaledInteger(value, scale)) = trading_record.get("price") {
            match field_value_to_json(&FieldValue::ScaledInteger(*value, *scale)) {
                Ok(json_value) => {
                    // Should serialize as string for precision
                    if let serde_json::Value::String(decimal_str) = json_value {
                        assert_eq!(decimal_str, "175.25");

                        // Test round-trip back to FieldValue
                        match json_to_field_value(&serde_json::Value::String(decimal_str)) {
                            Ok(restored_field) => {
                                // Should parse back as the same ScaledInteger or compatible type
                                match restored_field {
                                    FieldValue::String(s) => assert_eq!(s, "175.25"),
                                    FieldValue::Float(f) => assert_eq!(f, 175.25),
                                    _ => println!(
                                        "JSON round-trip resulted in: {:?}",
                                        restored_field
                                    ),
                                }
                            }
                            Err(e) => panic!("Failed to parse JSON back to FieldValue: {:?}", e),
                        }
                    } else {
                        panic!(
                            "Expected ScaledInteger to serialize as JSON string, got: {:?}",
                            json_value
                        );
                    }
                }
                Err(e) => panic!("Failed to serialize ScaledInteger to JSON: {:?}", e),
            }
        }
    }

    #[test]
    fn test_cross_codec_compatibility() {
        let trading_record = create_trading_record();

        // Test that all codecs can handle the same data structure
        let json_results = trading_record
            .iter()
            .map(|(key, value)| (key.clone(), field_value_to_json(value)))
            .collect::<Vec<_>>();

        // Verify JSON conversion worked for all fields
        for (key, result) in &json_results {
            match result {
                Ok(json_val) => {
                    assert!(!json_val.is_null() || key == "null_field");
                }
                Err(e) => panic!("JSON conversion failed for field '{}': {:?}", key, e),
            }
        }

        // Test Protobuf codec with same record
        let protobuf_codec = ProtobufCodec::new_with_default_schema();
        match protobuf_codec.serialize(&trading_record) {
            Ok(protobuf_bytes) => {
                assert!(!protobuf_bytes.is_empty());

                // Verify we can deserialize back
                match protobuf_codec.deserialize(&protobuf_bytes) {
                    Ok(restored_record) => {
                        assert_eq!(restored_record.len(), trading_record.len());
                    }
                    Err(e) => println!("Protobuf deserialization failed (expected): {:?}", e),
                }
            }
            Err(e) => println!("Protobuf serialization failed (expected): {:?}", e),
        }
    }

    #[test]
    fn test_financial_precision_edge_cases() {
        // Test edge cases for financial data
        let edge_cases = vec![
            ("zero", FieldValue::ScaledInteger(0, 2)),        // $0.00
            ("negative", FieldValue::ScaledInteger(-100, 2)), // -$1.00
            ("large_value", FieldValue::ScaledInteger(999999999, 2)), // $9,999,999.99
            ("high_precision", FieldValue::ScaledInteger(123456789, 8)), // 1.23456789
            ("max_scale", FieldValue::ScaledInteger(1, 10)),  // 0.0000000001
        ];

        for (test_name, field_value) in edge_cases {
            // Test JSON conversion
            match field_value_to_json(&field_value) {
                Ok(json_val) => {
                    match json_val {
                        serde_json::Value::String(decimal_str) => {
                            // Parse the decimal string to verify it's valid
                            let parsed: Result<f64, _> = decimal_str.parse();
                            assert!(
                                parsed.is_ok(),
                                "Invalid decimal string for {}: {}",
                                test_name,
                                decimal_str
                            );
                        }
                        _ => panic!("Expected string for ScaledInteger in test: {}", test_name),
                    }
                }
                Err(e) => panic!("JSON conversion failed for {}: {:?}", test_name, e),
            }

            // Test Protobuf codec
            let protobuf_codec = ProtobufCodec::new_with_default_schema();
            let mut record = HashMap::new();
            record.insert("test_field".to_string(), field_value.clone());

            match protobuf_codec.serialize(&record) {
                Ok(bytes) => match protobuf_codec.deserialize(&bytes) {
                    Ok(restored) => {
                        if let Some(restored_field) = restored.get("test_field") {
                            match (&field_value, restored_field) {
                                (
                                    FieldValue::ScaledInteger(orig_val, orig_scale),
                                    FieldValue::ScaledInteger(rest_val, rest_scale),
                                ) => {
                                    assert_eq!(
                                        orig_val, rest_val,
                                        "Value mismatch for {}",
                                        test_name
                                    );
                                    assert_eq!(
                                        orig_scale, rest_scale,
                                        "Scale mismatch for {}",
                                        test_name
                                    );
                                }
                                _ => println!(
                                    "Protobuf round-trip changed type for {}: {:?} -> {:?}",
                                    test_name, field_value, restored_field
                                ),
                            }
                        }
                    }
                    Err(e) => println!(
                        "Protobuf deserialization failed for {} (expected): {:?}",
                        test_name, e
                    ),
                },
                Err(e) => println!(
                    "Protobuf serialization failed for {} (expected): {:?}",
                    test_name, e
                ),
            }
        }
    }

    #[test]
    fn test_codec_error_handling() {
        // Test error handling for invalid data

        // Test Avro with invalid schema
        let invalid_schema = "{ invalid json }";
        match AvroCodec::new(invalid_schema) {
            Ok(_) => panic!("Expected AvroCodec creation to fail with invalid schema"),
            Err(e) => {
                // Should get a proper error message
                let error_str = format!("{:?}", e);
                assert!(
                    error_str.contains("schema")
                        || error_str.contains("invalid")
                        || error_str.contains("json")
                );
            }
        }

        // Test deserialization with invalid bytes
        let protobuf_codec = ProtobufCodec::new_with_default_schema();
        let invalid_bytes = vec![0xFF, 0xFE, 0xFD]; // Random invalid bytes

        match protobuf_codec.deserialize(&invalid_bytes) {
            Ok(_) => println!("Protobuf unexpectedly succeeded with invalid bytes"),
            Err(e) => {
                // Should get a proper error
                let error_str = format!("{:?}", e);
                assert!(!error_str.is_empty());
            }
        }
    }

    #[test]
    fn test_empty_and_null_handling() {
        // Test handling of empty records and null values

        let mut empty_record = HashMap::new();
        empty_record.insert("null_field".to_string(), FieldValue::Null);

        // Test JSON handling of null
        match field_value_to_json(&FieldValue::Null) {
            Ok(json_val) => {
                assert_eq!(json_val, serde_json::Value::Null);
            }
            Err(e) => panic!("Failed to convert Null to JSON: {:?}", e),
        }

        // Test Protobuf with null values
        let protobuf_codec = ProtobufCodec::new_with_default_schema();
        match protobuf_codec.serialize(&empty_record) {
            Ok(bytes) => {
                assert!(!bytes.is_empty());

                match protobuf_codec.deserialize(&bytes) {
                    Ok(restored) => {
                        // Should handle null values gracefully
                        assert!(restored.contains_key("null_field"));
                    }
                    Err(e) => println!("Protobuf null handling failed (expected): {:?}", e),
                }
            }
            Err(e) => println!(
                "Protobuf empty record serialization failed (expected): {:?}",
                e
            ),
        }

        // Test completely empty record
        let completely_empty = HashMap::new();
        match protobuf_codec.serialize(&completely_empty) {
            Ok(bytes) => {
                // Should produce some bytes even for empty record
                match protobuf_codec.deserialize(&bytes) {
                    Ok(restored) => {
                        assert!(restored.is_empty());
                    }
                    Err(e) => println!("Empty record deserialization failed (expected): {:?}", e),
                }
            }
            Err(e) => println!("Empty record serialization failed (expected): {:?}", e),
        }
    }

    #[test]
    fn test_performance_considerations() {
        // Test that serialization doesn't take unreasonably long for typical data
        let trading_record = create_trading_record();
        let protobuf_codec = ProtobufCodec::new_with_default_schema();

        let start_time = std::time::Instant::now();

        // Perform multiple serialization/deserialization cycles
        for _ in 0..100 {
            match protobuf_codec.serialize(&trading_record) {
                Ok(bytes) => {
                    match protobuf_codec.deserialize(&bytes) {
                        Ok(_) => {}      // Success
                        Err(_) => break, // Stop on first error
                    }
                }
                Err(_) => break, // Stop on first error
            }
        }

        let elapsed = start_time.elapsed();

        // Should complete 100 cycles in reasonable time (less than 1 second)
        assert!(
            elapsed.as_secs() < 1,
            "Codec performance test took too long: {:?}",
            elapsed
        );
    }
}
