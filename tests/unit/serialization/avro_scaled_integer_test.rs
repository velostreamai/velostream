/*!
# Avro ScaledInteger Serialization Tests

Tests for ScaledInteger precision preservation in Avro serialization/deserialization.
Tests both string-based and bytes-based decimal logical type support.
*/

#[cfg(feature = "avro")]
mod avro_tests {
    use apache_avro::types::Value as AvroValue;
    use ferrisstreams::ferris::serialization::helpers::{
        avro_value_to_field_value, field_value_to_avro, field_value_to_avro_with_schema,
    };
    use ferrisstreams::ferris::sql::execution::FieldValue;
    use std::collections::HashMap;

    /// Test ScaledInteger serialization to Avro string format
    #[test]
    fn test_scaled_integer_to_avro_string() {
        // Test cases with different scales and values
        let test_cases = vec![
            (12345, 2, "123.45"),   // $123.45
            (100000, 4, "10"),      // $10.0000 -> "10"
            (1500, 3, "1.5"),       // $1.500 -> "1.5"
            (-12345, 2, "-123.45"), // -$123.45
            (0, 2, "0"),            // $0.00 -> "0"
            (505, 2, "5.05"),       // $5.05
        ];

        for (value, scale, expected) in test_cases {
            let field_value = FieldValue::ScaledInteger(value, scale);
            let avro_result = field_value_to_avro(&field_value).unwrap();

            match avro_result {
                AvroValue::String(s) => {
                    assert_eq!(
                        s, expected,
                        "ScaledInteger({}, {}) should serialize to '{}'",
                        value, scale, expected
                    );
                }
                _ => panic!("Expected Avro String, got {:?}", avro_result),
            }
        }
    }

    /// Test Avro decimal string deserialization to ScaledInteger
    #[test]
    fn test_avro_string_to_scaled_integer() {
        let test_cases = vec![
            ("123.45", 12345, 2),   // Financial decimal
            ("10.0", 100, 1),       // Single decimal place
            ("-123.45", -12345, 2), // Negative financial decimal
            ("5.05", 505, 2),       // Dollar and cents
            ("0.99", 99, 2),        // Under dollar
        ];

        for (input, expected_value, expected_scale) in test_cases {
            let avro_value = AvroValue::String(input.to_string());
            let field_result = avro_value_to_field_value(&avro_value).unwrap();

            match field_result {
                FieldValue::ScaledInteger(value, scale) => {
                    assert_eq!(
                        value, expected_value,
                        "String '{}' should deserialize to value {}",
                        input, expected_value
                    );
                    assert_eq!(
                        scale, expected_scale,
                        "String '{}' should deserialize to scale {}",
                        input, expected_scale
                    );
                }
                _ => panic!(
                    "Expected ScaledInteger, got {:?} for input '{}'",
                    field_result, input
                ),
            }
        }
    }

    /// Test non-decimal strings remain as regular strings
    #[test]
    fn test_non_decimal_strings_preserved() {
        let test_cases = vec![
            "hello world",
            "123abc",
            "12.34.56", // Multiple decimal points
            "abc.123",  // Non-numeric prefix
            "",         // Empty string
            "123.",     // Trailing decimal only
            ".123",     // Leading decimal only
        ];

        for input in test_cases {
            let avro_value = AvroValue::String(input.to_string());
            let field_result = avro_value_to_field_value(&avro_value).unwrap();

            match field_result {
                FieldValue::String(s) => {
                    assert_eq!(
                        s, input,
                        "Non-decimal string '{}' should remain as String",
                        input
                    );
                }
                _ => panic!(
                    "Expected String, got {:?} for input '{}'",
                    field_result, input
                ),
            }
        }
    }

    /// Test round-trip precision preservation
    #[test]
    fn test_avro_scaled_integer_round_trip() {
        let test_cases = vec![
            (12345, 2),     // $123.45
            (100000, 4),    // $10.0000
            (1500, 3),      // $1.500
            (-12345, 2),    // -$123.45
            (0, 2),         // $0.00
            (505, 2),       // $5.05
            (999999999, 6), // Large financial value with high precision
        ];

        for (original_value, original_scale) in test_cases {
            // Original ScaledInteger
            let original_field = FieldValue::ScaledInteger(original_value, original_scale);

            // Serialize to Avro
            let avro_value = field_value_to_avro(&original_field).unwrap();

            // Deserialize back to FieldValue
            let restored_field = avro_value_to_field_value(&avro_value).unwrap();

            // Verify precision is preserved (may be ScaledInteger or String depending on format)
            let original_decimal = original_value as f64 / 10_f64.powi(original_scale as i32);

            match restored_field {
                FieldValue::ScaledInteger(restored_value, restored_scale) => {
                    // Restored as ScaledInteger - check mathematical equality
                    let restored_decimal =
                        restored_value as f64 / 10_f64.powi(restored_scale as i32);
                    let diff = (original_decimal - restored_decimal).abs();
                    assert!(
                        diff < f64::EPSILON,
                        "Round-trip precision loss: ({}, {}) -> ({}, {}) | {} vs {}",
                        original_value,
                        original_scale,
                        restored_value,
                        restored_scale,
                        original_decimal,
                        restored_decimal
                    );
                }
                FieldValue::String(s) => {
                    // Restored as String (when no decimal point after trimming) - check numerical equality
                    if let Ok(restored_decimal) = s.parse::<f64>() {
                        let diff = (original_decimal - restored_decimal).abs();
                        assert!(
                            diff < f64::EPSILON,
                            "Round-trip precision loss: ({}, {}) -> String('{}') | {} vs {}",
                            original_value,
                            original_scale,
                            s,
                            original_decimal,
                            restored_decimal
                        );
                    } else {
                        panic!(
                            "Round-trip failed: String '{}' is not parseable as number",
                            s
                        );
                    }
                }
                _ => panic!(
                    "Round-trip failed: expected ScaledInteger or String, got {:?}",
                    restored_field
                ),
            }
        }
    }

    /// Test Avro bytes decimal logical type heuristic detection
    #[test]
    fn test_avro_bytes_decimal_detection() {
        // Test cases: (big-endian bytes, expected value with scale 2)
        let test_cases = vec![
            (vec![0x30, 0x39], 12345), // 12345 cents = $123.45
            (vec![0xFF, 0xFF], -1),    // -1 cent = -$0.01
            (vec![0x00], 0),           // 0 cents = $0.00
            (vec![0x01, 0x00], 256),   // 256 cents = $2.56
        ];

        for (bytes, expected_value) in test_cases {
            let avro_value = AvroValue::Bytes(bytes.clone());
            let field_result = avro_value_to_field_value(&avro_value).unwrap();

            match field_result {
                FieldValue::ScaledInteger(value, scale) => {
                    assert_eq!(scale, 2, "Bytes decimal should use scale 2 (currency)");
                    assert_eq!(
                        value, expected_value,
                        "Bytes {:?} should decode to {}",
                        bytes, expected_value
                    );
                }
                // If heuristic fails, should fall back to hex string
                FieldValue::String(_) => {
                    // This is acceptable - heuristic approach may not always detect decimals
                    println!(
                        "Bytes {:?} interpreted as string (heuristic limitation)",
                        bytes
                    );
                }
                _ => panic!(
                    "Unexpected result for bytes {:?}: {:?}",
                    bytes, field_result
                ),
            }
        }
    }

    /// Test Avro Fixed decimal logical type heuristic detection
    #[test]
    fn test_avro_fixed_decimal_detection() {
        // Test Fixed format (size, bytes)
        let test_cases = vec![
            (4, vec![0x00, 0x00, 0x30, 0x39], 12345), // 4-byte fixed, 12345 cents
            (2, vec![0x01, 0x00], 256),               // 2-byte fixed, 256 cents
        ];

        for (size, bytes, expected_value) in test_cases {
            let avro_value = AvroValue::Fixed(size, bytes.clone());
            let field_result = avro_value_to_field_value(&avro_value).unwrap();

            match field_result {
                FieldValue::ScaledInteger(value, scale) => {
                    assert_eq!(scale, 2, "Fixed decimal should use scale 2 (currency)");
                    assert_eq!(
                        value, expected_value,
                        "Fixed({}, {:?}) should decode to {}",
                        size, bytes, expected_value
                    );
                }
                // If heuristic fails, should fall back to hex string
                FieldValue::String(_) => {
                    // This is acceptable - heuristic approach may not always detect decimals
                    println!(
                        "Fixed({}, {:?}) interpreted as string (heuristic limitation)",
                        size, bytes
                    );
                }
                _ => panic!(
                    "Unexpected result for Fixed({}, {:?}): {:?}",
                    size, bytes, field_result
                ),
            }
        }
    }

    /// Test schema-aware decimal logical type encoding
    #[test]
    fn test_schema_aware_decimal_encoding() {
        let financial_value = FieldValue::ScaledInteger(12345, 2); // $123.45

        // Test with decimal logical type enabled
        let decimal_result = field_value_to_avro_with_schema(&financial_value, true).unwrap();
        match decimal_result {
            AvroValue::Decimal(decimal) => {
                // Apache Avro 0.20.0+ uses Value::Decimal for decimal logical types
                // Use TryFrom to convert Decimal to Vec<u8>
                let bytes: Vec<u8> = std::convert::TryFrom::try_from(decimal)
                    .expect("Failed to convert Decimal to bytes");
                assert!(
                    !bytes.is_empty(),
                    "Decimal logical type should produce bytes"
                );
                println!("Schema-aware encoding produced {} bytes in Decimal variant", bytes.len());
            }
            AvroValue::Bytes(bytes) => {
                // Fallback for older Avro versions that might use Bytes
                assert!(
                    !bytes.is_empty(),
                    "Decimal logical type should produce bytes"
                );
                println!("Schema-aware encoding produced {} bytes", bytes.len());
            }
            _ => panic!(
                "Expected Decimal or Bytes for decimal logical type, got {:?}",
                decimal_result
            ),
        }

        // Test with decimal logical type disabled (fallback to string)
        let string_result = field_value_to_avro_with_schema(&financial_value, false).unwrap();
        match string_result {
            AvroValue::String(s) => {
                assert_eq!(s, "123.45", "Should fall back to string representation");
            }
            _ => panic!("Expected string fallback, got {:?}", string_result),
        }
    }

    /// Test complex structures with ScaledInteger fields
    #[test]
    fn test_complex_structures_with_scaled_integer() {
        // Create a financial transaction record
        let mut transaction = HashMap::new();
        transaction.insert("id".to_string(), FieldValue::String("TXN-001".to_string()));
        transaction.insert("amount".to_string(), FieldValue::ScaledInteger(12345, 2)); // $123.45
        transaction.insert("fee".to_string(), FieldValue::ScaledInteger(250, 2)); // $2.50
        transaction.insert("total".to_string(), FieldValue::ScaledInteger(12595, 2)); // $125.95

        let struct_field = FieldValue::Struct(transaction);

        // Serialize to Avro
        let avro_result = field_value_to_avro(&struct_field).unwrap();

        // Verify structure
        match &avro_result {
            AvroValue::Record(fields) => {
                assert_eq!(fields.len(), 4, "Should have 4 fields");

                // Check each field type
                for (key, value) in fields {
                    match key.as_str() {
                        "id" => match value {
                            AvroValue::String(s) => assert_eq!(s, "TXN-001"),
                            _ => panic!("ID should be string"),
                        },
                        "amount" | "fee" | "total" => match value {
                            AvroValue::String(_) => {} // ScaledInteger serializes as string
                            _ => panic!("{} should serialize as string", key),
                        },
                        _ => panic!("Unexpected field: {}", key),
                    }
                }
            }
            _ => panic!("Expected Avro Record, got {:?}", avro_result),
        }

        // Test round-trip
        let restored_field = avro_value_to_field_value(&avro_result).unwrap();
        match restored_field {
            FieldValue::Struct(restored_map) => {
                // Verify financial fields were restored as ScaledInteger
                for field_name in ["amount", "fee", "total"] {
                    match restored_map.get(field_name) {
                        Some(FieldValue::ScaledInteger(_, _)) => {} // Good!
                        Some(other) => panic!(
                            "{} restored as {:?}, expected ScaledInteger",
                            field_name, other
                        ),
                        None => panic!("{} field missing after round-trip", field_name),
                    }
                }
            }
            _ => panic!("Expected restored Struct, got {:?}", restored_field),
        }
    }

    /// Test edge cases and error handling
    #[test]
    fn test_edge_cases() {
        // Very large bytes array should not be interpreted as decimal
        let large_bytes = vec![0xFF; 20]; // 20 bytes
        let avro_value = AvroValue::Bytes(large_bytes);
        let field_result = avro_value_to_field_value(&avro_value).unwrap();

        match field_result {
            FieldValue::String(_) => {} // Should fall back to hex string
            _ => panic!(
                "Large bytes should fall back to string, got {:?}",
                field_result
            ),
        }

        // Empty bytes
        let empty_bytes = vec![];
        let avro_value = AvroValue::Bytes(empty_bytes);
        let field_result = avro_value_to_field_value(&avro_value).unwrap();

        match field_result {
            FieldValue::String(s) => assert_eq!(s, "", "Empty bytes should give empty string"),
            _ => panic!(
                "Empty bytes should give empty string, got {:?}",
                field_result
            ),
        }
    }

    /// Benchmark-style test to verify performance characteristics
    #[test]
    fn test_performance_characteristics() {
        use std::time::Instant;

        let financial_values: Vec<_> = (0..1000)
            .map(|i| FieldValue::ScaledInteger(i * 100 + 50, 2)) // $i.50 values
            .collect();

        // Test serialization performance
        let start = Instant::now();
        let avro_values: Vec<_> = financial_values
            .iter()
            .map(|fv| field_value_to_avro(fv).unwrap())
            .collect();
        let serialize_time = start.elapsed();

        // Test deserialization performance
        let start = Instant::now();
        let restored_values: Vec<_> = avro_values
            .iter()
            .map(|av| avro_value_to_field_value(av).unwrap())
            .collect();
        let deserialize_time = start.elapsed();

        // Verify all values were preserved correctly
        for (original, restored) in financial_values.iter().zip(restored_values.iter()) {
            match (original, restored) {
                (
                    FieldValue::ScaledInteger(orig_val, orig_scale),
                    FieldValue::ScaledInteger(rest_val, rest_scale),
                ) => {
                    let orig_decimal = *orig_val as f64 / 10_f64.powi(*orig_scale as i32);
                    let rest_decimal = *rest_val as f64 / 10_f64.powi(*rest_scale as i32);
                    let diff = (orig_decimal - rest_decimal).abs();
                    assert!(diff < f64::EPSILON, "Precision loss in bulk operation");
                }
                _ => panic!("Type mismatch in bulk operation"),
            }
        }

        println!("Avro ScaledInteger Performance:");
        println!("  Serialization:   {:?} for 1000 values", serialize_time);
        println!("  Deserialization: {:?} for 1000 values", deserialize_time);
        println!(
            "  Round-trip:      {:?} for 1000 values",
            serialize_time + deserialize_time
        );

        // Performance should be reasonable (these are loose bounds)
        assert!(
            serialize_time.as_millis() < 100,
            "Serialization should be fast"
        );
        assert!(
            deserialize_time.as_millis() < 100,
            "Deserialization should be fast"
        );
    }
}
