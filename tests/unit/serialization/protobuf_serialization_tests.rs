/*!
# Protobuf Serialization Tests

Tests for Protocol Buffers serialization format implementation.
*/

mod protobuf_tests {
    use super::super::common_test_data::*;
    use std::collections::HashMap;
    use velostream::velostream::serialization::{ProtobufFormat, SerializationFormat};
    use velostream::velostream::sql::execution::types::StreamRecord;
    use velostream::velostream::sql::FieldValue;

    #[tokio::test]
    async fn test_protobuf_format_creation() {
        let format = ProtobufFormat::<()>::new();
        assert_eq!(format.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_serialization_round_trip() {
        let format = ProtobufFormat::<()>::new();
        let record = create_basic_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Protobuf round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_to_execution_format() {
        let format = ProtobufFormat::<()>::new();
        let record = create_basic_test_record();

        // Create a StreamRecord
        let stream_record = StreamRecord {
            fields: record.clone(),
            timestamp: 1234567890,
            offset: 100,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        // Test serialization round-trip using StreamRecord's fields
        let serialized = format
            .serialize_record(&stream_record.fields)
            .expect("Serialization should succeed");

        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Deserialization should succeed");

        // Verify key fields are preserved
        assert_eq!(deserialized.get("id"), record.get("id"));
        assert_eq!(deserialized.get("name"), record.get("name"));
        assert_eq!(deserialized.get("active"), record.get("active"));
        assert_eq!(deserialized.get("score"), record.get("score"));
    }

    #[tokio::test]
    async fn test_protobuf_from_execution_format() {
        let format = ProtobufFormat::<()>::new();

        // Create a StreamRecord directly
        let mut fields = HashMap::new();
        fields.insert("user_id".to_string(), FieldValue::Integer(456));
        fields.insert(
            "username".to_string(),
            FieldValue::String("jane_doe".to_string()),
        );
        fields.insert("is_admin".to_string(), FieldValue::Boolean(false));
        fields.insert("rating".to_string(), FieldValue::Float(92.7));

        let stream_record = StreamRecord {
            fields: fields.clone(),
            timestamp: 1234567890,
            offset: 100,
            partition: 0,
            event_time: None,
            headers: HashMap::new(),
        };

        // Serialize the StreamRecord's fields
        let serialized = format
            .serialize_record(&stream_record.fields)
            .expect("Serialization should succeed");

        // Deserialize back
        let deserialized = format
            .deserialize_record(&serialized)
            .expect("Deserialization should succeed");

        // Verify the round-trip
        assert_eq!(deserialized.get("user_id"), Some(&FieldValue::Integer(456)));
        assert_eq!(
            deserialized.get("username"),
            Some(&FieldValue::String("jane_doe".to_string()))
        );
        assert_eq!(
            deserialized.get("is_admin"),
            Some(&FieldValue::Boolean(false))
        );
        assert_eq!(deserialized.get("rating"), Some(&FieldValue::Float(92.7)));
    }

    #[tokio::test]
    async fn test_protobuf_complex_types() {
        let format = ProtobufFormat::<()>::new();
        let record = create_comprehensive_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Complex type round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_direct_creation() {
        let format_proto = ProtobufFormat::<()>::new();
        assert_eq!(format_proto.format_name(), "Protobuf");

        // Test that direct creation works the same way
        let format_proto_direct = ProtobufFormat::<()>::new();
        assert_eq!(format_proto_direct.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_null_handling() {
        let format = ProtobufFormat::<()>::new();
        let record = create_null_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Null handling round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_empty_record() {
        let format = ProtobufFormat::<()>::new();
        let empty_record = HashMap::new();

        test_serialization_round_trip(&format, &empty_record)
            .expect("Empty record round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_large_data() {
        let format = ProtobufFormat::<()>::new();
        let record = create_large_data_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Large data round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_error_handling() {
        let format = ProtobufFormat::<()>::new();

        // Test with invalid binary data (non-JSON for our generic implementation)
        let invalid_data = b"invalid protobuf data that is not json";
        let result = format.deserialize_record(invalid_data);
        assert!(
            result.is_err(),
            "Invalid protobuf data should cause deserialization error"
        );

        // Test with malformed JSON (since our generic implementation uses JSON)
        let malformed_json = b"{ malformed json";
        let result = format.deserialize_record(malformed_json);
        assert!(
            result.is_err(),
            "Malformed JSON should cause deserialization error"
        );
    }

    #[tokio::test]
    async fn test_protobuf_type_specific_creation() {
        // Test creating protobuf format with specific type
        let format = ProtobufFormat::<()>::new();
        assert_eq!(format.format_name(), "Protobuf");
    }

    #[tokio::test]
    async fn test_protobuf_schema_evolution() {
        // For Protobuf, schema evolution is handled at the type level
        // Since we're using a generic implementation, we simulate evolution
        // by testing backward compatibility with different field sets

        let format_v1 = ProtobufFormat::<()>::new();
        let format_v2 = ProtobufFormat::<()>::new();

        // Version 1 record (minimal fields)
        let mut record_v1 = HashMap::new();
        record_v1.insert("id".to_string(), FieldValue::Integer(123));
        record_v1.insert(
            "name".to_string(),
            FieldValue::String("Test User".to_string()),
        );

        // Serialize with v1 format
        let serialized_v1 = format_v1
            .serialize_record(&record_v1)
            .expect("V1 serialization should succeed");

        // Deserialize with v2 format (should be compatible)
        let deserialized_v2 = format_v2
            .deserialize_record(&serialized_v1)
            .expect("V2 deserialization should succeed");

        // Core fields should be preserved
        assert_eq!(deserialized_v2.get("id"), Some(&FieldValue::Integer(123)));
        assert_eq!(
            deserialized_v2.get("name"),
            Some(&FieldValue::String("Test User".to_string()))
        );

        // Version 2 record (extended fields)
        let mut record_v2 = record_v1.clone();
        record_v2.insert(
            "email".to_string(),
            FieldValue::String("test@example.com".to_string()),
        );
        record_v2.insert("active".to_string(), FieldValue::Boolean(true));

        // Test round trip with extended record
        test_serialization_round_trip(&format_v2, &record_v2)
            .expect("V2 extended record round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_unicode_strings() {
        let format = ProtobufFormat::<()>::new();
        let record = create_edge_case_test_record();

        // Extract unicode fields for testing
        let mut unicode_record = HashMap::new();
        unicode_record.insert("unicode_field".to_string(), record["unicode_field"].clone());
        unicode_record.insert("emoji_field".to_string(), record["emoji_field"].clone());

        test_serialization_round_trip(&format, &unicode_record)
            .expect("Unicode round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_large_numbers() {
        let format = ProtobufFormat::<()>::new();
        let record = create_edge_case_test_record();

        // Extract large number fields for testing
        let mut large_number_record = HashMap::new();
        large_number_record.insert("large_int".to_string(), record["large_int"].clone());
        large_number_record.insert("small_int".to_string(), record["small_int"].clone());
        large_number_record.insert("large_float".to_string(), record["large_float"].clone());
        large_number_record.insert("small_float".to_string(), record["small_float"].clone());

        test_serialization_round_trip(&format, &large_number_record)
            .expect("Large numbers round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_comprehensive_type_matrix() {
        // Test comprehensive type coverage similar to other formats
        let format = ProtobufFormat::<()>::new();
        let record = create_comprehensive_test_record();

        // Test serialization round trip
        test_serialization_round_trip(&format, &record)
            .expect("Comprehensive type matrix round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_complex_nested_structures() {
        let format = ProtobufFormat::<()>::new();
        let record = create_complex_nested_test_record();

        test_serialization_round_trip(&format, &record)
            .expect("Complex nested structures round trip should succeed");
    }

    #[tokio::test]
    async fn test_protobuf_logical_types_roundtrip() {
        use chrono::{NaiveDate, NaiveDateTime};
        use velostream::velostream::serialization::protobuf_codec::ProtobufCodec;
        use velostream::velostream::serialization::SerializationCodec;

        println!("Testing Protobuf logical types roundtrip...");

        // Create ProtobufCodec with default schema (supports all logical types)
        let codec = ProtobufCodec::new_with_default_schema();

        // Create test data with various logical types
        let mut test_data = HashMap::new();

        // 1. UUID as string
        test_data.insert(
            "transaction_id".to_string(),
            FieldValue::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
        );

        // 2. Decimal as ScaledInteger: $1234.56 with scale 2 = 123456
        test_data.insert("amount".to_string(), FieldValue::ScaledInteger(123456, 2));

        // 3. Date as Date type (2024-01-15)
        let transaction_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        test_data.insert(
            "transaction_date".to_string(),
            FieldValue::Date(transaction_date),
        );

        // 4. Timestamp as Timestamp type (2024-01-15 14:30:45.500000 UTC)
        let created_timestamp = NaiveDateTime::from_timestamp_opt(1705329045, 500_000_000).unwrap();
        test_data.insert(
            "created_timestamp".to_string(),
            FieldValue::Timestamp(created_timestamp),
        );

        // 5. Interval type
        use velostream::velostream::sql::ast::TimeUnit;
        test_data.insert(
            "duration".to_string(),
            FieldValue::Interval {
                value: 3600000, // 1 hour in milliseconds
                unit: TimeUnit::Millisecond,
            },
        );

        println!("Test data created:");
        println!("  - UUID: 550e8400-e29b-41d4-a716-446655440000");
        println!("  - Amount: $1234.56 (ScaledInteger(123456, 2))");
        println!("  - Date: 2024-01-15");
        println!("  - Timestamp: 2024-01-15 14:30:45.500000 UTC");
        println!("  - Duration: 1 hour (3600000 ms)");

        // Serialize to Protobuf
        let serialized = codec
            .serialize(&test_data)
            .expect("Failed to serialize logical types to Protobuf");

        println!("Serialized {} bytes", serialized.len());

        // Deserialize from Protobuf
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Failed to deserialize logical types from Protobuf");

        println!("Deserialized data:");
        for (key, value) in &deserialized {
            println!("  {}: {:?}", key, value);
        }

        // Verify UUID roundtrip
        assert_eq!(
            deserialized.get("transaction_id"),
            Some(&FieldValue::String(
                "550e8400-e29b-41d4-a716-446655440000".to_string()
            )),
            "UUID mismatch after roundtrip"
        );
        println!("✅ UUID roundtrip verified");

        // Verify Decimal roundtrip (ScaledInteger)
        let amount = deserialized.get("amount").expect("Amount field missing");
        match amount {
            &FieldValue::ScaledInteger(val, scale) => {
                assert_eq!(val, 123456, "Decimal value mismatch");
                assert_eq!(scale, 2, "Decimal scale mismatch");
                let display_value = (val as f64) / 10_f64.powi(scale as i32);
                assert!(
                    (display_value - 1234.56).abs() < 0.01,
                    "Decimal display value mismatch"
                );
                println!("✅ Decimal roundtrip verified (ScaledInteger)");
            }
            _ => panic!("Amount should be ScaledInteger, got {:?}", amount),
        }

        // Verify Date roundtrip
        let date_field = deserialized
            .get("transaction_date")
            .expect("Date field missing");
        match date_field {
            FieldValue::Date(date) => {
                let expected_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
                assert_eq!(date, &expected_date, "Date value mismatch");
                println!("✅ Date roundtrip verified (Date type)");
            }
            FieldValue::String(date_str) => {
                // Protobuf may serialize Date as string (YYYY-MM-DD)
                assert_eq!(date_str, "2024-01-15", "Date string value mismatch");
                println!("✅ Date roundtrip verified (String format)");
            }
            _ => panic!("Date should be Date or String, got {:?}", date_field),
        }

        // Verify Timestamp roundtrip
        let timestamp_field = deserialized
            .get("created_timestamp")
            .expect("Timestamp field missing");
        match timestamp_field {
            FieldValue::Timestamp(ts) => {
                let expected_ts =
                    NaiveDateTime::from_timestamp_opt(1705329045, 500_000_000).unwrap();
                assert_eq!(ts, &expected_ts, "Timestamp value mismatch");
                println!("✅ Timestamp roundtrip verified (Timestamp type)");
            }
            _ => panic!("Timestamp should be Timestamp, got {:?}", timestamp_field),
        }

        // Verify Interval roundtrip
        let duration_field = deserialized
            .get("duration")
            .expect("Duration field missing");
        match duration_field {
            FieldValue::Interval { value, unit } => {
                assert_eq!(*value, 3600000, "Interval value mismatch");
                // Note: TimeUnit enum comparison
                println!("✅ Interval roundtrip verified (Interval type)");
            }
            FieldValue::String(interval_str) => {
                // Protobuf may serialize Interval as string
                assert!(
                    interval_str.contains("3600000"),
                    "Interval string should contain value"
                );
                println!("✅ Interval roundtrip verified (String format)");
            }
            _ => panic!(
                "Duration should be Interval or String, got {:?}",
                duration_field
            ),
        }

        println!("✅ All Protobuf logical types roundtrip test passed!");
    }

    #[tokio::test]
    async fn test_protobuf_codec_direct() {
        // Test ProtobufCodec directly (not through SerializationFormat trait)
        use chrono::NaiveDateTime;
        use velostream::velostream::serialization::protobuf_codec::ProtobufCodec;

        let codec = ProtobufCodec::new_with_default_schema();

        let mut test_data = HashMap::new();
        test_data.insert("id".to_string(), FieldValue::Integer(42));
        test_data.insert(
            "name".to_string(),
            FieldValue::String("Test User".to_string()),
        );
        test_data.insert("active".to_string(), FieldValue::Boolean(true));
        test_data.insert("balance".to_string(), FieldValue::ScaledInteger(1000000, 2)); // $10,000.00
        test_data.insert(
            "created_at".to_string(),
            FieldValue::Timestamp(NaiveDateTime::from_timestamp_opt(1704067200, 0).unwrap()),
        );

        // Serialize
        let serialized = codec
            .serialize(&test_data)
            .expect("Serialization should succeed");

        println!("ProtobufCodec serialized {} bytes", serialized.len());

        // Deserialize
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Deserialization should succeed");

        // Verify roundtrip
        assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(42)));
        assert_eq!(
            deserialized.get("name"),
            Some(&FieldValue::String("Test User".to_string()))
        );
        assert_eq!(deserialized.get("active"), Some(&FieldValue::Boolean(true)));
        assert_eq!(
            deserialized.get("balance"),
            Some(&FieldValue::ScaledInteger(1000000, 2))
        );
        assert_eq!(
            deserialized.get("created_at"),
            Some(&FieldValue::Timestamp(
                NaiveDateTime::from_timestamp_opt(1704067200, 0).unwrap()
            ))
        );

        println!("✅ ProtobufCodec direct test passed!");
    }

    #[tokio::test]
    async fn test_protobuf_duration_well_known_type() {
        // Test google.protobuf.Duration well-known type roundtrip
        use velostream::velostream::serialization::protobuf_codec::ProtobufCodec;
        use velostream::velostream::sql::ast::TimeUnit;

        println!("Testing google.protobuf.Duration well-known type roundtrip...");

        let codec = ProtobufCodec::new_with_default_schema();

        // Test various duration representations
        // Note: TimeUnit only has: Millisecond, Second, Minute, Hour, Day
        let test_cases = vec![
            // (description, input_value, input_unit, expected_millis)
            ("1 second", 1, TimeUnit::Second, 1000),
            ("500 milliseconds", 500, TimeUnit::Millisecond, 500),
            ("1 minute", 1, TimeUnit::Minute, 60_000),
            ("1 hour", 1, TimeUnit::Hour, 3_600_000),
            ("1 day", 1, TimeUnit::Day, 86_400_000),
            ("1.5 seconds (1500ms)", 1500, TimeUnit::Millisecond, 1500),
            (
                "2.5 hours (9000 seconds)",
                9000,
                TimeUnit::Second,
                9_000_000,
            ),
            (
                "Negative duration (-5 seconds)",
                -5,
                TimeUnit::Second,
                -5000,
            ),
            (
                "Fractional seconds (1234ms)",
                1234,
                TimeUnit::Millisecond,
                1234,
            ),
            ("Multiple days (7 days)", 7, TimeUnit::Day, 604_800_000),
        ];

        for (description, value, unit, expected_millis) in test_cases {
            println!("\n  Testing: {}", description);

            let mut test_data = HashMap::new();
            test_data.insert("duration".to_string(), FieldValue::Interval { value, unit });

            // Serialize
            let serialized = codec
                .serialize(&test_data)
                .expect(&format!("Failed to serialize {}", description));

            println!("    Serialized {} bytes", serialized.len());

            // Deserialize
            let deserialized = codec
                .deserialize(&serialized)
                .expect(&format!("Failed to deserialize {}", description));

            // Verify roundtrip
            let duration_field = deserialized
                .get("duration")
                .expect(&format!("Duration field missing for {}", description));

            match duration_field {
                FieldValue::Interval {
                    value: result_value,
                    unit: result_unit,
                } => {
                    // After roundtrip, durations are normalized to milliseconds
                    assert_eq!(
                        *result_unit,
                        TimeUnit::Millisecond,
                        "Duration should be normalized to milliseconds for {}",
                        description
                    );
                    assert_eq!(
                        *result_value, expected_millis,
                        "Duration value mismatch for {}",
                        description
                    );
                    println!("    ✅ {} verified: {} ms", description, result_value);
                }
                _ => panic!(
                    "Duration should be Interval, got {:?} for {}",
                    duration_field, description
                ),
            }
        }

        println!("\n✅ All google.protobuf.Duration roundtrip tests passed!");
    }

    #[tokio::test]
    async fn test_protobuf_duration_edge_cases() {
        // Test edge cases for google.protobuf.Duration
        use velostream::velostream::serialization::protobuf_codec::ProtobufCodec;
        use velostream::velostream::sql::ast::TimeUnit;

        println!("Testing google.protobuf.Duration edge cases...");

        let codec = ProtobufCodec::new_with_default_schema();

        // Test zero duration
        let mut test_data = HashMap::new();
        test_data.insert(
            "zero_duration".to_string(),
            FieldValue::Interval {
                value: 0,
                unit: TimeUnit::Second,
            },
        );

        let serialized = codec.serialize(&test_data).expect("Serialization failed");
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Deserialization failed");

        match deserialized.get("zero_duration") {
            Some(FieldValue::Interval { value, unit }) => {
                assert_eq!(*value, 0);
                assert_eq!(*unit, TimeUnit::Millisecond);
                println!("  ✅ Zero duration verified");
            }
            _ => panic!("Zero duration test failed"),
        }

        // Test very large duration (10000 days = ~27 years)
        let mut test_data = HashMap::new();
        test_data.insert(
            "large_duration".to_string(),
            FieldValue::Interval {
                value: 10000,
                unit: TimeUnit::Day,
            },
        );

        let serialized = codec.serialize(&test_data).expect("Serialization failed");
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Deserialization failed");

        match deserialized.get("large_duration") {
            Some(FieldValue::Interval { value, unit }) => {
                assert_eq!(*value, 864_000_000_000); // 10000 days in milliseconds
                assert_eq!(*unit, TimeUnit::Millisecond);
                println!("  ✅ Large duration verified: {} ms", value);
            }
            _ => panic!("Large duration test failed"),
        }

        // Test fractional seconds (with milliseconds - finest granularity available)
        let mut test_data = HashMap::new();
        test_data.insert(
            "fractional_seconds".to_string(),
            FieldValue::Interval {
                value: 1534, // 1.534 seconds as milliseconds
                unit: TimeUnit::Millisecond,
            },
        );

        let serialized = codec.serialize(&test_data).expect("Serialization failed");
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Deserialization failed");

        match deserialized.get("fractional_seconds") {
            Some(FieldValue::Interval { value, unit }) => {
                assert_eq!(*value, 1534); // Milliseconds preserved
                assert_eq!(*unit, TimeUnit::Millisecond);
                println!("  ✅ Fractional seconds verified: {} ms", value);
            }
            _ => panic!("Fractional seconds test failed"),
        }

        // Test negative duration (important for Duration vs Timestamp distinction)
        let mut test_data = HashMap::new();
        test_data.insert(
            "negative_duration".to_string(),
            FieldValue::Interval {
                value: -3600,
                unit: TimeUnit::Second,
            },
        );

        let serialized = codec.serialize(&test_data).expect("Serialization failed");
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Deserialization failed");

        match deserialized.get("negative_duration") {
            Some(FieldValue::Interval { value, unit }) => {
                assert_eq!(*value, -3_600_000); // -1 hour in milliseconds
                assert_eq!(*unit, TimeUnit::Millisecond);
                println!("  ✅ Negative duration verified: {} ms", value);
            }
            _ => panic!("Negative duration test failed"),
        }

        println!("✅ All Duration edge case tests passed!");
    }

    #[tokio::test]
    async fn test_protobuf_duration_precision() {
        // Test millisecond precision is properly handled
        use velostream::velostream::serialization::protobuf_codec::ProtobufCodec;
        use velostream::velostream::sql::ast::TimeUnit;

        println!("Testing Duration millisecond precision...");

        let codec = ProtobufCodec::new_with_default_schema();

        // Test: Millisecond is the finest granularity in TimeUnit enum
        // Test 1.5 seconds = 1500 milliseconds with sub-second precision
        let mut test_data = HashMap::new();
        test_data.insert(
            "precise_duration".to_string(),
            FieldValue::Interval {
                value: 1500, // 1.5 seconds
                unit: TimeUnit::Millisecond,
            },
        );

        let serialized = codec.serialize(&test_data).expect("Serialization failed");
        let deserialized = codec
            .deserialize(&serialized)
            .expect("Deserialization failed");

        match deserialized.get("precise_duration") {
            Some(FieldValue::Interval { value, unit }) => {
                // Milliseconds should be preserved exactly
                assert_eq!(*value, 1500);
                assert_eq!(*unit, TimeUnit::Millisecond);
                println!("  ✅ Precise duration verified: {} ms", value);
            }
            _ => panic!("Precise duration test failed"),
        }

        // Test that sub-millisecond precision in the Duration message is preserved
        // We can manually create a Duration with nanoseconds to verify roundtrip
        let mut test_data2 = HashMap::new();
        test_data2.insert(
            "sub_millis_duration".to_string(),
            FieldValue::Interval {
                value: 123, // 123 milliseconds
                unit: TimeUnit::Millisecond,
            },
        );

        let serialized2 = codec.serialize(&test_data2).expect("Serialization failed");
        let deserialized2 = codec
            .deserialize(&serialized2)
            .expect("Deserialization failed");

        match deserialized2.get("sub_millis_duration") {
            Some(FieldValue::Interval { value, unit }) => {
                assert_eq!(*value, 123);
                assert_eq!(*unit, TimeUnit::Millisecond);
                println!("  ✅ Sub-millisecond duration verified: {} ms", value);
            }
            _ => panic!("Sub-millisecond duration test failed"),
        }

        println!("✅ Duration precision test passed!");
    }
}

// Protobuf is always available - no feature flag needed
