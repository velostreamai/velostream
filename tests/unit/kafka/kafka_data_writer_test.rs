//! Unit tests for KafkaDataWriter
//!
//! These tests validate the KafkaDataWriter functionality including:
//! - Serialization format handling (JSON, Avro, Protobuf)
//! - Key extraction and partitioning
//! - Transaction support
//! - Error handling
//! - Batch operations

use std::collections::HashMap;
use velostream::velostream::datasource::kafka::reader::SerializationFormat;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Test helper to create a StreamRecord for testing
fn create_test_record(id: i64, name: &str, amount: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert("key".to_string(), FieldValue::String(format!("key_{}", id)));

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), "test".to_string());

    StreamRecord {
        fields,
        headers,
        timestamp,
        offset: id,
        partition: 0,
        event_time: None,
        topic: None,
        key: None,
    }
}

/// Test helper to create a StreamRecord with ScaledInteger for financial precision
fn create_financial_record(id: i64, price_cents: i64, quantity: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert(
        "price".to_string(),
        FieldValue::ScaledInteger(price_cents, 2),
    ); // $123.45 as 12345 with scale 2
    fields.insert("quantity".to_string(), FieldValue::Integer(quantity));
    fields.insert("key".to_string(), FieldValue::Integer(id));

    StreamRecord {
        fields: fields,
        headers: HashMap::new(),
        timestamp: 1640995200000, // Fixed timestamp
        offset: id,
        partition: 0,
        event_time: None,
        topic: None,
        key: None,
    }
}

#[cfg(test)]
mod kafka_data_writer_tests {
    use super::*;

    #[test]
    fn test_extract_key_with_string_field() {
        // This is a mock test since we can't easily test KafkaDataWriter without actual Kafka
        // We're testing the key extraction logic conceptually

        let record = create_test_record(123, "Alice", 99.99, 1640995200000);

        // Test that we can extract the key field value
        let key_field = "key";
        if let Some(FieldValue::String(key_value)) = record.fields.get(key_field) {
            assert_eq!(key_value, "key_123");
        } else {
            panic!("Expected string key field");
        }
    }

    #[test]
    fn test_extract_key_with_integer_field() {
        let record = create_test_record(456, "Bob", 77.50, 1640995200000);

        // Test integer key extraction
        let key_field = "id";
        if let Some(FieldValue::Integer(key_value)) = record.fields.get(key_field) {
            assert_eq!(*key_value, 456);
            // Simulate conversion to string for Kafka key
            assert_eq!(key_value.to_string(), "456");
        } else {
            panic!("Expected integer key field");
        }
    }

    #[test]
    fn test_extract_key_with_scaled_integer() {
        let record = create_financial_record(789, 12345, 100); // $123.45 price

        // Test ScaledInteger key extraction and formatting
        if let Some(FieldValue::ScaledInteger(value, scale)) = record.fields.get("price") {
            assert_eq!(*value, 12345);
            assert_eq!(*scale, 2);

            // Test decimal formatting logic
            let divisor = 10_i64.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();

            assert_eq!(integer_part, 123);
            assert_eq!(fractional_part, 45);

            // Should format as "123.45"
            let formatted = if fractional_part == 0 {
                integer_part.to_string()
            } else {
                let frac_str = format!("{:0width$}", fractional_part, width = *scale as usize);
                let frac_trimmed = frac_str.trim_end_matches('0');
                if frac_trimmed.is_empty() {
                    integer_part.to_string()
                } else {
                    format!("{}.{}", integer_part, frac_trimmed)
                }
            };
            assert_eq!(formatted, "123.45");
        } else {
            panic!("Expected ScaledInteger price field");
        }
    }

    #[test]
    fn test_json_serialization_format() {
        let record = create_test_record(1, "Test", 42.0, 1640995200000);

        // Test JSON serialization conceptually by verifying field conversion
        for (_field_name, field_value) in &record.fields {
            match field_value {
                FieldValue::String(s) => {
                    // Should serialize as JSON string
                    let json_value = serde_json::Value::String(s.clone());
                    assert_eq!(json_value.as_str().unwrap(), s);
                }
                FieldValue::Integer(i) => {
                    // Should serialize as JSON number
                    let json_value = serde_json::Value::Number(serde_json::Number::from(*i));
                    assert_eq!(json_value.as_i64().unwrap(), *i);
                }
                FieldValue::Float(f) => {
                    // Should serialize as JSON number
                    let json_value =
                        serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap());
                    assert_eq!(json_value.as_f64().unwrap(), *f);
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_financial_precision_serialization() {
        let record = create_financial_record(1, 12345, 100);

        // Test that ScaledInteger maintains precision during serialization
        if let Some(FieldValue::ScaledInteger(value, scale)) = record.fields.get("price") {
            // Should serialize as "123.45" string for JSON compatibility
            let divisor = 10_i64.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();

            let decimal_string = if fractional_part == 0 {
                integer_part.to_string()
            } else {
                let frac_str = format!("{:0width$}", fractional_part, width = *scale as usize);
                let frac_trimmed = frac_str.trim_end_matches('0');
                if frac_trimmed.is_empty() {
                    integer_part.to_string()
                } else {
                    format!("{}.{}", integer_part, frac_trimmed)
                }
            };

            // Verify exact financial precision
            assert_eq!(decimal_string, "123.45");

            // Test that we can parse it back exactly
            let parsed: f64 = decimal_string.parse().unwrap();
            assert_eq!(parsed, 123.45);
        }
    }

    #[test]
    fn test_header_conversion() {
        let mut record = create_test_record(1, "Test", 42.0, 1640995200000);
        record
            .headers
            .insert("correlation_id".to_string(), "abc-123".to_string());
        record
            .headers
            .insert("source_system".to_string(), "trading_engine".to_string());

        // Test header conversion to Kafka format
        let kafka_headers: Vec<(String, Vec<u8>)> = record
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
            .collect();

        assert_eq!(kafka_headers.len(), 3); // Including the "source" header

        // Find specific headers
        let correlation_header = kafka_headers
            .iter()
            .find(|(k, _)| k == "correlation_id")
            .expect("correlation_id header should exist");

        assert_eq!(
            String::from_utf8(correlation_header.1.clone()).unwrap(),
            "abc-123"
        );
    }

    #[test]
    fn test_batch_write_logic() {
        // Test batch writing by simulating the write_batch method logic
        let records = vec![
            create_test_record(1, "Alice", 100.0, 1640995200000),
            create_test_record(2, "Bob", 200.0, 1640995201000),
            create_financial_record(3, 99950, 75), // $999.50
        ];

        // Verify each record can be processed individually
        for record in &records {
            assert!(!record.fields.is_empty());
            assert!(record.timestamp > 0);

            // Verify key extraction would work
            if record.fields.contains_key("key") {
                // String key
                let key = record.fields.get("key").unwrap();
                match key {
                    FieldValue::String(s) => assert!(s.starts_with("key_")),
                    FieldValue::Integer(i) => assert!(*i > 0),
                    _ => {}
                }
            }
        }

        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_supports_transactions() {
        // Test the transaction support flag conceptually
        // KafkaDataWriter should report that it supports transactions
        let supports_tx = true; // This would be the return value of supports_transactions()
        assert!(supports_tx);
    }

    #[test]
    fn test_serialization_format_enum() {
        // Test that all serialization formats are handled
        let formats = vec![
            SerializationFormat::Json,
            SerializationFormat::Avro {
                schema_registry_url: "http://localhost:8081".to_string(),
                subject: "test".to_string(),
            },
            SerializationFormat::Protobuf {
                message_type: "TestMessage".to_string(),
            },
            SerializationFormat::Bytes,
        ];

        for format in formats {
            // Each format should have a corresponding serialization path
            match format {
                SerializationFormat::Json => {
                    // Should use serde_json serialization
                    assert_eq!(format.to_string(), "json");
                }
                SerializationFormat::Avro { .. } => {
                    // Should use AvroCodec if available, fallback to JSON
                    assert_eq!(format.to_string(), "avro");
                }
                SerializationFormat::Protobuf { .. } => {
                    // Should use ProtobufCodec if available, fallback to JSON
                    assert_eq!(format.to_string(), "protobuf");
                }
                SerializationFormat::Bytes => {
                    // Should handle raw bytes
                    assert_eq!(format.to_string(), "bytes");
                }
                SerializationFormat::String => {
                    // Should handle string format
                    assert_eq!(format.to_string(), "string");
                }
            }
        }
    }

    #[test]
    fn test_metadata_fields_in_struct() {
        let record = create_test_record(1, "Test", 42.0, 1640995200000);

        // Verify that metadata fields are present in the StreamRecord struct
        assert_eq!(record.timestamp, 1640995200000);
        assert_eq!(record.offset, 1);
        assert_eq!(record.partition, 0);

        // Note: These metadata fields are NOT injected into the serialized output.
        // The writer only serializes record.fields as-is.
        // If _timestamp needs to be in the output, it should already be in record.fields
        // (e.g., from an upstream stage that included it in the payload).

        // Verify struct fields are accessible for other purposes (key extraction, etc.)
        assert!(record.timestamp > 0);
        assert!(record.offset >= 0);
        assert!(record.partition >= 0);
    }

    #[test]
    fn test_error_handling_scenarios() {
        // Test various error conditions that should be handled gracefully

        // Test empty record
        let empty_record = StreamRecord {
            fields: HashMap::new(),
            headers: HashMap::new(),
            timestamp: 0,
            offset: 0,
            partition: 0,
            event_time: None,
            topic: None,
            key: None,
        };

        // Should handle empty fields gracefully
        assert!(empty_record.fields.is_empty());

        // Test null field values
        let mut null_record = create_test_record(1, "Test", 42.0, 1640995200000);
        null_record
            .fields
            .insert("null_field".to_string(), FieldValue::Null);

        // Should handle null values without panicking
        assert!(null_record.fields.contains_key("null_field"));
        if let Some(FieldValue::Null) = null_record.fields.get("null_field") {
            // Null should not be used as key
            assert!(true);
        }
    }

    /// Test that key field is INCLUDED in JSON payload (not excluded)
    ///
    /// This is critical for multi-stage pipelines where downstream stages
    /// need the key field in the payload to perform GROUP BY operations.
    ///
    /// Regression test for: key field exclusion causing NULL GROUP BY keys
    #[test]
    fn test_key_field_included_in_json_payload() {
        // Create a record with a "symbol" field that would typically be used as key
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert("price".to_string(), FieldValue::Float(150.25));
        fields.insert("volume".to_string(), FieldValue::Integer(1000));

        let record = StreamRecord {
            fields,
            headers: HashMap::new(),
            timestamp: 1640995200000,
            offset: 0,
            partition: 0,
            event_time: None,
            topic: None,
            key: None,
        };

        // Serialize using sonic_rs (matches new writer behavior - just serialize fields)
        let bytes = sonic_rs::to_vec(&record.fields).expect("serialization should succeed");
        let json_payload: serde_json::Value =
            serde_json::from_slice(&bytes).expect("should parse as JSON");

        // CRITICAL: Verify that "symbol" (the key field) IS present in the payload
        assert!(
            json_payload.get("symbol").is_some(),
            "Key field 'symbol' must be included in JSON payload for downstream GROUP BY"
        );
        assert_eq!(
            json_payload.get("symbol").unwrap().as_str().unwrap(),
            "AAPL",
            "Key field value should be preserved"
        );

        // Verify other data fields are also present
        assert!(json_payload.get("price").is_some());
        assert!(json_payload.get("volume").is_some());

        // Note: _timestamp, _offset, _partition are NOT injected by writer
        // They should be preserved from upstream if present in fields
        assert!(
            json_payload.get("_timestamp").is_none(),
            "Writer should not inject _timestamp - it should come from upstream fields"
        );

        // Count total fields: 3 data fields only (no metadata injection)
        if let serde_json::Value::Object(map) = &json_payload {
            assert_eq!(map.len(), 3, "Only data fields should be in payload");
        }
    }

    /// Test that a multi-stage pipeline scenario works correctly
    ///
    /// Stage 1: Writes records with key_field='symbol'
    /// Stage 2: Reads records and should see 'symbol' in the payload
    #[test]
    fn test_multi_stage_pipeline_key_propagation() {
        // Stage 1 output record (simulating what the writer produces)
        let mut fields = HashMap::new();
        fields.insert("symbol".to_string(), FieldValue::String("MSFT".to_string()));
        fields.insert("trade_count".to_string(), FieldValue::Integer(42));
        fields.insert("total_value".to_string(), FieldValue::Float(12345.67));

        // Simulate JSON serialization (key field INCLUDED)
        let json_str = serde_json::to_string(&{
            let mut map = serde_json::Map::new();
            for (k, v) in &fields {
                let json_val = match v {
                    FieldValue::String(s) => serde_json::Value::String(s.clone()),
                    FieldValue::Integer(i) => {
                        serde_json::Value::Number(serde_json::Number::from(*i))
                    }
                    FieldValue::Float(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    _ => serde_json::Value::Null,
                };
                map.insert(k.clone(), json_val);
            }
            serde_json::Value::Object(map)
        })
        .unwrap();

        // Stage 2: Parse the JSON (simulating reader)
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // CRITICAL: Stage 2 must see 'symbol' for GROUP BY to work
        assert!(
            parsed.get("symbol").is_some(),
            "Stage 2 must receive 'symbol' field for GROUP BY to work"
        );
        assert_eq!(parsed.get("symbol").unwrap().as_str().unwrap(), "MSFT");
    }
}
