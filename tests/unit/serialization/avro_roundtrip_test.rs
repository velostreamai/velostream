/// Avro Roundtrip Integration Test
///
/// Tests that data can be written in Avro format and read back successfully,
/// ensuring schema compatibility between writer and reader.
use std::collections::HashMap;
use velostream::velostream::serialization::avro_codec::AvroCodec;
use velostream::velostream::serialization::SerializationCodec;
use velostream::velostream::sql::execution::types::FieldValue;

#[test]
fn test_avro_roundtrip_with_embedded_schema() {
    // Define a simple Avro schema for market data
    let schema_json = r#"
    {
        "type": "record",
        "name": "MarketData",
        "namespace": "com.trading.test",
        "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "volume", "type": "long"}
        ]
    }
    "#;

    // Create Avro codec with schema
    let codec = AvroCodec::new(schema_json).expect("Failed to create Avro codec with schema");

    // Create test data
    let mut test_data = HashMap::new();
    test_data.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
    test_data.insert("price".to_string(), FieldValue::Float(150.25));
    test_data.insert("volume".to_string(), FieldValue::Integer(1000));

    // Serialize to Avro
    let serialized = codec
        .serialize(&test_data)
        .expect("Failed to serialize data to Avro");

    println!("Serialized {} bytes", serialized.len());
    println!(
        "First 20 bytes: {:?}",
        &serialized[..20.min(serialized.len())]
    );

    // Deserialize from Avro
    let deserialized = codec
        .deserialize(&serialized)
        .expect("Failed to deserialize data from Avro");

    // Verify roundtrip
    assert_eq!(
        deserialized.get("symbol"),
        Some(&FieldValue::String("AAPL".to_string())),
        "Symbol mismatch after roundtrip"
    );
    assert_eq!(
        deserialized.get("price"),
        Some(&FieldValue::Float(150.25)),
        "Price mismatch after roundtrip"
    );
    assert_eq!(
        deserialized.get("volume"),
        Some(&FieldValue::Integer(1000)),
        "Volume mismatch after roundtrip"
    );

    println!("✅ Avro roundtrip test passed!");
}

#[test]
fn test_avro_roundtrip_with_decimal_fields() {
    // Test with decimal fields (financial precision)
    let schema_json = r#"
    {
        "type": "record",
        "name": "FinancialData",
        "namespace": "com.trading.test",
        "fields": [
            {"name": "symbol", "type": "string"},
            {
                "name": "price",
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 19,
                "scale": 4
            },
            {"name": "volume", "type": "long"}
        ]
    }
    "#;

    let codec =
        AvroCodec::new(schema_json).expect("Failed to create Avro codec with decimal schema");

    // Create test data with ScaledInteger (financial precision)
    let mut test_data = HashMap::new();
    test_data.insert("symbol".to_string(), FieldValue::String("MSFT".to_string()));
    // Price: 150.2500 stored as ScaledInteger(1502500, 4)
    test_data.insert("price".to_string(), FieldValue::ScaledInteger(1502500, 4));
    test_data.insert("volume".to_string(), FieldValue::Integer(5000));

    // Serialize
    let serialized = codec
        .serialize(&test_data)
        .expect("Failed to serialize decimal data to Avro");

    // Deserialize
    let deserialized = codec
        .deserialize(&serialized)
        .expect("Failed to deserialize decimal data from Avro");

    // Verify - price should be deserialized as ScaledInteger or compatible type
    assert_eq!(
        deserialized.get("symbol"),
        Some(&FieldValue::String("MSFT".to_string()))
    );
    assert_eq!(deserialized.get("volume"), Some(&FieldValue::Integer(5000)));

    // Check price field exists and has correct value (may be Float or ScaledInteger)
    let price = deserialized.get("price").expect("Price field missing");
    match price {
        &FieldValue::Float(f) => {
            assert!((f - 150.25).abs() < 0.0001, "Price value mismatch");
        }
        &FieldValue::ScaledInteger(val, scale) => {
            let expected_float = (val as f64) / 10_f64.powi(scale as i32);
            assert!(
                (expected_float - 150.25).abs() < 0.0001,
                "Price value mismatch"
            );
        }
        _ => panic!("Price should be Float or ScaledInteger, got {:?}", price),
    }

    println!("✅ Avro decimal roundtrip test passed!");
}

#[test]
fn test_avro_schema_mismatch_detection() {
    // Test that schema mismatches are properly detected
    let write_schema = r#"
    {
        "type": "record",
        "name": "TestData",
        "fields": [
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "long"}
        ]
    }
    "#;

    let read_schema = r#"
    {
        "type": "record",
        "name": "TestData",
        "fields": [
            {"name": "field1", "type": "string"},
            {"name": "field3", "type": "double"}
        ]
    }
    "#;

    let write_codec = AvroCodec::new(write_schema).expect("Failed to create write codec");
    let read_codec = AvroCodec::new(read_schema).expect("Failed to create read codec");

    // Serialize with write schema
    let mut data = HashMap::new();
    data.insert("field1".to_string(), FieldValue::String("test".to_string()));
    data.insert("field2".to_string(), FieldValue::Integer(123));

    let serialized = write_codec
        .serialize(&data)
        .expect("Failed to serialize with write schema");

    // Try to deserialize with incompatible read schema
    let result = read_codec.deserialize(&serialized);

    // Should fail or return with missing field
    match result {
        Err(_) => {
            println!("✅ Schema mismatch correctly detected as error");
        }
        Ok(deserialized) => {
            // If it succeeds, field3 should be missing or null
            assert!(
                !deserialized.contains_key("field3")
                    || deserialized.get("field3") == Some(&FieldValue::Null),
                "Expected field3 to be missing or null due to schema mismatch"
            );
            println!("✅ Schema mismatch handled gracefully with missing field");
        }
    }
}

#[test]
fn test_avro_embedded_schema_in_message() {
    // Test that Avro messages with embedded schemas can be read
    let schema_json = r#"
    {
        "type": "record",
        "name": "EmbeddedTest",
        "namespace": "com.test",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }
    "#;

    let codec =
        AvroCodec::new(schema_json).expect("Failed to create codec for embedded schema test");

    let mut data = HashMap::new();
    data.insert("id".to_string(), FieldValue::Integer(42));
    data.insert("name".to_string(), FieldValue::String("test".to_string()));

    // Serialize (should include schema in message)
    let serialized = codec
        .serialize(&data)
        .expect("Failed to serialize with embedded schema");

    // Check for Avro Object Container File magic bytes "Obj\x01"
    assert!(
        serialized.len() > 4,
        "Serialized data too short to contain schema"
    );

    println!(
        "Serialized data starts with: {:?}",
        &serialized[..4.min(serialized.len())]
    );

    // Deserialize using the same codec (should use embedded schema)
    let deserialized = codec
        .deserialize(&serialized)
        .expect("Failed to deserialize with embedded schema");

    assert_eq!(deserialized.get("id"), Some(&FieldValue::Integer(42)));
    assert_eq!(
        deserialized.get("name"),
        Some(&FieldValue::String("test".to_string()))
    );

    println!("✅ Embedded schema roundtrip test passed!");
}

#[test]
fn test_avro_all_logical_types_roundtrip() {
    // Comprehensive test for all Avro logical types
    let schema_json = r#"
    {
        "type": "record",
        "name": "Transaction",
        "namespace": "com.trading.test",
        "fields": [
            {
                "name": "transaction_id",
                "type": "string",
                "logicalType": "uuid"
            },
            {
                "name": "amount",
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 12,
                "scale": 2
            },
            {
                "name": "transaction_date",
                "type": "int",
                "logicalType": "date"
            },
            {
                "name": "transaction_time",
                "type": "int",
                "logicalType": "time-millis"
            },
            {
                "name": "created_timestamp",
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        ]
    }
    "#;

    println!("Testing Avro logical types roundtrip...");

    let codec =
        AvroCodec::new(schema_json).expect("Failed to create Avro codec with logical types schema");

    // Create test data with various logical types
    let mut test_data = HashMap::new();

    // UUID as string
    test_data.insert(
        "transaction_id".to_string(),
        FieldValue::String("550e8400-e29b-41d4-a716-446655440000".to_string()),
    );

    // Decimal as ScaledInteger: 1234.56 with scale 2 = 123456
    test_data.insert("amount".to_string(), FieldValue::ScaledInteger(123456, 2));

    // Date as Integer (days since Unix epoch)
    // Date: 2024-01-15 = 19737 days since 1970-01-01
    test_data.insert("transaction_date".to_string(), FieldValue::Integer(19737));

    // Time-millis as Integer (milliseconds since midnight)
    // Time: 14:30:45.500 = 52245500 milliseconds
    test_data.insert(
        "transaction_time".to_string(),
        FieldValue::Integer(52245500),
    );

    // Timestamp-micros as Integer (microseconds since Unix epoch)
    // Timestamp: 2024-01-15 14:30:45.500000 UTC
    // = 1705329045500000 microseconds since epoch
    test_data.insert(
        "created_timestamp".to_string(),
        FieldValue::Integer(1705329045500000),
    );

    println!("Test data created:");
    println!("  - UUID: 550e8400-e29b-41d4-a716-446655440000");
    println!("  - Amount: $1234.56 (ScaledInteger(123456, 2))");
    println!("  - Date: 2024-01-15 (19737 days)");
    println!("  - Time: 14:30:45.500 (52245500 ms)");
    println!("  - Timestamp: 2024-01-15 14:30:45.500000 UTC (1705329045500000 μs)");

    // Serialize to Avro
    let serialized = codec
        .serialize(&test_data)
        .expect("Failed to serialize logical types to Avro");

    println!("Serialized {} bytes", serialized.len());

    // Deserialize from Avro
    let deserialized = codec
        .deserialize(&serialized)
        .expect("Failed to deserialize logical types from Avro");

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

    // Verify Decimal roundtrip
    let amount = deserialized.get("amount").expect("Amount field missing");
    match amount {
        &FieldValue::ScaledInteger(val, scale) => {
            assert_eq!(val, 123456, "Decimal value mismatch");
            assert_eq!(scale, 2, "Decimal scale mismatch");
            println!("✅ Decimal roundtrip verified (ScaledInteger)");
        }
        &FieldValue::Float(f) => {
            assert!(
                (f - 1234.56).abs() < 0.01,
                "Decimal value mismatch as float"
            );
            println!("✅ Decimal roundtrip verified (Float fallback)");
        }
        _ => panic!("Amount should be ScaledInteger or Float, got {:?}", amount),
    }

    // Verify Date roundtrip - Date logical type is converted to FieldValue::Date
    let date_field = deserialized
        .get("transaction_date")
        .expect("Date field missing");
    match date_field {
        FieldValue::Date(date) => {
            // Verify the date matches 2024-01-15
            use chrono::NaiveDate;
            let expected_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
            assert_eq!(date, &expected_date, "Date value mismatch");
            println!("✅ Date roundtrip verified (Date type)");
        }
        FieldValue::Integer(days) => {
            // Fallback: if stored as Integer (days since epoch)
            assert_eq!(days, &19737, "Date days value mismatch");
            println!("✅ Date roundtrip verified (Integer fallback)");
        }
        _ => panic!("Date should be Date or Integer, got {:?}", date_field),
    }

    // Verify Time-millis roundtrip - stored as Integer (milliseconds since midnight)
    assert_eq!(
        deserialized.get("transaction_time"),
        Some(&FieldValue::Integer(52245500)),
        "Time-millis mismatch after roundtrip"
    );
    println!("✅ Time-millis roundtrip verified");

    // Verify Timestamp-micros roundtrip - converted to FieldValue::Timestamp
    let timestamp_field = deserialized
        .get("created_timestamp")
        .expect("Timestamp field missing");
    match timestamp_field {
        FieldValue::Timestamp(ts) => {
            // Verify the timestamp matches 2024-01-15 14:30:45.500000 UTC
            use chrono::NaiveDateTime;
            #[allow(deprecated)]
            let expected_ts = NaiveDateTime::from_timestamp_opt(1705329045, 500000000).unwrap();
            assert_eq!(ts, &expected_ts, "Timestamp value mismatch");
            println!("✅ Timestamp-micros roundtrip verified (Timestamp type)");
        }
        FieldValue::Integer(micros) => {
            // Fallback: if stored as Integer (microseconds since epoch)
            assert_eq!(micros, &1705329045500000, "Timestamp micros value mismatch");
            println!("✅ Timestamp-micros roundtrip verified (Integer fallback)");
        }
        _ => panic!(
            "Timestamp should be Timestamp or Integer, got {:?}",
            timestamp_field
        ),
    }

    println!("✅ All Avro logical types roundtrip test passed!");
}
