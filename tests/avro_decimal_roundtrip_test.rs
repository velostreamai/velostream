#!/usr/bin/env rust-script

//! Integration test for Avro decimal logical type round-trip conversion
//! Tests that ScaledInteger can properly serialize/deserialize through Avro with schema-aware precision/scale

#[cfg(feature = "avro")]
mod avro_decimal_tests {
    use ferrisstreams::ferris::serialization::AvroFormat;
    use ferrisstreams::ferris::serialization::{FieldValue, SerializationFormat};
    use std::collections::HashMap;

    #[test]
    fn test_price_decimal_roundtrip() {
        println!("🧪 Testing price decimal round-trip (precision=19, scale=4)");

        // Define schema with decimal logical type for price
        let schema_json = r#"
        {
            "type": "record",
            "name": "MarketData",
            "fields": [
                {
                    "name": "symbol",
                    "type": "string"
                },
                {
                    "name": "price", 
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 19,
                    "scale": 4,
                    "doc": "Price with 4 decimal places precision"
                }
            ]
        }
        "#;

        // Create Avro format with schema
        let avro_format = AvroFormat::new(schema_json).expect("Failed to create AvroFormat");

        // Create test record with ScaledInteger price: $1234.5678
        let mut record = HashMap::new();
        record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        record.insert("price".to_string(), FieldValue::ScaledInteger(12345678, 4));

        println!("📥 Original: price = ScaledInteger(12345678, 4) = $1234.5678");

        // Serialize to Avro bytes
        let serialized = avro_format
            .serialize_record(&record)
            .expect("Failed to serialize record");

        println!("🔄 Serialized to {} bytes", serialized.len());

        // Deserialize back from Avro bytes
        let deserialized = avro_format
            .deserialize_record(&serialized)
            .expect("Failed to deserialize record");

        // Check that price field is still ScaledInteger with correct scale
        let price_field = deserialized.get("price").expect("Price field missing");

        println!("📤 Deserialized: price = {:?}", price_field);

        // Verify round-trip: should be exactly the same ScaledInteger
        match price_field {
            FieldValue::ScaledInteger(value, scale) => {
                assert_eq!(*value, 12345678, "Value should match original");
                assert_eq!(*scale, 4, "Scale should be 4 from schema, not hardcoded!");
                println!(
                    "✅ SUCCESS: Scale={} came from schema (not hardcoded)",
                    scale
                );
            }
            _ => {
                panic!(
                    "❌ FAILED: Price field should be ScaledInteger, got {:?}",
                    price_field
                );
            }
        }

        // Verify complete record round-trip
        assert_eq!(
            record, deserialized,
            "Complete record should round-trip perfectly"
        );

        println!("🎉 Round-trip test PASSED! Schema-aware decimal conversion works!");
    }

    #[test]
    fn test_pnl_decimal_roundtrip() {
        println!("🧪 Testing P&L decimal round-trip (precision=19, scale=2)");

        let schema_json = r#"
        {
            "type": "record", 
            "name": "Position",
            "fields": [
                {
                    "name": "trader_id",
                    "type": "string"
                },
                {
                    "name": "pnl",
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 19, 
                    "scale": 2,
                    "doc": "P&L with 2 decimal places precision"
                }
            ]
        }
        "#;

        let avro_format = AvroFormat::new(schema_json).expect("Failed to create AvroFormat");

        // P&L: -$1500.50
        let mut record = HashMap::new();
        record.insert(
            "trader_id".to_string(),
            FieldValue::String("trader123".to_string()),
        );
        record.insert("pnl".to_string(), FieldValue::ScaledInteger(-150050, 2));

        println!("📥 Original: pnl = ScaledInteger(-150050, 2) = -$1500.50");

        // Round-trip test
        let serialized = avro_format
            .serialize_record(&record)
            .expect("Serialize failed");
        let deserialized = avro_format
            .deserialize_record(&serialized)
            .expect("Deserialize failed");

        let pnl_field = deserialized.get("pnl").expect("PnL field missing");
        println!("📤 Deserialized: pnl = {:?}", pnl_field);

        if let FieldValue::ScaledInteger(value, scale) = pnl_field {
            assert_eq!(*value, -150050);
            assert_eq!(*scale, 2, "Scale should be 2 from schema");
            println!("✅ SUCCESS: P&L scale={} from schema", scale);
        } else {
            panic!("❌ FAILED: Expected ScaledInteger, got {:?}", pnl_field);
        }

        println!("🎉 P&L round-trip test PASSED!");
    }

    #[test]
    fn test_mixed_precision_scales() {
        println!("🧪 Testing mixed precision/scale in same record");

        let schema_json = r#"
        {
            "type": "record",
            "name": "TradingData", 
            "fields": [
                {
                    "name": "price",
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 19,
                    "scale": 4,
                    "doc": "Price: 4 decimal places"
                },
                {
                    "name": "pnl", 
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 19,
                    "scale": 2,
                    "doc": "P&L: 2 decimal places"
                },
                {
                    "name": "percentage",
                    "type": "bytes", 
                    "logicalType": "decimal",
                    "precision": 10,
                    "scale": 4,
                    "doc": "Percentage: 4 decimal places"
                }
            ]
        }
        "#;

        let avro_format = AvroFormat::new(schema_json).expect("Failed to create AvroFormat");

        let mut record = HashMap::new();
        record.insert("price".to_string(), FieldValue::ScaledInteger(12345678, 4)); // $1234.5678
        record.insert("pnl".to_string(), FieldValue::ScaledInteger(-150050, 2)); // -$1500.50
        record.insert(
            "percentage".to_string(),
            FieldValue::ScaledInteger(52500, 4),
        ); // 5.2500%

        println!("📥 Original record with mixed scales: 4, 2, 4");

        // Round-trip
        let serialized = avro_format
            .serialize_record(&record)
            .expect("Serialize failed");
        let deserialized = avro_format
            .deserialize_record(&serialized)
            .expect("Deserialize failed");

        // Verify each field maintains its correct scale from schema
        if let FieldValue::ScaledInteger(_, scale) = deserialized.get("price").unwrap() {
            assert_eq!(*scale, 4, "Price should have scale=4 from schema");
            println!("✅ Price scale: {} ✓", scale);
        }

        if let FieldValue::ScaledInteger(_, scale) = deserialized.get("pnl").unwrap() {
            assert_eq!(*scale, 2, "PnL should have scale=2 from schema");
            println!("✅ P&L scale: {} ✓", scale);
        }

        if let FieldValue::ScaledInteger(_, scale) = deserialized.get("percentage").unwrap() {
            assert_eq!(*scale, 4, "Percentage should have scale=4 from schema");
            println!("✅ Percentage scale: {} ✓", scale);
        }

        assert_eq!(
            record, deserialized,
            "Complete mixed-scale record should round-trip"
        );
        println!("🎉 Mixed precision/scale test PASSED!");
    }
}

#[cfg(not(feature = "avro"))]
fn main() {
    println!("⚠️  Avro feature not enabled. Run with: cargo test --features avro");
}

#[cfg(feature = "avro")]
fn main() {
    println!("🚀 Running Avro decimal round-trip tests...");

    avro_decimal_tests::test_price_decimal_roundtrip();
    println!();
    avro_decimal_tests::test_pnl_decimal_roundtrip();
    println!();
    avro_decimal_tests::test_mixed_precision_scales();

    println!("\n🎯 All tests demonstrate that:");
    println!("   ✅ ScaledInteger serializes to proper Avro decimal bytes");
    println!("   ✅ Scale comes from schema metadata (not hardcoded)");
    println!("   ✅ Different fields can have different precision/scale");
    println!("   ✅ Round-trip conversion preserves exact values");
}
