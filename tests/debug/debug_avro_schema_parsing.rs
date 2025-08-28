#!/usr/bin/env rust-script

//! Debug script to test Avro schema parsing for decimal logical types

use std::collections::HashMap;

#[cfg(feature = "avro")]
fn main() {
    use ferrisstreams::ferris::serialization::{AvroFormat, FieldValue, SerializationFormat};

    println!("🧪 Testing Avro Decimal Schema Parsing");
    println!("=====================================");

    // Define schema with custom decimal properties (Flink-compatible)
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
                "decimalPrecision": 19,
                "decimalScale": 4,
                "doc": "Price stored as decimal bytes (precision=19, scale=4)"
            }
        ]
    }
    "#;

    println!("📋 Schema JSON:");
    println!("{}", schema_json);
    println!();

    // Create Avro format with schema
    match AvroFormat::new(schema_json) {
        Ok(avro_format) => {
            println!("✅ AvroFormat created successfully");

            // Debug: Check what decimal fields were extracted
            println!("🔍 Debug: Checking decimal fields extraction...");

            // Create test record with ScaledInteger price: $1234.5678
            let mut record = HashMap::new();
            record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            record.insert("price".to_string(), FieldValue::ScaledInteger(12345678, 4));

            println!("📥 Original record:");
            println!("   symbol = String(\"AAPL\")");
            println!("   price = ScaledInteger(12345678, 4) // $1234.5678");
            println!();

            // Test serialization
            match avro_format.serialize_record(&record) {
                Ok(serialized) => {
                    println!("✅ Serialization successful - {} bytes", serialized.len());

                    // Test deserialization
                    match avro_format.deserialize_record(&serialized) {
                        Ok(deserialized) => {
                            println!("✅ Deserialization successful");

                            // Check the price field specifically
                            if let Some(price_field) = deserialized.get("price") {
                                println!("📤 Deserialized price field: {:?}", price_field);

                                match price_field {
                                    FieldValue::ScaledInteger(value, scale) => {
                                        println!(
                                            "🎯 SUCCESS: Got ScaledInteger({}, {}) = ${:.4}",
                                            value,
                                            scale,
                                            *value as f64 / 10_f64.powi(*scale as i32)
                                        );

                                        if *scale == 4 {
                                            println!("🎉 PERFECT: Scale={} matches schema (not hardcoded)!", scale);
                                        } else {
                                            println!(
                                                "⚠️  WARNING: Scale={} != 4 from schema",
                                                scale
                                            );
                                        }
                                    }
                                    _ => {
                                        println!(
                                            "❌ FAILED: Expected ScaledInteger, got {:?}",
                                            price_field
                                        );
                                    }
                                }
                            } else {
                                println!("❌ FAILED: Price field missing from deserialized record");
                            }

                            // Check full round-trip
                            if record == deserialized {
                                println!("🎉 PERFECT: Complete record round-trip successful!");
                            } else {
                                println!("⚠️  PARTIAL: Records don't match exactly");
                                println!("   Original: {:?}", record);
                                println!("   Deserialized: {:?}", deserialized);
                            }
                        }
                        Err(e) => println!("❌ Deserialization failed: {}", e),
                    }
                }
                Err(e) => println!("❌ Serialization failed: {}", e),
            }
        }
        Err(e) => println!("❌ Failed to create AvroFormat: {}", e),
    }
}

#[cfg(not(feature = "avro"))]
fn main() {
    println!("⚠️  Avro feature not enabled. Compile with --features avro");
}
