#!/usr/bin/env rust-script

//! Test standard Avro decimal logical type support in version 0.20.0

use std::collections::HashMap;

#[cfg(feature = "avro")]
fn main() {
    use ferrisstreams::ferris::serialization::{AvroFormat, FieldValue, SerializationFormat};

    println!("🧪 Testing Standard Avro Decimal Logical Type (v0.20.0)");
    println!("======================================================");

    // Test schema with standard decimal logical type
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
                "doc": "Current price with 4 decimal places precision (e.g., 123.4567)"
            }
        ]
    }
    "#;

    println!("📋 Schema JSON (Standard Decimal Logical Type):");
    println!("{}", schema_json);
    println!();

    // Create Avro format with schema
    match AvroFormat::new(schema_json) {
        Ok(avro_format) => {
            println!("✅ AvroFormat created successfully with standard logicalType");

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
                                        let decimal_value =
                                            *value as f64 / 10_f64.powi(*scale as i32);
                                        println!(
                                            "🎯 SUCCESS: Got ScaledInteger({}, {}) = ${:.4}",
                                            value, scale, decimal_value
                                        );

                                        if *scale == 4 {
                                            println!(
                                                "🎉 PERFECT: Scale={} matches schema precision!",
                                                scale
                                            );
                                        } else {
                                            println!(
                                                "⚠️  WARNING: Scale={} != 4 from schema",
                                                scale
                                            );
                                        }

                                        if *value == 12345678 && *scale == 4 {
                                            println!("🎉 EXCELLENT: Standard decimal logical type works perfectly!");
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
                                println!("✨ Standard Avro decimal logical type is FULLY SUPPORTED in v0.20.0!");
                            } else {
                                println!("⚠️  PARTIAL: Records don't match exactly");
                                println!("   Original: {:?}", record);
                                println!("   Deserialized: {:?}", deserialized);
                            }
                        }
                        Err(e) => {
                            println!("❌ Deserialization failed: {}", e);
                            println!("💡 This suggests standard decimal logical type may not be fully supported");
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Serialization failed: {}", e);
                    println!(
                        "💡 This suggests standard decimal logical type may not be fully supported"
                    );
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to create AvroFormat: {}", e);
            println!(
                "💡 Schema parsing failed - standard decimal logical type may not be supported"
            );
        }
    }

    println!();
    println!("🔍 Conclusion:");
    println!("   If you see 'EXCELLENT' above, standard decimal logical type works!");
    println!("   If you see errors, we'll need to continue using custom properties approach.");
}

#[cfg(not(feature = "avro"))]
fn main() {
    println!("⚠️  Avro feature not enabled. Compile with --features avro");
}
