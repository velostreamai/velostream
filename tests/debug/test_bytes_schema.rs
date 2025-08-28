#!/usr/bin/env rust-script

//! Test creating a proper bytes schema that doesn't get converted to Decimal

use std::collections::HashMap;

fn main() {
    println!("🧪 Testing Different Schema Approaches for Bytes + Decimal");
    println!("==========================================================");

    // Test 1: The problematic schema (gets converted to Decimal)
    let decimal_schema = r#"
    {
        "type": "record",
        "name": "Test",
        "fields": [
            {
                "name": "price", 
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 19,
                "scale": 4
            }
        ]
    }
    "#;

    println!("🔍 Test 1: Schema with logicalType");
    test_schema_conversion("decimal_schema", decimal_schema);

    // Test 2: Pure bytes schema (should stay as bytes)
    let bytes_schema = r#"
    {
        "type": "record", 
        "name": "Test",
        "fields": [
            {
                "name": "price",
                "type": "bytes"
            }
        ]
    }
    "#;

    println!("\n🔍 Test 2: Pure bytes schema");
    test_schema_conversion("bytes_schema", bytes_schema);

    // Test 3: Bytes with custom properties (not logicalType)
    let custom_schema = r#"
    {
        "type": "record",
        "name": "Test", 
        "fields": [
            {
                "name": "price",
                "type": "bytes",
                "decimalPrecision": 19,
                "decimalScale": 4,
                "doc": "Decimal stored as bytes"
            }
        ]
    }
    "#;

    println!("\n🔍 Test 3: Bytes with custom properties");
    test_schema_conversion("custom_schema", custom_schema);
}

fn test_schema_conversion(name: &str, schema_json: &str) {
    match apache_avro::Schema::parse_str(schema_json) {
        Ok(schema) => {
            println!("   ✅ {} parsed successfully", name);
            if let apache_avro::Schema::Record(record) = &schema {
                for field in &record.fields {
                    println!("   📋 Field '{}': {:?}", field.name, field.schema);
                    match &field.schema {
                        apache_avro::Schema::Decimal(decimal_schema) => {
                            println!("      🔄 Converted to Decimal - precision: {}, scale: {}, inner: {:?}",
                                   decimal_schema.precision, decimal_schema.scale, decimal_schema.inner);
                        }
                        apache_avro::Schema::Bytes => {
                            println!("      ✅ Stays as Bytes");
                            if let Some(attrs) = field.schema.custom_attributes() {
                                println!("      🏷️  Custom attributes: {:?}", attrs);
                            }
                        }
                        _ => {
                            println!("      ℹ️  Other type: {:?}", field.schema);
                        }
                    }
                }
            }

            // Test serialization with bytes value
            test_bytes_serialization(&schema);
        }
        Err(e) => {
            println!("   ❌ {} parsing failed: {}", name, e);
        }
    }
}

fn test_bytes_serialization(schema: &apache_avro::Schema) {
    use apache_avro::{types::Value, Writer};

    let test_bytes = vec![188, 97, 78]; // Same test data

    // Try serializing with Value::Bytes
    let record = vec![("price".to_string(), Value::Bytes(test_bytes.clone()))];

    let mut writer = Writer::new(schema, Vec::new());
    match writer.append(Value::Record(record)) {
        Ok(_) => match writer.into_inner() {
            Ok(serialized) => {
                println!(
                    "      ✅ Serialization successful: {} bytes",
                    serialized.len()
                );
            }
            Err(e) => {
                println!("      ⚠️  Writer finalization failed: {}", e);
            }
        },
        Err(e) => {
            println!("      ❌ Serialization failed: {}", e);
        }
    }
}
