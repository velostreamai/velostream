#!/usr/bin/env rust-script

//! Test what Value types Apache Avro 0.16.0 expects for Decimal schemas

use std::collections::HashMap;

fn main() {
    println!("🧪 Testing Apache Avro 0.16.0 Decimal Value Types");
    println!("===================================================");

    // Define the problematic schema
    let schema_json = r#"
    {
        "type": "record",
        "name": "Test",
        "fields": [
            {
                "name": "decimal_field", 
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 19,
                "scale": 4
            }
        ]
    }
    "#;

    match apache_avro::Schema::parse_str(schema_json) {
        Ok(schema) => {
            println!("✅ Schema parsed successfully");
            println!("Schema: {:?}", schema);

            // Test different value types
            test_value_types(&schema);
        }
        Err(e) => {
            println!("❌ Schema parsing failed: {}", e);
        }
    }
}

fn test_value_types(schema: &apache_avro::Schema) {
    use apache_avro::types::Value;
    use apache_avro::Writer;

    let test_bytes = vec![188, 97, 78]; // Same bytes from our test

    println!("\n🔬 Testing different Value types:");

    // Test 1: Value::Bytes
    println!("\n1. Testing Value::Bytes:");
    let record_bytes = vec![(
        "decimal_field".to_string(),
        Value::Bytes(test_bytes.clone()),
    )];
    test_serialization(schema, Value::Record(record_bytes), "Bytes");

    // Test 2: Value::String (decimal as string)
    println!("\n2. Testing Value::String:");
    let record_string = vec![(
        "decimal_field".to_string(),
        Value::String("1234.5678".to_string()),
    )];
    test_serialization(schema, Value::Record(record_string), "String");

    // Test 3: Value::Decimal (if available)
    println!("\n3. Testing Value::Decimal (direct):");
    // Try creating a Decimal value directly
    match apache_avro::types::Value::Decimal(apache_avro::Decimal::from(test_bytes.clone())) {
        value => {
            let record_decimal = vec![("decimal_field".to_string(), value)];
            test_serialization(schema, Value::Record(record_decimal), "Decimal(from bytes)");
        }
    }

    // Test 4: Check available Value variants
    println!("\n4. Exploring Value enum variants:");
    println!("Available Value types in apache_avro::types::Value:");
    // This will help us understand what's available

    // Let's see what the schema actually expects
    if let apache_avro::Schema::Record(record_schema) = schema {
        for field in &record_schema.fields {
            println!("Field '{}' has schema: {:?}", field.name, field.schema);
            println!(
                "  -> Field schema variant: {:?}",
                std::mem::discriminant(&field.schema)
            );
        }
    }
}

fn test_serialization(
    schema: &apache_avro::Schema,
    value: apache_avro::types::Value,
    test_name: &str,
) {
    use apache_avro::Writer;

    let mut writer = Writer::new(schema, Vec::new());
    match writer.append(value) {
        Ok(_) => {
            println!("   ✅ {}: Serialization succeeded", test_name);
            match writer.into_inner() {
                Ok(bytes) => println!("   📦 Produced {} bytes", bytes.len()),
                Err(e) => println!("   ⚠️  Writer finalization failed: {}", e),
            }
        }
        Err(e) => {
            println!("   ❌ {}: Serialization failed: {}", test_name, e);
        }
    }
}
