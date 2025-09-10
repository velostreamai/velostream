#!/usr/bin/env rust-script
//! Debug script to understand apache-avro 0.16.0 API

fn main() {
    let schema_json = r#"
    {
        "type": "record",
        "name": "TestRecord",
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

    println!("🔍 Debugging apache-avro 0.16.0 API...");
    
    match apache_avro::Schema::parse_str(schema_json) {
        Ok(schema) => {
            println!("✅ Schema parsed successfully!");
            println!("Schema type: {:?}", std::any::type_name_of_val(&schema));
            
            // Try to access the schema structure
            match schema {
                apache_avro::Schema::Record { name, doc, fields, lookup, .. } => {
                    println!("📋 Record schema:");
                    println!("  - name: {:?}", name);
                    println!("  - doc: {:?}", doc);
                    println!("  - fields count: {}", fields.len());
                    
                    for (i, field) in fields.iter().enumerate() {
                        println!("  - field[{}]: {:?}", i, field.name);
                        println!("    schema: {:?}", field.schema);
                    }
                }
                _ => println!("❌ Not a record schema"),
            }
        }
        Err(e) => {
            println!("❌ Schema parse failed: {}", e);
        }
    }
}