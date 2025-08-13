/*!
# Serialization Formats Example

This example demonstrates the different serialization formats supported by FerrisStreams:
- JSON (default, always available)
- Avro (feature-gated, requires schema)
- Protocol Buffers (feature-gated, supports generic usage)

Run with different features enabled:
- cargo run --example serialization_formats_example --features json
- cargo run --example serialization_formats_example --features avro
- cargo run --example serialization_formats_example --features protobuf
- cargo run --example serialization_formats_example --features avro,protobuf
*/

use ferrisstreams::ferris::serialization::{
    InternalValue, SerializationFormat, SerializationFormatFactory,
};
use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;

fn create_sample_record() -> HashMap<String, FieldValue> {
    let mut record = HashMap::new();

    // Basic fields
    record.insert("id".to_string(), FieldValue::Integer(12345));
    record.insert(
        "name".to_string(),
        FieldValue::String("Alice Johnson".to_string()),
    );
    record.insert("age".to_string(), FieldValue::Integer(28));
    record.insert("active".to_string(), FieldValue::Boolean(true));
    record.insert("score".to_string(), FieldValue::Float(95.7));
    record.insert("notes".to_string(), FieldValue::Null);

    // Array field
    record.insert(
        "tags".to_string(),
        FieldValue::Array(vec![
            FieldValue::String("employee".to_string()),
            FieldValue::String("senior".to_string()),
            FieldValue::String("tech".to_string()),
        ]),
    );

    // Map field
    let mut metadata = HashMap::new();
    metadata.insert(
        "department".to_string(),
        FieldValue::String("Engineering".to_string()),
    );
    metadata.insert("level".to_string(), FieldValue::String("L5".to_string()));
    record.insert("metadata".to_string(), FieldValue::Map(metadata));

    record
}

async fn test_json_format() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Testing JSON Format ===");

    let format = SerializationFormatFactory::create_format("json")?;
    let record = create_sample_record();

    println!("Format: {}", format.format_name());
    println!("Original record fields: {}", record.len());

    // Serialize
    let serialized = format.serialize_record(&record)?;
    println!("Serialized size: {} bytes", serialized.len());

    // Show JSON content (pretty print)
    if let Ok(json_str) = String::from_utf8(serialized.clone()) {
        if let Ok(pretty_json) = serde_json::from_str::<serde_json::Value>(&json_str) {
            println!(
                "JSON content:\n{}",
                serde_json::to_string_pretty(&pretty_json)?
            );
        }
    }

    // Deserialize
    let deserialized = format.deserialize_record(&serialized)?;
    println!("Deserialized fields: {}", deserialized.len());

    // Convert to execution format
    let execution_format = format.to_execution_format(&record)?;
    println!("Execution format fields: {}", execution_format.len());

    // Verify round trip
    let restored = format.from_execution_format(&execution_format)?;
    println!("Round trip successful: {}", record == restored);

    Ok(())
}

#[cfg(feature = "avro")]
async fn test_avro_format() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing Avro Format ===");

    // Define schema for our record (simplified to avoid complex type issues)
    let schema_json = r#"
    {
        "type": "record",
        "name": "UserRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "long"},
            {"name": "active", "type": "boolean"},
            {"name": "score", "type": "double"}
        ]
    }
    "#;

    let format = SerializationFormatFactory::create_avro_format(schema_json)?;

    // Create simplified record for Avro
    let mut record = HashMap::new();
    record.insert("id".to_string(), FieldValue::Integer(12345));
    record.insert(
        "name".to_string(),
        FieldValue::String("Alice Johnson".to_string()),
    );
    record.insert("age".to_string(), FieldValue::Integer(28));
    record.insert("active".to_string(), FieldValue::Boolean(true));
    record.insert("score".to_string(), FieldValue::Float(95.7));

    println!("Format: {}", format.format_name());

    // Serialize
    let serialized = format.serialize_record(&record)?;
    println!("Avro serialized size: {} bytes", serialized.len());

    // Deserialize
    let deserialized = format.deserialize_record(&serialized)?;
    println!("Avro deserialized fields: {}", deserialized.len());

    // Test schema evolution
    println!("\n--- Testing Schema Evolution ---");
    let evolved_schema = r#"
    {
        "type": "record",
        "name": "UserRecord",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "age", "type": "long"},
            {"name": "active", "type": "boolean"},
            {"name": "score", "type": "double"},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let evolved_format =
        SerializationFormatFactory::create_avro_format_with_schemas(schema_json, evolved_schema)?;

    // Write with old schema, read with new schema
    let old_serialized = format.serialize_record(&record)?;
    let evolved_deserialized = evolved_format.deserialize_record(&old_serialized)?;
    println!(
        "Schema evolution successful: {} fields",
        evolved_deserialized.len()
    );

    Ok(())
}

#[cfg(feature = "protobuf")]
async fn test_protobuf_format() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing Protobuf Format ===");

    let format = SerializationFormatFactory::create_format("protobuf")?;
    let record = create_sample_record();

    println!("Format: {}", format.format_name());

    // Serialize
    let serialized = format.serialize_record(&record)?;
    println!("Protobuf serialized size: {} bytes", serialized.len());

    // Deserialize
    let deserialized = format.deserialize_record(&serialized)?;
    println!("Protobuf deserialized fields: {}", deserialized.len());

    // Test with typed format
    let typed_format = SerializationFormatFactory::create_protobuf_format::<()>();
    let typed_serialized = typed_format.serialize_record(&record)?;
    println!("Typed protobuf size: {} bytes", typed_serialized.len());

    Ok(())
}

async fn test_format_factory() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing Format Factory ===");

    let supported = SerializationFormatFactory::supported_formats();
    println!("Supported formats: {:?}", supported);

    // Test each supported format
    for format_name in supported {
        match SerializationFormatFactory::create_format(format_name) {
            Ok(format) => {
                println!("✓ {} format created successfully", format.format_name());

                // Quick test
                let mut test_record = HashMap::new();
                test_record.insert("test".to_string(), FieldValue::String("value".to_string()));

                let serialized = format.serialize_record(&test_record)?;
                let _deserialized = format.deserialize_record(&serialized)?;
                println!("  ✓ Serialization round trip successful");
            }
            Err(e) => {
                println!("✗ Failed to create {} format: {}", format_name, e);
            }
        }
    }

    // Test error handling
    match SerializationFormatFactory::create_format("nonexistent") {
        Ok(_) => println!("✗ Should have failed for nonexistent format"),
        Err(_) => println!("✓ Properly rejected nonexistent format"),
    }

    Ok(())
}

async fn compare_formats() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Format Comparison ===");

    let record = create_sample_record();
    let supported_formats = SerializationFormatFactory::supported_formats();

    println!("Serialization size comparison for {} fields:", record.len());

    for format_name in supported_formats {
        match SerializationFormatFactory::create_format(format_name) {
            Ok(format) => match format.serialize_record(&record) {
                Ok(serialized) => {
                    println!("  {}: {} bytes", format.format_name(), serialized.len());
                }
                Err(e) => {
                    println!("  {}: Error - {}", format.format_name(), e);
                }
            },
            Err(e) => {
                println!("  {}: Failed to create - {}", format_name, e);
            }
        }
    }

    Ok(())
}

async fn test_execution_format_conversion() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Execution Format Conversion ===");

    let format = SerializationFormatFactory::default_format();
    let record = create_sample_record();

    // Convert to execution format
    let execution_data = format.to_execution_format(&record)?;
    println!(
        "Converted {} fields to execution format",
        execution_data.len()
    );

    // Show some execution values
    for (key, value) in execution_data.iter().take(3) {
        println!("  {}: {:?}", key, value);
    }

    // Convert back
    let restored_record = format.from_execution_format(&execution_data)?;
    println!(
        "Restored {} fields from execution format",
        restored_record.len()
    );

    // Verify integrity
    println!("Round trip successful: {}", record == restored_record);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 FerrisStreams Serialization Formats Demo");
    println!("============================================");

    // Always test JSON (default format)
    test_json_format().await?;

    // Test Avro if feature is enabled
    #[cfg(feature = "avro")]
    test_avro_format().await?;

    // Test Protobuf if feature is enabled
    #[cfg(feature = "protobuf")]
    test_protobuf_format().await?;

    // Test factory functionality
    test_format_factory().await?;

    // Compare format sizes
    compare_formats().await?;

    // Test execution format conversions
    test_execution_format_conversion().await?;

    println!("\n✨ All tests completed successfully!");
    Ok(())
}
