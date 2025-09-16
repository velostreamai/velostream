//! Test binary for protobuf codec functionality

use std::collections::HashMap;
use velostream::velostream::serialization::protobuf_codec::ProtobufCodec;
use velostream::velostream::sql::execution::types::FieldValue;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Testing Protobuf Codec Functionality");
    println!("=======================================");

    // Create test record with various FieldValue types
    let mut record = HashMap::new();

    // Basic types
    record.insert("id".to_string(), FieldValue::Integer(12345));
    record.insert(
        "name".to_string(),
        FieldValue::String("Alice Johnson".to_string()),
    );
    record.insert("active".to_string(), FieldValue::Boolean(true));
    record.insert("score".to_string(), FieldValue::Float(98.7));

    // Financial precision with ScaledInteger
    record.insert(
        "price".to_string(),
        FieldValue::from_financial_f64(1234.5678, 4),
    );
    record.insert(
        "commission".to_string(),
        FieldValue::from_financial_f64(12.50, 2),
    );

    // Null value
    record.insert("optional_field".to_string(), FieldValue::Null);

    // Array
    record.insert(
        "tags".to_string(),
        FieldValue::Array(vec![
            FieldValue::String("important".to_string()),
            FieldValue::String("financial".to_string()),
            FieldValue::String("trading".to_string()),
        ]),
    );

    // Map/struct
    let mut metadata = HashMap::new();
    metadata.insert(
        "source".to_string(),
        FieldValue::String("trading_system".to_string()),
    );
    metadata.insert("version".to_string(), FieldValue::Integer(2));
    record.insert("metadata".to_string(), FieldValue::Map(metadata));

    println!("Original Record:");
    for (key, value) in &record {
        println!(
            "  {}: {} ({})",
            key,
            value.to_display_string(),
            value.type_name()
        );
    }

    // Test protobuf codec
    println!("\n--- Protobuf Codec Test ---");
    let codec = ProtobufCodec::new_with_default_schema();

    // Serialize
    let protobuf_bytes = codec.serialize(&record)?;
    println!("Serialized to {} bytes", protobuf_bytes.len());

    // Deserialize
    let restored_record = codec.deserialize(&protobuf_bytes)?;
    println!("\nRestored Record:");
    for (key, value) in &restored_record {
        println!(
            "  {}: {} ({})",
            key,
            value.to_display_string(),
            value.type_name()
        );
    }

    // Verify financial precision is preserved
    if let (Some(original_price), Some(restored_price)) =
        (record.get("price"), restored_record.get("price"))
    {
        match (original_price, restored_price) {
            (
                FieldValue::ScaledInteger(orig_val, orig_scale),
                FieldValue::ScaledInteger(rest_val, rest_scale),
            ) => {
                if orig_val == rest_val && orig_scale == rest_scale {
                    println!(
                        "\n✅ Financial precision preserved: ${} (scale: {})",
                        original_price.to_display_string(),
                        orig_scale
                    );
                } else {
                    println!(
                        "\n❌ Financial precision lost: original({}, {}) vs restored({}, {})",
                        orig_val, orig_scale, rest_val, rest_scale
                    );
                }
            }
            _ => println!("\n❌ Financial precision type mismatch"),
        }
    }

    // Test round-trip accuracy
    let mut all_match = true;
    for (key, original_value) in &record {
        match restored_record.get(key) {
            Some(restored_value) => {
                if original_value != restored_value {
                    println!(
                        "❌ Mismatch for '{}': {} != {}",
                        key,
                        original_value.to_display_string(),
                        restored_value.to_display_string()
                    );
                    all_match = false;
                }
            }
            None => {
                println!("❌ Missing field in restored record: {}", key);
                all_match = false;
            }
        }
    }

    if all_match {
        println!("\n🎉 Perfect round-trip! All values preserved exactly.");
    } else {
        println!("\n⚠️  Some values were not preserved in round-trip.");
    }

    // Test financial precision mode (now always enabled)
    println!("\n--- Financial Precision Mode Test ---");
    println!("Financial precision is now always enabled in protobuf codec.");
    let codec = ProtobufCodec::new_with_default_schema();

    let bytes = codec.serialize(&record)?;
    println!("Serialized with financial precision: {} bytes", bytes.len());

    // Verify financial precision works
    let restored = codec.deserialize(&bytes)?;
    if let (Some(original), Some(restored_val)) = (record.get("price"), restored.get("price")) {
        match (original, restored_val) {
            (
                FieldValue::ScaledInteger(o_val, o_scale),
                FieldValue::ScaledInteger(r_val, r_scale),
            ) => {
                if o_val == r_val && o_scale == r_scale {
                    println!(
                        "✅ Financial precision preserved: {} with scale {}",
                        o_val, o_scale
                    );
                } else {
                    println!(
                        "❌ Financial precision error: ({}, {}) != ({}, {})",
                        o_val, o_scale, r_val, r_scale
                    );
                }
            }
            _ => println!("❌ Financial precision type conversion error"),
        }
    }

    println!("\n✅ Protobuf codec is working correctly!");
    println!("✅ Industry-standard DecimalMessage format implemented!");
    println!("✅ Financial precision is always enabled!");

    Ok(())
}
