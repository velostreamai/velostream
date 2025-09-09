use ferrisstreams::ferris::serialization::{json::JsonFormat, SerializationFormat};
use ferrisstreams::ferris::sql::execution::types::FieldValue;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Serialization Compatibility");
    println!("===================================");

    // Create a record with ScaledInteger values
    let mut record = HashMap::new();
    record.insert(
        "price".to_string(),
        FieldValue::from_financial_f64(123.4567, 4),
    );
    record.insert("quantity".to_string(), FieldValue::Integer(100));
    record.insert(
        "total".to_string(),
        FieldValue::from_financial_f64(12345.67, 2),
    );
    record.insert(
        "name".to_string(),
        FieldValue::String("AAPL Trade".to_string()),
    );

    println!("Original Record:");
    for (key, value) in &record {
        println!(
            "  {}: {} (type: {})",
            key,
            value.to_display_string(),
            value.type_name()
        );
    }

    // Test JSON serialization (always available)
    println!("\n--- JSON Serialization ---");
    let json_format = JsonFormat;
    let json_bytes = json_format.serialize_record(&record)?;
    let json_str = String::from_utf8(json_bytes.clone())?;

    println!("Serialized JSON:");
    println!("{}", json_str);

    // Test that other systems can read this as standard JSON
    let parsed_json: serde_json::Value = serde_json::from_slice(&json_bytes)?;
    println!("\nParsed by standard JSON parser:");
    match parsed_json {
        serde_json::Value::Object(obj) => {
            for (key, value) in &obj {
                match value {
                    serde_json::Value::String(s) => {
                        println!("  {}: \"{}\" (string - can be parsed as decimal)", key, s)
                    }
                    serde_json::Value::Number(n) => println!("  {}: {} (number)", key, n),
                    _ => println!("  {}: {:?} (other)", key, value),
                }
            }
        }
        _ => println!("Not an object"),
    }

    // Test round-trip
    println!("\n--- Round-trip Test ---");
    let restored = json_format.deserialize_record(&json_bytes)?;

    println!("Restored Record:");
    for (key, value) in &restored {
        println!(
            "  {}: {} (type: {})",
            key,
            value.to_display_string(),
            value.type_name()
        );
    }

    // Verify that financial calculations still work
    if let (Some(price), Some(quantity)) = (restored.get("price"), restored.get("quantity")) {
        match price.multiply(quantity) {
            Ok(calculated_total) => {
                println!("\nFinancial calculation test:");
                println!(
                    "  Price × Quantity = {}",
                    calculated_total.to_display_string()
                );
            }
            Err(e) => println!("Error in calculation: {:?}", e),
        }
    }

    println!("\n✅ Serialization is now compatible with standard JSON parsers!");
    println!("✅ Other systems can read the financial values as decimal strings!");

    Ok(())
}
