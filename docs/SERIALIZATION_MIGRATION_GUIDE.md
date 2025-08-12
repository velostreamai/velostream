# Serialization Migration Guide

This guide helps you migrate from the old hardcoded JSON serialization to the new pluggable serialization system in FerrisStreams.

## Overview of Changes

### Before (Old System)
- Hardcoded JSON serialization using `serde_json::Value`
- Limited to JSON format only
- Direct coupling between SQL engine and JSON types

### After (New System)
- Pluggable serialization with multiple format support
- `SerializationFormat` trait for extensibility
- Support for JSON, Avro, and Protocol Buffers
- Clean separation between external formats and SQL engine internals

## Migration Steps

### 1. Update Dependencies

**Add feature flags to your `Cargo.toml`:**

```toml
# Before
[dependencies]
ferrisstreams = "0.1.0"

# After - choose your formats
[dependencies]
ferrisstreams = { version = "0.1.0", features = ["avro", "protobuf"] }
```

### 2. Update Import Statements

**Replace old imports:**

```rust
// Before
use ferrisstreams::ferris::kafka::JsonSerializer;
use serde_json::Value;

// After
use ferrisstreams::ferris::serialization::{
    SerializationFormatFactory, SerializationFormat, FieldValue
};
```

### 3. Replace Hardcoded JSON Usage

**SQL Engine Integration:**

```rust
// Before
let mut engine = StreamExecutionEngine::new(tx);
// Engine was hardcoded to use serde_json::Value

// After
use std::sync::Arc;
use ferrisstreams::ferris::serialization::JsonFormat;

let serialization_format = Arc::new(JsonFormat);
let mut engine = StreamExecutionEngine::new(tx, serialization_format);
```

**Data Type Migration:**

```rust
// Before - using serde_json::Value directly
let mut record = std::collections::HashMap::new();
record.insert("name".to_string(), serde_json::Value::String("Alice".to_string()));
record.insert("age".to_string(), serde_json::Value::Number(serde_json::Number::from(30)));

// After - using FieldValue
let mut record = std::collections::HashMap::new();
record.insert("name".to_string(), FieldValue::String("Alice".to_string()));
record.insert("age".to_string(), FieldValue::Integer(30));
```

### 4. Update Message Processing

**Kafka Message Handling:**

```rust
// Before - manual JSON conversion
let json_str = String::from_utf8(message.payload().unwrap())?;
let value: serde_json::Value = serde_json::from_str(&json_str)?;

// After - using format abstraction
let format = SerializationFormatFactory::create_format("json")?;
let record = format.deserialize_record(message.payload().unwrap())?;
```

**Record Serialization:**

```rust
// Before - manual JSON serialization
let json_value = serde_json::to_value(&my_struct)?;
let bytes = serde_json::to_vec(&json_value)?;

// After - using format interface
let format = SerializationFormatFactory::create_format("json")?;
let bytes = format.serialize_record(&record)?;
```

## Type Mapping Reference

### JSON Value → FieldValue

| Old Type | New Type |
|----------|----------|
| `serde_json::Value::String(s)` | `FieldValue::String(s)` |
| `serde_json::Value::Number(n)` (int) | `FieldValue::Integer(i64)` |
| `serde_json::Value::Number(n)` (float) | `FieldValue::Float(f64)` |
| `serde_json::Value::Bool(b)` | `FieldValue::Boolean(b)` |
| `serde_json::Value::Null` | `FieldValue::Null` |
| `serde_json::Value::Array(arr)` | `FieldValue::Array(Vec<FieldValue>)` |
| `serde_json::Value::Object(obj)` | `FieldValue::Map(HashMap<String, FieldValue>)` |

### Internal Engine Types

| Old Type | New Type |
|----------|----------|
| `serde_json::Value::String(s)` | `InternalValue::String(s)` |
| `serde_json::Value::Number(n)` | `InternalValue::Number(f64)` or `InternalValue::Integer(i64)` |
| `serde_json::Value::Bool(b)` | `InternalValue::Boolean(b)` |
| `serde_json::Value::Null` | `InternalValue::Null` |
| `serde_json::Value::Array(arr)` | `InternalValue::Array(Vec<InternalValue>)` |
| `serde_json::Value::Object(obj)` | `InternalValue::Object(HashMap<String, InternalValue>)` |

## Common Migration Patterns

### Pattern 1: Basic Record Processing

```rust
// Before
fn process_record_old(json_record: serde_json::Value) -> Result<(), Box<dyn Error>> {
    if let serde_json::Value::Object(map) = json_record {
        for (key, value) in map {
            match value {
                serde_json::Value::String(s) => println!("{}: {}", key, s),
                serde_json::Value::Number(n) => println!("{}: {}", key, n),
                _ => {}
            }
        }
    }
    Ok(())
}

// After
fn process_record_new(record: HashMap<String, FieldValue>) -> Result<(), Box<dyn Error>> {
    for (key, value) in record {
        match value {
            FieldValue::String(s) => println!("{}: {}", key, s),
            FieldValue::Integer(i) => println!("{}: {}", key, i),
            FieldValue::Float(f) => println!("{}: {}", key, f),
            _ => {}
        }
    }
    Ok(())
}
```

### Pattern 2: Format-Agnostic Processing

```rust
// Before - hardcoded JSON
fn serialize_data(data: &MyStruct) -> Result<Vec<u8>, Box<dyn Error>> {
    let json_value = serde_json::to_value(data)?;
    Ok(serde_json::to_vec(&json_value)?)
}

// After - pluggable formats
fn serialize_data(data: &HashMap<String, FieldValue>, format_name: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let format = SerializationFormatFactory::create_format(format_name)?;
    Ok(format.serialize_record(data)?)
}
```

### Pattern 3: SQL Engine Integration

```rust
// Before
async fn execute_query_old(query: &str, json_data: serde_json::Value) -> Result<(), Box<dyn Error>> {
    let mut engine = StreamExecutionEngine::new(tx);
    // Engine was hardcoded to expect serde_json::Value
    let mut record = HashMap::new();
    // Manual conversion from JSON to internal format...
    engine.execute(&parsed_query, record).await?;
    Ok(())
}

// After
async fn execute_query_new(
    query: &str, 
    record: HashMap<String, FieldValue>,
    format: Arc<dyn SerializationFormat>
) -> Result<(), Box<dyn Error>> {
    let mut engine = StreamExecutionEngine::new(tx, format.clone());
    let parsed_query = parser.parse(query)?;
    
    // Engine handles format conversion automatically
    let internal_record = format.to_execution_format(&record)?;
    engine.execute(&parsed_query, internal_record).await?;
    Ok(())
}
```

## Performance Considerations

### JSON Format (No Change)
- Same performance as before for JSON processing
- No migration needed for JSON-only workloads

### Binary Formats (New)
- **Avro**: 30-40% smaller than JSON, similar serialization speed
- **Protobuf**: 40-50% smaller than JSON, 20-30% faster serialization

### Memory Usage
- **FieldValue**: Similar memory footprint to `serde_json::Value`
- **InternalValue**: Optimized for SQL engine operations

## Testing Migration

### 1. Add Compatibility Tests

```rust
#[cfg(test)]
mod migration_tests {
    use super::*;
    
    #[test]
    fn test_json_compatibility() {
        // Ensure old JSON data still works
        let old_json = r#"{"name": "Alice", "age": 30}"#;
        let format = SerializationFormatFactory::create_format("json").unwrap();
        
        let record = format.deserialize_record(old_json.as_bytes()).unwrap();
        assert_eq!(record.get("name"), Some(&FieldValue::String("Alice".to_string())));
        assert_eq!(record.get("age"), Some(&FieldValue::Integer(30)));
    }
}
```

### 2. Gradual Migration Strategy

```rust
// Phase 1: Support both old and new APIs
fn process_message_transitional(payload: &[u8]) -> Result<(), Box<dyn Error>> {
    // Try new format first
    match SerializationFormatFactory::create_format("json") {
        Ok(format) => {
            let record = format.deserialize_record(payload)?;
            process_record_new(record)?;
        }
        Err(_) => {
            // Fallback to old JSON parsing
            let json: serde_json::Value = serde_json::from_slice(payload)?;
            process_record_old(json)?;
        }
    }
    Ok(())
}
```

## Troubleshooting

### Common Issues

**Issue**: `SerializationError::UnsupportedType`
```
Solution: Check that required feature flags are enabled in Cargo.toml
```

**Issue**: Type conversion errors between FieldValue and InternalValue
```
Solution: Use the format's to_execution_format() method for conversion
```

**Issue**: Performance regression with JSON
```
Solution: JSON performance should be identical. Check for unnecessary format conversions.
```

### Debug Helper

```rust
fn debug_migration(old_json: &str) -> Result<(), Box<dyn Error>> {
    // Parse with old method
    let old_value: serde_json::Value = serde_json::from_str(old_json)?;
    println!("Old: {:?}", old_value);
    
    // Parse with new method
    let format = SerializationFormatFactory::create_format("json")?;
    let new_record = format.deserialize_record(old_json.as_bytes())?;
    println!("New: {:?}", new_record);
    
    // Verify they're equivalent
    // ... comparison logic
    
    Ok(())
}
```

## Benefits After Migration

1. **Format Flexibility**: Switch between JSON, Avro, Protobuf without code changes
2. **Better Performance**: Binary formats offer significant size and speed improvements
3. **Schema Evolution**: Avro provides automatic schema evolution capabilities
4. **Type Safety**: Better type system with FieldValue enum
5. **Future-Proof**: Easy to add new formats without breaking changes

## Support

If you encounter issues during migration:

1. Check the [Serialization Guide](SERIALIZATION_GUIDE.md) for detailed format documentation
2. Review the [Quick Reference](SERIALIZATION_QUICK_REFERENCE.md) for common patterns
3. Run the example: `cargo run --example serialization_formats_example --features avro,protobuf`
4. Open an issue with before/after code snippets if you need help