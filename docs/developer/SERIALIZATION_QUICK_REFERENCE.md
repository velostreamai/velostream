# Serialization Quick Reference

## 🚀 Quick Start

### Enable Features
```toml
# Cargo.toml
[dependencies]
ferrisstreams = { version = "0.1.0", features = ["avro", "protobuf"] }
```

### Basic Usage
```rust
use ferrisstreams::ferris::serialization::SerializationFormatFactory;

// Create any format
let format = SerializationFormatFactory::create_format("json")?;
let bytes = format.serialize_record(&record)?;
let restored = format.deserialize_record(&bytes)?;
```

## 📋 Supported Formats

| Format | Feature Flag | Size | Schema | Evolution | Use Case |
|--------|-------------|------|---------|-----------|----------|
| JSON | Always on | Medium | No | Manual | Development, APIs |
| Avro | `avro` | Small | Yes | Automatic | Production, Analytics |
| Protobuf | `protobuf` | Smallest | Yes | Manual | High Performance |

## 🔧 Factory Methods

### Generic Creation
```rust
let json = SerializationFormatFactory::create_format("json")?;
let avro = SerializationFormatFactory::create_format("avro")?;
let proto = SerializationFormatFactory::create_format("protobuf")?;
```

### Avro with Custom Schema
```rust
let schema = r#"{"type": "record", "name": "User", "fields": [...]}"#;
let avro = SerializationFormatFactory::create_avro_format(schema)?;
```

### Schema Evolution
```rust
let avro = SerializationFormatFactory::create_avro_format_with_schemas(
    writer_schema, reader_schema
)?;
```

### Typed Protobuf
```rust
let proto = SerializationFormatFactory::create_protobuf_format::<MyMessage>();
```

## 📊 Data Types

### FieldValue (SQL Engine Format)
```rust
FieldValue::String("text")
FieldValue::Integer(123)
FieldValue::Float(3.14)
FieldValue::ScaledInteger(12345, 2)  // For financial precision (123.45)
FieldValue::Boolean(true)
FieldValue::Null
FieldValue::Array(vec![...])
FieldValue::Map(HashMap::new())
FieldValue::Struct(HashMap::new())
```

### Type System Architecture
- **FieldValue**: Primary type system for SQL execution and serialization
- **Direct Integration**: FieldValue used throughout the engine for consistency
- **Financial Precision**: ScaledInteger provides exact arithmetic for financial data
- **Cross-Format**: Same types work with JSON, Avro, and Protobuf serialization

## 🔄 Core Operations

### Serialize to Bytes
```rust
let bytes: Vec<u8> = format.serialize_record(&record)?;
```

### Deserialize from Bytes
```rust
let record: HashMap<String, FieldValue> = format.deserialize_record(&bytes)?;
```

### Get Format Metadata
```rust
let metadata: Option<HashMap<String, String>> = format.get_metadata();
```

### Direct SQL Engine Usage
```rust
// FieldValue is used directly in SQL execution - no conversion needed
engine.execute(&query, record).await?;
```

## 📈 Performance Tips

### Size Optimization
- **JSON**: Use for development only
- **Avro**: Good balance of features and size
- **Protobuf**: Smallest size, fastest processing

### Schema Management
- Version Avro schemas for evolution
- Use schema registry in production
- Test schema changes before deployment

### Caching
```rust
// Reuse format instances (thread-safe)
static FORMAT: Lazy<Box<dyn SerializationFormat>> = 
    Lazy::new(|| SerializationFormatFactory::create_format("avro").unwrap());
```

## 🚨 Error Handling

```rust
match format.serialize_record(&record) {
    Ok(bytes) => { /* success */ }
    Err(SerializationError::SerializationFailed(msg)) => { /* handle */ }
    Err(SerializationError::UnsupportedType(msg)) => { /* handle */ }
    Err(e) => { /* other errors */ }
}
```

## 🔍 Discovery

### List Supported Formats
```rust
let formats = SerializationFormatFactory::supported_formats();
// Returns: ["json", "avro", "protobuf", "proto"]
```

### Get Default Format
```rust
let default = SerializationFormatFactory::default_format(); // JSON
```

### Check Format Name
```rust
println!("Using: {}", format.format_name());
```

## 🎯 Common Patterns

### Kafka Integration
```rust
// Producer side
let bytes = format.serialize_record(&record)?;
producer.send(key, &bytes, headers, None).await?;

// Consumer side  
let message = consumer.poll(timeout).await?;
let record = format.deserialize_record(message.value())?;
```

### SQL Engine Integration
```rust
// Direct integration with FieldValue
engine.execute(&query, record).await?;
```

### Format Selection
```rust
#[cfg(debug_assertions)]
let format = SerializationFormatFactory::create_format("json")?;

#[cfg(not(debug_assertions))]
let format = SerializationFormatFactory::create_format("avro")?;
```

## 🧪 Testing

```rust
// Test round trip
let serialized = format.serialize_record(&original)?;
let deserialized = format.deserialize_record(&serialized)?;
assert_eq!(original, deserialized);

// Test format metadata if available
if let Some(metadata) = format.get_metadata() {
    println!("Format metadata: {:?}", metadata);
}
```

## 📝 Example Schemas

### Avro Schema
```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

### Evolution Example
```json
{
  "type": "record", 
  "name": "User",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null},
    {"name": "created_at", "type": "long", "default": 0}
  ]
}
```

## ⚡ Benchmarks

Typical 8-field record sizes:
- JSON: ~173 bytes
- Avro: ~150 bytes  
- Protobuf: ~120 bytes

Performance (ops/sec):
- JSON: ~50K serialize, ~45K deserialize
- Avro: ~60K serialize, ~55K deserialize
- Protobuf: ~80K serialize, ~75K deserialize