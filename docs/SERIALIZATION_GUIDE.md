# FerrisStreams Serialization Guide

FerrisStreams provides a pluggable serialization system that supports multiple data formats for Kafka message serialization and SQL execution engine data processing.

## Overview

The serialization system is built around the `SerializationFormat` trait, which provides a consistent interface for:
- Serializing records to bytes for Kafka production
- Deserializing bytes from Kafka into records
- Converting between external formats and internal execution format
- Schema evolution and format migration support

## Supported Formats

### JSON (Always Available)
JSON is the default serialization format and is always available without additional features.

**Features:**
- Human-readable format
- Schema-less operation
- Wide ecosystem support
- Good for development and debugging

**Example:**
```rust
use ferrisstreams::ferris::serialization::{SerializationFormatFactory, FieldValue};
use std::collections::HashMap;

let format = SerializationFormatFactory::create_format("json")?;
let mut record = HashMap::new();
record.insert("name".to_string(), FieldValue::String("Alice".to_string()));
record.insert("age".to_string(), FieldValue::Integer(30));

let serialized = format.serialize_record(&record)?;
let deserialized = format.deserialize_record(&serialized)?;
```

### Avro (Feature: `avro`)
Apache Avro provides schema-based serialization with evolution support.

**Features:**
- Compact binary format
- Schema evolution support
- Strong typing with runtime validation
- Efficient for high-throughput scenarios

**Example:**
```rust
#[cfg(feature = "avro")]
use ferrisstreams::ferris::serialization::{SerializationFormatFactory, FieldValue};

let schema_json = r#"
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "long"}
    ]
}
"#;

let format = SerializationFormatFactory::create_avro_format(schema_json)?;
// Use format for serialization...
```

**Schema Evolution:**
```rust
let writer_schema = r#"{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]}"#;
let reader_schema = r#"{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": ["null", "string"], "default": null}]}"#;

let format = SerializationFormatFactory::create_avro_format_with_schemas(
    writer_schema, 
    reader_schema
)?;
```

### Protocol Buffers (Feature: `protobuf`)
Google Protocol Buffers provide efficient binary serialization.

**Features:**
- Very compact binary format
- Fast serialization/deserialization
- Language-agnostic schemas
- Excellent performance characteristics

**Example:**
```rust
#[cfg(feature = "protobuf")]
use ferrisstreams::ferris::serialization::SerializationFormatFactory;

// Generic protobuf format
let format = SerializationFormatFactory::create_format("protobuf")?;

// Typed protobuf format (for specific message types)
let typed_format = SerializationFormatFactory::create_protobuf_format::<YourMessageType>();
```

## Feature Configuration

Add the desired serialization formats to your `Cargo.toml`:

```toml
[dependencies]
ferrisstreams = { version = "0.1.0", features = ["avro", "protobuf"] }

# Or enable specific formats
ferrisstreams = { version = "0.1.0", features = ["avro"] }
ferrisstreams = { version = "0.1.0", features = ["protobuf"] }

# JSON is always available (no feature flag needed)
ferrisstreams = "0.1.0"
```

### Feature Flags
- `json`: Always enabled (built-in)
- `avro`: Enables Apache Avro support (requires `apache-avro` crate)
- `protobuf`: Enables Protocol Buffers support (requires `prost` crate)

## SerializationFormat Trait

The core trait provides these methods:

```rust
pub trait SerializationFormat: Send + Sync {
    /// Serialize a record to bytes for Kafka production
    fn serialize_record(&self, record: &HashMap<String, FieldValue>) -> Result<Vec<u8>, SerializationError>;
    
    /// Deserialize bytes from Kafka into a record
    fn deserialize_record(&self, bytes: &[u8]) -> Result<HashMap<String, FieldValue>, SerializationError>;
    
    /// Convert record to internal execution format
    fn to_execution_format(&self, record: &HashMap<String, FieldValue>) -> Result<HashMap<String, InternalValue>, SerializationError>;
    
    /// Convert from internal execution format back to record
    fn from_execution_format(&self, data: &HashMap<String, InternalValue>) -> Result<HashMap<String, FieldValue>, SerializationError>;
    
    /// Get the format name
    fn format_name(&self) -> &'static str;
}
```

## Data Types

### FieldValue Enum
Represents data in external record format:
- `Integer(i64)`: 64-bit signed integers
- `Float(f64)`: 64-bit floating point numbers
- `String(String)`: UTF-8 strings
- `Boolean(bool)`: Boolean values
- `Null`: Null values
- `Array(Vec<FieldValue>)`: Arrays of values
- `Map(HashMap<String, FieldValue>)`: Key-value maps
- `Struct(HashMap<String, FieldValue>)`: Structured data

### InternalValue Enum
Represents data in SQL execution engine format:
- `Integer(i64)`: 64-bit signed integers
- `Number(f64)`: 64-bit floating point numbers
- `String(String)`: UTF-8 strings
- `Boolean(bool)`: Boolean values
- `Null`: Null values
- `Array(Vec<InternalValue>)`: Arrays of values
- `Object(HashMap<String, InternalValue>)`: Objects/maps

## Factory Pattern

Use `SerializationFormatFactory` to create format instances:

```rust
use ferrisstreams::ferris::serialization::SerializationFormatFactory;

// Get list of supported formats
let formats = SerializationFormatFactory::supported_formats();
println!("Available: {:?}", formats); // ["json", "avro", "protobuf"]

// Create format by name
let json_format = SerializationFormatFactory::create_format("json")?;
let avro_format = SerializationFormatFactory::create_format("avro")?;

// Get default format (JSON)
let default_format = SerializationFormatFactory::default_format();

// Create custom Avro format
let custom_avro = SerializationFormatFactory::create_avro_format(schema_json)?;
```

## Performance Characteristics

| Format | Serialization Speed | Size Efficiency | Schema Evolution | Human Readable |
|--------|-------------------|-----------------|------------------|----------------|
| JSON | Fast | Medium | Manual | Yes |
| Avro | Fast | High | Automatic | No |
| Protobuf | Very Fast | Very High | Manual/Versioned | No |

### Size Comparison Example
For a typical record with 8 fields:
- JSON: ~250 bytes
- Avro: ~150 bytes  
- Protobuf: ~120 bytes

## Integration with Kafka

```rust
use ferrisstreams::{KafkaProducer, KafkaConsumer, JsonSerializer};
use ferrisstreams::ferris::serialization::SerializationFormatFactory;

// Producer with custom serialization
let format = SerializationFormatFactory::create_format("avro")?;
// Use format.serialize_record() before sending to Kafka

// Consumer with custom deserialization  
let consumer = KafkaConsumer::<String, Vec<u8>, _, _>::new(
    "localhost:9092",
    "my-group",
    JsonSerializer,
    JsonSerializer, // Raw bytes serializer
)?;

// Deserialize received messages
let message = consumer.poll(timeout).await?;
let deserialized = format.deserialize_record(message.value())?;
```

## SQL Execution Engine Integration

The serialization system integrates seamlessly with the SQL execution engine:

```rust
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::serialization::JsonFormat;
use std::sync::Arc;

let serialization_format = Arc::new(JsonFormat);
let mut engine = StreamExecutionEngine::new(output_sender, serialization_format);

// Engine automatically handles format conversions
```

## Error Handling

```rust
use ferrisstreams::ferris::serialization::SerializationError;

match format.serialize_record(&record) {
    Ok(bytes) => println!("Serialized {} bytes", bytes.len()),
    Err(SerializationError::SerializationFailed(msg)) => {
        eprintln!("Serialization failed: {}", msg);
    }
    Err(SerializationError::UnsupportedType(msg)) => {
        eprintln!("Unsupported type: {}", msg);
    }
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Best Practices

### Choosing a Format
1. **JSON**: Use for development, debugging, and human-readable data
2. **Avro**: Use for production with schema evolution requirements
3. **Protobuf**: Use for high-performance, low-latency applications

### Schema Management
1. Version your Avro schemas in a schema registry
2. Use schema evolution features for backward compatibility
3. Test schema changes thoroughly before deployment

### Performance Optimization
1. Reuse format instances (they're thread-safe)
2. Pre-compile schemas when possible
3. Consider compression at the Kafka level for JSON
4. Use appropriate Kafka batch settings for your format

### Development Workflow
```rust
// Development: Use JSON for easy debugging
#[cfg(debug_assertions)]
let format = SerializationFormatFactory::create_format("json")?;

// Production: Use binary format
#[cfg(not(debug_assertions))]
let format = SerializationFormatFactory::create_format("avro")?;
```

## Examples

See the complete example:
```bash
# Run with different feature combinations
cargo run --example serialization_formats_example --features json
cargo run --example serialization_formats_example --features avro
cargo run --example serialization_formats_example --features protobuf
cargo run --example serialization_formats_example --features avro,protobuf
```

## Testing

The serialization system includes comprehensive tests:
```bash
# Test all formats (requires features)
cargo test serialization --features avro,protobuf

# Test specific format
cargo test json_serialization_tests
cargo test avro_serialization_tests --features avro  
cargo test protobuf_serialization_tests --features protobuf
```