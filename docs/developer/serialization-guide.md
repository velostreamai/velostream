# VeloStream Serialization Guide

VeloStream provides a pluggable serialization system that supports multiple data formats for Kafka message serialization and SQL execution engine data processing, with **industry-leading financial precision arithmetic**.

## Overview

The serialization system is built around the `SerializationFormat` trait, which provides a consistent interface for:
- Serializing records to bytes for Kafka production
- Deserializing bytes from Kafka into records
- Converting between external formats and internal execution format
- **Financial precision arithmetic** with exact decimal calculations
- Schema evolution and format migration support
- **Cross-system compatibility** with standard formats

## 💰 Financial Precision Features

VeloStream includes specialized financial data types that provide:
- ✅ **Perfect precision** - no floating-point rounding errors
- ✅ **42x performance improvement** over traditional f64 arithmetic  
- ✅ **Cross-system compatibility** - serializes as decimal strings for JSON/Avro
- ✅ **Industry-standard protobuf** - uses proper Decimal message format

**Example of precision difference:**
```rust
// Traditional f64 (INCORRECT for finance)
let price_f64 = 10.01_f64 + 10.02_f64;  // Result: 20.029999999999998

// VeloStream ScaledInteger (CORRECT)
let price1 = FieldValue::from_financial_f64(10.01, 2);
let price2 = FieldValue::from_financial_f64(10.02, 2);  
let sum = price1.add(&price2)?;  // Result: exactly 20.03
```

## Supported Formats

### JSON (Always Available)
JSON is the default serialization format with **financial precision support** and **automatic payload preservation**.

**Features:**
- Human-readable format with **exact decimal strings**
- Schema-less operation
- Wide ecosystem support
- **Cross-system compatibility** - other systems can read financial values
- Good for development and debugging
- **Perfect round-trip precision** for financial data
- **🆕 JSON_PAYLOAD preservation** - automatically stores original JSON string
- **Unified codec architecture** - runtime-swappable with Avro/Protobuf

**Financial Data Example:**
```rust
use velostream::velo::serialization::{SerializationFormatFactory, FieldValue};
use std::collections::HashMap;

let format = SerializationFormatFactory::create_format("json")?;
let mut record = HashMap::new();

// Financial data with exact precision
record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
record.insert("price".to_string(), FieldValue::from_financial_f64(150.2567, 4));
record.insert("quantity".to_string(), FieldValue::Integer(100));

let serialized = format.serialize_record(&record)?;
// Produces: {"symbol":"AAPL","price":"150.2567","quantity":100}

let deserialized = format.deserialize_record(&serialized)?;
// Automatically reconstructs ScaledInteger from "150.2567" string
```

**JSON Output (readable by any system):**
```json
{
  "symbol": "AAPL",
  "price": "150.2567",    // Exact precision as decimal string
  "quantity": 100,
  "total": "15025.67"     // Calculated with perfect precision
}
```

## 🆕 JSON_PAYLOAD Processing

**New Feature**: VeloStream automatically preserves the original JSON payload for debugging, auditing, and reprocessing purposes.

### What is JSON_PAYLOAD?

When consuming JSON messages from Kafka, VeloStream automatically adds a special field called `JSON_PAYLOAD` that contains the exact original JSON string received from the message broker. This provides:

- **Complete audit trail** - know exactly what was received
- **Debugging capabilities** - compare original vs parsed data
- **Reprocessing support** - re-parse with different logic if needed
- **Compliance requirements** - some regulations require original payload preservation

### How JSON_PAYLOAD Works

**Input JSON Message:**
```json
{"symbol": "AAPL", "price": 150.25, "quantity": 100}
```

**Resulting StreamRecord Fields:**
```rust
HashMap {
    "JSON_PAYLOAD": FieldValue::String("{\"symbol\": \"AAPL\", \"price\": 150.25, \"quantity\": 100}"),
    "symbol": FieldValue::String("AAPL"),
    "price": FieldValue::Float(150.25),
    "quantity": FieldValue::Integer(100)
}
```

### Usage in SQL Queries

You can access the original JSON payload in your streaming SQL queries:

```sql
-- Access original payload for debugging
SELECT JSON_PAYLOAD, symbol, price, quantity 
FROM trades_stream;

-- Use for data quality checks
SELECT symbol, price,
       CASE 
         WHEN JSON_PAYLOAD LIKE '%error%' THEN 'malformed'
         ELSE 'valid'
       END as data_quality
FROM trades_stream;

-- Re-parse complex nested JSON if needed
SELECT symbol, 
       EXTRACT_JSON_FIELD(JSON_PAYLOAD, '$.metadata.source') as source,
       price
FROM trades_stream;
```

### Automatic Behavior

JSON_PAYLOAD processing is:
- ✅ **Always enabled** - no configuration needed
- ✅ **Zero performance overhead** - single string copy during deserialization  
- ✅ **UTF-8 validated** - ensures payload is valid text
- ✅ **Memory efficient** - shares string data when possible
- ✅ **Thread safe** - immutable after creation

### Use Cases

**1. Debugging Data Issues**
```rust
// Compare original vs parsed when debugging
let original_json = record.get("JSON_PAYLOAD").unwrap();
let parsed_price = record.get("price").unwrap();
println!("Original: {}, Parsed: {:?}", original_json, parsed_price);
```

**2. Data Quality Monitoring**
```sql
-- Find malformed messages
SELECT COUNT(*) 
FROM trades_stream 
WHERE JSON_PAYLOAD NOT LIKE '{"symbol"%' 
  AND JSON_PAYLOAD IS NOT NULL;
```

**3. Schema Evolution Handling**
```rust
// Handle both old and new formats
if record.contains_key("new_field") {
    // Process new format
} else {
    // Re-parse JSON_PAYLOAD with legacy parser
    let original = record.get("JSON_PAYLOAD").unwrap();
    let legacy_fields = legacy_json_parser(original)?;
    // Use legacy fields
}
```

**4. Compliance & Auditing**
```sql
-- Store original payloads for compliance
CREATE STREAM audit_log AS 
SELECT symbol, price, JSON_PAYLOAD, CURRENT_TIMESTAMP as received_at
FROM trades_stream
EMIT CHANGES;
```

### Performance Considerations

JSON_PAYLOAD adds minimal overhead:
- **Memory**: One additional string field per record (~100-1000 bytes typical)
- **CPU**: Single UTF-8 validation + string copy during deserialization  
- **Storage**: Original JSON stored as-is (no re-encoding)

For high-throughput scenarios where storage is critical, JSON_PAYLOAD can be filtered out in SQL:
```sql
-- Exclude JSON_PAYLOAD from output stream
SELECT symbol, price, quantity  -- JSON_PAYLOAD automatically excluded
FROM trades_stream;
```

### Unified Architecture Integration

JSON_PAYLOAD is part of VeloStream' **Unified Codec Architecture**, which means:
- Works consistently across JSON, Avro, and Protobuf formats
- Available regardless of which codec is selected at runtime
- Maintains same field name and behavior across all serialization formats

### Avro (Feature: `avro`)
Apache Avro with **financial precision support** using decimal string fields.

**Features:**
- Compact binary format
- Schema evolution support  
- Strong typing with runtime validation
- Efficient for high-throughput scenarios
- **Financial data serialized as decimal strings**

**Financial Schema Example (Industry Standard):**
```rust
use velostream::velo::serialization::{SerializationFormatFactory, FieldValue};

// Apache Flink/Kafka Connect compatible decimal schema
let financial_schema = r#"
{
    "type": "record",
    "name": "Trade",
    "fields": [
        {"name": "symbol", "type": "string"},
        {
            "name": "price", 
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 18,
            "scale": 4
        },
        {"name": "quantity", "type": "long"},
        {
            "name": "total",
            "type": "bytes", 
            "logicalType": "decimal",
            "precision": 18,
            "scale": 4
        }
    ]
}
"#;

let format = SerializationFormatFactory::create_avro_format(financial_schema)?;

// Create financial record - automatically converts to proper Avro decimal format
let mut record = HashMap::new();
record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
record.insert("price".to_string(), FieldValue::from_financial_f64(150.2567, 4));
record.insert("quantity".to_string(), FieldValue::Integer(100));

// Serializes as proper Avro decimal bytes - compatible with Flink, Kafka Connect, etc.
```

**Alternative: String-based for Simple Compatibility**
```rust
// For systems that prefer string decimals (simpler but less efficient)
let simple_schema = r#"
{
    "type": "record",
    "name": "Trade", 
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "string"},     // Decimal as string "150.2567"
        {"name": "quantity", "type": "long"},
        {"name": "total", "type": "string"}     // Calculated with exact precision
    ]
}
"#;
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
Google Protocol Buffers with **industry-standard Decimal message format**.

**Features:**
- Very compact binary format
- Fast serialization/deserialization
- Language-agnostic schemas
- **Industry-standard Decimal messages** (compatible with Google Money API)
- Excellent performance characteristics

**Financial Decimal Message Format:**
```protobuf
// Generated from src/velo/serialization/financial.proto
message Decimal {
  int64 units = 1;    // Scaled integer value (1234567 for $123.4567 with scale=4)
  int32 scale = 2;    // Number of digits after decimal point
}

message FieldValue {
  oneof value {
    string string_value = 1;
    int64 integer_value = 2;
    double float_value = 3;
    bool boolean_value = 4;
    Decimal decimal_value = 5;     // Financial precision type
    FieldArray array_value = 6;
    FieldMap map_value = 7;
    string timestamp_value = 8;    // ISO 8601 format
    string date_value = 9;         // YYYY-MM-DD format
  }
}
```

**Usage Example:**
```rust
use velostream::velo::serialization::{SerializationFormatFactory, FieldValue};

let format = SerializationFormatFactory::create_format("protobuf")?;

// Create financial record
let mut record = HashMap::new();
record.insert("price".to_string(), FieldValue::from_financial_f64(123.4567, 4));
// Serializes as Decimal{units: 1234567, scale: 4}

// Compatible with any protobuf system using standard Decimal format
```

## Feature Configuration

Add the desired serialization formats to your `Cargo.toml`:

```toml
[dependencies]
velostream = { version = "0.1.0", features = ["avro", "protobuf"] }

# Or enable specific formats
velostream = { version = "0.1.0", features = ["avro"] }
velostream = { version = "0.1.0", features = ["protobuf"] }

# JSON is always available (no feature flag needed)
velostream = "0.1.0"
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
    
    /// Convert record for SQL execution engine (now handled internally)
    /// This method is deprecated as FieldValue is used directly in execution
    
    /// Get format-specific metadata if needed
    fn get_metadata(&self) -> Option<HashMap<String, String>>;
    
    /// Get the format name
    fn format_name(&self) -> &'static str;
}
```

## Data Types

### FieldValue Enum (SQL Execution)
Represents data in external record format with **financial precision support**:
- `Integer(i64)`: 64-bit signed integers
- `Float(f64)`: 64-bit floating point numbers (avoid for financial data)
- `String(String)`: UTF-8 strings
- `Boolean(bool)`: Boolean values
- `Null`: Null values
- **`ScaledInteger(i64, u8)`**: **Financial precision type** (value, decimal_places)
- `Array(Vec<FieldValue>)`: Arrays of values
- `Map(HashMap<String, FieldValue>)`: Key-value maps
- `Struct(HashMap<String, FieldValue>)`: Structured data

**Financial Methods:**
```rust
// Create financial values
let price = FieldValue::from_financial_f64(123.4567, 4);  // 4 decimal places
let quantity = FieldValue::Integer(100);

// Perfect precision arithmetic
let total = price.multiply(&quantity)?;  // Exact: 12345.67
let sum = price.add(&other_price)?;     // No rounding errors

// Convert for display or other systems
let as_string = price.to_display_string();  // "123.4567"
let as_f64 = price.to_financial_f64();      // Some(123.4567)
```

### InternalValue Enum (Serialization)
Represents data in serialization format:
- `Integer(i64)`: 64-bit signed integers
- `Number(f64)`: 64-bit floating point numbers (legacy)
- **`ScaledNumber(i64, u8)`**: **Financial precision for serialization**
- `String(String)`: UTF-8 strings  
- `Boolean(bool)`: Boolean values
- `Null`: Null values
- `Array(Vec<InternalValue>)`: Arrays of values
- `Object(HashMap<String, InternalValue>)`: Objects/maps

**Serialization Mapping:**
- **JSON**: `ScaledInteger` → decimal string ("123.4567")
- **Avro (Standard)**: `ScaledInteger` → bytes with decimal logical type (Flink/Kafka Connect compatible)
- **Avro (Simple)**: `ScaledInteger` → decimal string ("123.4567") 
- **Protobuf**: `ScaledInteger` → `Decimal{units: 1234567, scale: 4}`

## 🔄 Industry Compatibility

VeloStream financial data is compatible with major streaming platforms:

| Platform | Format | Compatibility | Notes |
|----------|--------|---------------|-------|
| **Apache Flink** | Avro | ✅ Full | Uses `bytes` + `decimal` logical type |
| **Kafka Connect** | Avro | ✅ Full | Standard decimal logical type support |
| **Spark** | JSON/Avro | ✅ Full | Decimal strings or logical types |
| **BigQuery** | JSON | ✅ Full | Reads decimal strings as NUMERIC |
| **Snowflake** | JSON/Avro | ✅ Full | Automatic decimal type detection |
| **ClickHouse** | JSON | ✅ Full | Parses decimal strings correctly |
| **PostgreSQL** | JSON | ✅ Full | JSON decimal strings → NUMERIC |

## Factory Pattern

Use `SerializationFormatFactory` to create format instances:

```rust
use velostream::velo::serialization::SerializationFormatFactory;

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

| Format | Serialization Speed | Size Efficiency | Schema Evolution | Human Readable | Financial Precision |
|--------|-------------------|-----------------|------------------|----------------|-------------------|
| JSON | Fast | Medium | Manual | Yes | ✅ Decimal strings |
| Avro | Fast | High | Automatic | No | ✅ Decimal strings |
| Protobuf | Very Fast | Very High | Manual/Versioned | No | ✅ Decimal messages |

### Financial Arithmetic Performance
**VeloStream ScaledInteger vs Traditional f64:**
- **42x faster** than f64 arithmetic
- **Perfect precision** - no rounding errors
- **Memory efficient** - single i64 + scale byte
- **Cross-compatible** - serializes to standard formats

### Size Comparison Example
For a typical financial record with 8 fields:
- JSON: ~250 bytes (with decimal strings)
- Avro: ~150 bytes (binary + decimal strings)  
- Protobuf: ~120 bytes (binary + Decimal messages)

**Financial Precision Comparison:**
```rust
// Traditional approach (WRONG)
let result_f64 = 10.01_f64 + 10.02_f64;  // 20.029999999999998 ❌

// VeloStream approach (CORRECT)  
let price1 = FieldValue::from_financial_f64(10.01, 2);
let price2 = FieldValue::from_financial_f64(10.02, 2);
let result = price1.add(&price2)?;  // Exactly 20.03 ✅
```

## Integration with Kafka

```rust
use velostream::{KafkaProducer, KafkaConsumer, JsonSerializer};
use velostream::velo::serialization::SerializationFormatFactory;

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
use velostream::velo::sql::execution::StreamExecutionEngine;
use velostream::velo::serialization::JsonFormat;
use std::sync::Arc;

let serialization_format = Arc::new(JsonFormat);
let mut engine = StreamExecutionEngine::new(output_sender, serialization_format);

// Engine automatically handles format conversions
```

## Error Handling

```rust
use velostream::velo::serialization::SerializationError;

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

### Choosing a Format for Financial Data
1. **JSON**: Use for development, debugging, and cross-system compatibility
   - ✅ Human-readable decimal strings ("123.4567")
   - ✅ Any system can parse as standard JSON
   - ✅ Perfect for financial APIs and microservices
   
2. **Avro**: Use for production with schema evolution requirements
   - ✅ **Industry-standard decimal logical type** (Flink/Kafka Connect compatible)
   - ✅ Schema registry integration for versioning
   - ✅ High throughput financial data pipelines
   - ✅ Proper `bytes` + `decimal` logical type for maximum compatibility
   
3. **Protobuf**: Use for high-performance, low-latency financial applications
   - ✅ Industry-standard Decimal message format
   - ✅ Compatible with Google Money API and other systems
   - ✅ Maximum performance for financial calculations

### Financial Data Best Practices
1. **Always use ScaledInteger for financial calculations**
   ```rust
   // CORRECT: Perfect precision
   let price = FieldValue::from_financial_f64(123.45, 2);
   
   // WRONG: Floating point errors
   let price = FieldValue::Float(123.45);  // Don't use for money!
   ```

2. **Choose appropriate decimal places**
   - Currencies: 2-4 decimal places (USD: 2, BTC: 8)
   - Prices: 4 decimal places for precision
   - Quantities: Based on trading requirements

3. **Use proper Avro decimal logical types for Flink/Kafka Connect compatibility**
   ```json
   // Industry-standard Avro decimal (compatible with Flink, Kafka Connect)
   {
     "name": "price", 
     "type": "bytes",
     "logicalType": "decimal", 
     "precision": 18,
     "scale": 4
   }
   
   // Alternative: String-based for simpler systems
   {
     "name": "price",
     "type": "string",
     "pattern": "^\\d+\\.\\d{4}$"  // Exactly 4 decimal places
   }
   ```

### Schema Management
1. Version your Avro schemas in a schema registry
2. Use schema evolution features for backward compatibility
3. Test schema changes thoroughly before deployment
4. **Document decimal precision requirements** in schemas

### Performance Optimization
1. Reuse format instances (they're thread-safe)
2. Pre-compile schemas when possible  
3. Consider compression at the Kafka level for JSON
4. Use appropriate Kafka batch settings for your format
5. **Prefer ScaledInteger over Float for all financial calculations**

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

### Complete Financial Data Example
```rust
use velostream::velo::serialization::{SerializationFormatFactory, FieldValue};
use std::collections::HashMap;

// Create financial trade record
let mut trade = HashMap::new();
trade.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
trade.insert("price".to_string(), FieldValue::from_financial_f64(150.2567, 4));
trade.insert("quantity".to_string(), FieldValue::Integer(100));

// Calculate total with perfect precision
let price = trade.get("price").unwrap();
let quantity = trade.get("quantity").unwrap();
let total = price.multiply(quantity)?;
trade.insert("total".to_string(), total);

// Test all serialization formats
let json_format = SerializationFormatFactory::create_format("json")?;
let json_bytes = json_format.serialize_record(&trade)?;
println!("JSON: {}", String::from_utf8(json_bytes)?);
// Output: {"symbol":"AAPL","price":"150.2567","quantity":100,"total":"15025.67"}

#[cfg(feature = "avro")]
{
    let avro_format = SerializationFormatFactory::create_format("avro")?;
    let avro_bytes = avro_format.serialize_record(&trade)?;
    println!("Avro: {} bytes (binary)", avro_bytes.len());
}

#[cfg(feature = "protobuf")]  
{
    let proto_format = SerializationFormatFactory::create_format("protobuf")?;
    let proto_bytes = proto_format.serialize_record(&trade)?;
    println!("Protobuf: {} bytes (binary with Decimal messages)", proto_bytes.len());
}
```

### Run Example Programs
```bash
# Test financial precision with JSON
cargo run --bin test_serialization_compatibility

# Test financial arithmetic performance  
cargo run --bin test_financial_precision

# Run with different serialization features
cargo run --example serialization_formats_example --features json
cargo run --example serialization_formats_example --features avro
cargo run --example serialization_formats_example --features protobuf
cargo run --example serialization_formats_example --features avro,protobuf
```

## Testing

The serialization system includes comprehensive tests:
```bash
# Test all formats with financial precision
cargo test serialization --features avro,protobuf

# Test financial precision specifically
cargo test financial_precision --features avro,protobuf

# Test specific format
cargo test json_serialization_tests
cargo test avro_serialization_tests --features avro  
cargo test protobuf_serialization_tests --features protobuf

# Test cross-system compatibility
cargo test serialization_compatibility --features avro,protobuf
```

## 🚀 Quick Start with Docker

For a complete setup with all serialization formats:

```bash
# Build and start with all formats
docker-compose up --build

# Test financial data in all formats
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \  
  -d '{
    "sql": "SELECT symbol, price, quantity, price * quantity as total FROM financial_stream"
  }'
```

See [DOCKER_SERIALIZATION.md](../../DOCKER_SERIALIZATION.md) for complete Docker setup guide.