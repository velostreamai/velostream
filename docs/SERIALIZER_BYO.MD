**How to Use Your Own Serializer with FerrisStreams SQL Engine**

**Overview**

The FerrisStreams SQL engine is designed to work with any serialization format through the SerializationFormat trait. Here's how to integrate your own custom serializer:

1. Implement the SerializationFormat Trait

```rust
use ferrisstreams::ferris::serialization::{SerializationFormat, SerializationError, InternalValue};
use ferrisstreams::ferris::sql::FieldValue;
use std::collections::HashMap;

pub struct MyCustomSerializer {
// Your serializer configuration
config: MyCustomConfig,
}

impl SerializationFormat for MyCustomSerializer {
fn serialize_record(
&self,
record: &HashMap<String, FieldValue>,
) -> Result<Vec<u8>, SerializationError> {
// Convert FieldValue types to your custom format
// Example implementation:
match self.custom_serialize(record) {
Ok(bytes) => Ok(bytes),
Err(e) => Err(SerializationError::SerializationFailed(
format!("Custom serialization failed: {}", e)
)),
}
}

      fn deserialize_record(
          &self,
          bytes: &[u8],
      ) -> Result<HashMap<String, FieldValue>, SerializationError> {
          // Convert your custom format back to FieldValue types
          match self.custom_deserialize(bytes) {
              Ok(record) => Ok(record),
              Err(e) => Err(SerializationError::DeserializationFailed(
                  format!("Custom deserialization failed: {}", e)
              )),
          }
      }

      fn to_execution_format(
          &self,
          record: &HashMap<String, FieldValue>,
      ) -> Result<HashMap<String, InternalValue>, SerializationError> {
          // Convert FieldValue to InternalValue for SQL engine
          // You can use the built-in helper or implement your own:
          field_value_to_internal_value_map(record)
      }

      fn from_execution_format(
          &self,
          data: &HashMap<String, InternalValue>,
      ) -> Result<HashMap<String, FieldValue>, SerializationError> {
          // Convert InternalValue back to FieldValue
          // You can use the built-in helper or implement your own:
          internal_value_to_field_value_map(data)
      }

      fn format_name(&self) -> &'static str {
          "MyCustomFormat"
      }
}
```
2. Type Mapping: FieldValue ↔ Your Format

You need to handle conversion between FerrisStreams' FieldValue enum and your format:

```rust
// FieldValue types you need to support:
pub enum FieldValue {
  Integer(i64),
  Float(f64),
  String(String),
  Boolean(bool),
  Null,
  Array(Vec<FieldValue>),
  Map(HashMap<String, FieldValue>),
  Struct(HashMap<String, FieldValue>),
  Date(NaiveDate),           // Optional
  Timestamp(NaiveDateTime),  // Optional  
  Decimal(BigDecimal),       // Optional
}

impl MyCustomSerializer {
fn custom_serialize(&self, record: &HashMap<String, FieldValue>) -> Result<Vec<u8>, MyError> {
// Your custom serialization logic
for (key, value) in record {
match value {
FieldValue::Integer(i) => {
// Handle integer serialization
}
FieldValue::String(s) => {
// Handle string serialization
}
FieldValue::Array(arr) => {
// Handle array serialization (recursive)
}
// ... handle other types
}
}

          // Return serialized bytes
          Ok(your_serialized_bytes)
      }
}
```

3. SQL Engine Integration

Basic Integration:

```
use ferrisstreams::ferris::sql::StreamExecutionEngine;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
// Create your custom serializer
let custom_serializer = Arc::new(MyCustomSerializer::new(config));

      // Create output channel for query results
      let (output_tx, mut output_rx) = mpsc::unbounded_channel();

      // Create SQL engine with your serializer
      let mut engine = StreamExecutionEngine::new(output_tx, custom_serializer);

      // Use the engine normally
      let parser = StreamingSqlParser::new();
      let query = parser.parse("SELECT customer_id, amount FROM orders WHERE amount > 100")?;

      // Process records (engine automatically handles format conversions)
      let mut record = HashMap::new();
      record.insert("customer_id".to_string(), InternalValue::String("123".to_string()));
      record.insert("amount".to_string(), InternalValue::Number(150.0));

      engine.execute(&query, record).await?;

      // Receive results
      while let Some(result) = output_rx.recv().await {
          println!("Query result: {:?}", result);
      }

      Ok(())
}
```
Advanced Integration with Kafka:

```rust


use ferrisstreams::ferris::multi_job_server::MultiJobSqlServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
let custom_serializer = Arc::new(MyCustomSerializer::new(config));

      // Create multi-job SQL server with custom serializer
      let server = MultiJobSqlServer::new("localhost:9092".to_string());

      // Run SQL job with custom serialization
      server.run_job(
          "my-job",
          "input-topic",
          "SELECT * FROM stream",
          custom_serializer,  // Your serializer
      ).await?;

      Ok(())
}
```

4. Helper Functions for Format Conversion

You can use built-in helpers for FieldValue ↔ InternalValue conversion:

```rust
// Helper function for FieldValue -> InternalValue conversion
fn field_value_to_internal_value_map(
record: &HashMap<String, FieldValue>,
) -> Result<HashMap<String, InternalValue>, SerializationError> {
let mut result = HashMap::new();

      for (key, field_value) in record {
          let internal_value = match field_value {
              FieldValue::Integer(i) => InternalValue::Integer(*i),
              FieldValue::Float(f) => InternalValue::Number(*f),
              FieldValue::String(s) => InternalValue::String(s.clone()),
              FieldValue::Boolean(b) => InternalValue::Boolean(*b),
              FieldValue::Null => InternalValue::Null,
              FieldValue::Array(arr) => {
                  let internal_arr: Result<Vec<_>, _> = arr
                      .iter()
                      .map(field_value_to_internal_value)
                      .collect();
                  InternalValue::Array(internal_arr?)
              }
              FieldValue::Map(map) | FieldValue::Struct(map) => {
                  let internal_map = field_value_to_internal_value_map(&map)?;
                  InternalValue::Object(internal_map)
              }
              // Handle optional types
              FieldValue::Date(d) => InternalValue::String(d.format("%Y-%m-%d").to_string()),
              FieldValue::Timestamp(ts) => InternalValue::String(ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
              FieldValue::Decimal(dec) => InternalValue::String(dec.to_string()),
          };

          result.insert(key.clone(), internal_value);
      }

      Ok(result)
}
```

5. Factory Pattern Integration (Optional)

You can extend the factory to support your format:

```rust
use ferrisstreams::ferris::serialization::SerializationFormatFactory;

impl SerializationFormatFactory {
pub fn create_my_custom_format(config: MyCustomConfig) -> Result<Box<dyn SerializationFormat>, SerializationError> {
Ok(Box::new(MyCustomSerializer::new(config)))
}
}

// Usage:
let format = SerializationFormatFactory::create_my_custom_format(config)?;
```
6. Real-World Example: MessagePack Serializer

```rust
use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};

pub struct MessagePackFormat;

impl SerializationFormat for MessagePackFormat {
fn serialize_record(
&self,
record: &HashMap<String, FieldValue>,
) -> Result<Vec<u8>, SerializationError> {
// Convert to serde-compatible structure
let serde_map = field_value_map_to_serde_value(record)?;

          // Serialize with MessagePack
          rmp_serde::to_vec(&serde_map)
              .map_err(|e| SerializationError::SerializationFailed(e.to_string()))
      }

      fn deserialize_record(
          &self,
          bytes: &[u8],
      ) -> Result<HashMap<String, FieldValue>, SerializationError> {
          // Deserialize from MessagePack
          let serde_value: serde_json::Value = rmp_serde::from_slice(bytes)
              .map_err(|e| SerializationError::DeserializationFailed(e.to_string()))?;

          // Convert back to FieldValue map
          serde_value_to_field_value_map(&serde_value)
      }

      fn to_execution_format(&self, record: &HashMap<String, FieldValue>) -> Result<HashMap<String, InternalValue>, SerializationError> {
          field_value_to_internal_value_map(record)
      }

      fn from_execution_format(&self, data: &HashMap<String, InternalValue>) -> Result<HashMap<String, FieldValue>, SerializationError> {
          internal_value_to_field_value_map(data)
      }

      fn format_name(&self) -> &'static str {
          "MessagePack"
      }
}
```
7. Testing Your Custom Serializer

Use the same test patterns as the built-in formats:

```rust
#[cfg(test)]
mod tests {
use super::*;
use ferrisstreams::ferris::serialization::test_helpers::*;

      #[tokio::test]
      async fn test_my_custom_serializer_round_trip() {
          let format = MyCustomSerializer::new(default_config());
          let record = create_comprehensive_test_record();

          // Test serialization round trip
          test_serialization_round_trip(&format, &record)
              .expect("Round trip should succeed");
      }

      #[tokio::test]
      async fn test_execution_format_conversion() {
          let format = MyCustomSerializer::new(default_config());
          let record = create_basic_test_record();

          test_execution_format_round_trip(&format, &record)
              .expect("Execution format conversion should succeed");
      }
}
```
**Key Benefits of This Approach**

✅ Format Agnostic SQL Engine: Your SQL queries work the same regardless of serialization format

✅ Type Safety: Strong typing between external format and SQL engine

✅ Performance: Direct integration without extra conversion layers

✅ Consistency: Same patterns as built-in JSON, Avro, and Protobuf formats

✅ Testing: Comprehensive test utilities available

**Common Use Cases**

- Custom Binary Formats: Company-specific binary protocols
- MessagePack/CBOR: Alternative compact formats
- Hybrid Formats: Combining multiple serialization approaches
- Legacy Formats: Integrating with existing systems
- Encrypted Formats: Adding encryption layers to existing formats

The FerrisStreams architecture makes it straightforward to plug in any serialization format while maintaining full SQL engine functionality and type safety.