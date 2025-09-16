# Bring Your Own Serializer Guide

This guide shows you how to integrate custom serialization formats with the VeloStream SQL engine.

## Overview

VeloStream uses a trait-based serialization system that allows you to plug in any serialization format while maintaining full SQL engine compatibility. Your custom serializer integrates seamlessly with:

- ✅ **SQL Engine**: Full query processing capabilities
- ✅ **Type Safety**: Strong typing between external format and SQL engine
- ✅ **Kafka Integration**: Native streaming with your format
- ✅ **Format Agnostic**: Same SQL queries work regardless of serialization format

## Quick Start

### 1. Implement the SerializationFormat Trait

```rust
use velostream::velostream::serialization::{SerializationFormat, SerializationError};
use velostream::velostream::sql::FieldValue;
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

    /// Convert record for SQL execution engine (now handled internally)
    /// This method is deprecated as FieldValue is used directly in execution
    fn to_execution_format(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        // Direct pass-through since FieldValue is used for SQL execution
        Ok(record.clone())
    }

    /// Get format-specific metadata if needed
    fn get_metadata(&self) -> Option<HashMap<String, String>> {
        None
    }

    fn format_name(&self) -> &'static str {
        "MyCustomFormat"
    }
}
```

### 2. Handle Type Mapping

You need to convert between VeloStream' `FieldValue` enum and your format:

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

## SQL Engine Integration

### Basic Integration

```rust
use velostream::velostream::sql::StreamExecutionEngine;
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
    record.insert("customer_id".to_string(), FieldValue::String("123".to_string()));
    record.insert("amount".to_string(), FieldValue::Float(150.0));
    
    engine.execute(&query, record).await?;
    
    // Receive results
    while let Some(result) = output_rx.recv().await {
        println!("Query result: {:?}", result);
    }
    
    Ok(())
}
```

### Advanced Kafka Integration

```rust
use velostream::velostream::multi_job_server::MultiJobSqlServer;

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

## Helper Functions for Type Handling

Use built-in helpers for comprehensive FieldValue type handling:

```rust
// Helper function for comprehensive FieldValue type handling
fn handle_field_value_types(
    record: &HashMap<String, FieldValue>,
) -> Result<HashMap<String, CustomType>, SerializationError> {
    let mut result = HashMap::new();
    
    for (key, field_value) in record {
        let custom_value = match field_value {
            FieldValue::Integer(i) => CustomType::Integer(*i),
            FieldValue::Float(f) => CustomType::Float(*f),
            FieldValue::String(s) => CustomType::String(s.clone()),
            FieldValue::Boolean(b) => CustomType::Boolean(*b),
            FieldValue::Null => CustomType::Null,
            FieldValue::Array(arr) => {
                let custom_arr: Result<Vec<_>, _> = arr
                    .iter()
                    .map(|v| convert_field_value_to_custom(v))
                    .collect();
                CustomType::Array(custom_arr?)
            }
            FieldValue::Map(map) | FieldValue::Struct(map) => {
                let custom_map = handle_field_value_types(&map)?;
                CustomType::Object(custom_map)
            }
            // Handle specialized types for financial data
            FieldValue::ScaledInteger(value, scale) => {
                CustomType::Decimal(*value, *scale)
            }
            // Handle optional types
            FieldValue::Date(d) => CustomType::String(d.format("%Y-%m-%d").to_string()),
            FieldValue::Timestamp(ts) => CustomType::String(ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
            FieldValue::Decimal(dec) => CustomType::String(dec.to_string()),
        };
        
        result.insert(key.clone(), custom_value);
    }
    
    Ok(result)
}
```

## Real-World Example: MessagePack Serializer

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

    fn to_execution_format(&self, record: &HashMap<String, FieldValue>) -> Result<HashMap<String, FieldValue>, SerializationError> {
        Ok(record.clone())
    }

    fn get_metadata(&self) -> Option<HashMap<String, String>> {
        None
    }

    fn format_name(&self) -> &'static str {
        "MessagePack"
    }
}
```

## Factory Pattern Integration (Optional)

Extend the factory to support your format:

```rust
use velostream::velostream::serialization::SerializationFormatFactory;

impl SerializationFormatFactory {
    pub fn create_my_custom_format(config: MyCustomConfig) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        Ok(Box::new(MyCustomSerializer::new(config)))
    }
}

// Usage:
let format = SerializationFormatFactory::create_my_custom_format(config)?;
```

## Testing Your Custom Serializer

Use the same test patterns as built-in formats:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use velostream::velostream::serialization::test_helpers::*;

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

## SQL Queries with Your Format

Once integrated, your format works with all SQL features:

```rust
// Complex streaming SQL with your custom format
sql_context.execute_streaming("
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_order_value
    FROM orders 
    WHERE amount > 100.0
    GROUP BY customer_id
    WINDOW TUMBLING(5m)
    HAVING total_revenue > 10000.0
").await?;

// JSON processing (if your format supports it)
sql_context.execute_streaming("
    SELECT 
        JSON_VALUE(payload, '$.user.id') as user_id,
        JSON_VALUE(payload, '$.order.total') as order_total
    FROM events
    WHERE JSON_VALUE(payload, '$.type') = 'purchase'
").await?;

// Window functions and aggregations
sql_context.execute_streaming("
    SELECT 
        product_id,
        sales_amount,
        LAG(sales_amount, 1) OVER (
            PARTITION BY product_id 
            ORDER BY timestamp
        ) as previous_sale
    FROM sales
").await?;
```

## Key Benefits

✅ **Format Agnostic SQL Engine**: Your SQL queries work the same regardless of serialization format  
✅ **Type Safety**: Strong typing between external format and SQL engine  
✅ **Performance**: Direct integration without extra conversion layers  
✅ **Consistency**: Same patterns as built-in JSON, Avro, and Protobuf formats  
✅ **Testing**: Comprehensive test utilities available

## Common Use Cases

- **Custom Binary Formats**: Company-specific binary protocols
- **MessagePack/CBOR**: Alternative compact formats
- **Hybrid Formats**: Combining multiple serialization approaches
- **Legacy Formats**: Integrating with existing systems
- **Encrypted Formats**: Adding encryption layers to existing formats

## Advanced Features

### Schema Evolution Support

```rust
impl MyCustomSerializer {
    pub fn with_schema_evolution(
        reader_schema: MySchema,
        writer_schema: MySchema,
    ) -> Self {
        // Handle schema evolution in your format
        Self {
            reader_schema,
            writer_schema,
            config: MyCustomConfig::default(),
        }
    }
}
```

### Performance Optimization

```rust
impl MyCustomSerializer {
    // Cache expensive operations
    fn get_cached_schema(&self) -> &CompiledSchema {
        &self.compiled_schema_cache
    }
    
    // Reuse buffers
    fn serialize_with_buffer(&self, record: &HashMap<String, FieldValue>, buffer: &mut Vec<u8>) -> Result<(), SerializationError> {
        buffer.clear();
        // Use pre-allocated buffer for performance
    }
}
```

### Error Handling Best Practices

```rust
impl MyCustomSerializer {
    fn handle_serialization_error(&self, error: MySerializationError) -> SerializationError {
        match error {
            MySerializationError::InvalidType(msg) => SerializationError::UnsupportedType(msg),
            MySerializationError::EncodingError(msg) => SerializationError::SerializationFailed(msg),
            MySerializationError::DecodingError(msg) => SerializationError::DeserializationFailed(msg),
            MySerializationError::SchemaError(msg) => SerializationError::FormatConversionFailed(msg),
        }
    }
}
```

The VeloStream architecture makes it straightforward to plug in any serialization format while maintaining full SQL engine functionality and type safety.