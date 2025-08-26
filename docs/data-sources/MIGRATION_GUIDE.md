# Migration Guide: From Kafka-Coupled to Pluggable Data Sources

This guide helps you migrate existing FerrisStreams applications to use the new pluggable data sources architecture.

## 📋 Overview

The new architecture maintains **100% backward compatibility** while enabling heterogeneous data sources. Your existing Kafka code continues to work unchanged.

## 🔄 Migration Path

### Option 1: No Changes Required (Recommended)
Your existing Kafka-based applications continue to work without any changes:

```rust
// ✅ EXISTING CODE CONTINUES TO WORK
use ferrisstreams::ferris::kafka::{KafkaConsumer, KafkaProducer};

let consumer = KafkaConsumer::new("localhost:9092", "group1", JsonSerializer, JsonSerializer)?;
// ... existing code unchanged
```

### Option 2: Gradual Migration to New Traits
Migrate individual components to use the new trait-based architecture:

```rust
// 🔄 MIGRATING TO NEW ARCHITECTURE
use ferrisstreams::ferris::sql::datasource::{create_source, create_sink};

// Old way (still works)
let consumer = KafkaConsumer::new(...)?;

// New way (preferred for new code)
let source = create_source("kafka://localhost:9092/topic?group_id=analytics")?;
let mut reader = source.create_reader().await?;
```

## 🚀 Benefits of Migration

### Before (Kafka-Coupled)
```rust
use ferrisstreams::ferris::kafka::{KafkaConsumer, KafkaProducer};

// Limited to Kafka only
let consumer = KafkaConsumer::new("localhost:9092", "group", serializer, serializer)?;
let producer = KafkaProducer::new("localhost:9092", "topic", serializer, serializer)?;

// Manual message handling
while let Some(message) = consumer.poll().await? {
    // Process Kafka-specific message format
    let processed = process_kafka_message(message)?;
    producer.send(None, &processed, Headers::new(), None).await?;
}
```

### After (Pluggable Sources)
```rust
use ferrisstreams::ferris::sql::datasource::{create_source, create_sink};

// ✨ ANY source to ANY sink
let source = create_source("kafka://localhost:9092/input?group_id=processor")?;
let sink = create_sink("file:///output/processed.jsonl?format=jsonl")?;

// Or mix and match:
// let source = create_source("file:///data/input.csv?format=csv")?;
// let sink = create_sink("postgresql://localhost/db?table=results")?;

let mut reader = source.create_reader().await?;
let mut writer = sink.create_writer().await?;

// Unified record handling
while let Some(record) = reader.read().await? {
    // Process generic StreamRecord format
    let processed = process_stream_record(record)?;
    writer.write(processed).await?;
}
```

## 📚 Migration Examples

### 1. Simple Kafka Consumer
**Before:**
```rust
use ferrisstreams::ferris::kafka::{KafkaConsumer, JsonSerializer};

let consumer = KafkaConsumer::<String, String, _, _>::new(
    "localhost:9092",
    "my-group",
    JsonSerializer,
    JsonSerializer,
)?;
consumer.subscribe(&["orders"])?;

while let Some(message) = consumer.poll(Duration::from_millis(1000)).await? {
    println!("Key: {:?}, Value: {}", message.key(), message.value());
}
```

**After (Optional Migration):**
```rust
use ferrisstreams::ferris::sql::datasource::create_source;

let source = create_source("kafka://localhost:9092/orders?group_id=my-group")?;
let mut reader = source.create_reader().await?;

while let Some(record) = reader.read().await? {
    if let Some(key) = record.fields.get("key") {
        println!("Key: {:?}", key);
    }
    if let Some(value) = record.fields.get("value") {
        println!("Value: {:?}", value);
    }
}
```

### 2. Producer with Headers
**Before:**
```rust
use ferrisstreams::ferris::kafka::{KafkaProducer, Headers, JsonSerializer};

let producer = KafkaProducer::<String, String, _, _>::new(
    "localhost:9092",
    "results",
    JsonSerializer,
    JsonSerializer,
)?;

let mut headers = Headers::new();
headers = headers.insert("correlation-id", "12345");

producer.send(Some("key".to_string()), "value", headers, None).await?;
```

**After (Optional Migration):**
```rust
use ferrisstreams::ferris::sql::datasource::create_sink;
use ferrisstreams::ferris::sql::execution::types::{StreamRecord, FieldValue};

let sink = create_sink("kafka://localhost:9092/results")?;
let mut writer = sink.create_writer().await?;

let mut fields = HashMap::new();
fields.insert("key".to_string(), FieldValue::String("key".to_string()));
fields.insert("value".to_string(), FieldValue::String("value".to_string()));

let mut headers = HashMap::new();
headers.insert("correlation-id".to_string(), "12345".to_string());

let record = StreamRecord {
    fields,
    timestamp: chrono::Utc::now().timestamp_millis(),
    offset: 0,
    partition: 0,
    headers,
};

writer.write(record).await?;
```

### 3. End-to-End Pipeline
**Before (Kafka-only):**
```rust
// Limited to Kafka → Kafka
let consumer = KafkaConsumer::new("localhost:9092", "group", JsonSerializer, JsonSerializer)?;
let producer = KafkaProducer::new("localhost:9092", "output", JsonSerializer, JsonSerializer)?;

consumer.subscribe(&["input"])?;
while let Some(message) = consumer.poll(Duration::from_millis(1000)).await? {
    let processed = transform_data(message.value())?;
    producer.send(message.key().clone(), &processed, Headers::new(), None).await?;
}
```

**After (Any source to any sink):**
```rust
// ✨ File → PostgreSQL, S3 → ClickHouse, etc.
let source = create_source("file:///data/input.csv?format=csv")?;
let sink = create_sink("postgresql://localhost/warehouse?table=processed_data")?;

let mut reader = source.create_reader().await?;
let mut writer = sink.create_writer().await?;

while let Some(record) = reader.read().await? {
    let processed = transform_stream_record(record)?;
    writer.write(processed).await?;
}
```

## 🛠️ New Capabilities

### 1. Configuration via URIs
```rust
// Kafka with custom configuration
let source = create_source(
    "kafka://broker1:9092,broker2:9092/topic?group_id=analytics&auto_offset_reset=earliest"
)?;

// File processing with format specification
let source = create_source("file:///data/*.csv?format=csv&delimiter=|&header=true")?;

// PostgreSQL with connection pooling
let sink = create_sink("postgresql://user:pass@localhost/db?table=results&pool_size=10")?;
```

### 2. Schema Discovery
```rust
use ferrisstreams::ferris::sql::datasource::create_source;

let source = create_source("file:///data/sample.parquet")?;
let schema = source.fetch_schema().await?;

println!("Discovered {} fields:", schema.fields.len());
for field in &schema.fields {
    println!("  {}: {:?} (nullable: {})", field.name, field.data_type, field.nullable);
}
```

### 3. Batch Processing
```rust
let source = create_source("s3://bucket/data/*.parquet?region=us-west-2")?;
let mut reader = source.create_reader().await?;

// Process in batches for better performance
let batch = reader.read_batch(1000).await?;
for record in batch {
    process_record(record)?;
}
```

## 📋 Migration Checklist

### Phase 1: Immediate (No Code Changes)
- [ ] ✅ **Verify existing code works** - Run your test suite
- [ ] ✅ **Update dependencies** - `cargo update` (if needed)
- [ ] ✅ **No functional changes required**

### Phase 2: Gradual Migration (Optional)
- [ ] **Identify pipeline components** to migrate
- [ ] **Start with new features** using pluggable architecture
- [ ] **Migrate non-critical pipelines** first
- [ ] **Keep critical pipelines** on existing Kafka code initially

### Phase 3: Full Migration (Future)
- [ ] **Convert all consumers** to use `DataSource` traits
- [ ] **Convert all producers** to use `DataSink` traits
- [ ] **Add heterogeneous sources** (files, databases, etc.)
- [ ] **Leverage new capabilities** (schema discovery, URI config)

## 🚨 Breaking Changes

**None.** This is a fully backward-compatible migration.

### What Continues to Work:
- ✅ All existing Kafka consumer/producer code
- ✅ All existing message handling logic
- ✅ All existing serialization/deserialization
- ✅ All existing error handling patterns
- ✅ All existing test suites

### What's New (Additive):
- ✨ Pluggable data source traits
- ✨ URI-based configuration
- ✨ Schema discovery
- ✨ Heterogeneous data flows
- ✨ Unified record format across sources

## 💡 Best Practices

### 1. **Start Small**
Begin with new pipelines or non-critical components:
```rust
// Good: New feature using pluggable architecture
let source = create_source("file:///new-data-feed.jsonl")?;
let sink = create_sink("kafka://localhost:9092/processed")?;
```

### 2. **Keep Existing Critical Code**
Don't migrate business-critical pipelines immediately:
```rust
// Good: Keep existing critical Kafka code unchanged
let critical_consumer = KafkaConsumer::new(...)?; // Existing code
```

### 3. **Use URI Configuration**
Leverage the new configuration system for flexibility:
```rust
// Good: Environment-based configuration
let kafka_uri = std::env::var("KAFKA_URI")?;
let source = create_source(&kafka_uri)?;
```

### 4. **Test Both Paths**
Ensure both old and new code paths work during transition:
```rust
#[cfg(test)]
mod tests {
    // Test existing Kafka code
    #[test]
    fn test_kafka_direct() { ... }
    
    // Test new pluggable architecture
    #[test] 
    fn test_pluggable_kafka() { ... }
}
```

## 🎯 Common Migration Patterns

### Consumer Migration
```rust
// Pattern: Wrap existing consumer logic
fn create_kafka_reader(brokers: &str, topic: &str, group_id: &str) -> Result<Box<dyn DataReader>> {
    let uri = format!("kafka://{}/{}?group_id={}", brokers, topic, group_id);
    let source = create_source(&uri)?;
    source.create_reader().await
}
```

### Producer Migration
```rust
// Pattern: Wrap existing producer logic
fn create_kafka_writer(brokers: &str, topic: &str) -> Result<Box<dyn DataWriter>> {
    let uri = format!("kafka://{}/{}", brokers, topic);
    let sink = create_sink(&uri)?;
    sink.create_writer().await
}
```

### Configuration Migration
```rust
// Before: Hardcoded configuration
let consumer = KafkaConsumer::new("localhost:9092", "group1", ...)?;

// After: Environment-driven configuration
let kafka_config = std::env::var("DATA_SOURCE_URI")
    .unwrap_or_else(|_| "kafka://localhost:9092/topic?group_id=group1".to_string());
let source = create_source(&kafka_config)?;
```

## 📞 Support

- **Documentation**: [Architecture Overview](./README.md)
- **Implementation Plan**: [ARCHITECTURAL_DECOUPLING_PLAN.md](./ARCHITECTURAL_DECOUPLING_PLAN.md)
- **Source Code**: [`src/ferris/sql/datasource/`](../../src/ferris/sql/datasource/)
- **Issues**: Report problems in the project issue tracker

## ✅ Summary

The migration to pluggable data sources is **completely optional** and maintains full backward compatibility. You can:

1. **Keep everything as-is** - Your existing code continues to work
2. **Gradually adopt** new features as needed
3. **Mix and match** old and new approaches
4. **Leverage new capabilities** when ready (heterogeneous sources, URI config, schema discovery)

The architecture provides a foundation for the future while preserving all existing investments in Kafka-based code.