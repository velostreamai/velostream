# ferris streams

![Rust CI](https://github.com/bluemonk3y/ferrisstreams/workflows/Rust%20CI/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/ferrisstreams.svg)](https://crates.io/crates/ferrisstreams)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](./LICENSE)

A Rust-idiomatic and robust client library for Apache Kafka, designed for high-performance, fault-tolerant, and flexible processing of **multiple Kafka topics and data streams** with full support for **keys, values, and headers**.

## 🌟 Key Features

* **Type-Safe Kafka Operations:** Full support for typed keys, values, and headers with automatic serialization/deserialization
* **Rich Headers Support:** Custom `Headers` type with clean API for message metadata
* **Asynchronous Processing:** Built on `rdkafka` & `tokio` for efficient, non-blocking I/O
* **Flexible Serialization:** Modular `serde` framework with JSON support and extensible traits for custom formats
* **Stream Processing:** Both polling and streaming consumption patterns with implicit deserialization
* **Builder Patterns:** Ergonomic APIs for creating producers and consumers
* **Robust Error Handling:** Comprehensive error types with proper error propagation

## 🔧 Current API

### Producer API
```rust
// Create producer with key and value serializers
let producer = KafkaProducer::<String, MyMessage, _, _>::new(
    "localhost:9092", 
    "my-topic", 
    JsonSerializer,    // Key serializer
    JsonSerializer     // Value serializer
)?;

// Send with headers
let headers = Headers::new()
    .insert("source", "web-api")
    .insert("version", "1.0.0");

producer.send(Some(&key), &message, headers, None).await?;
```

### Consumer API
```rust
// Create consumer with key and value serializers
let consumer = KafkaConsumer::<String, MyMessage, _, _>::new(
    "localhost:9092", 
    "my-group", 
    JsonSerializer,    // Key deserializer
    JsonSerializer     // Value deserializer
)?;

// Poll for messages - returns Message<K, V> with headers
let message = consumer.poll_message(Duration::from_secs(5)).await?;
println!("Key: {:?}", message.key());
println!("Value: {:?}", message.value());
println!("Headers: {:?}", message.headers());

// Or use streaming
consumer.stream()
    .for_each(|result| async move {
        if let Ok(message) = result {
            // Access key, value, and headers
            let headers = message.headers();
            if let Some(source) = headers.get("source") {
                println!("Message from: {}", source);
            }
        }
    })
    .await;
```

### Headers API
```rust
// Create headers
let headers = Headers::new()
    .insert("source", "inventory-service")
    .insert("event-type", "product-created")
    .insert("timestamp", "2024-01-15T10:30:00Z");

// Query headers
if let Some(source) = headers.get("source") {
    println!("Source: {}", source);
}

// Iterate over all headers
for (key, value) in headers.iter() {
    match value {
        Some(v) => println!("{}: {}", key, v),
        None => println!("{}: <null>", key),
    }
}
```

## 📚 Examples

### Basic Usage
- **[Producer Example](examples/typed_kafka_example.rs)** - Basic producer/consumer with JSON serialization
- **[Headers Example](examples/headers_example.rs)** - Simple headers usage demonstration
- **[Consumer with Headers](examples/consumer_with_headers.rs)** - Comprehensive headers, keys, and values demo

### Advanced Usage
- **[Builder Configuration](examples/builder_configuration.rs)** - Advanced builder pattern and performance presets
- **[Fluent API Example](examples/fluent_api_example.rs)** - Stream processing with fluent API patterns

### Test Suite Examples
- **[Builder Pattern Tests](tests/ferris/kafka/builder_pattern_test.rs)** - Comprehensive builder pattern test suite (16 tests)
- **[Error Handling Tests](tests/ferris/kafka/error_handling_test.rs)** - Error scenarios and edge cases (12 tests)
- **[Serialization Tests](tests/ferris/kafka/serialization_unit_test.rs)** - JSON serialization validation (7 tests)
- **[Integration Tests](tests/ferris/kafka/kafka_integration_test.rs)** - Complete test suite including headers functionality
- **[Performance Tests](tests/ferris/kafka/kafka_performance_test.rs)** - Performance benchmarks and load testing
- **[Advanced Tests](tests/ferris/kafka/kafka_advanced_test.rs)** - Advanced patterns and edge cases

### Shared Test Infrastructure
- **[Test Messages](tests/ferris/kafka/test_messages.rs)** - Unified message types for testing
- **[Test Utils](tests/ferris/kafka/test_utils.rs)** - Shared utilities and helper functions
- **[Common Imports](tests/ferris/kafka/common.rs)** - Consolidated imports for all tests

## 🚀 Quick Start

Add `ferrisstreams` to your `Cargo.toml`:

```toml
[dependencies]
ferrisstreams = "0.1.0"
tokio = { version = "1", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
```

### Simple Producer Example

```rust
use ferrisstreams::{KafkaProducer, JsonSerializer};
use ferrisstreams::ferris::kafka::Headers;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderEvent {
    order_id: u64,
    customer_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "orders",
        JsonSerializer,
        JsonSerializer,
    )?;

    let order = OrderEvent {
        order_id: 12345,
        customer_id: "cust_001".to_string(),
        amount: 99.99,
    };

    let headers = Headers::new()
        .insert("source", "web-frontend")
        .insert("version", "1.2.3");

    producer.send(
        Some(&"order-12345".to_string()),
        &order,
        headers,
        None
    ).await?;

    Ok(())
}
```

### Simple Consumer Example

```rust
use ferrisstreams::{KafkaConsumer, JsonSerializer};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "order-processors",
        JsonSerializer,
        JsonSerializer,
    )?;

    consumer.subscribe(&["orders"])?;

    loop {
        match consumer.poll_message(Duration::from_secs(1)).await {
            Ok(message) => {
                println!("Received order: {:?}", message.value());
                
                // Access headers
                if let Some(source) = message.headers().get("source") {
                    println!("From: {}", source);
                }
                
                // Access key
                if let Some(key) = message.key() {
                    println!("Key: {}", key);
                }
            }
            Err(e) => println!("No message: {}", e),
        }
    }
}
```

## 🔄 Message Processing Patterns

### 1. Polling Pattern
```rust
// Traditional polling approach
while let Ok(message) = consumer.poll_message(timeout).await {
    let (key, value, headers) = message.into_parts();
    // Process message...
}
```

### 2. Streaming Pattern
```rust
// Reactive streaming approach - recommended!
consumer.stream()
    .filter_map(|result| async move { result.ok() })
    .for_each(|message| async move {
        // Message is automatically deserialized!
        println!("Processing: {:?}", message.value());
    })
    .await;
```

### 3. Fluent Processing
```rust
// Functional processing pipeline - see examples/fluent_api_example.rs
let high_priority: Vec<_> = consumer.stream()
    .take(100)
    .filter_map(|result| async move { result.ok() })
    .filter(|message| {
        // Filter by headers
        futures::future::ready(
            message.headers().get("priority") == Some("high")
        )
    })
    .map(|message| message.value().clone())
    .collect()
    .await;
```

## 🏗️ Architecture

### Type System
- **`KafkaProducer<K, V, KS, VS>`** - Generic producer with key/value types and serializers
- **`KafkaConsumer<K, V, KS, VS>`** - Generic consumer with key/value types and serializers  
- **`Message<K, V>`** - Typed message container with key, value, and headers
- **`Headers`** - Custom headers type with HashMap backing

### Serialization
- **`JsonSerializer`** - Built-in JSON serialization support
- **`Serializer<T>`** - Trait for custom serialization implementations
- **Extensible** - Support for Avro, Protobuf, and custom formats

## 🚀 Roadmap

### Current Features ✅
- ✅ Type-safe producer and consumer implementations
- ✅ Full headers support with custom Headers API
- ✅ Key and value serialization with separate serializers
- ✅ Stream-based consumption with implicit deserialization
- ✅ Builder patterns for ergonomic configuration
- ✅ Comprehensive error handling
- ✅ JSON serialization support

### Planned Features 🔄
- **Advanced Stream Processing:**
  - Fan-in (multiple topics → single topic)
  - Fan-out (single topic → multiple topics)
  - Stream filtering, mapping, and reducing
  - Header propagation and transformation

- **State Management:**
  - KTable-like stateful processing
  - Local state stores for aggregations
  - Fault-tolerant state recovery

- **Performance Optimizations:**
  - Compacting producer for high-throughput scenarios
  - Batch processing support
  - Time-based consumption (consume from time 'T')

- **Extended Serialization:**
  - Avro schema registry integration
  - Protobuf support
  - Custom binary formats

## 🧪 Testing

The project includes a comprehensive test suite with shared infrastructure to eliminate duplication:

### Running Tests

```bash
# All tests (unit + integration)
cargo test

# Unit tests only
cargo test --lib

# Integration tests (requires Kafka running on localhost:9092)
cargo test --test builder_pattern_test
cargo test --test error_handling_test
cargo test --test serialization_unit_test

# Specific test categories
cargo test test_headers_functionality  # Headers functionality
cargo test test_builder_pattern        # Builder patterns
cargo test test_error_handling         # Error scenarios
cargo test test_performance            # Performance benchmarks
```

### Test Structure

The test suite has been consolidated to eliminate duplication:
- **Shared Messages**: `tests/ferris/kafka/test_messages.rs` - Unified message types
- **Shared Utilities**: `tests/ferris/kafka/test_utils.rs` - Common test helpers
- **Common Imports**: `tests/ferris/kafka/common.rs` - Single import module
- **35+ Tests**: Covering builder patterns, error handling, serialization, and integration scenarios

### Current Test Status ✅
- Builder Pattern: 16/16 tests passing
- Error Handling: 12/12 tests passing  
- Serialization: 7/7 tests passing
- Integration: All tests passing
- Performance: Benchmarks available

## 🤝 Contributing

Contributions are welcome! Please see our [contributing guidelines](CONTRIBUTING.md) for details.

## 📄 License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## 📋 TODO

### Planned Features 🔄
- **Advanced Stream Processing:**
  - Fan-in (multiple topics → single topic)
  - Fan-out (single topic → multiple topics)
  - Stream filtering, mapping, and reducing
  - Header propagation and transformation

- **State Management:**
  - KTable-like stateful processing
  - Local state stores for aggregations
  - Fault-tolerant state recovery

- **Performance Optimizations:**
  - Compacting producer for high-throughput scenarios
  - Batch processing support
  - Time-based consumption (consume from time 'T')

- **Extended Serialization:**
  - Avro schema registry integration
  - Protobuf support
  - Custom binary formats

### Documentation Improvements 📖
- [ ] Add more comprehensive examples for complex use cases
- [ ] Create migration guide from other Kafka clients
- [ ] Add troubleshooting guide
- [ ] Performance tuning guide

### Test Coverage Enhancements 🧪
- [ ] Add stress testing scenarios
- [ ] Add chaos engineering tests
- [ ] Expand error injection testing
- [ ] Add multi-broker failover tests

## 🙏 Acknowledgments

Built on top of the excellent [rdkafka](https://github.com/fede1024/rust-rdkafka) library.