# Headers Guide - ferrisstreams

This guide demonstrates how to work with Kafka message headers in ferrisstreams, showing how the consumer returns headers along with keys and values.

## Overview

ferrisstreams provides full support for Kafka message headers through:

- **Headers Type**: A custom `Headers` struct that wraps `HashMap<String, Option<String>>`
- **Message Type**: `Message<K, V>` struct containing key, value, and headers
- **Producer Support**: Send messages with rich header metadata
- **Consumer Support**: Receive messages with automatic header deserialization

## Table of Contents

1. [Headers API](#headers-api)
2. [Producer Usage](#producer-usage)
3. [Consumer Usage](#consumer-usage)
4. [Processing Patterns](#processing-patterns)
5. [Examples](#examples)
6. [Common Use Cases](#common-use-cases)

## Headers API

### Creating Headers

```rust
use ferrisstreams::ferris::kafka::Headers;

// Empty headers
let headers = Headers::new();

// Headers with builder pattern
let headers = Headers::new()
    .insert("source", "web-api")
    .insert("version", "1.2.3")
    .insert("trace-id", "abc-123-def")
    .insert_null("optional-field");

// Headers with capacity hint
let headers = Headers::with_capacity(10);
```

### Querying Headers

```rust
// Get a header value
if let Some(source) = headers.get("source") {
    println!("Source: {}", source);
}

// Check if header exists
if headers.contains_key("trace-id") {
    println!("Has trace ID");
}

// Get optional header (includes null values)
match headers.get_optional("optional-field") {
    Some(Some(value)) => println!("Field: {}", value),
    Some(None) => println!("Field is null"),
    None => println!("Field not present"),
}

// Header count and emptiness
println!("Header count: {}", headers.len());
println!("Is empty: {}", headers.is_empty());
```

### Iterating Headers

```rust
// Iterate over all headers
for (key, value) in headers.iter() {
    match value {
        Some(v) => println!("{}: {}", key, v),
        None => println!("{}: <null>", key),
    }
}
```

## Producer Usage

### Basic Producer with Headers

```rust
use ferrisstreams::{KafkaProducer, JsonSerializer};
use ferrisstreams::ferris::kafka::Headers;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct OrderEvent {
    order_id: u64,
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
        amount: 99.99,
    };

    let headers = Headers::new()
        .insert("source", "checkout-service")
        .insert("event-type", "order-created")
        .insert("version", "1.0.0")
        .insert("user-id", "user-456");

    producer.send(
        Some(&"order-12345".to_string()),
        &order,
        headers,
        None
    ).await?;

    Ok(())
}
```

## Consumer Usage

The consumer **automatically returns headers** with every message through the `Message<K, V>` struct.

### Basic Consumer

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
        match consumer.poll_message(Duration::from_secs(5)).await {
            Ok(message) => {
                // Access all three components
                println!("Key: {:?}", message.key());
                println!("Value: {:?}", message.value());
                println!("Headers: {:?}", message.headers());

                // Process specific headers
                if let Some(source) = message.headers().get("source") {
                    println!("Message from: {}", source);
                }

                if let Some(event_type) = message.headers().get("event-type") {
                    match event_type {
                        "order-created" => handle_order_created(&message),
                        "order-updated" => handle_order_updated(&message),
                        _ => println!("Unknown event: {}", event_type),
                    }
                }
            }
            Err(e) => println!("No message: {}", e),
        }
    }
}

fn handle_order_created(message: &ferrisstreams::ferris::kafka::Message<String, OrderEvent>) {
    println!("Processing new order: {:?}", message.value());
}

fn handle_order_updated(message: &ferrisstreams::ferris::kafka::Message<String, OrderEvent>) {
    println!("Processing order update: {:?}", message.value());
}
```

### Stream-based Consumer

```rust
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "order-processors",
        JsonSerializer,
        JsonSerializer,
    )?;

    consumer.subscribe(&["orders"])?;

    consumer.stream()
        .for_each(|result| async move {
            match result {
                Ok(message) => {
                    // Headers are automatically available
                    let headers = message.headers();
                    let key = message.key();
                    let value = message.value();

                    // Route based on headers
                    if let Some(priority) = headers.get("priority") {
                        if priority == "high" {
                            handle_priority_order(key, value, headers).await;
                        } else {
                            handle_normal_order(key, value, headers).await;
                        }
                    }
                }
                Err(e) => eprintln!("Error processing message: {}", e),
            }
        })
        .await;

    Ok(())
}

async fn handle_priority_order(
    key: Option<&String>, 
    value: &OrderEvent, 
    headers: &ferrisstreams::ferris::kafka::Headers
) {
    println!("Priority order: {:?} with key: {:?}", value, key);
    
    // Access tracing headers
    if let Some(trace_id) = headers.get("trace-id") {
        println!("Trace ID: {}", trace_id);
    }
}

async fn handle_normal_order(
    key: Option<&String>, 
    value: &OrderEvent, 
    headers: &ferrisstreams::ferris::kafka::Headers
) {
    println!("Normal order: {:?} with key: {:?}", value, key);
}
```

## Processing Patterns

### 1. Header-based Routing

```rust
// Route messages based on headers
consumer.stream()
    .for_each(|result| async move {
        if let Ok(message) = result {
            match message.headers().get("event-type") {
                Some("user-created") => user_service.handle_created(message).await,
                Some("user-updated") => user_service.handle_updated(message).await,
                Some("user-deleted") => user_service.handle_deleted(message).await,
                _ => println!("Unknown event type"),
            }
        }
    })
    .await;
```

### 2. Filtering by Headers

```rust
// Process only high-priority messages
let high_priority_messages: Vec<_> = consumer.stream()
    .filter_map(|result| async move { result.ok() })
    .filter(|message| {
        futures::future::ready(
            message.headers().get("priority") == Some("high")
        )
    })
    .take(100)
    .collect()
    .await;
```

### 3. Header Transformation

```rust
// Extract and transform based on headers
consumer.stream()
    .filter_map(|result| async move { result.ok() })
    .map(|message| {
        // Create enriched event with headers
        EnrichedEvent {
            key: message.key().cloned(),
            data: message.into_value(),
            source: message.headers().get("source").map(String::from),
            timestamp: message.headers().get("timestamp").map(String::from),
            trace_id: message.headers().get("trace-id").map(String::from),
        }
    })
    .for_each(|enriched| async move {
        process_enriched_event(enriched).await;
    })
    .await;

struct EnrichedEvent {
    key: Option<String>,
    data: OrderEvent,
    source: Option<String>,
    timestamp: Option<String>,
    trace_id: Option<String>,
}
```

### 4. Message Consumption Patterns

```rust
// Pattern 1: Reference access (borrowing)
let message = consumer.poll_message(timeout).await?;
println!("Key: {:?}", message.key());
println!("Value: {:?}", message.value());
println!("Headers: {:?}", message.headers());

// Pattern 2: Owned consumption
let (key, value, headers) = message.into_parts();
// Now you own the key, value, and headers

// Pattern 3: Selective consumption
let value = message.into_value();  // Take just the value
let headers = message.into_headers();  // Take just the headers
```

## Examples

### Complete Examples Available

1. **[Basic Headers Example](../examples/headers_example.rs)**
   - Simple producer/consumer with headers
   - Demonstrates basic header operations

2. **[Consumer with Headers Example](../examples/consumer_with_headers.rs)**
   - Comprehensive headers demonstration
   - Shows all access patterns and processing methods
   - Production-ready patterns

3. **[Integration Test](../tests/ferris/kafka/kafka_integration_test.rs)**
   - `test_headers_functionality` - Complete test case
   - Validates headers round-trip functionality

### Running Examples

```bash
# Run the basic headers example
cargo run --example headers_example

# Run the comprehensive consumer example  
cargo run --example consumer_with_headers

# Run headers integration test
cargo test test_headers_functionality
```

## Common Use Cases

### 1. Distributed Tracing

```rust
// Producer adds tracing headers
let headers = Headers::new()
    .insert("trace-id", &trace_context.trace_id)
    .insert("span-id", &trace_context.span_id)
    .insert("parent-span-id", &trace_context.parent_span_id);

producer.send(Some(&key), &event, headers, None).await?;

// Consumer extracts tracing context
if let Ok(message) = consumer.poll_message(timeout).await {
    let trace_context = TraceContext {
        trace_id: message.headers().get("trace-id").unwrap_or("unknown"),
        span_id: message.headers().get("span-id").unwrap_or("unknown"),
        parent_span_id: message.headers().get("parent-span-id"),
    };
    
    process_with_tracing(message.value(), trace_context).await;
}
```

### 2. Message Versioning

```rust
// Producer sets schema version
let headers = Headers::new()
    .insert("schema-version", "v2.1")
    .insert("content-type", "application/json");

// Consumer handles different versions
match message.headers().get("schema-version") {
    Some("v1.0") => handle_v1_message(message.value()),
    Some("v2.0" | "v2.1") => handle_v2_message(message.value()),
    _ => return Err("Unsupported schema version"),
}
```

### 3. Source Attribution

```rust
// Producer identifies source
let headers = Headers::new()
    .insert("source-service", "user-service")
    .insert("source-version", "1.2.3")
    .insert("environment", "production");

// Consumer routes based on source
match message.headers().get("source-service") {
    Some("user-service") => user_handler.process(message).await,
    Some("order-service") => order_handler.process(message).await,
    Some("inventory-service") => inventory_handler.process(message).await,
    _ => default_handler.process(message).await,
}
```

### 4. Feature Flags and Routing

```rust
// Producer sets routing hints
let headers = Headers::new()
    .insert("feature-flag", "new-checkout-flow")
    .insert("ab-test-group", "variant-b")
    .insert("region", "us-west-2");

// Consumer handles feature flags
if message.headers().get("feature-flag") == Some("new-checkout-flow") {
    new_checkout_handler.process(message).await;
} else {
    legacy_checkout_handler.process(message).await;
}
```

## Best Practices

1. **Use Consistent Header Keys**: Establish conventions for header names across your organization
2. **Include Tracing Information**: Always include trace/correlation IDs for debugging
3. **Version Your Messages**: Include schema versions to handle evolution
4. **Validate Headers**: Check for required headers and handle missing ones gracefully
5. **Avoid Large Headers**: Keep header values small - they add overhead to every message
6. **Use Typed Processing**: Leverage the type system to ensure header handling correctness

## Summary

ferrisstreams provides comprehensive support for Kafka headers:

- ✅ **Consumer returns headers automatically** in every `Message<K, V>`
- ✅ **Rich Headers API** with builder patterns and query methods
- ✅ **Type-safe access** to keys, values, and headers
- ✅ **Multiple consumption patterns** (polling, streaming, functional)
- ✅ **Production-ready examples** for common use cases

The consumer **already fully supports** returning headers with keys and values - no additional implementation needed!