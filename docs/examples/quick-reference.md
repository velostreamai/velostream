# Quick Reference - ferrisstreams

## API Overview

```rust
// Producer Type
KafkaProducer<K, V, KS, VS>
// K: Key type, V: Value type, KS: Key serializer, VS: Value serializer

// Consumer Type  
KafkaConsumer<K, V, KS, VS>
// Returns Message<K, V> with key, value, and headers

// Message Type
Message<K, V> {
    key: Option<K>,
    value: V,
    headers: Headers,
}

// Headers Type
Headers // wraps HashMap<String, Option<String>>
```

## Quick Start

```rust
use ferrisstreams::{KafkaProducer, KafkaConsumer, JsonSerializer};
use ferrisstreams::ferris::kafka::Headers;

// Producer
let producer = KafkaProducer::<String, MyData, _, _>::new(
    "localhost:9092",
    "topic",
    JsonSerializer,
    JsonSerializer,
)?;

let headers = Headers::new().insert("source", "api");
producer.send(Some(&key), &data, headers, None).await?;

// Consumer  
let consumer = KafkaConsumer::<String, MyData, _, _>::new(
    "localhost:9092",
    "group",
    JsonSerializer,
    JsonSerializer,
)?;

let message = consumer.poll(Duration::from_secs(5)).await?;
println!("Key: {:?}", message.key());
println!("Value: {:?}", message.value());
println!("Headers: {:?}", message.headers());
```

## Headers Operations

```rust
// Create
let headers = Headers::new()
    .insert("key", "value")
    .insert_null("null-key");

// Query
headers.get("key")           // Option<&str>
headers.contains_key("key")  // bool
headers.len()               // usize
headers.is_empty()          // bool

// Iterate
for (key, value) in headers.iter() {
    // process header
}
```

## Message Access

```rust
// Reference access (borrowing)
message.key()       // Option<&K>
message.value()     // &V
message.headers()   // &Headers

// Owned consumption
message.into_key()     // Option<K>
message.into_value()   // V
message.into_headers() // Headers
message.into_parts()   // (Option<K>, V, Headers)
```

## Consumer Patterns

```rust
// Polling
while let Ok(message) = consumer.poll(timeout).await {
    // Process message with headers
}

// Streaming
consumer.stream()
    .for_each(|result| async move {
        if let Ok(message) = result {
            // Headers automatically available
            let headers = message.headers();
        }
    })
    .await;

// Filtering by headers
consumer.stream()
    .filter_map(|result| async move { result.ok() })
    .filter(|message| {
        futures::future::ready(
            message.headers().get("priority") == Some("high")
        )
    })
    .collect()
    .await;
```

## Examples

- **[Basic Headers](../../examples/headers_example.rs)**
- **[Consumer with Headers](../../examples/consumer_with_headers.rs)**
- **[Integration Tests](../tests/integration/kafka_integration_test.rs)**

## Key Points

✅ **Consumer returns headers automatically** in every message  
✅ **Type-safe** key/value/header access  
✅ **Builder patterns** for ergonomic header construction  
✅ **Multiple processing patterns** (polling, streaming, functional)  
✅ **Rich examples** and documentation available