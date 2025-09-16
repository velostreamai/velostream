# Simplified Type-Safe Kafka API

## Overview

The Kafka integration has been completely redesigned to be type-safe by default, removing the need for manual serialization and providing a clean, intuitive API.

## Key Simplifications

### ✨ **Implicit Deserialization (Latest Feature!)**

The consumer `stream()` method now returns already-deserialized typed messages:

```rust
// Before (manual deserialization)
let mut stream = consumer.stream();
while let Some(msg_result) = stream.next().await {
    match msg_result {
        Ok(borrowed_message) => {
            if let Some(payload) = borrowed_message.payload() {
                match JsonSerializer.deserialize(payload) {
                    Ok(typed_message) => { /* process */ }
                    Err(e) => eprintln!("Deserialization error: {}", e),
                }
            }
        }
        Err(e) => eprintln!("Kafka error: {}", e),
    }
}

// After (implicit deserialization - much cleaner!)
let mut stream = consumer.stream();
while let Some(msg_result) = stream.next().await {
    match msg_result {
        Ok(typed_message) => { 
            // Already deserialized! Ready to use.
            process(typed_message);
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}

// Fluent style is now incredibly clean:
let messages: Vec<MyMessage> = consumer.stream()
    .take(10)
    .filter_map(|result| async move { result.ok() })
    .collect()
    .await;
```


#### API
```rust
use futures::StreamExt;
use rdkafka::message::Message;

// Type-safe with automatic serialization
let producer = KafkaProducer::<User, _>::new("localhost:9092", "topic", JsonSerializer)?;
producer.send(Some("key"), &user, None).await?;

// Type-safe with automatic deserialization (using streaming - recommended)
let consumer = KafkaConsumer::<User, _>::new("localhost:9092", "group", JsonSerializer)?;
consumer.subscribe(&["user-topic"])?;
let mut stream = consumer.stream();
while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(user) => {
            // User is already deserialized! No manual work needed.
            println!("Received user: {:?}", user);
        }
        Err(e) => eprintln!("Stream error: {:?}", e),
    }
}
```

## Core Components

### **KafkaProducer<T, S, C>**
Generic producer with automatic serialization:
- **T**: Message type (e.g., `User`, `Order`)  
- **S**: Serializer (e.g., `JsonSerializer`)
- **C**: Context (defaults to `LoggingProducerContext`)

```rust
use velostream::{KafkaProducer, JsonSerializer};

let producer = KafkaProducer::<MyMessage, _>::new(
    "localhost:9092",
    "my-topic", 
    JsonSerializer,
)?;

producer.send(Some("key"), &my_message, None).await?;
```

### **KafkaConsumer<T, S, C>**
Generic consumer with automatic deserialization:
- **T**: Message type (e.g., `User`, `Order`)
- **S**: Serializer (e.g., `JsonSerializer`)  
- **C**: Context (defaults to `DefaultConsumerContext`)

**Methods:**
- `stream()` - Returns `impl Stream<Item = Result<T, ConsumerError>>` with **implicit deserialization**
- `raw_stream()` - Returns raw Kafka messages for advanced use cases that need metadata

```rust
use velostream::{KafkaConsumer, JsonSerializer};
use futures::StreamExt;
use rdkafka::message::Message;

let consumer = KafkaConsumer::<MyMessage, _>::new(
    "localhost:9092",
    "my-group",
    JsonSerializer,
)?;

consumer.subscribe(&["my-topic"])?;

// Recommended: Use streaming for efficient message processing
let mut stream = consumer.stream();
while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(my_message) => {
            // Message is already deserialized! Super clean API.
            println!("Received: {:?}", my_message);
        }
        Err(e) => {
            println!("Stream error: {}", e);
            break;
        }
    }
}
```

### **Stream Processing (Recommended)**
For efficient message processing, always use streaming:
```rust
use futures::StreamExt;
use rdkafka::message::Message;

// Get stream and process messages efficiently with implicit deserialization
let mut stream = consumer.stream();
while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(my_message) => {
            // Message is already deserialized! 
            // Process my_message directly...
            println!("Processing: {:?}", my_message);
        }
        Err(e) => eprintln!("Stream error: {}", e),
    }
}

// For advanced use cases that need raw message metadata, use raw_stream()
let mut raw_stream = consumer.raw_stream();
while let Some(message_result) = raw_stream.next().await {
    match message_result {
        Ok(borrowed_message) => {
            // Access message metadata
            let key = borrowed_message.key();
            let partition = borrowed_message.partition();
            
            // Manual deserialization if needed
            if let Some(payload) = borrowed_message.payload() {
                let my_message: MyMessage = JsonSerializer.deserialize(payload)?;
                // Process my_message...
            }
        }
        Err(e) => eprintln!("Stream error: {}", e),
    }
}
```

### **Fluent Stream Processing**

Chain multiple operations for powerful message processing:

```rust
use futures::StreamExt;
use rdkafka::message::Message;

// Collect messages with fluent processing - much simpler with implicit deserialization!
let processed_messages = consumer.stream()
    .take(100) // Process first 100 messages
    .filter_map(|msg_result| async move {
        // Just extract successful results - no manual deserialization!
        msg_result.ok()
    })
    .filter(|message: &MyMessage| {
        // Filter by business logic
        futures::future::ready(message.is_active())
    })
    .map(|message| message.transform()) // Transform to different type
    .collect::<Vec<TransformedMessage>>()
    .await;

// Process each message in streaming fashion - beautifully simple!
consumer.stream()
    .take_while(|msg_result| async move {
        // Continue processing until error or specific condition
        msg_result.is_ok()
    })
    .filter_map(|msg_result| async move {
        // Automatic deserialization - just extract successful results!
        msg_result.ok()
    })
    .for_each(|message| async move {
        // Process each message individually
        process_message(message).await;
    })
    .await;
```

## Usage Examples

### **Basic Producer/Consumer**
```rust
use velostream::{KafkaProducer, KafkaConsumer, JsonSerializer};
use futures::StreamExt;
use rdkafka::message::Message;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: u32,
    name: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Producer
    let producer = KafkaProducer::<User, _>::new(
        "localhost:9092", 
        "users", 
        JsonSerializer
    )?;
    
    let user = User {
        id: 1,
        name: "Alice".to_string(), 
        email: "alice@example.com".to_string(),
    };
    
    producer.send(Some("user-1"), &user, None).await?;
    
    // Consumer
    let consumer = KafkaConsumer::<User, _>::new(
        "localhost:9092",
        "user-processor", 
        JsonSerializer
    )?;
    
    consumer.subscribe(&["users"])?;
    
    // Use streaming for efficient message processing with implicit deserialization
    let mut stream = consumer.stream();
    if let Some(Ok(user)) = stream.next().await {
        // User is already deserialized - no manual work needed!
        println!("User: {:?}", user);
    }
    
    Ok(())
}
```

### **Builder Pattern**
```rust
use velostream::{ProducerBuilder, ConsumerBuilder, JsonSerializer};

// Producer builder
let producer = ProducerBuilder::<User, _>::new(
    "localhost:9092",
    "users", 
    JsonSerializer
).build()?;

// Consumer builder  
let consumer = ConsumerBuilder::<User, _>::new(
    "localhost:9092",
    "user-group",
    JsonSerializer
).build()?;
```

### **Convenience Trait**
```rust
use velostream::KafkaConsumable;

// Create consumer via trait method
let consumer = User::consumer("localhost:9092", "group", JsonSerializer)?;
```

### **Multiple Topics**
```rust
// Send to specific topic (not default)
producer.send_to_topic("special-users", Some("key"), &user, None).await?;

// Subscribe to multiple topics
consumer.subscribe(&["users", "admin-users", "guest-users"])?;
```

## Error Handling

All operations return proper `Result` types:

```rust
// Producer errors
match producer.send(Some("key"), &user, None).await {
    Ok(_delivery) => println!("Sent successfully"),
    Err(ProducerError::KafkaError(e)) => println!("Kafka error: {}", e),
    Err(ProducerError::SerializationError(e)) => println!("Serialization error: {}", e),
}

// Consumer with streaming (recommended for error handling) - much cleaner now!
let mut stream = consumer.stream();
match stream.next().await {
    Some(Ok(message)) => {
        // Message is already deserialized!
        println!("Received: {:?}", message);
    }
    Some(Err(ConsumerError::SerializationError(e))) => {
        println!("Deserialization error: {}", e);
    }
    Some(Err(ConsumerError::KafkaError(e))) => {
        println!("Kafka error: {}", e);
    }
    Some(Err(e)) => println!("Other error: {}", e),
    None => println!("Stream ended"),
}
```

## Serialization Support

### **Built-in Serializers**
- `JsonSerializer` - JSON using serde_json
- `AvroSerializer` - Avro (feature-gated) 
- `ProtoSerializer` - Protocol Buffers (feature-gated)

### **Custom Serializers**
```rust
struct MySerializer;

impl<T> Serializer<T> for MySerializer 
where T: MyTrait {
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerializationError> {
        // Custom serialization
    }
    
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerializationError> {
        // Custom deserialization  
    }
}
```

## Running Examples

```bash
# Basic example
cargo run --bin typed_example

# Functional tests (requires Kafka)
./test-typed-kafka.sh

# Specific test
cargo test typed_kafka_func_test::test_producer_consumer_basic -- --nocapture
```

## Migration Benefits

### ✅ **Advantages of Simplified API**

1. **Compile-time Type Safety** - Impossible to send/receive wrong types
2. **Zero Boilerplate** - No manual serialization code needed
3. **✨ Implicit Deserialization** - Stream returns typed messages directly
4. **Better Ergonomics** - Intuitive, clean API design  
5. **Reduced Errors** - Automatic error handling and type checking
6. **Performance** - Zero-cost abstractions over rdkafka
7. **Flexibility** - Support for custom serializers and contexts
8. **🚀 Fluent API** - Incredibly clean chaining with `result.ok()`

### ✅ **Removed Complexity** 

- ❌ Manual byte array handling
- ❌ Separate serialization steps
- ❌ Error-prone type casting
- ❌ Boilerplate serialization code
- ❌ Manual `payload()` extraction
- ❌ Repeated deserialization calls
- ❌ Backward compatibility layers
- ❌ Confusing dual APIs

### ✅ **What's Left**

- ✅ Simple, type-safe producer/consumer
- ✅ **Implicit deserialization** - no manual payload handling
- ✅ Automatic serialization/deserialization
- ✅ Builder patterns for configuration
- ✅ Comprehensive error types
- ✅ **Ultra-clean fluent API** - just `result.ok()`
- ✅ Convenience traits and helpers
- ✅ Custom serializer support

The simplified API focuses on the 90% use case while maintaining the flexibility to handle advanced scenarios through custom contexts and serializers.