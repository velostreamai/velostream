# Simplified Type-Safe Kafka API

## Overview

The Kafka integration has been completely redesigned to be type-safe by default, removing the need for manual serialization and providing a clean, intuitive API.

## Key Simplifications


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
    if let Ok(borrowed_message) = message_result {
        if let Some(payload) = borrowed_message.payload() {
            if let Ok(user) = JsonSerializer.deserialize(payload) {
                // Process user - already deserialized!
            }
        }
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
use ferrisstreams::{KafkaProducer, JsonSerializer};

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

```rust
use ferrisstreams::{KafkaConsumer, JsonSerializer};
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
        Ok(borrowed_message) => {
            if let Some(payload) = borrowed_message.payload() {
                if let Ok(my_message) = JsonSerializer.deserialize(payload) {
                    println!("Received: {:?}", my_message);
                }
            }
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

// Get stream and process messages efficiently
let mut stream = consumer.stream();
while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(borrowed_message) => {
            // Access message metadata
            let key = borrowed_message.key();
            let partition = borrowed_message.partition();
            
            // Deserialize payload
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

// Collect messages with fluent processing
let processed_messages = consumer.stream()
    .take(100) // Process first 100 messages
    .filter_map(|msg_result| async move {
        match msg_result {
            Ok(borrowed_message) => {
                if let Some(payload) = borrowed_message.payload() {
                    JsonSerializer.deserialize(payload).ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    })
    .filter(|message: &MyMessage| {
        // Filter by business logic
        futures::future::ready(message.is_active())
    })
    .map(|message| message.transform()) // Transform to different type
    .collect::<Vec<TransformedMessage>>()
    .await;

// Process each message in streaming fashion
consumer.stream()
    .take_while(|msg_result| async move {
        // Continue processing until error or specific condition
        msg_result.is_ok()
    })
    .filter_map(|msg_result| async move {
        match msg_result {
            Ok(borrowed_message) => {
                if let Some(payload) = borrowed_message.payload() {
                    JsonSerializer.deserialize(payload).ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
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
use ferrisstreams::{KafkaProducer, KafkaConsumer, JsonSerializer};
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
    
    // Use streaming for efficient message processing
    let mut stream = consumer.stream();
    if let Some(Ok(borrowed_message)) = stream.next().await {
        if let Some(payload) = borrowed_message.payload() {
            if let Ok(user) = JsonSerializer.deserialize(payload) {
                println!("User: {:?}", user);
            }
        }
    }
    
    Ok(())
}
```

### **Builder Pattern**
```rust
use ferrisstreams::{ProducerBuilder, ConsumerBuilder, JsonSerializer};

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
use ferrisstreams::KafkaConsumable;

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

// Consumer with streaming (recommended for error handling)
let mut stream = consumer.stream();
match stream.next().await {
    Some(Ok(borrowed_message)) => {
        if let Some(payload) = borrowed_message.payload() {
            match JsonSerializer.deserialize(payload) {
                Ok(message) => println!("Received: {:?}", message),
                Err(e) => println!("Deserialization error: {}", e),
            }
        }
    }
    Some(Err(e)) => println!("Kafka error: {}", e),
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
3. **Better Ergonomics** - Intuitive, clean API design  
4. **Reduced Errors** - Automatic error handling and type checking
5. **Performance** - Zero-cost abstractions over rdkafka
6. **Flexibility** - Support for custom serializers and contexts

### ✅ **Removed Complexity** 

- ❌ Manual byte array handling
- ❌ Separate serialization steps
- ❌ Error-prone type casting
- ❌ Boilerplate serialization code
- ❌ Backward compatibility layers
- ❌ Confusing dual APIs

### ✅ **What's Left**

- ✅ Simple, type-safe producer/consumer
- ✅ Automatic serialization/deserialization
- ✅ Builder patterns for configuration
- ✅ Comprehensive error types
- ✅ Convenience traits and helpers
- ✅ Custom serializer support

The simplified API focuses on the 90% use case while maintaining the flexibility to handle advanced scenarios through custom contexts and serializers.