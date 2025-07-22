# Type-Safe Kafka Integration

## Overview

The improved serialization integration provides type-safe Kafka producers and consumers that automatically handle serialization/deserialization without manual byte array management.

## Key Improvements

### Before (Manual Serialization)
```rust
// Manual serialization - error prone
let user = User { id: 1, name: "Alice".to_string() };
let bytes = serde_json::to_vec(&user)?;
producer.send(Some("user-1"), &bytes, None).await?;

// Manual deserialization - type unsafe
let (bytes, key) = consumer.poll_message(timeout).await?;
let user: User = serde_json::from_slice(&bytes)?;
```

### After (Type-Safe)
```rust
// Automatic serialization - type safe
let user = User { id: 1, name: "Alice".to_string() };
let typed_producer = TypedKafkaProducer::<User, _>::new(brokers, topic, JsonSerializer)?;
typed_producer.send(Some("user-1"), &user, None).await?;

// Automatic deserialization - compile-time type safety
let typed_consumer = TypedKafkaConsumer::<User, _>::new(brokers, group_id, JsonSerializer);
let typed_message = typed_consumer.poll_message(timeout).await?;
let user = typed_message.value(); // Already a User struct!
```

## Components

### TypedKafkaProducer<T, S, C>
- **T**: Message type (e.g., `User`, `OrderEvent`)
- **S**: Serializer implementation (e.g., `JsonSerializer`)
- **C**: Producer context (defaults to `LoggingProducerContext`)

### TypedKafkaConsumer<T, S, C>
- **T**: Message type (e.g., `User`, `OrderEvent`)
- **S**: Serializer implementation (e.g., `JsonSerializer`)
- **C**: Consumer context (defaults to `DefaultConsumerContext`)

### TypedMessage<T>
- Wrapper containing deserialized value and optional key
- Methods: `value()`, `key()`, `into_value()`, `into_parts()`

## Usage Examples

### Basic Producer/Consumer
```rust
use ferrisstreams::{TypedKafkaProducer, TypedKafkaConsumer, JsonSerializer};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct OrderEvent {
    order_id: String,
    amount: f64,
    status: String,
}

// Producer
let producer = TypedKafkaProducer::<OrderEvent, _>::new(
    "localhost:9092",
    "orders",
    JsonSerializer,
)?;

let order = OrderEvent {
    order_id: "order-123".to_string(),
    amount: 99.99,
    status: "created".to_string(),
};

producer.send(Some("order-123"), &order, None).await?;

// Consumer
let consumer = TypedKafkaConsumer::<OrderEvent, _>::new(
    "localhost:9092",
    "order-processor",
    JsonSerializer,
);

consumer.subscribe(&["orders"]);

match consumer.poll_message(Duration::from_secs(5)).await {
    Ok(typed_message) => {
        println!("Order: {:?}", typed_message.value());
        // Process order...
    }
    Err(e) => println!("Error: {}", e),
}
```

### Builder Pattern
```rust
use ferrisstreams::{TypedProducerBuilder, TypedConsumerBuilder, JsonSerializer};

// Producer with builder
let producer = TypedProducerBuilder::<OrderEvent, _>::new(
    "localhost:9092",
    "orders",
    JsonSerializer,
).build()?;

// Consumer with builder
let consumer = TypedConsumerBuilder::<OrderEvent, _>::new(
    "localhost:9092",
    "order-processor",
    JsonSerializer,
).build();
```

### Streaming Messages
```rust
let mut stream = consumer.typed_stream();

while let Some(result) = stream.next_typed().await {
    match result {
        Ok(typed_message) => {
            println!("Received: {:?}", typed_message.value());
        }
        Err(e) => {
            println!("Stream error: {:?}", e);
            break;
        }
    }
}
```

### Convenience Trait
```rust
use ferrisstreams::KafkaConsumable;

// Use trait method for ergonomic consumer creation
let consumer = OrderEvent::consumer("localhost:9092", "my-group", JsonSerializer);
```

## Serialization Support

### Built-in Serializers
- **JsonSerializer**: JSON serialization using serde_json
- **AvroSerializer**: Avro serialization (feature-gated)
- **ProtoSerializer**: Protocol Buffers (feature-gated)

### Custom Serializers
Implement the `Serializer<T>` trait:

```rust
use ferrisstreams::{Serializer, SerializationError};

struct CustomSerializer;

impl<T> Serializer<T> for CustomSerializer 
where 
    T: MySerializeTrait + MyDeserializeTrait
{
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerializationError> {
        // Custom serialization logic
    }
    
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerializationError> {
        // Custom deserialization logic
    }
}
```

## Benefits

1. **Compile-Time Type Safety**: Impossible to deserialize wrong message type
2. **Reduced Boilerplate**: No manual serialization/deserialization code
3. **Better Error Handling**: Comprehensive error types with context
4. **Performance**: Zero-cost abstractions over raw Kafka operations
5. **Flexibility**: Support for custom serializers and contexts
6. **Ergonomics**: Clean, intuitive API design

## Migration Guide

### From Raw KafkaProducer
```rust
// Old way
let producer = KafkaProducer::new(brokers, topic)?;
let bytes = serde_json::to_vec(&message)?;
producer.send(key, &bytes, timestamp).await?;

// New way
let producer = TypedKafkaProducer::<MyMessage, _>::new(brokers, topic, JsonSerializer)?;
producer.send(key, &message, timestamp).await?;
```

### From Raw KafkaConsumer
```rust
// Old way
let consumer = KafkaConsumer::new(brokers, group_id);
let (bytes, key) = consumer.poll_message(timeout).await?;
let message: MyMessage = serde_json::from_slice(&bytes)?;

// New way
let consumer = TypedKafkaConsumer::<MyMessage, _>::new(brokers, group_id, JsonSerializer);
let typed_message = consumer.poll_message(timeout).await?;
let message = typed_message.value();
```

## Running Examples

```bash
# Basic typed example
cargo run --bin typed_example

# Comprehensive example
cargo run --example typed_kafka_example
```

## Testing

All type-safe components include comprehensive unit tests:

```bash
cargo test typed_producer
cargo test typed_consumer
```