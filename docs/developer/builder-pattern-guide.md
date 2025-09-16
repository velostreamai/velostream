# Kafka Builder Pattern Guide

## Overview

The Kafka integration provides both **direct constructors** and **builder patterns** for creating producers and consumers. This guide explains when and how to use each approach.

## Quick Comparison

| Approach | Best For | Complexity | Flexibility |
|----------|----------|------------|-------------|
| **Direct Constructor** | Simple, standard use cases | Low | Limited |
| **Builder Pattern** | Advanced configuration, custom contexts | Medium | High |

## Direct Constructors (Recommended for Most Cases)

### ✅ **When to Use Direct Constructors**
- Standard producer/consumer setup
- Default logging and error handling is sufficient  
- Simple configuration needs
- Getting started quickly

### **Example: Direct Constructor**
```rust
use velostream::{KafkaProducer, KafkaConsumer, JsonSerializer};
use futures::StreamExt;
use rdkafka::message::Message;

// Simple and direct - covers 90% of use cases
let producer = KafkaProducer::<MyMessage, _>::new(
    "localhost:9092",
    "my-topic", 
    JsonSerializer,
)?;

let consumer = KafkaConsumer::<MyMessage, _>::new(
    "localhost:9092",
    "my-group",
    JsonSerializer,
)?;
consumer.subscribe(&["my-topic"])?;

// Recommended: Use streaming for message consumption with implicit deserialization
let mut stream = consumer.stream();
while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(my_message) => {
            // Message is already deserialized! Beautiful and clean.
            println!("Received: {:?}", my_message);
        }
        Err(e) => eprintln!("Stream error: {}", e),
    }
}
```

## Builder Pattern (Advanced Use Cases)

### ✅ **When to Use Builders**
- **Custom contexts** for advanced logging/monitoring
- **Complex configuration** that may grow over time
- **Step-by-step construction** with conditional logic
- **Advanced error handling** and delivery callbacks
- **Custom metrics collection**
- **Integration with monitoring systems**

### **Example: Basic Builder Usage**
```rust
use velostream::{ProducerBuilder, ConsumerBuilder, JsonSerializer};

// Equivalent to direct constructor, but more extensible
let producer = ProducerBuilder::<MyMessage, _>::new(
    "localhost:9092",
    "my-topic",
    JsonSerializer,
)
.build()?;

let consumer = ConsumerBuilder::<MyMessage, _>::new(
    "localhost:9092", 
    "my-group",
    JsonSerializer,
)
.build()?;
```

### **Example: Advanced Builder with Custom Context**

```rust
use velostream::{ProducerBuilder, JsonSerializer};
use rdkafka::{ClientContext, producer::ProducerContext};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Custom producer context for metrics collection
struct MetricsProducerContext {
    delivery_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
}

impl MetricsProducerContext {
    fn new() -> Self {
        Self {
            delivery_count: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_metrics(&self) -> (u64, u64) {
        (
            self.delivery_count.load(Ordering::Relaxed),
            self.error_count.load(Ordering::Relaxed),
        )
    }
}

impl ClientContext for MetricsProducerContext {
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        println!("[METRICS] [{:?}] {}: {}", level, fac, log_message);
    }
    
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        eprintln!("Kafka error: {} - {}", error, reason);
    }
}

impl ProducerContext for MetricsProducerContext {
    type DeliveryOpaque = ();
    
    fn delivery(&self, result: &rdkafka::message::DeliveryResult, _: ()) {
        match result {
            Ok(_) => { self.delivery_count.fetch_add(1, Ordering::Relaxed); }
            Err(_) => { self.error_count.fetch_add(1, Ordering::Relaxed); }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = MetricsProducerContext::new();
    let metrics = context.delivery_count.clone(); // Keep reference for monitoring
    
    // Build producer with custom context using builder pattern
    let producer = ProducerBuilder::<MyMessage, _, _>::new(
        "localhost:9092",
        "metrics-topic",
        JsonSerializer,
    )
    .with_context(context)  // This is only possible with builders!
    .build()?;
    
    // Send messages...
    producer.send(Some("key"), &my_message, None).await?;
    
    // Check metrics
    let (delivered, errors) = (
        metrics.load(Ordering::Relaxed),
        // ... get error count
    );
    println!("Delivered: {}, Errors: {}", delivered, errors);
    
    Ok(())
}
```

## Custom Consumer Context Example

```rust
use velostream::{ConsumerBuilder, JsonSerializer};
use rdkafka::{ClientContext, consumer::ConsumerContext};

struct MonitoringConsumerContext {
    client_id: String,
}

impl MonitoringConsumerContext {
    fn new(client_id: String) -> Self {
        Self { client_id }
    }
}

impl ClientContext for MonitoringConsumerContext {
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, message: &str) {
        println!("[{}] [{:?}] {}: {}", self.client_id, level, fac, message);
    }
}

impl ConsumerContext for MonitoringConsumerContext {
    fn pre_rebalance(&self, rebalance: &rdkafka::consumer::Rebalance) {
        println!("[{}] Pre-rebalance: {:?}", self.client_id, rebalance);
    }
    
    fn post_rebalance(&self, rebalance: &rdkafka::consumer::Rebalance) {
        println!("[{}] Post-rebalance: {:?}", self.client_id, rebalance);
    }
    
    fn commit_callback(
        &self, 
        result: rdkafka::error::KafkaResult<()>, 
        _offsets: &rdkafka::TopicPartitionList
    ) {
        match result {
            Ok(_) => println!("[{}] Commit successful", self.client_id),
            Err(e) => eprintln!("[{}] Commit failed: {}", self.client_id, e),
        }
    }
}

// Usage
let consumer = ConsumerBuilder::<MyMessage, _, _>::new(
    "localhost:9092",
    "monitoring-group", 
    JsonSerializer,
)
.with_context(MonitoringConsumerContext::new("my-consumer".to_string()))
.build()?;
```

## Configuration Flexibility Example

```rust
use velostream::{ProducerBuilder, JsonSerializer};

// Conditional configuration based on environment
fn create_producer(env: &str) -> Result<KafkaProducer<MyMessage, _>, Error> {
    let mut builder = ProducerBuilder::<MyMessage, _>::new(
        "localhost:9092",
        "my-topic",
        JsonSerializer,
    );
    
    // Add environment-specific configuration
    if env == "production" {
        let prod_context = ProductionProducerContext::new();
        builder = builder.with_context(prod_context);
    }
    
    builder.build()
}
```

## Consumer Usage Patterns

### **Streaming vs Polling (Recommended: Streaming)**

The consumer provides two ways to receive messages:

#### ✅ **Streaming (Recommended)**
```rust
use velostream::{KafkaConsumer, JsonSerializer};
use futures::StreamExt;
use rdkafka::message::Message;

let consumer = KafkaConsumer::<MyMessage, _>::new(
    "localhost:9092", 
    "my-group", 
    JsonSerializer
)?;
consumer.subscribe(&["my-topic"])?;

// Use streaming for efficient async processing with implicit deserialization
let mut stream = consumer.stream();
while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(my_message) => {
            // Message is already deserialized automatically!
            println!("Received: {:?}", my_message);
        }
        Err(e) => eprintln!("Stream error: {}", e),
    }
}
```

**Why prefer streaming:**
- ✅ **More efficient** - native rdkafka pattern
- ✅ **Better resource usage** - no timeout overhead per message
- ✅ **Async-friendly** - works naturally with tokio and futures
- ✅ **Backpressure handling** - automatic flow control
- ✅ **Production-ready** - used by most real-world applications

#### ⚠️ **Polling (Legacy Pattern)**
```rust
// Less efficient - avoid for new code
for _ in 0..100 {
    match consumer.poll(Duration::from_secs(1)).await {
        Ok(message) => {
            // Process message.into_value()
        }
        Err(_) => break, // Timeout or error
    }
}
```

**Why avoid polling:**
- ❌ **Less efficient** - timeout overhead on every call
- ❌ **Brittle** - requires manual timeout management  
- ❌ **Harder to scale** - tight coupling with timeout values
- ❌ **Not async-native** - doesn't integrate well with streams

### **Fluent API Patterns**

You can chain multiple operations together for powerful data processing:

```rust
use futures::StreamExt;
use rdkafka::message::Message;

// Pattern 1: Collect all messages - much simpler with implicit deserialization!
let messages = consumer.stream()
    .take(10) // Process only first 10 messages
    .filter_map(|msg_result| async move {
        // Automatic deserialization - just extract successful results!
        msg_result.ok()
    })
    .collect::<Vec<MyMessage>>()
    .await;

// Pattern 2: Filter and transform - beautifully clean!
let filtered_ids = consumer.stream()
    .take(100)
    .filter_map(|msg_result| async move {
        // Implicit deserialization - no manual work!
        msg_result.ok()
    })
    .filter(|message: &MyMessage| {
        // Only process messages with even IDs
        futures::future::ready(message.id % 2 == 0)
    })
    .map(|message| message.id) // Extract just the ID
    .collect::<Vec<u32>>()
    .await;

// Pattern 3: Process each message - super clean!
consumer.stream()
    .take(50)
    .filter_map(|msg_result| async move {
        // Automatic deserialization!
        msg_result.ok()
    })
    .for_each(|message| async move {
        // Process each message
        println!("Processing: {:?}", message);
        // ... business logic here
    })
    .await;
```

### **Recommendation**
Always use `consumer.stream()` for new applications - it now provides **implicit deserialization** making your code incredibly clean! Combine with `StreamExt` methods for powerful fluent processing. Only use `poll()` for simple examples or when integrating with legacy code that requires explicit polling.

### **✨ Implicit Deserialization Benefits**

The `stream()` method now returns `Result<T, ConsumerError>` directly:

```rust
// New beautifully simple API:
let messages: Vec<MyMessage> = consumer.stream()
    .take(10)
    .filter_map(|result| async move { result.ok() }) // Just extract successful results!
    .collect()
    .await;

// No more manual payload extraction or deserialization calls needed!
```

## Performance Considerations

### **Direct Constructor Performance**
- ✅ **Fastest creation** - minimal overhead
- ✅ **Least memory usage** - no intermediate objects  
- ✅ **Simplest code** - fewer allocations

### **Builder Pattern Performance**
- ⚠️ **Slightly slower creation** - additional allocation for builder
- ⚠️ **More memory during construction** - builder object overhead
- ✅ **Same runtime performance** - no difference after construction

### **Performance Test Results**
Based on functional tests (`builder_pattern_func_test.rs`):
- Direct construction: ~2-5ms per producer/consumer
- Builder construction: ~3-7ms per producer/consumer  
- **Overhead: ~1-2ms per construction** (negligible for most applications)

## When to Choose Each Approach

### ✅ **Use Direct Constructors When:**
- Building a simple application
- Default behavior is sufficient
- Getting started quickly
- Performance-critical tight loops creating many producers/consumers
- Following the 80/20 rule (most use cases)

### ✅ **Use Builder Pattern When:**
- Need custom contexts for logging/monitoring
- Building enterprise applications with observability requirements
- Configuration depends on runtime conditions
- Integrating with external monitoring systems (Prometheus, DataDog, etc.)
- Need advanced error handling and delivery callbacks
- Future configuration expansion is likely

## Migration Between Approaches

### **From Direct to Builder (Easy)**
```rust
// Before: Direct constructor
let producer = KafkaProducer::<MyMessage, _>::new(brokers, topic, serializer)?;

// After: Builder (same functionality)  
let producer = ProducerBuilder::<MyMessage, _>::new(brokers, topic, serializer)
    .build()?;
```

### **From Builder to Direct (Easy)**
```rust  
// Before: Basic builder
let producer = ProducerBuilder::<MyMessage, _>::new(brokers, topic, serializer)
    .build()?;

// After: Direct constructor (same functionality)
let producer = KafkaProducer::<MyMessage, _>::new(brokers, topic, serializer)?;
```

## Best Practices

### **1. Start Simple**
Begin with direct constructors and migrate to builders when you need advanced features:

```rust
// Phase 1: Start simple
let producer = KafkaProducer::<Message, _>::new(brokers, topic, JsonSerializer)?;

// Phase 2: Add monitoring when needed  
let producer = ProducerBuilder::<Message, _, _>::new(brokers, topic, JsonSerializer)
    .with_context(MonitoringContext::new())
    .build()?;
```

### **2. Use Builders for Production Systems**
Production systems often need observability:

```rust
let producer = ProducerBuilder::<Message, _, _>::new(brokers, topic, JsonSerializer)
    .with_context(ProductionContext::with_metrics())
    .build()?;
```

### **3. Keep Contexts Focused**
Design custom contexts for specific purposes:

```rust  
// Good: Focused context
struct MetricsContext { /* metrics only */ }

// Avoid: Kitchen sink context  
struct EverythingContext { /* metrics, logging, caching, etc. */ }
```

### **4. Test Both Paths**
If you use builders, test both simple and advanced configurations:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_simple_builder() {
        let producer = ProducerBuilder::<Msg, _>::new(brokers, topic, serializer)
            .build().unwrap();
        // test basic functionality
    }
    
    #[test] 
    fn test_custom_context_builder() {
        let producer = ProducerBuilder::<Msg, _, _>::new(brokers, topic, serializer)
            .with_context(CustomContext::new())
            .build().unwrap();
        // test advanced functionality
    }
}
```

## Summary

- **90% of applications** should use **direct constructors** for simplicity
- **10% of applications** need **builders** for advanced configuration  
- **Start simple** with direct constructors
- **Migrate to builders** when you need custom contexts
- **Both approaches** have the same runtime performance
- **Test your specific use case** to determine the best fit

The type-safe Kafka API is designed to make the simple case simple (direct constructors) while keeping the complex case possible (builder pattern).