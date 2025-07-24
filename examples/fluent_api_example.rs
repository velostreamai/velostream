//! # Fluent API & Stream Processing Example
//!
//! This example showcases the power of ferrisstreams' fluent API and stream processing:
//! - Stream-based message consumption (recommended for production)
//! - Functional programming patterns with method chaining
//! - Complex filtering and transformation operations
//! - Header-based routing and processing
//! - Asynchronous stream processing pipelines
//!
//! ## What You'll Learn
//! - Stream processing vs polling patterns
//! - Filtering messages by headers and content
//! - Transforming messages to business objects
//! - Collecting, folding, and reducing streams
//! - Error handling in stream contexts
//! - Async processing with different priorities
//! - Method chaining for readable, maintainable code
//!
//! ## Prerequisites
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example fluent_api_example`
//!
//! ## Stream Processing Patterns Demonstrated
//! 1. **Basic Collection**: Simple stream collection with `take()` and `collect()`
//! 2. **Header Filtering**: Filter messages by priority, source, region
//! 3. **Transformation**: Convert messages to business objects with tax calculation
//! 4. **Complex Filtering**: Multiple filter conditions with method chaining
//! 5. **Async Processing**: Priority-based processing with different timings
//! 6. **Error Handling**: Graceful error handling in stream processing
//! 7. **Folding/Reducing**: Aggregate stream results (sum, count, etc.)
//!
//! ## Why Use Streams Over Polling?
//! - ✅ **More Efficient**: No timeout overhead per message
//! - ✅ **Better Resource Usage**: Native rdkafka streaming pattern
//! - ✅ **Async-Friendly**: Works naturally with tokio and futures
//! - ✅ **Backpressure**: Automatic flow control
//! - ✅ **Production-Ready**: Used by most real-world applications
//! - ✅ **Composable**: Easy to chain operations and build pipelines
//!
//! ## Key APIs Demonstrated
//! - `consumer.stream()` - Create message stream
//! - `take(n)` - Limit number of messages
//! - `filter()` - Filter messages by conditions
//! - `map()` - Transform messages
//! - `for_each()` - Process each message
//! - `collect()` - Collect into vectors
//! - `fold()` - Reduce stream to single value
//!
//! ## Production Benefits
//! This pattern is **recommended for production** because it:
//! - Provides automatic deserialization
//! - Enables powerful functional composition
//! - Handles backpressure automatically
//! - Integrates seamlessly with async/await
//! - Reduces boilerplate code significantly

use ferrisstreams::{KafkaProducer, KafkaConsumer, JsonSerializer, Headers};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use futures::StreamExt;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct OrderEvent {
    order_id: u64,
    customer_id: String,
    amount: f64,
    status: OrderStatus,
    category: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
enum OrderStatus {
    Created,
    Paid,
    Shipped,
    Delivered,
    Cancelled,
}

impl OrderEvent {
    fn new(order_id: u64, customer_id: &str, amount: f64, status: OrderStatus, category: &str) -> Self {
        Self {
            order_id,
            customer_id: customer_id.to_string(),
            amount,
            status,
            category: category.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct ProcessedOrder {
    order_id: u64,
    total_amount: f64,
    source: String,
    priority: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Fluent API & Stream Processing Demo ===\n");

    let broker = "localhost:9092";
    let topic = format!("fluent-demo-{}", Uuid::new_v4());
    let group_id = format!("fluent-group-{}", Uuid::new_v4());

    // Create producer and consumer
    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        broker, 
        &topic, 
        JsonSerializer, 
        JsonSerializer
    )?;

    let consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker, 
        &group_id, 
        JsonSerializer, 
        JsonSerializer
    )?;

    consumer.subscribe(&[&topic])?;

    // Generate sample orders with different priorities and sources
    let orders = vec![
        (
            OrderEvent::new(1001, "customer-1", 150.99, OrderStatus::Created, "electronics"),
            Headers::new()
                .insert("source", "web-api")
                .insert("priority", "high")
                .insert("region", "us-west")
        ),
        (
            OrderEvent::new(1002, "customer-2", 25.50, OrderStatus::Paid, "books"),
            Headers::new()
                .insert("source", "mobile-app")
                .insert("priority", "normal")
                .insert("region", "us-east")
        ),
        (
            OrderEvent::new(1003, "customer-3", 299.99, OrderStatus::Created, "electronics"),
            Headers::new()
                .insert("source", "web-api")
                .insert("priority", "high")
                .insert("region", "eu-west")
        ),
        (
            OrderEvent::new(1004, "customer-4", 12.99, OrderStatus::Shipped, "books"),
            Headers::new()
                .insert("source", "api")
                .insert("priority", "low")
                .insert("region", "us-west")
        ),
        (
            OrderEvent::new(1005, "customer-5", 450.00, OrderStatus::Created, "furniture"),
            Headers::new()
                .insert("source", "web-api")
                .insert("priority", "high")
                .insert("region", "us-east")
        ),
        (
            OrderEvent::new(1006, "customer-1", 75.25, OrderStatus::Paid, "clothing"),
            Headers::new()
                .insert("source", "mobile-app")
                .insert("priority", "normal")
                .insert("region", "us-west")
        ),
    ];

    // Send all orders
    println!("📤 Sending {} orders with metadata...", orders.len());
    for (order, headers) in &orders {
        producer.send(
            Some(&format!("order-{}", order.order_id)),
            order,
            headers.clone(),
            None
        ).await?;
    }
    producer.flush(5000)?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("✅ All orders sent successfully!\n");

    // Example 1: Basic streaming with take and collect
    println!("🌊 Example 1: Basic Stream Collection");
    let all_messages: Vec<_> = consumer.stream()
        .take(3) // Take only first 3 messages
        .filter_map(|result| async move { result.ok() })
        .collect()
        .await;

    println!("Collected {} messages:", all_messages.len());
    for message in &all_messages {
        println!("  📦 Order {}: ${:.2} from {}", 
            message.value().order_id, 
            message.value().amount,
            message.headers().get("source").unwrap_or("unknown")
        );
    }
    println!();

    // Example 2: Filter by headers - High Priority Only
    println!("🔥 Example 2: High Priority Orders Only");
    let high_priority_orders: Vec<_> = consumer.stream()
        .take(6) // Process remaining messages
        .filter_map(|result| async move { result.ok() })
        .filter(|message| {
            futures::future::ready(
                message.headers().get("priority") == Some("high")
            )
        })
        .collect()
        .await;

    println!("Found {} high priority orders:", high_priority_orders.len());
    for message in &high_priority_orders {
        println!("  🚨 Order {}: ${:.2} ({})", 
            message.value().order_id, 
            message.value().amount,
            message.value().category
        );
    }
    println!();

    // Example 3: Transform and process - Convert to business objects
    println!("🔄 Example 3: Transform to Business Objects");
    
    // Create new consumer for fresh stream
    let consumer2 = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &format!("fluent-group2-{}", Uuid::new_v4()),
        JsonSerializer,
        JsonSerializer,
    )?;
    consumer2.subscribe(&[&topic])?;

    let processed_orders: Vec<ProcessedOrder> = consumer2.stream()
        .take(6)
        .filter_map(|result| async move { result.ok() })
        .map(|message| {
            ProcessedOrder {
                order_id: message.value().order_id,
                total_amount: message.value().amount * 1.08, // Add tax
                source: message.headers().get("source").unwrap_or("unknown").to_string(),
                priority: message.headers().get("priority").unwrap_or("normal").to_string(),
            }
        })
        .collect()
        .await;

    println!("Processed {} orders with tax calculation:", processed_orders.len());
    for order in &processed_orders {
        println!("  💰 Order {}: ${:.2} (with tax) from {} [{}]", 
            order.order_id, 
            order.total_amount,
            order.source,
            order.priority
        );
    }
    println!();

    // Example 4: Complex filtering and grouping
    println!("📊 Example 4: Complex Stream Processing - Web API Orders Only");
    
    let consumer3 = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &format!("fluent-group3-{}", Uuid::new_v4()),
        JsonSerializer,
        JsonSerializer,
    )?;
    consumer3.subscribe(&[&topic])?;

    let web_orders: Vec<_> = consumer3.stream()
        .take(6)
        .filter_map(|result| async move { result.ok() })
        .filter(|message| {
            futures::future::ready(
                message.headers().get("source") == Some("web-api")
            )
        })
        .filter(|message| {
            futures::future::ready(
                message.value().amount > 100.0 // Orders over $100
            )
        })
        .map(|message| {
            (
                message.value().clone(),
                message.headers().get("region").unwrap_or("unknown").to_string()
            )
        })
        .collect()
        .await;

    println!("Found {} high-value web orders:", web_orders.len());
    for (order, region) in &web_orders {
        println!("  🌐 Order {} in {}: ${:.2} ({})", 
            order.order_id, 
            region,
            order.amount,
            order.category
        );
    }
    println!();

    // Example 5: Asynchronous processing with for_each
    println!("⚡ Example 5: Async Processing Pipeline");
    
    let consumer4 = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &format!("fluent-group4-{}", Uuid::new_v4()),
        JsonSerializer,
        JsonSerializer,
    )?;
    consumer4.subscribe(&[&topic])?;

    consumer4.stream()
        .take(6)
        .filter_map(|result| async move { result.ok() })
        .for_each(|message| async move {
            // Simulate async processing
            let processing_time = match message.headers().get("priority").unwrap_or("normal") {
                "high" => Duration::from_millis(100),
                "normal" => Duration::from_millis(200),
                _ => Duration::from_millis(300),
            };
            
            tokio::time::sleep(processing_time).await;
            
            println!("  ⚙️  Processed order {} from {} ({:?} processing)", 
                message.value().order_id,
                message.headers().get("source").unwrap_or("unknown"),
                processing_time
            );
        })
        .await;

    println!();

    // Example 6: Error handling in streams
    println!("🛡️  Example 6: Stream Error Handling");
    
    let consumer5 = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &format!("fluent-group5-{}", Uuid::new_v4()),
        JsonSerializer,
        JsonSerializer,
    )?;
    consumer5.subscribe(&[&topic])?;

    let mut successful_count = 0;
    let mut error_count = 0;

    consumer5.stream()
        .take(6)
        .for_each(|result| async {
            match result {
                Ok(message) => {
                    successful_count += 1;
                    println!("  ✅ Successfully processed order {}", message.value().order_id);
                }
                Err(e) => {
                    error_count += 1;
                    println!("  ❌ Error processing message: {}", e);
                }
            }
        })
        .await;

    println!("📈 Processing Summary: {} successful, {} errors\n", successful_count, error_count);

    // Example 7: Chain multiple stream operations
    println!("🔗 Example 7: Chained Stream Operations");
    
    let consumer6 = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &format!("fluent-group6-{}", Uuid::new_v4()),
        JsonSerializer,
        JsonSerializer,
    )?;
    consumer6.subscribe(&[&topic])?;

    let summary = consumer6.stream()
        .take(6)
        .filter_map(|result| async move { result.ok() })
        .filter(|message| {
            futures::future::ready(
                matches!(message.value().status, OrderStatus::Created | OrderStatus::Paid)
            )
        })
        .map(|message| message.value().amount)
        .fold(0.0, |acc, amount| async move { acc + amount })
        .await;

    println!("💵 Total value of Created/Paid orders: ${:.2}", summary);

    println!("\n=== Fluent API Demo Complete ===");
    println!("🎯 Demonstrated Features:");
    println!("  ✅ Stream-based consumption with automatic deserialization");
    println!("  ✅ Filtering by headers and message content");
    println!("  ✅ Transforming messages to business objects");
    println!("  ✅ Complex stream processing pipelines");
    println!("  ✅ Asynchronous processing with for_each");
    println!("  ✅ Error handling in streams");
    println!("  ✅ Folding/reducing stream results");
    println!("  ✅ Method chaining for readable, functional code");
    println!("\n🚀 The fluent API provides powerful, composable stream processing!");
    println!("🔥 Use .stream() for production applications - it's more efficient than polling!");

    Ok(())
}