//! # Basic Producer/Consumer Example
//!
//! This example demonstrates the core functionality of velostream:
//! - Creating type-safe producers and consumers with JSON serialization
//! - Sending and receiving structured messages
//! - Basic message processing with polling pattern
//! - Working with keys, values, and headers
//! - Proper error handling and offset management
//!
//! ## What You'll Learn
//! - How to create producers and consumers with direct constructors
//! - Sending messages with automatic JSON serialization
//! - Consuming messages with automatic deserialization
//! - Processing different order statuses
//! - Committing offsets after processing
//!
//! ## Prerequisites
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example typed_kafka_example`
//!
//! ## Key Concepts
//! - **Type Safety**: Messages are strongly typed (OrderEvent)
//! - **Automatic Serialization**: JSON serialization happens transparently
//! - **Polling Pattern**: Traditional message-by-message consumption
//! - **Error Handling**: Proper handling of timeouts and errors
//!
//! ## Next Steps
//! After this example, try:
//! - `headers_example.rs` - Working with message headers
//! - `fluent_api_example.rs` - Stream-based processing (recommended for production)

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time;
use velostream::velostream::kafka::Headers;
use velostream::{JsonSerializer, FastConsumer, KafkaProducer};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct OrderEvent {
    pub order_id: String,
    pub customer_id: String,
    pub amount: f64,
    pub status: OrderStatus,
    pub timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum OrderStatus {
    Created,
    Paid,
    Shipped,
    Delivered,
    Cancelled,
}

impl OrderEvent {
    fn new(order_id: &str, customer_id: &str, amount: f64, status: OrderStatus) -> Self {
        Self {
            order_id: order_id.to_string(),
            customer_id: customer_id.to_string(),
            amount,
            status,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // Example 1: Type-safe producer with automatic serialization
    println!("=== Type-Safe Producer Example ===");

    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "order-events",
        JsonSerializer,
        JsonSerializer,
    )?;

    let order = OrderEvent::new("order-123", "customer-456", 99.99, OrderStatus::Created);

    match producer
        .send(Some(&"order-123".to_string()), &order, Headers::new(), None)
        .await
    {
        Ok(_delivery) => println!("✅ Order event sent successfully: {:?}", order),
        Err(e) => println!("❌ Failed to send order event: {}", e),
    }

    // Example 2: Type-safe consumer with automatic deserialization
    println!("\n=== Type-Safe Consumer Example ===");

    let consumer = FastConsumer::<String, OrderEvent>::new(
        "localhost:9092",
        "order-processor-group",
        JsonSerializer,
        JsonSerializer,
    )?;

    consumer.subscribe(&["order-events"])?;

    // Example 3: Processing messages with automatic deserialization
    println!("\n=== Message Processing Example ===");

    // Send a few more messages for demonstration
    for i in 1..=3 {
        let order = OrderEvent::new(
            &format!("order-{}", i),
            &format!("customer-{}", i * 100),
            (i as f64) * 25.0,
            match i % 3 {
                0 => OrderStatus::Created,
                1 => OrderStatus::Paid,
                _ => OrderStatus::Shipped,
            },
        );

        producer
            .send(Some(&format!("order-{}", i)), &order, Headers::new(), None)
            .await?;
        println!("📤 Sent order: {}", order.order_id);
    }

    // Flush producer to ensure all messages are sent
    producer.flush(5000)?;
    println!("✅ All messages flushed");

    // Wait a bit for messages to be available for consumption
    time::sleep(Duration::from_secs(2)).await;

    // Example 4: Consuming messages with timeout
    println!("\n=== Consuming Messages ===");

    let mut processed_count = 0;
    let max_messages = 5;

    for _i in 0..max_messages {
        match consumer.poll(Duration::from_secs(2)).await {
            Ok(message) => {
                let order_event = message.value();
                let key = message.key();

                println!(
                    "📥 Received order: key={:?}, order_id={}, amount={}, status={:?}",
                    key, order_event.order_id, order_event.amount, order_event.status
                );

                processed_count += 1;

                // Simulate processing
                match &order_event.status {
                    OrderStatus::Created => println!("   🔄 Processing new order..."),
                    OrderStatus::Paid => println!("   💳 Payment confirmed, preparing shipment..."),
                    OrderStatus::Shipped => println!("   🚚 Order shipped, tracking available..."),
                    OrderStatus::Delivered => println!("   ✅ Order delivered successfully!"),
                    OrderStatus::Cancelled => println!("   ❌ Order cancelled, refunding..."),
                }
            }
            Err(e) => {
                println!("⏰ No more messages or error: {:?}", e);
                break;
            }
        }
    }

    // Commit the processed messages
    if let Err(e) = consumer.commit() {
        println!("❌ Failed to commit offset: {}", e);
    } else {
        println!("✅ Committed {} processed messages", processed_count);
    }

    println!("\n=== Example Complete ===");
    println!(
        "Processed {} messages using type-safe Kafka integration",
        processed_count
    );

    Ok(())
}
