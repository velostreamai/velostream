//! # Headers Usage Example
//!
//! This example demonstrates how to work with Kafka message headers in velostream:
//! - Creating and sending messages with rich header metadata  
//! - Consuming messages and accessing header information
//! - Using headers for message routing and metadata
//! - Best practices for header usage
//!
//! ## What You'll Learn
//! - How to create headers with the builder pattern
//! - Sending messages with headers using producers
//! - Accessing headers from consumed messages
//! - Iterating over all headers in a message
//! - Common header patterns (source, version, trace-id, etc.)
//!
//! ## Prerequisites  
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example headers_example`
//!
//! ## Key Concepts
//! - **Headers API**: Custom Headers type with HashMap backing
//! - **Metadata**: Headers carry metadata alongside the message payload
//! - **Builder Pattern**: Fluent API for creating headers
//! - **Optional Values**: Headers can have null values
//!
//! ## Use Cases Demonstrated
//! - **Source Attribution**: Identifying where messages originate
//! - **Versioning**: Including version information in headers
//! - **Tracing**: Distributed tracing with trace IDs
//! - **User Context**: Including user agent and session information
//!
//! ## Next Steps
//! After this example, try:
//! - `consumer_with_headers.rs` - More comprehensive header processing
//! - `fluent_api_example.rs` - Stream-based header filtering

use serde::{Deserialize, Serialize};
use std::time::Duration;
use velostream::{Headers, JsonSerializer, FastConsumer, KafkaProducer};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct OrderEvent {
    order_id: u64,
    customer_id: String,
    amount: f64,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = "localhost:9092";
    let topic = "headers-demo-topic";
    let group_id = "headers-demo-group";

    // Create producer with key/value serializers
    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        broker,
        topic,
        JsonSerializer,
        JsonSerializer,
    )?;

    // Create consumer with key/value serializers
    let consumer = FastConsumer::<String, OrderEvent>::from_brokers(
        broker,
        group_id,
        Box::new(JsonSerializer),
        Box::new(JsonSerializer),
    )?;

    consumer.subscribe(&[topic])?;

    // Create a test message
    let order = OrderEvent {
        order_id: 12345,
        customer_id: "cust_001".to_string(),
        amount: 99.99,
        status: "pending".to_string(),
    };

    // Create headers with metadata
    let headers = Headers::new()
        .insert("source", "web-frontend")
        .insert("version", "1.2.3")
        .insert("trace-id", "abc-123-def")
        .insert("user-agent", "Mozilla/5.0");

    println!("Sending message with headers:");
    println!("Key: order-{}", order.order_id);
    println!("Value: {:?}", order);
    println!("Headers: {:?}", headers);

    // Send message with headers
    producer
        .send(
            Some(&format!("order-{}", order.order_id)),
            &order,
            headers,
            None,
        )
        .await?;

    producer.flush(5000)?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Consume the message
    println!("\nConsuming message...");
    match consumer.poll(Duration::from_secs(5)).await {
        Ok(message) => {
            println!("Received message:");
            println!("Key: {:?}", message.key());
            println!("Value: {:?}", message.value());
            println!("Headers:");

            // Access individual headers
            if let Some(source) = message.headers().get("source") {
                println!("  source: {}", source);
            }
            if let Some(version) = message.headers().get("version") {
                println!("  version: {}", version);
            }
            if let Some(trace_id) = message.headers().get("trace-id") {
                println!("  trace-id: {}", trace_id);
            }
            if let Some(user_agent) = message.headers().get("user-agent") {
                println!("  user-agent: {}", user_agent);
            }

            // Iterate over all headers
            println!("All headers:");
            for (key, value) in message.headers().iter() {
                match value {
                    Some(v) => println!("  {}: {}", key, v),
                    None => println!("  {}: <null>", key),
                }
            }
        }
        Err(e) => {
            eprintln!("Error consuming message: {}", e);
        }
    }

    consumer.commit()?;
    println!("Example completed successfully!");

    Ok(())
}
