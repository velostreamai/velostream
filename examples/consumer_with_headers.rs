//! # Comprehensive Consumer with Headers Example
//!
//! This example provides a comprehensive demonstration of consuming Kafka messages 
//! with full access to keys, values, and headers:
//! - Multiple message types with different header patterns
//! - Comprehensive header processing and routing
//! - Different ways to extract and use message data
//! - Production-ready patterns for header-based processing
//!
//! ## What You'll Learn
//! - Advanced header creation with multiple metadata fields
//! - Consuming messages with complex header processing
//! - Different message extraction patterns (references vs owned)
//! - Header-based message routing and processing
//! - Best practices for production header usage
//!
//! ## Prerequisites
//! - Kafka running on localhost:9092  
//! - Run with: `cargo run --example consumer_with_headers`
//!
//! ## Key Features Demonstrated
//! - **Multiple Message Types**: Different products with varied headers
//! - **Rich Headers**: source, event-type, timestamp, session-id, app-version, platform
//! - **Message Processing**: Both reference access and owned consumption
//! - **UUID Topics**: Dynamic topic generation for isolation
//! - **Comprehensive Logging**: Detailed output showing all message components
//!
//! ## Production Patterns
//! - **Source Routing**: Messages from web-api, mobile-app, inventory-service
//! - **Event Types**: product-created, product-updated events
//! - **Tracing**: Session IDs and timestamps for debugging
//! - **Platform Context**: App versions and platform information
//!
//! ## Next Steps  
//! After this example, try:
//! - `builder_configuration.rs` - Advanced producer/consumer configuration
//! - `fluent_api_example.rs` - Stream-based processing with header filtering

use ferrisstreams::{
    KafkaProducer, KafkaConsumer, JsonSerializer, Headers
};
use serde::{Serialize, Deserialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct ProductEvent {
    product_id: u64,
    name: String,
    price: f64,
    category: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Kafka Consumer with Headers, Key, and Value Demo ===\n");

    let broker = "localhost:9092";
    let topic = format!("consumer-headers-demo-{}", Uuid::new_v4());
    let group_id = format!("consumer-headers-group-{}", Uuid::new_v4());

    // Create producer and consumer with String keys and ProductEvent values
    let producer = KafkaProducer::<String, ProductEvent, _, _>::new(
        broker, 
        &topic, 
        JsonSerializer, 
        JsonSerializer
    )?;

    let consumer = KafkaConsumer::<String, ProductEvent, _, _>::new(
        broker, 
        &group_id, 
        JsonSerializer, 
        JsonSerializer
    )?;

    consumer.subscribe(&[&topic])?;

    // Create test messages with different keys, values, and headers
    let messages = vec![
        (
            "product-001".to_string(),
            ProductEvent {
                product_id: 1,
                name: "Laptop".to_string(),
                price: 999.99,
                category: "Electronics".to_string(),
            },
            Headers::new()
                .insert("source", "inventory-service")
                .insert("event-type", "product-created")
                .insert("timestamp", "2024-01-15T10:30:00Z")
                .insert("user-id", "admin-123")
        ),
        (
            "product-002".to_string(),
            ProductEvent {
                product_id: 2,
                name: "Coffee Mug".to_string(),
                price: 12.50,
                category: "Kitchen".to_string(),
            },
            Headers::new()
                .insert("source", "web-api")
                .insert("event-type", "product-updated")
                .insert("timestamp", "2024-01-15T11:15:00Z")
                .insert("session-id", "sess-xyz-789")
        ),
        (
            "product-003".to_string(),
            ProductEvent {
                product_id: 3,
                name: "Book".to_string(),
                price: 24.99,
                category: "Education".to_string(),
            },
            Headers::new()
                .insert("source", "mobile-app")
                .insert("event-type", "product-created")
                .insert("timestamp", "2024-01-15T12:00:00Z")
                .insert("app-version", "2.1.0")
                .insert("platform", "iOS")
        ),
    ];

    // Send all messages
    println!("📤 Sending {} messages with keys, values, and headers...\n", messages.len());
    for (key, product, headers) in &messages {
        println!("Sending product: {} (key: {})", product.name, key);
        producer.send(Some(key), product, headers.clone(), None).await?;
    }

    producer.flush(5000)?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Consume messages and demonstrate accessing headers, keys, and values
    println!("📥 Consuming messages and showing headers, keys, and values...\n");
    
    let mut received_count = 0;
    let max_messages = messages.len();
    
    while received_count < max_messages {
        match consumer.poll(Duration::from_secs(5)).await {
            Ok(message) => {
                received_count += 1;
                
                println!("📨 Message {} of {}:", received_count, max_messages);
                println!("   🔑 Key: {:?}", message.key());
                println!("   📦 Value: {:?}", message.value());
                println!("   🏷️  Headers:");
                
                // Access individual headers
                if let Some(source) = message.headers().get("source") {
                    println!("      source: {}", source);
                }
                if let Some(event_type) = message.headers().get("event-type") {
                    println!("      event-type: {}", event_type);
                }
                if let Some(timestamp) = message.headers().get("timestamp") {
                    println!("      timestamp: {}", timestamp);
                }
                
                // Show all headers
                for (header_key, header_value) in message.headers().iter() {
                    match header_value {
                        Some(v) => println!("      {}: {}", header_key, v),
                        None => println!("      {}: <null>", header_key),
                    }
                }
                
                // Demonstrate different ways to extract data
                println!("   🔄 Extraction methods:");
                println!("      Key extracted: {:?}", message.key());
                println!("      Value.name: {}", message.value().name);
                println!("      Value.price: ${:.2}", message.value().price);
                println!("      Headers count: {}", message.headers().len());
                
                // Demonstrate consuming the message (extracting owned values)
                let (key, value, headers) = message.into_parts();
                println!("   ✅ Consumed into parts:");
                println!("      Owned key: {:?}", key);
                println!("      Owned value.category: {}", value.category);
                println!("      Owned headers.len(): {}", headers.len());
                
                println!("   {}", "─".repeat(50));
            }
            Err(e) => {
                eprintln!("❌ Error consuming message: {}", e);
                break;
            }
        }
    }

    consumer.commit()?;
    println!("\n✅ Consumer demo completed successfully!");
    println!("📊 Summary:");
    println!("   • Consumed {} messages", received_count);
    println!("   • Each message contained: Key (String), Value (ProductEvent), Headers (metadata)");
    println!("   • Headers included source, event-type, timestamp, and custom fields");
    println!("   • Demonstrated both reference access (.key(), .value(), .headers()) and owned consumption (.into_parts())");
    
    Ok(())
}