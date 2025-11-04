//! # Advanced Builder Configuration Example
//!
//! This example demonstrates advanced producer and consumer configuration using:
//! - Builder patterns for flexible configuration
//! - Performance presets for common use cases  
//! - Custom configuration options
//! - Different optimization strategies (throughput, latency, durability)
//!
//! ## What You'll Learn
//! - When to use builders vs direct constructors
//! - Performance presets: high_throughput, low_latency, max_durability, development
//! - Custom configuration with compression, batching, retries
//! - Consumer configuration patterns
//! - Comparing different producer configurations
//!
//! ## Prerequisites
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example builder_configuration`
//!
//! ## Configuration Types Demonstrated
//! ### Producers:
//! - **Basic**: Default settings for most use cases
//! - **High-Throughput**: Optimized for maximum message volume
//! - **Low-Latency**: Optimized for minimal message delivery time
//! - **Max-Durability**: Optimized for data safety and fault tolerance
//! - **Custom**: Manual configuration of all parameters
//!
//! ### Consumers:
//! - **Basic**: Default settings for most use cases
//! - **High-Throughput**: Optimized for processing large message volumes
//! - **Streaming**: Optimized for continuous message processing
//! - **Development**: Easy debugging with auto-commit enabled
//!
//! ## Key Concepts
//! - **Performance Presets**: Pre-configured settings for common scenarios
//! - **Builder Pattern**: Flexible configuration construction
//! - **Client IDs**: Identifying producers and consumers in monitoring
//! - **Compression**: LZ4, Snappy, Gzip options for reducing network usage
//!
//! ## Next Steps
//! After this example, try:
//! - `fluent_api_example.rs` - Stream processing patterns
//! - Review test files for more configuration examples

use serde::{Deserialize, Serialize};
use std::time::Duration;
use velostream::FastConsumer;
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use velostream::velostream::kafka::performance_presets::PerformancePresets;
use velostream::velostream::kafka::producer_config::{AckMode, CompressionType, ProducerConfig};
use velostream::{Headers, JsonSerializer, KafkaProducer, ProducerBuilder};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct OrderEvent {
    order_id: u64,
    customer_id: String,
    amount: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Enhanced Builder Configuration Demo ===\n");

    // 1. Basic producer (uses defaults)
    println!("1. Basic Producer (with defaults):");
    let basic_producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "orders",
        JsonSerializer,
        JsonSerializer,
    )?;
    println!("✅ Created basic producer with default configuration\n");

    // 2. Producer with builder pattern and presets
    println!("2. High-Throughput Producer (using builder with preset):");
    let high_throughput_producer = ProducerBuilder::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "orders-ht",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("high-throughput-producer")
    .high_throughput() // Applies optimized settings
    .build()?;
    println!("✅ Created high-throughput producer\n");

    // 3. Producer with custom configuration
    println!("3. Custom Producer Configuration:");
    let config = ProducerConfig::new("localhost:9092", "orders-custom")
        .client_id("custom-producer")
        .compression(CompressionType::Lz4)
        .acks(AckMode::All)
        .batching(32768, Duration::from_millis(10)) // 32KB batches, 10ms linger
        .retries(5, Duration::from_millis(200))
        .custom_property("security.protocol", "PLAINTEXT")
        .custom_property("batch.size", "65536"); // Override batch size

    let custom_producer = ProducerBuilder::<String, OrderEvent, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .build()?;
    println!("✅ Created producer with custom configuration\n");

    // 4. Low-latency producer
    println!("4. Low-Latency Producer:");
    let low_latency_producer = ProducerBuilder::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "orders-ll",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("low-latency-producer")
    .low_latency() // Optimized for minimal latency
    .build()?;
    println!("✅ Created low-latency producer\n");

    // 5. Maximum durability producer
    println!("5. Maximum Durability Producer:");
    let durable_producer = ProducerBuilder::<String, OrderEvent, _, _>::new(
        "localhost:9092",
        "orders-durable",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("durable-producer")
    .max_durability() // Optimized for data safety
    .build()?;
    println!("✅ Created maximum durability producer\n");

    // 6. Consumer configuration examples
    println!("6. Consumer Configurations:");

    // Basic consumer
    let _basic_consumer = FastConsumer::<String, OrderEvent>::new(
        "localhost:9092",
        "order-processors",
        JsonSerializer,
        JsonSerializer,
    )?;
    println!("✅ Created basic consumer with defaults");

    // High-throughput consumer
    let _ht_consumer_config = ConsumerConfig::new("localhost:9092", "ht-processors")
        .client_id("ht-consumer")
        .high_throughput();

    let _ht_consumer = FastConsumer::<String, OrderEvent>::with_config(
        _ht_consumer_config,
        JsonSerializer,
        JsonSerializer,
    )?;
    println!("✅ Created high-throughput consumer");

    // Streaming consumer
    let _streaming_config = ConsumerConfig::new("localhost:9092", "stream-processors")
        .client_id("streaming-consumer")
        .auto_offset_reset(OffsetReset::Latest)
        .streaming(); // Optimized for continuous processing

    let _streaming_consumer = FastConsumer::<String, OrderEvent>::with_config(
        _streaming_config,
        JsonSerializer,
        JsonSerializer,
    )?;
    println!("✅ Created streaming consumer");

    // Development consumer
    let _dev_config = ConsumerConfig::new("localhost:9092", "dev-processors")
        .client_id("dev-consumer")
        .development() // Development-friendly settings
        .auto_commit(true, Duration::from_secs(1));

    let _dev_consumer = FastConsumer::<String, OrderEvent>::with_config(
        _dev_config,
        JsonSerializer,
        JsonSerializer,
    )?;
    println!("✅ Created development consumer\n");

    // 7. Demonstrate sending with different producers
    println!("7. Sending Messages with Different Producers:");

    let order = OrderEvent {
        order_id: 12345,
        customer_id: "customer-123".to_string(),
        amount: 99.99,
    };

    let headers = Headers::new()
        .insert("producer-type", "demo")
        .insert("example", "builder-configuration");

    // Send with different producers to show they all work
    let producers = vec![
        ("basic", &basic_producer),
        ("high-throughput", &high_throughput_producer),
        ("custom", &custom_producer),
        ("low-latency", &low_latency_producer),
        ("durable", &durable_producer),
    ];

    for (name, producer) in producers {
        match producer
            .send(
                Some(&format!("order-{}", order.order_id)),
                &order,
                headers.clone(),
                None,
            )
            .await
        {
            Ok(_) => println!("✅ Successfully sent with {} producer", name),
            Err(e) => println!("❌ Failed to send with {} producer: {}", name, e),
        }
    }

    println!("\n=== Configuration Demo Complete ===");
    println!("All producers and consumers created successfully!");
    println!("Each configuration optimizes for different use cases:");
    println!("  • Basic: Good defaults for most use cases");
    println!("  • High-throughput: Larger batches, optimized for volume");
    println!("  • Low-latency: Minimal batching, optimized for speed");
    println!("  • Max durability: All replicas, retries, optimized for safety");
    println!("  • Development: Easy debugging, auto-commit enabled");
    println!("  • Streaming: Continuous processing optimizations");

    Ok(())
}
