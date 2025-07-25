//! # JSON Performance Testing Example
//!
//! This example demonstrates high-throughput JSON message processing with performance monitoring:
//! - Configurable message sizes and batch configurations
//! - Real-time throughput measurement
//! - Memory-efficient JSON serialization
//! - Producer and consumer performance optimization
//! - Comprehensive performance metrics
//!
//! ## Performance Optimizations
//! - High-throughput presets for maximum message volume
//! - Efficient batching and compression
//! - Minimal memory allocations
//! - Real-time metrics collection
//!
//! ## Prerequisites
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example json_performance_test`
//!
//! ## Configuration Options
//! Modify constants below to test different scenarios:
//! - MESSAGE_COUNT: Total messages to send/receive
//! - BATCH_SIZE: Producer batch size
//! - MESSAGE_SIZE: Size category (Small/Medium/Large)
//! - CONCURRENT_PRODUCERS: Number of concurrent producers

use ferrisstreams::{ProducerBuilder, KafkaConsumer, JsonSerializer, Headers, Message, KafkaAdminClient};
use ferrisstreams::ferris::kafka::producer_config::{ProducerConfig, CompressionType, AckMode};
use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
use serde::{Serialize, Deserialize};
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use futures::StreamExt; // Add this import for stream extension traits

// Performance test configuration
const MESSAGE_COUNT: u64 = 10_000;
const BATCH_SIZE: u32 = 65536; // 64KB batches for high throughput
const CONCURRENT_PRODUCERS: usize = 4;
const CONSUMER_TIMEOUT: Duration = Duration::from_secs(30);

// Message size variants for testing different scenarios
#[derive(Debug, Clone, Copy)]
enum MessageSize {
    Small,   // ~100 bytes
    Medium,  // ~1KB  
    Large,   // ~10KB
}

impl MessageSize {
    fn generate_payload(&self) -> String {
        match self {
            Self::Small => "x".repeat(50),
            Self::Medium => "x".repeat(500),
            Self::Large => "x".repeat(5000),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PerformanceTestMessage {
    id: u64,
    timestamp: u64,
    producer_id: String,
    payload: String,
    metadata: TestMetadata,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TestMetadata {
    test_run: String,
    message_size: String,
    batch_number: u64,
}

#[derive(Debug)]
struct PerformanceMetrics {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    send_errors: AtomicU64,
    start_time: Instant,
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            send_errors: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_send(&self, message_size: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(message_size as u64, Ordering::Relaxed);
    }

    fn record_receive(&self, message_size: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(message_size as u64, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.send_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn get_throughput_stats(&self) -> ThroughputStats {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let sent = self.messages_sent.load(Ordering::Relaxed);
        let received = self.messages_received.load(Ordering::Relaxed);
        let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
        let bytes_received = self.bytes_received.load(Ordering::Relaxed);
        let errors = self.send_errors.load(Ordering::Relaxed);

        ThroughputStats {
            messages_per_second_sent: sent as f64 / elapsed,
            messages_per_second_received: received as f64 / elapsed,
            mbps_sent: (bytes_sent as f64 / 1_048_576.0) / elapsed,
            mbps_received: (bytes_received as f64 / 1_048_576.0) / elapsed,
            total_sent: sent,
            total_received: received,
            total_errors: errors,
            elapsed_seconds: elapsed,
        }
    }
}

#[derive(Debug)]
struct ThroughputStats {
    messages_per_second_sent: f64,
    messages_per_second_received: f64,
    mbps_sent: f64,
    mbps_received: f64,
    total_sent: u64,
    total_received: u64,
    total_errors: u64,
    elapsed_seconds: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    println!("🚀 JSON Performance Testing Suite");
    println!("==================================");
    println!("Messages: {}", MESSAGE_COUNT);
    println!("Batch Size: {} bytes", BATCH_SIZE);
    println!("Concurrent Producers: {}", CONCURRENT_PRODUCERS);
    println!();

    // Test different message sizes
    let test_scenarios = vec![
        ("Small Messages (~100B)", MessageSize::Small),
        ("Medium Messages (~1KB)", MessageSize::Medium),
        ("Large Messages (~10KB)", MessageSize::Large),
    ];

    for (scenario_name, message_size) in test_scenarios {
        println!("📊 Testing: {}", scenario_name);
        println!("─────────────────────────────────");
        
        match run_performance_test(message_size).await {
            Ok(stats) => print_performance_results(&stats, scenario_name),
            Err(e) => println!("❌ Test failed: {}", e),
        }
        
        println!();
        // Brief pause between tests
        sleep(Duration::from_secs(2)).await;
    }

    println!("✅ All performance tests completed!");
    Ok(())
}

async fn run_performance_test(message_size: MessageSize) -> Result<ThroughputStats, Box<dyn std::error::Error>> {
    let topic = format!("perf-test-{}", chrono::Utc::now().timestamp_millis());
    let metrics = Arc::new(PerformanceMetrics::new());
    
    // Create admin client and topic with 3 partitions
    println!("🔧 Creating topic '{}' with 3 partitions...", topic);
    let admin_client = KafkaAdminClient::new("localhost:9092")?;
    admin_client.create_performance_topic(&topic, 3).await?;

    // Verify topic was created with correct partition count
    let partition_count = admin_client.get_partition_count(&topic).await?;
    println!("✅ Topic created with {} partitions", partition_count);

    // Create high-throughput producer configuration
    let producer_config = ProducerConfig::new("localhost:9092", &topic)
        .client_id("perf-test-producer")
        .compression(CompressionType::Lz4)  // Fast compression
        .acks(AckMode::Leader)  // Balance between throughput and durability
        .batching(BATCH_SIZE, Duration::from_millis(5))  // Small linger for throughput
        .retries(3, Duration::from_millis(100))
        .high_throughput();  // Apply high-throughput preset with consolidated settings

    // Create multiple producers for concurrent sending
    let mut producers = Vec::new();
    for i in 0..CONCURRENT_PRODUCERS {
        let producer = ProducerBuilder::<String, PerformanceTestMessage, _, _>::with_config(
            producer_config.clone().client_id(&format!("perf-producer-{}", i)),
            JsonSerializer,
            JsonSerializer,
        ).build()?;
        producers.push(producer);
    }

    // Create high-throughput consumer
    let consumer_config = ConsumerConfig::new("localhost:9092", "perf-test-group")
        .client_id("perf-test-consumer")
        .auto_offset_reset(OffsetReset::Earliest)
        .max_poll_records(1000)  // Process many messages per poll
        .fetch_min_bytes(1024)   // Wait for reasonable batch size
        .fetch_max_wait(Duration::from_millis(500))
        .high_throughput();  // Apply high-throughput preset

    let consumer = KafkaConsumer::<String, PerformanceTestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )?;

    // Subscribe to topic
    consumer.subscribe(&[&topic])?;

    // Start consumer task
    let consumer_metrics = metrics.clone();
    let consumer_task = tokio::spawn(async move {
        let timeout_future = tokio::time::sleep(CONSUMER_TIMEOUT);
        tokio::pin!(timeout_future);

        // Use stream instead of polling
        let stream_future = consumer.stream()
            .take(MESSAGE_COUNT as usize) // Limit to expected message count
            .for_each(|message_result| {
                let metrics = consumer_metrics.clone();
                async move {
                    match message_result {
                        Ok(message) => {
                            let message_size = estimate_message_size(&message);
                            metrics.record_receive(message_size);

                            // Log progress every 1000 messages
                            let count = metrics.messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                            if count % 1000 == 0 {
                                let stats = metrics.get_throughput_stats();
                                println!("📈 Progress: {}/{} msgs ({:.1} msg/s)",
                                    count, MESSAGE_COUNT, stats.messages_per_second_received);
                            }
                        }
                        Err(e) => {
                            println!("❌ Consumer error: {}", e);
                        }
                    }
                }
            });

        // Race between the timeout and stream completion
        tokio::select! {
            _ = timeout_future => {
                println!("⚠️ Consumer timeout reached");
            }
            _ = stream_future => {
                println!("✅ Received all expected messages");
            }
        }

        println!("✅ Received {} messages", consumer_metrics.messages_received.load(std::sync::atomic::Ordering::Relaxed));
    });

    // Start producer tasks
    let semaphore = Arc::new(Semaphore::new(CONCURRENT_PRODUCERS));
    let messages_per_producer = MESSAGE_COUNT / CONCURRENT_PRODUCERS as u64;
    let mut producer_tasks = Vec::new();

    for (producer_id, producer) in producers.into_iter().enumerate() {
        let producer_metrics = metrics.clone();
        let sem = semaphore.clone();
        let producer_id_str = format!("producer-{}", producer_id);
        
        let task = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            for i in 0..messages_per_producer {
                let message = PerformanceTestMessage {
                    id: (producer_id as u64 * messages_per_producer) + i,
                    timestamp: chrono::Utc::now().timestamp_millis() as u64,
                    producer_id: producer_id_str.clone(),
                    payload: message_size.generate_payload(),
                    metadata: TestMetadata {
                        test_run: format!("perf-test-{}", chrono::Utc::now().timestamp()),
                        message_size: format!("{:?}", message_size),
                        batch_number: i / 100, // Group messages into batches of 100
                    },
                };

                let headers = Headers::new()
                    .insert("test-type", "performance")
                    .insert("producer-id", &producer_id_str)
                    .insert("message-size", &format!("{:?}", message_size));

                let key = format!("key-{}", message.id);
                let estimated_size = estimate_json_size(&message);

                match producer.send(Some(&key), &message, headers, None).await {
                    Ok(_) => {
                        producer_metrics.record_send(estimated_size);
                    }
                    Err(e) => {
                        producer_metrics.record_error();
                        println!("❌ Send error: {}", e);
                    }
                }

                // Brief pause every 100 messages to avoid overloading
                if i % 100 == 0 && i > 0 {
                    tokio::task::yield_now().await;
                }
            }
            
            println!("✅ Producer {} completed sending {} messages", producer_id, messages_per_producer);
        });
        
        producer_tasks.push(task);
    }

    // Wait for all producers to complete
    for task in producer_tasks {
        task.await?;
    }

    println!("📤 All producers finished, waiting for consumer...");

    // Wait for consumer to finish or timeout
    consumer_task.await?;

    Ok(metrics.get_throughput_stats())
}

fn estimate_message_size<K, V>(message: &Message<K, V>) -> usize {
    // Rough estimation of message size including key, value, headers, and metadata
    let key_size = match message.key() {
        Some(_) => 20, // Estimated key size
        None => 0,
    };
    let value_size = 1000; // Rough JSON message size estimate - value is always present
    let headers_size = message.headers().len() * 50; // Rough header overhead
    key_size + value_size + headers_size
}

fn estimate_json_size(message: &PerformanceTestMessage) -> usize {
    // Rough estimation of JSON serialized size
    50 + // base object overhead
    8 + // id field
    20 + // timestamp field
    message.producer_id.len() +
    message.payload.len() +
    100 // metadata overhead
}

fn print_performance_results(stats: &ThroughputStats, scenario: &str) {
    println!("📊 {} Results:", scenario);
    println!("   Messages Sent:     {:8} ({:.1} msg/s)", stats.total_sent, stats.messages_per_second_sent);
    println!("   Messages Received: {:8} ({:.1} msg/s)", stats.total_received, stats.messages_per_second_received);
    println!("   Throughput (Send): {:8.2} MB/s", stats.mbps_sent);
    println!("   Throughput (Recv): {:8.2} MB/s", stats.mbps_received);
    println!("   Errors:            {:8}", stats.total_errors);
    println!("   Duration:          {:8.1} seconds", stats.elapsed_seconds);
    
    if stats.total_received > 0 {
        let success_rate = (stats.total_received as f64 / stats.total_sent as f64) * 100.0;
        println!("   Success Rate:      {:8.1}%", success_rate);
    }
    
    // Performance rating
    let rating = if stats.messages_per_second_sent > 5000.0 {
        "🚀 Excellent"
    } else if stats.messages_per_second_sent > 2000.0 {
        "✅ Good"
    } else if stats.messages_per_second_sent > 500.0 {
        "⚡ Moderate"
    } else {
        "🐌 Needs Optimization"
    };
    println!("   Performance:       {}", rating);
}