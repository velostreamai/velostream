//! # Raw Bytes Performance Testing Example
//!
//! This example demonstrates maximum throughput by using raw bytes and avoiding serialization overhead:
//! - Uses consumer.raw_stream() for direct Kafka message access
//! - Sends raw bytes instead of serialized JSON
//! - Minimal memory allocations and processing overhead
//! - Real-time throughput measurement for raw performance
//! - Comprehensive performance metrics for bytes-per-second
//!
//! ## Performance Optimizations
//! - No serialization/deserialization overhead
//! - Direct access to Kafka message payloads
//! - Efficient batching and compression at Kafka level
//! - Raw bytes measurement for true network throughput
//!
//! ## Prerequisites
//! - Kafka running on localhost:9092
//! - Run with: `cargo run --example raw_bytes_performance_test`
//!
//! ## Configuration Options
//! Modify constants below to test different scenarios:
//! - MESSAGE_COUNT: Total messages to send/receive
//! - BATCH_SIZE: Producer batch size
//! - PAYLOAD_SIZE: Size of raw byte payload
//! - CONCURRENT_PRODUCERS: Number of concurrent producers

use ferrisstreams::{ProducerBuilder, KafkaConsumer, Headers, KafkaAdminClient};
use ferrisstreams::ferris::kafka::producer_config::{ProducerConfig, CompressionType, AckMode};
use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
use ferrisstreams::ferris::kafka::serialization::{BytesSerializer, StringSerializer};

use futures::StreamExt;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;
use tokio::time::sleep;

// Performance test configuration
const MESSAGE_COUNT: u64 = 50_000;  // Higher count for raw performance
const BATCH_SIZE: u32 = 131072; // 128KB batches for maximum throughput
const CONCURRENT_PRODUCERS: usize = 4;
const CONSUMER_TIMEOUT: Duration = Duration::from_secs(60);

// Message size variants for testing different scenarios
#[derive(Debug, Clone, Copy)]
enum PayloadSize {
    Tiny,    // 64 bytes
    Small,   // 512 bytes
    Medium,  // 4KB  
    Large,   // 32KB
}

impl PayloadSize {
    fn generate_payload(&self) -> Vec<u8> {
        match self {
            Self::Tiny => vec![0x42; 64],
            Self::Small => vec![0x42; 512],
            Self::Medium => vec![0x42; 4096],
            Self::Large => vec![0x42; 32768],
        }
    }

    fn size_bytes(&self) -> usize {
        match self {
            Self::Tiny => 64,
            Self::Small => 512,
            Self::Medium => 4096,
            Self::Large => 32768,
        }
    }
}

#[derive(Debug)]
struct RawPerformanceMetrics {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    send_errors: AtomicU64,
    start_time: Instant,
}

impl RawPerformanceMetrics {
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

    fn get_throughput_stats(&self) -> RawThroughputStats {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let sent = self.messages_sent.load(Ordering::Relaxed);
        let received = self.messages_received.load(Ordering::Relaxed);
        let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
        let bytes_received = self.bytes_received.load(Ordering::Relaxed);
        let errors = self.send_errors.load(Ordering::Relaxed);

        RawThroughputStats {
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
struct RawThroughputStats {
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
    
    println!("🔥 Raw Bytes Performance Testing Suite");
    println!("=====================================");
    println!("Messages: {}", MESSAGE_COUNT);
    println!("Batch Size: {} bytes", BATCH_SIZE);
    println!("Concurrent Producers: {}", CONCURRENT_PRODUCERS);
    println!("⚡ Using raw bytes - NO serialization overhead!");
    println!();

    // Test different payload sizes
    let test_scenarios = vec![
        ("Tiny Payloads (64B)", PayloadSize::Tiny),
        ("Small Payloads (512B)", PayloadSize::Small),
        ("Medium Payloads (4KB)", PayloadSize::Medium),
        ("Large Payloads (32KB)", PayloadSize::Large),
    ];

    for (scenario_name, payload_size) in test_scenarios {
        println!("🚀 Testing: {}", scenario_name);
        println!("─────────────���───────────────────");

        match run_raw_performance_test(payload_size).await {
            Ok(stats) => print_raw_performance_results(&stats, scenario_name),
            Err(e) => println!("❌ Test failed: {}", e),
        }
        
        println!();
        // Brief pause between tests
        sleep(Duration::from_secs(2)).await;
    }

    println!("🏁 All raw performance tests completed!");
    Ok(())
}

async fn run_raw_performance_test(payload_size: PayloadSize) -> Result<RawThroughputStats, Box<dyn std::error::Error>> {
    let topic = format!("raw-perf-test-{}", chrono::Utc::now().timestamp_millis());
    let metrics = Arc::new(RawPerformanceMetrics::new());
    
    // Create admin client and topic with 3 partitions
    println!("🔧 Creating raw topic '{}' with 3 partitions...", topic);
    let admin_client = KafkaAdminClient::new("localhost:9092")?;
    admin_client.create_performance_topic(&topic, 3).await?;

    // Verify topic was created with correct partition count
    let partition_count = admin_client.get_partition_count(&topic).await?;
    println!("✅ Raw topic created with {} partitions", partition_count);

    // Create high-throughput producer configuration using FerrisStreams
    let producer_config = ProducerConfig::new("localhost:9092", &topic)
        .client_id("raw-perf-producer")
        .compression(CompressionType::Lz4)  // Fast compression
        .acks(AckMode::Leader)  // Balance between throughput and durability
        .batching(BATCH_SIZE, Duration::from_millis(5))  // Small linger for throughput
        .retries(3, Duration::from_millis(100))
        .high_throughput();  // Apply high-throughput preset with consolidated settings

    let key_serializer = StringSerializer;
    let value_serializer = BytesSerializer;
    let producer = ProducerBuilder::<String, Vec<u8>, _, _>::with_config(
        producer_config,
        key_serializer.clone(),
        value_serializer.clone(),
    ).build()?;

    // Create high-throughput consumer
    let consumer_config = ConsumerConfig::new("localhost:9092", "raw-perf-group")
        .client_id("raw-perf-consumer")
        .auto_offset_reset(OffsetReset::Earliest)
        .max_poll_records(1000)  // Process many messages per poll
        .fetch_min_bytes(50000)   // Wait for reasonable batch size
        .fetch_max_wait(Duration::from_millis(100))
        .high_throughput();  // Apply high-throughput preset with consolidated settings

    let consumer = KafkaConsumer::<String, Vec<u8>, _, _>::with_config(
        consumer_config,
        key_serializer.clone(),
        value_serializer.clone(),
    )?;

    // Subscribe to topic
    consumer.subscribe(&[&topic])?;

    // Start consumer task using raw_stream
    let consumer_metrics = metrics.clone();
    let consumer_task = tokio::spawn(async move {
        let timeout_future = tokio::time::sleep(CONSUMER_TIMEOUT);
        tokio::pin!(timeout_future);

        // Use stream instead of polling for better performance
        let stream_future = consumer.stream()
            .take(MESSAGE_COUNT as usize) // Limit to expected message count
            .for_each(|message_result| {
                let metrics = consumer_metrics.clone();
                async move {
                    match message_result {
                        Ok(message) => {
                            // Access the raw bytes directly
                            let payload = message.value();
                            let payload_size = payload.len();
                            metrics.record_receive(payload_size);

                            // Log progress every 5000 messages
                            let count = metrics.messages_received.fetch_add(1, Ordering::Relaxed) + 1;
                            if count % 5000 == 0 {
                                let stats = metrics.get_throughput_stats();
                                println!("🔥 Raw Progress: {}/{} msgs ({:.1} msg/s, {:.1} MB/s)",
                                    count, MESSAGE_COUNT,
                                    stats.messages_per_second_received,
                                    stats.mbps_received);
                            }
                        }
                        Err(e) => {
                            println!("❌ Raw consumer error: {}", e);
                        }
                    }
                }
            });

        // Race between timeout and stream completion
        tokio::select! {
            _ = timeout_future => {
                println!("⚠️ Raw consumer timeout reached");
            }
            _ = stream_future => {
                println!("✅ Raw consumer received all expected messages");
            }
        }

        println!("🔥 Raw consumer processed {} messages", 
            consumer_metrics.messages_received.load(Ordering::Relaxed));
    });

    // Start producer tasks
    let semaphore = Arc::new(Semaphore::new(CONCURRENT_PRODUCERS));
    let messages_per_producer = MESSAGE_COUNT / CONCURRENT_PRODUCERS as u64;
    let mut producer_tasks = Vec::new();

    // Generate payload once for reuse
    let raw_payload = payload_size.generate_payload();
    let payload_size_bytes = payload_size.size_bytes();

    // Removed the `.clone()` call and used an `Arc` to share the producer instance across tasks.
    let producer = Arc::new(producer);

    for producer_id in 0..CONCURRENT_PRODUCERS {
        let producer_metrics = metrics.clone();
        let sem = semaphore.clone();
        let producer_clone = producer.clone(); // Use the Arc clone here
        let payload_clone = raw_payload.clone();
        let producer_id_str = format!("producer-{}", producer_id);

        let task = tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            
            for i in 0..messages_per_producer {
                let message_id = (producer_id as u64 * messages_per_producer) + i;
                let key = format!("raw-key-{}", message_id);
                
                // Create headers using FerrisStreams Headers
                let headers = Headers::new()
                    .insert("test-type", "raw-performance")
                    .insert("producer-id", &producer_id_str)
                    .insert("message-id", &message_id.to_string())
                    .insert("payload-size", &payload_size_bytes.to_string());

                // Send raw bytes directly using FerrisStreams producer
                match producer_clone.send(Some(&key), &payload_clone, headers, None).await {
                    Ok(_) => {
                        producer_metrics.record_send(payload_size_bytes);
                    }
                    Err(e) => {
                        producer_metrics.record_error();
                        println!("❌ Raw send error: {}", e);
                    }
                }

                // Brief yield every 1000 messages to avoid overloading
                if i % 1000 == 0 && i > 0 {
                    tokio::task::yield_now().await;
                }
            }
            
            println!("🔥 Raw producer {} completed sending {} messages", 
                producer_id, messages_per_producer);
        });
        
        producer_tasks.push(task);
    }

    // Wait for all producers to complete
    for task in producer_tasks {
        task.await?;
    }

    println!("📤 All raw producers finished, waiting for consumer...");

    // Wait for consumer to finish or timeout
    consumer_task.await?;

    Ok(metrics.get_throughput_stats())
}

fn print_raw_performance_results(stats: &RawThroughputStats, scenario: &str) {
    println!("🔥 {} Raw Results:", scenario);
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
    
    // Performance rating for raw bytes (higher expectations)
    let rating = if stats.messages_per_second_sent > 20000.0 {
        "🚀 Blazing Fast"
    } else if stats.messages_per_second_sent > 10000.0 {
        "🔥 Excellent"
    } else if stats.messages_per_second_sent > 5000.0 {
        "✅ Very Good"
    } else if stats.messages_per_second_sent > 2000.0 {
        "⚡ Good"
    } else {
        "🐌 Needs Optimization"
    };
    println!("   Raw Performance:   {}", rating);
    
    // Bandwidth utilization estimate
    let estimated_bandwidth_mbps = stats.mbps_sent + stats.mbps_received;
    println!("   Est. Bandwidth:    {:8.1} MB/s total", estimated_bandwidth_mbps);
}