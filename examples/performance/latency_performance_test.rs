//! # Latency Performance Testing Example
//!
//! This example focuses on measuring end-to-end latency in addition to throughput:
//! - Message production to consumption latency
//! - Percentile latency measurements (P50, P95, P99)
//! - Latency vs throughput trade-offs
//! - Low-latency configuration testing

use ferrisstreams::ferris::kafka::admin_client::KafkaAdminClient;
use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
use ferrisstreams::ferris::kafka::producer_config::{AckMode, CompressionType, ProducerConfig};
use ferrisstreams::{Headers, JsonSerializer, KafkaConsumer, Message, ProducerBuilder};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tokio::time::sleep;

// Latency test configuration
const MESSAGE_COUNT: u64 = 1_000; // Smaller count for latency focus
const CONCURRENT_PRODUCERS: usize = 1; // Single producer for consistent latency
const CONSUMER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LatencyTestMessage {
    id: u64,
    send_timestamp: u64, // Microseconds since epoch
    payload: String,
}

#[derive(Debug)]
struct LatencyMetrics {
    latencies_us: Arc<std::sync::Mutex<Vec<u64>>>,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    start_time: Instant,
}

impl LatencyMetrics {
    fn new() -> Self {
        Self {
            latencies_us: Arc::new(std::sync::Mutex::new(Vec::new())),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_send(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_receive(&self, latency_us: u64) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut latencies) = self.latencies_us.lock() {
            latencies.push(latency_us);
        }
    }

    fn get_latency_stats(&self) -> LatencyStats {
        let mut latencies = self.latencies_us.lock().unwrap().clone();
        latencies.sort_unstable();

        let len = latencies.len();
        let elapsed = self.start_time.elapsed().as_secs_f64();

        LatencyStats {
            total_messages: len,
            throughput_msg_per_sec: len as f64 / elapsed,
            min_latency_us: latencies.first().copied().unwrap_or(0),
            max_latency_us: latencies.last().copied().unwrap_or(0),
            p50_latency_us: percentile(&latencies, 50.0),
            p95_latency_us: percentile(&latencies, 95.0),
            p99_latency_us: percentile(&latencies, 99.0),
            avg_latency_us: latencies.iter().sum::<u64>() as f64 / len as f64,
        }
    }
}

#[derive(Debug)]
struct LatencyStats {
    total_messages: usize,
    throughput_msg_per_sec: f64,
    min_latency_us: u64,
    max_latency_us: u64,
    p50_latency_us: u64,
    p95_latency_us: u64,
    p99_latency_us: u64,
    avg_latency_us: f64,
}

fn percentile(sorted_data: &[u64], percentile: f64) -> u64 {
    if sorted_data.is_empty() {
        return 0;
    }
    let index = (percentile / 100.0 * (sorted_data.len() - 1) as f64).round() as usize;
    sorted_data[index.min(sorted_data.len() - 1)]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("⚡ Latency Performance Testing Suite");
    println!("===================================");
    println!("Messages: {}", MESSAGE_COUNT);
    println!("Focus: End-to-end latency measurement");
    println!();

    // Test different latency-focused configurations
    let test_scenarios = vec![
        ("Ultra Low Latency", create_ultra_low_latency_config()),
        ("Balanced Latency", create_balanced_latency_config()),
        ("High Throughput", create_high_throughput_config()),
    ];

    for (scenario_name, (producer_config, consumer_config)) in test_scenarios {
        println!("📊 Testing: {}", scenario_name);
        println!("─────────────────────────────────");

        match run_latency_test(producer_config, consumer_config).await {
            Ok(stats) => print_latency_results(&stats, scenario_name),
            Err(e) => println!("❌ Test failed: {}", e),
        }

        println!();
        sleep(Duration::from_secs(2)).await;
    }

    println!("✅ All latency tests completed!");
    Ok(())
}

fn create_ultra_low_latency_config() -> (ProducerConfig, ConsumerConfig) {
    // Start with low latency preset
    let mut producer_config = ProducerConfig::new("localhost:9092", "latency-test")
        .client_id("ultra-low-latency-producer")
        .low_latency() // Apply baseline low-latency preset first
        // Then add ultra-low-latency specific optimizations
        .compression(CompressionType::None) // No compression for lowest latency
        .acks(AckMode::Leader) // Only wait for leader acknowledgment
        .idempotence(false) // Disable idempotence for lowest latency
        .batching(1, Duration::from_millis(0)) // No batching
        .retries(0, Duration::from_millis(1)); // Minimum allowed retry backoff

    let consumer_config = ConsumerConfig::new("localhost:9092", "ultra-low-latency-group")
        .client_id("ultra-low-latency-consumer")
        .low_latency() // Apply baseline low-latency preset first
        // Then add ultra-low-latency specific optimizations
        .auto_offset_reset(OffsetReset::Latest)
        .max_poll_records(1)
        .fetch_min_bytes(1)
        .fetch_max_wait(Duration::from_millis(1));

    (producer_config, consumer_config)
}

fn create_balanced_latency_config() -> (ProducerConfig, ConsumerConfig) {
    let mut producer_config = ProducerConfig::new("localhost:9092", "latency-test")
        .client_id("balanced-latency-producer")
        .compression(CompressionType::Lz4)
        .acks(AckMode::Leader)
        .idempotence(false) // Disable idempotence for balanced config
        .batching(16384, Duration::from_millis(5)) // Small batches
        .retries(3, Duration::from_millis(50));

    let consumer_config = ConsumerConfig::new("localhost:9092", "balanced-latency-group")
        .client_id("balanced-latency-consumer")
        .auto_offset_reset(OffsetReset::Latest)
        .max_poll_records(10)
        .fetch_min_bytes(100)
        .fetch_max_wait(Duration::from_millis(50));

    (producer_config, consumer_config)
}

fn create_high_throughput_config() -> (ProducerConfig, ConsumerConfig) {
    let producer_config = ProducerConfig::new("localhost:9092", "latency-test")
        .client_id("high-throughput-producer")
        .high_throughput();

    let consumer_config = ConsumerConfig::new("localhost:9092", "high-throughput-group")
        .client_id("high-throughput-consumer")
        .auto_offset_reset(OffsetReset::Latest)
        .high_throughput();

    (producer_config, consumer_config)
}

fn print_latency_results(stats: &LatencyStats, scenario: &str) {
    println!("⚡ {} Latency Results:", scenario);
    println!("   Total Messages:    {:8}", stats.total_messages);
    println!(
        "   Throughput:        {:8.1} msg/s",
        stats.throughput_msg_per_sec
    );
    println!(
        "   Min Latency:       {:8.1} ms",
        stats.min_latency_us as f64 / 1000.0
    );
    println!(
        "   Avg Latency:       {:8.1} ms",
        stats.avg_latency_us / 1000.0
    );
    println!(
        "   P50 Latency:       {:8.1} ms",
        stats.p50_latency_us as f64 / 1000.0
    );
    println!(
        "   P95 Latency:       {:8.1} ms",
        stats.p95_latency_us as f64 / 1000.0
    );
    println!(
        "   P99 Latency:       {:8.1} ms",
        stats.p99_latency_us as f64 / 1000.0
    );
    println!(
        "   Max Latency:       {:8.1} ms",
        stats.max_latency_us as f64 / 1000.0
    );

    // Latency rating
    let p95_ms = stats.p95_latency_us as f64 / 1000.0;
    let rating = if p95_ms < 5.0 {
        "🚀 Excellent"
    } else if p95_ms < 20.0 {
        "✅ Good"
    } else if p95_ms < 100.0 {
        "⚡ Acceptable"
    } else {
        "🐌 Needs Tuning"
    };
    println!("   P95 Rating:        {}", rating);
}

async fn run_latency_test(
    producer_config: ProducerConfig,
    consumer_config: ConsumerConfig,
) -> Result<LatencyStats, Box<dyn std::error::Error>> {
    let topic = format!("latency-test-{}", chrono::Utc::now().timestamp_millis());
    let metrics = Arc::new(LatencyMetrics::new());

    // Create topic
    let admin_client = KafkaAdminClient::new("localhost:9092")?;
    admin_client.create_performance_topic(&topic, 1).await?; // Single partition for consistent ordering

    // Create producer
    let mut producer_config = producer_config;
    producer_config.default_topic = topic.clone();
    let producer = ProducerBuilder::<String, LatencyTestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .build()?;

    // Create consumer
    let consumer = KafkaConsumer::<String, LatencyTestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )?;

    consumer.subscribe(&[&topic])?;

    // Start consumer task
    let consumer_metrics = metrics.clone();
    let consumer_task = tokio::spawn(async move {
        let timeout_future = tokio::time::sleep(CONSUMER_TIMEOUT);
        tokio::pin!(timeout_future);

        let stream_future =
            consumer
                .stream()
                .take(MESSAGE_COUNT as usize)
                .for_each(|message_result| {
                    let metrics = consumer_metrics.clone();
                    async move {
                        match message_result {
                            Ok(message) => {
                                let receive_time = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_micros()
                                    as u64;

                                let send_time = message.value().send_timestamp;
                                let latency_us = receive_time.saturating_sub(send_time);

                                metrics.record_receive(latency_us);

                                // Print metadata for each message
                                println!("Message metadata: {}", message.metadata_string());
                            }
                            Err(e) => {
                                println!("❌ Consumer error: {}", e);
                            }
                        }
                    }
                });

        tokio::select! {
            _ = timeout_future => {
                println!("⚠️ Consumer timeout reached");
            }
            _ = stream_future => {
                println!("✅ Received all latency test messages");
            }
        }
    });

    // Wait a moment for consumer to start
    sleep(Duration::from_millis(500)).await;

    // Send messages
    for i in 0..MESSAGE_COUNT {
        let send_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let message = LatencyTestMessage {
            id: i,
            send_timestamp,
            payload: "latency-test-payload".to_string(),
        };

        let headers = Headers::new()
            .insert("test-type", "latency")
            .insert("message-id", &i.to_string());

        producer
            .send(Some(&format!("key-{}", i)), &message, headers, None)
            .await?;
        metrics.record_send();

        // Small delay between messages for consistent latency measurement
        sleep(Duration::from_millis(10)).await;
    }

    // Wait for consumer to finish
    consumer_task.await?;

    Ok(metrics.get_latency_stats())
}
