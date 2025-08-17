//! # Simple Zero-Copy Performance Test
//!
//! This example demonstrates basic zero-copy optimization techniques:
//! - Buffer pooling to reduce allocations
//! - Processing borrowed data without copying
//! - Memory-efficient message handling

use ferrisstreams::ferris::kafka::admin_client::KafkaAdminClient;
use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
use ferrisstreams::ferris::kafka::producer_config::{AckMode, CompressionType, ProducerConfig};
use ferrisstreams::ferris::kafka::serialization::{BytesSerializer, StringSerializer};
use ferrisstreams::{KafkaConsumer, ProducerBuilder};
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Test configuration
const MESSAGE_COUNT: u64 = 10_000;
const PAYLOAD_SIZE: usize = 4096; // 4KB payloads

/// Simple buffer pool for demonstration
struct SimpleBufferPool {
    buffers: crossbeam_queue::ArrayQueue<Vec<u8>>,
}

impl SimpleBufferPool {
    fn new(capacity: usize, buffer_size: usize) -> Self {
        let buffers = crossbeam_queue::ArrayQueue::new(capacity);

        // Pre-allocate some buffers
        for _ in 0..capacity.min(100) {
            let buffer = vec![0u8; buffer_size];
            let _ = buffers.push(buffer);
        }

        Self { buffers }
    }

    fn get_buffer(&self) -> Vec<u8> {
        self.buffers
            .pop()
            .unwrap_or_else(|| vec![0u8; PAYLOAD_SIZE])
    }

    fn return_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        buffer.resize(PAYLOAD_SIZE, 0);
        let _ = self.buffers.push(buffer);
    }
}

/// Simple metrics tracker
struct Metrics {
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    bytes_processed: AtomicU64,
    start_time: Instant,
}

impl Metrics {
    fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_send(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_receive(&self, bytes: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_processed
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn get_results(&self) -> (u64, u64, u64, f64) {
        let sent = self.messages_sent.load(Ordering::Relaxed);
        let received = self.messages_received.load(Ordering::Relaxed);
        let bytes = self.bytes_processed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        (sent, received, bytes, elapsed)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Simple Zero-Copy Performance Test");
    println!("====================================");
    println!("Messages: {}", MESSAGE_COUNT);
    println!("Payload Size: {} bytes", PAYLOAD_SIZE);
    println!();

    let result = run_simple_zero_copy_test().await?;

    let (sent, received, bytes, elapsed) = result;
    let throughput = received as f64 / elapsed;
    let mb_per_sec = (bytes as f64 / 1_048_576.0) / elapsed;

    println!("📊 Results:");
    println!("   Messages Sent:     {:8}", sent);
    println!("   Messages Received: {:8}", received);
    println!("   Throughput:        {:8.1} msg/s", throughput);
    println!("   Bandwidth:         {:8.1} MB/s", mb_per_sec);
    println!("   Duration:          {:8.1} seconds", elapsed);

    let efficiency = if throughput > 5000.0 {
        "🚀 Excellent"
    } else if throughput > 2000.0 {
        "✅ Good"
    } else {
        "⚡ Standard"
    };
    println!("   Performance:       {}", efficiency);

    Ok(())
}

async fn run_simple_zero_copy_test() -> Result<(u64, u64, u64, f64), Box<dyn std::error::Error>> {
    let topic = format!("simple-zero-copy-{}", chrono::Utc::now().timestamp_millis());
    let metrics = Arc::new(Metrics::new());
    let buffer_pool = Arc::new(SimpleBufferPool::new(1000, PAYLOAD_SIZE));

    // Create topic
    let admin_client = KafkaAdminClient::new("localhost:9092")?;
    admin_client.create_performance_topic(&topic, 3).await?;

    // Create producer
    let producer_config = ProducerConfig::new("localhost:9092", &topic)
        .client_id("simple-zero-copy-producer")
        .compression(CompressionType::None) // No compression for zero-copy demo
        .acks(AckMode::Leader)
        .batching(65536, Duration::from_millis(10))
        .high_throughput();

    let producer = ProducerBuilder::<String, Vec<u8>, _, _>::with_config(
        producer_config,
        StringSerializer,
        BytesSerializer,
    )
    .build()?;

    // Create consumer
    let consumer_config = ConsumerConfig::new("localhost:9092", "simple-zero-copy-group")
        .client_id("simple-zero-copy-consumer")
        .auto_offset_reset(OffsetReset::Earliest)
        .max_poll_records(1000)
        .fetch_min_bytes(10240)
        .fetch_max_wait(Duration::from_millis(100))
        .high_throughput();

    let consumer = KafkaConsumer::<String, Vec<u8>, _, _>::with_config(
        consumer_config,
        StringSerializer,
        BytesSerializer,
    )?;

    consumer.subscribe(&[&topic])?;

    // Start consumer task
    let consumer_metrics = metrics.clone();
    let consumer_task = tokio::spawn(async move {
        futures::StreamExt::take(consumer.stream(), MESSAGE_COUNT as usize)
            .for_each(|message_result| async {
                match message_result {
                    Ok(message) => {
                        // Process borrowed data without copying
                        let data = message.value();
                        let _checksum = data
                            .iter()
                            .fold(0u64, |acc, &byte| acc.wrapping_add(byte as u64));

                        consumer_metrics.record_receive(data.len());

                        // Log progress
                        let count = consumer_metrics.messages_received.load(Ordering::Relaxed);
                        if count % 1000 == 0 {
                            println!("📈 Processed: {} messages", count);
                        }
                    }
                    Err(e) => {
                        println!("❌ Consumer error: {}", e);
                    }
                }
            })
            .await;
    });

    // Start producer task with buffer reuse
    let producer_metrics = metrics.clone();
    let producer_pool = buffer_pool.clone();
    let producer_task = tokio::spawn(async move {
        for i in 0..MESSAGE_COUNT {
            // Get buffer from pool (zero-copy optimization)
            let mut payload = producer_pool.get_buffer();

            // Fill with test data
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte = ((i + idx as u64) % 256) as u8;
            }

            let key = format!("key-{}", i);

            match producer
                .send(
                    Some(&key),
                    &payload,
                    ferrisstreams::Headers::new()
                        .insert("test-type", "zero-copy")
                        .insert("message-id", &i.to_string()),
                    None,
                )
                .await
            {
                Ok(_) => {
                    producer_metrics.record_send();
                }
                Err(e) => {
                    println!("❌ Send error: {}", e);
                }
            }

            // Return buffer to pool for reuse
            producer_pool.return_buffer(payload);

            // Yield occasionally
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // Wait for completion
    let (producer_result, consumer_result) = tokio::join!(producer_task, consumer_task);
    producer_result?;
    consumer_result?;

    Ok(metrics.get_results())
}
