//! # Simple Async Optimization Test
//!
//! This example demonstrates basic async I/O optimization techniques:
//! - Concurrent message processing
//! - Adaptive batch sizes
//! - Non-blocking operations

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use velostream::velostream::kafka::admin_client::KafkaAdminClient;
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use velostream::velostream::kafka::performance_presets::PerformancePresets;
use velostream::velostream::kafka::producer_config::{AckMode, CompressionType, ProducerConfig};
use velostream::{JsonSerializer, KafkaConsumer, ProducerBuilder};

// Test configuration
const MESSAGE_COUNT: u64 = 5_000;
const INITIAL_CONCURRENCY: usize = 5;
const MAX_CONCURRENCY: usize = 20;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AsyncTestMessage {
    id: u64,
    timestamp: u64,
    work_duration_ms: u64,
    payload: String,
}

/// Simple adaptive concurrency controller
struct ConcurrencyController {
    current_concurrency: AtomicUsize,
    successful_ops: AtomicU64,
    failed_ops: AtomicU64,
}

impl ConcurrencyController {
    fn new(initial: usize) -> Self {
        Self {
            current_concurrency: AtomicUsize::new(initial),
            successful_ops: AtomicU64::new(0),
            failed_ops: AtomicU64::new(0),
        }
    }

    fn get_concurrency(&self) -> usize {
        self.current_concurrency.load(Ordering::Relaxed)
    }

    fn record_success(&self) {
        self.successful_ops.fetch_add(1, Ordering::Relaxed);
        self.maybe_adjust();
    }

    fn record_failure(&self) {
        self.failed_ops.fetch_add(1, Ordering::Relaxed);
        self.maybe_adjust();
    }

    fn maybe_adjust(&self) {
        let total_ops =
            self.successful_ops.load(Ordering::Relaxed) + self.failed_ops.load(Ordering::Relaxed);

        // Adjust every 1000 operations
        if total_ops % 1000 == 0 && total_ops > 0 {
            let success_rate =
                self.successful_ops.load(Ordering::Relaxed) as f64 / total_ops as f64;
            let current = self.current_concurrency.load(Ordering::Relaxed);

            let new_concurrency = if success_rate > 0.95 && current < MAX_CONCURRENCY {
                current + 1
            } else if success_rate < 0.85 && current > 1 {
                current - 1
            } else {
                current
            };

            if new_concurrency != current {
                self.current_concurrency
                    .store(new_concurrency, Ordering::Relaxed);
                println!(
                    "🎛️  Concurrency adjusted: {} (success rate: {:.1}%)",
                    new_concurrency,
                    success_rate * 100.0
                );
            }
        }
    }
}

/// Simple metrics
struct AsyncMetrics {
    processed: AtomicU64,
    start_time: Instant,
}

impl AsyncMetrics {
    fn new() -> Self {
        Self {
            processed: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn record_processed(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }

    fn get_results(&self) -> (u64, f64) {
        let processed = self.processed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        (processed, elapsed)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Simple Async Optimization Test");
    println!("=================================");
    println!("Messages: {}", MESSAGE_COUNT);
    println!("Initial Concurrency: {}", INITIAL_CONCURRENCY);
    println!();

    let result = run_simple_async_test().await?;

    let (processed, elapsed) = result;
    let throughput = processed as f64 / elapsed;

    println!("📊 Results:");
    println!("   Messages Processed: {:8}", processed);
    println!("   Throughput:         {:8.1} msg/s", throughput);
    println!("   Duration:           {:8.1} seconds", elapsed);

    let efficiency = if throughput > 1000.0 {
        "🚀 Excellent"
    } else if throughput > 500.0 {
        "✅ Good"
    } else {
        "⚡ Standard"
    };
    println!("   Performance:        {}", efficiency);

    Ok(())
}

async fn run_simple_async_test() -> Result<(u64, f64), String> {
    let topic = format!("simple-async-{}", chrono::Utc::now().timestamp_millis());
    let metrics = Arc::new(AsyncMetrics::new());
    let controller = Arc::new(ConcurrencyController::new(INITIAL_CONCURRENCY));

    // Create topic
    let admin_client = KafkaAdminClient::new("localhost:9092").map_err(|e| e.to_string())?;
    admin_client
        .create_performance_topic(&topic, 3)
        .await
        .map_err(|e| e.to_string())?;

    // Create producer
    let producer_config = ProducerConfig::new("localhost:9092", &topic)
        .client_id("simple-async-producer")
        .compression(CompressionType::Lz4)
        .acks(AckMode::Leader)
        .batching(32768, Duration::from_millis(10))
        .high_throughput();

    let producer = ProducerBuilder::<String, AsyncTestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .build()
    .map_err(|e| e.to_string())?;

    // Create consumer
    let consumer_config = ConsumerConfig::new("localhost:9092", "simple-async-group")
        .client_id("simple-async-consumer")
        .auto_offset_reset(OffsetReset::Earliest)
        .max_poll_records(500)
        .fetch_min_bytes(1024)
        .fetch_max_wait(Duration::from_millis(100))
        .high_throughput();

    let consumer = FastConsumer::<String, AsyncTestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .map_err(|e| e.to_string())?;

    consumer.subscribe(&[&topic]).map_err(|e| e.to_string())?;

    // Start async processor
    let processor_metrics = metrics.clone();
    let processor_controller = controller.clone();
    let processor_task = tokio::spawn(async move {
        futures::StreamExt::take(consumer.stream(), MESSAGE_COUNT as usize)
            .for_each_concurrent(None, |message_result| {
                let metrics = processor_metrics.clone();
                let controller = processor_controller.clone();

                async move {
                    match message_result {
                        Ok(message) => {
                            let concurrency = controller.get_concurrency();
                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let _permit = semaphore.acquire().await.unwrap();

                            // Simulate async work
                            let work_duration = message.value().work_duration_ms;
                            tokio::time::sleep(Duration::from_millis(work_duration)).await;

                            metrics.record_processed();
                            controller.record_success();

                            // Log progress
                            let count = metrics.processed.load(Ordering::Relaxed);
                            if count % 500 == 0 {
                                println!(
                                    "📈 Processed: {} messages (concurrency: {})",
                                    count, concurrency
                                );
                            }
                        }
                        Err(e) => {
                            println!("❌ Consumer error: {}", e);
                            controller.record_failure();
                        }
                    }
                }
            })
            .await;
    });

    // Start producer
    let producer_task = tokio::spawn(async move {
        for i in 0..MESSAGE_COUNT {
            let message = AsyncTestMessage {
                id: i,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                work_duration_ms: 5, // 5ms simulated work
                payload: format!("async-message-{}", i),
            };

            let key = format!("key-{}", i);

            producer
                .send(
                    Some(&key),
                    &message,
                    velostream::Headers::new()
                        .insert("test-type", "async")
                        .insert("message-id", i.to_string()),
                    None,
                )
                .await
                .map_err(|e| e.to_string())?;

            // Yield occasionally
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }

        Ok::<(), String>(())
    });

    // Wait for completion
    let (producer_result, processor_result) = tokio::join!(producer_task, processor_task);
    producer_result.map_err(|e| e.to_string())??;
    processor_result.map_err(|e| e.to_string())?;

    Ok(metrics.get_results())
}
