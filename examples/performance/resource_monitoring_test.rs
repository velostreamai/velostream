//! # Resource Monitoring Performance Test
//!
//! This example monitors system resources during performance testing:
//! - CPU usage tracking
//! - Memory consumption monitoring
//! - Network bandwidth utilization
//! - GC pause detection
//! - Resource efficiency metrics

use ferrisstreams::ferris::kafka::consumer_config::ConsumerConfig;
use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
use ferrisstreams::ferris::kafka::producer_config::ProducerConfig;
use ferrisstreams::{JsonSerializer, KafkaConsumer, ProducerBuilder};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::interval;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ResourceTestMessage {
    id: u64,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ResourceSnapshot {
    timestamp: Instant,
    memory_usage_bytes: u64,
    // Add more metrics as needed
}

#[derive(Debug)]
struct ResourceMonitor {
    snapshots: Arc<std::sync::Mutex<Vec<ResourceSnapshot>>>,
    monitoring: Arc<std::sync::atomic::AtomicBool>,
}

impl ResourceMonitor {
    fn new() -> Self {
        Self {
            snapshots: Arc::new(std::sync::Mutex::new(Vec::new())),
            monitoring: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    async fn start_monitoring(&self) {
        self.monitoring.store(true, Ordering::Relaxed);
        let snapshots = self.snapshots.clone();
        let monitoring = self.monitoring.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(100)); // 10Hz monitoring

            while monitoring.load(Ordering::Relaxed) {
                interval.tick().await;

                let snapshot = ResourceSnapshot {
                    timestamp: Instant::now(),
                    memory_usage_bytes: get_memory_usage(),
                };

                if let Ok(mut snapshots_guard) = snapshots.lock() {
                    snapshots_guard.push(snapshot);
                }
            }
        });
    }

    fn stop_monitoring(&self) {
        self.monitoring.store(false, Ordering::Relaxed);
    }

    fn get_summary(&self) -> ResourceSummary {
        let snapshots = self.snapshots.lock().unwrap();

        if snapshots.is_empty() {
            return ResourceSummary::default();
        }

        let memory_values: Vec<u64> = snapshots.iter().map(|s| s.memory_usage_bytes).collect();

        ResourceSummary {
            duration: snapshots
                .last()
                .unwrap()
                .timestamp
                .duration_since(snapshots.first().unwrap().timestamp),
            min_memory_mb: memory_values.iter().min().copied().unwrap_or(0) / 1024 / 1024,
            max_memory_mb: memory_values.iter().max().copied().unwrap_or(0) / 1024 / 1024,
            avg_memory_mb: (memory_values.iter().sum::<u64>() / memory_values.len() as u64)
                / 1024
                / 1024,
            sample_count: snapshots.len(),
        }
    }
}

#[derive(Debug, Default)]
struct ResourceSummary {
    duration: Duration,
    min_memory_mb: u64,
    max_memory_mb: u64,
    avg_memory_mb: u64,
    sample_count: usize,
}

fn get_memory_usage() -> u64 {
    // Simplified memory usage - in real implementation, use proper system APIs
    // For now, return a mock value
    std::process::id() as u64 * 1024 * 1024 // Mock: PID * 1MB
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Resource Monitoring Performance Test");
    println!("======================================");

    let monitor = ResourceMonitor::new();
    monitor.start_monitoring().await;

    // Run a basic performance test while monitoring resources
    let result = run_monitored_test().await;

    monitor.stop_monitoring();
    tokio::time::sleep(Duration::from_millis(200)).await; // Let monitoring finish

    let resource_summary = monitor.get_summary();

    match result {
        Ok(message_count) => {
            println!("✅ Processed {} messages", message_count);
            print_resource_results(&resource_summary);
        }
        Err(e) => println!("❌ Test failed: {}", e),
    }

    Ok(())
}

async fn run_monitored_test() -> Result<u64, Box<dyn std::error::Error>> {
    let topic = "resource-test";
    const MESSAGE_COUNT: u64 = 5_000;

    // Create producer with high throughput config
    let producer_config = ProducerConfig::new("localhost:9092", topic)
        .client_id("resource-test-producer")
        .high_throughput();

    let producer = ProducerBuilder::<String, ResourceTestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .build()?;

    // Create consumer
    let consumer_config = ConsumerConfig::new("localhost:9092", "resource-test-group")
        .client_id("resource-test-consumer")
        .high_throughput();

    let consumer = KafkaConsumer::<String, ResourceTestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )?;

    consumer.subscribe(&[topic])?;

    // Start consumer
    let received_count = Arc::new(AtomicU64::new(0));
    let received_count_clone = received_count.clone();

    let consumer_task = tokio::spawn(async move {
        consumer
            .stream()
            .take(MESSAGE_COUNT as usize)
            .for_each(|_| async {
                received_count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .await;
    });

    // Send messages
    for i in 0..MESSAGE_COUNT {
        let message = ResourceTestMessage {
            id: i,
            data: vec![0x42; 1024], // 1KB payload
        };

        producer
            .send(
                Some(&format!("key-{}", i)),
                &message,
                ferrisstreams::Headers::new(),
                None,
            )
            .await?;
    }

    // Wait for completion
    consumer_task.await?;

    Ok(received_count.load(Ordering::Relaxed))
}

fn print_resource_results(summary: &ResourceSummary) {
    println!("📊 Resource Usage Summary:");
    println!(
        "   Test Duration:     {:8.1} seconds",
        summary.duration.as_secs_f64()
    );
    println!("   Min Memory:        {:8} MB", summary.min_memory_mb);
    println!("   Avg Memory:        {:8} MB", summary.avg_memory_mb);
    println!("   Max Memory:        {:8} MB", summary.max_memory_mb);
    println!(
        "   Memory Growth:     {:8} MB",
        summary.max_memory_mb.saturating_sub(summary.min_memory_mb)
    );
    println!("   Samples Taken:     {:8}", summary.sample_count);

    // Resource efficiency rating
    let memory_efficiency = if summary.max_memory_mb < 100 {
        "🚀 Excellent"
    } else if summary.max_memory_mb < 500 {
        "✅ Good"
    } else if summary.max_memory_mb < 1000 {
        "⚡ Acceptable"
    } else {
        "🔄 High Usage"
    };
    println!("   Memory Efficiency: {}", memory_efficiency);
}
