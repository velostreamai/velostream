//! Performance benchmarks comparing Kafka consumer polling strategies
//!
//! This test compares different FastConsumer polling strategies:
//! 1. FastConsumer.stream() - Async stream polling (baseline)
//! 2. FastConsumer.buffered_stream(N) - Buffered async stream with configurable batch size
//! 3. FastConsumer.dedicated_stream() - Dedicated thread polling
//! 4. FastConsumer.poll_batch() - Synchronous batch polling returning StreamRecords
//!
//! All benchmarks use the same FastConsumer implementation with different access patterns.
//!
//! Run with: cargo test --test kafka_consumer_benchmark --release -- --ignored --nocapture

use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdKafkaConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use velostream::velostream::kafka::kafka_fast_consumer::Consumer as FastConsumer;
use velostream::velostream::kafka::serialization::{JsonSerializer, StringSerializer};
use velostream::velostream::sql::execution::types::FieldValue;

// =============================================================================
// Configuration Constants
// =============================================================================

const TEST_MESSAGE_COUNT: usize = 10_000;
const WARMUP_MESSAGES: usize = 100;
/// Maximum time to wait for a single message during warmup/benchmark
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time to wait for warmup phase to complete
const WARMUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time for entire benchmark to complete (per consumer type)
const BENCHMARK_TIMEOUT: Duration = Duration::from_secs(120);

// =============================================================================
// Test Message Structure
// =============================================================================

/// Test message structure
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct TestMessage {
    id: u64,
    timestamp: i64,
    data: String,
}

impl TestMessage {
    fn new(id: u64) -> Self {
        Self {
            id,
            timestamp: chrono::Utc::now().timestamp_millis(),
            data: format!("Test message {}", id),
        }
    }
}

// =============================================================================
// Performance Metrics
// =============================================================================

/// Latency statistics computed from a set of durations
#[derive(Debug, Clone, Default)]
struct LatencyStats {
    avg_ms: f64,
    min_ms: f64,
    max_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

impl LatencyStats {
    fn from_durations(latencies: &[Duration]) -> Self {
        if latencies.is_empty() {
            return Self::default();
        }

        let mut latency_ms: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
        latency_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg = latency_ms.iter().sum::<f64>() / latency_ms.len() as f64;
        let min = latency_ms.first().copied().unwrap_or(0.0);
        let max = latency_ms.last().copied().unwrap_or(0.0);

        let p50_idx = (latency_ms.len() as f64 * 0.50) as usize;
        let p95_idx = (latency_ms.len() as f64 * 0.95) as usize;
        let p99_idx = (latency_ms.len() as f64 * 0.99) as usize;

        Self {
            avg_ms: avg,
            min_ms: min,
            max_ms: max,
            p50_ms: latency_ms.get(p50_idx).copied().unwrap_or(0.0),
            p95_ms: latency_ms.get(p95_idx).copied().unwrap_or(0.0),
            p99_ms: latency_ms.get(p99_idx).copied().unwrap_or(0.0),
        }
    }
}

/// Performance metrics for a consumer test (includes both warmup and benchmark phases)
#[derive(Debug)]
struct PerformanceMetrics {
    consumer_type: String,
    // Warmup phase metrics
    warmup_messages: usize,
    warmup_duration: Duration,
    warmup_msgs_per_sec: f64,
    // Benchmark phase metrics
    benchmark_messages: usize,
    benchmark_duration: Duration,
    benchmark_msgs_per_sec: f64,
    benchmark_latency: LatencyStats,
}

impl PerformanceMetrics {
    fn new(
        consumer_type: String,
        warmup_messages: usize,
        warmup_duration: Duration,
        benchmark_messages: usize,
        benchmark_duration: Duration,
        benchmark_latencies: Vec<Duration>,
    ) -> Self {
        let warmup_secs = warmup_duration.as_secs_f64();
        let warmup_msgs_per_sec = if warmup_secs > 0.0 {
            warmup_messages as f64 / warmup_secs
        } else {
            0.0
        };

        let benchmark_secs = benchmark_duration.as_secs_f64();
        let benchmark_msgs_per_sec = if benchmark_secs > 0.0 {
            benchmark_messages as f64 / benchmark_secs
        } else {
            0.0
        };

        Self {
            consumer_type,
            warmup_messages,
            warmup_duration,
            warmup_msgs_per_sec,
            benchmark_messages,
            benchmark_duration,
            benchmark_msgs_per_sec,
            benchmark_latency: LatencyStats::from_durations(&benchmark_latencies),
        }
    }

    fn print(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Consumer Type: {}", self.consumer_type);
        println!("{}", "=".repeat(80));

        println!("\n📊 WARMUP PHASE:");
        println!("  Messages:    {:>10}", self.warmup_messages);
        println!(
            "  Duration:    {:>10.3}s",
            self.warmup_duration.as_secs_f64()
        );
        println!("  Throughput:  {:>10.0} msg/s", self.warmup_msgs_per_sec);

        println!("\n📊 BENCHMARK PHASE (post-warmup):");
        println!("  Messages:    {:>10}", self.benchmark_messages);
        println!(
            "  Duration:    {:>10.3}s",
            self.benchmark_duration.as_secs_f64()
        );
        println!("  Throughput:  {:>10.0} msg/s", self.benchmark_msgs_per_sec);

        println!("\n  Latency Statistics (ms):");
        println!("    Average:  {:>10.3}", self.benchmark_latency.avg_ms);
        println!("    Min:      {:>10.3}", self.benchmark_latency.min_ms);
        println!("    Max:      {:>10.3}", self.benchmark_latency.max_ms);
        println!("    P50:      {:>10.3}", self.benchmark_latency.p50_ms);
        println!("    P95:      {:>10.3}", self.benchmark_latency.p95_ms);
        println!("    P99:      {:>10.3}", self.benchmark_latency.p99_ms);

        // Show warmup vs benchmark comparison
        if self.warmup_msgs_per_sec > 0.0 {
            let improvement = self.benchmark_msgs_per_sec / self.warmup_msgs_per_sec;
            println!(
                "\n  ⚡ Warmup → Benchmark: {:.2}x {}",
                improvement,
                if improvement > 1.0 {
                    "faster"
                } else {
                    "slower"
                }
            );
        }
    }
}

/// Print comparison table showing both warmup and benchmark phases
fn print_comparison(metrics: &[PerformanceMetrics]) {
    println!("\n{}", "=".repeat(140));
    println!("{:^140}", "PERFORMANCE COMPARISON");
    println!("{}", "=".repeat(140));

    // Header
    println!(
        "{:<35} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "Consumer Type",
        "Warmup",
        "Warmup",
        "Benchmark",
        "Benchmark",
        "Avg Lat",
        "P95 Lat",
        "P99 Lat"
    );
    println!(
        "{:<35} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "", "msgs", "(msg/s)", "msgs", "(msg/s)", "(ms)", "(ms)", "(ms)"
    );
    println!("{}", "-".repeat(140));

    for metric in metrics {
        println!(
            "{:<35} {:>12} {:>12.0} {:>12} {:>12.0} {:>12.3} {:>12.3} {:>12.3}",
            metric.consumer_type,
            metric.warmup_messages,
            metric.warmup_msgs_per_sec,
            metric.benchmark_messages,
            metric.benchmark_msgs_per_sec,
            metric.benchmark_latency.avg_ms,
            metric.benchmark_latency.p95_ms,
            metric.benchmark_latency.p99_ms,
        );
    }

    println!("{}", "=".repeat(140));

    // Calculate speedups relative to baseline (first entry) - using benchmark throughput
    if let Some(baseline) = metrics.first() {
        println!(
            "\nSpeedup vs {} (baseline benchmark throughput):",
            baseline.consumer_type
        );
        println!("{}", "-".repeat(80));
        for metric in metrics.iter().skip(1) {
            let speedup = if baseline.benchmark_msgs_per_sec > 0.0 {
                metric.benchmark_msgs_per_sec / baseline.benchmark_msgs_per_sec
            } else {
                0.0
            };
            println!("  {:<45} {:>6.2}x", metric.consumer_type, speedup);
        }
    }

    println!();
}

// =============================================================================
// Kafka Test Environment
// =============================================================================

/// Kafka test environment for benchmarks
struct KafkaTestEnv {
    bootstrap_servers: String,
    producer: FutureProducer,
}

impl KafkaTestEnv {
    async fn new() -> Self {
        // Use local Kafka for testing
        // Note: Requires Kafka running on localhost:9092
        let bootstrap_servers = "localhost:9092".to_string();

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create producer");

        Self {
            bootstrap_servers,
            producer,
        }
    }

    async fn create_topic(&self, topic: &str) -> Result<(), Box<dyn std::error::Error>> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()?;

        let new_topic = NewTopic::new(topic, 3, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(5)));

        // Try to create, ignore if already exists
        let _ = admin.create_topics(&[new_topic], &opts).await;

        // Wait for topic to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    async fn produce_messages(
        &self,
        topic: &str,
        count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Producing {} messages to topic '{}'...", count, topic);

        for i in 0..count {
            let message = TestMessage::new(i as u64);
            let payload = serde_json::to_string(&message)?;
            let key = i.to_string();

            let record = FutureRecord::to(topic).payload(&payload).key(&key);

            self.producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| e)?;

            if (i + 1) % 1000 == 0 {
                print!(".");
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }

        println!("\nProduced {} messages", count);
        Ok(())
    }

    /// Create a BaseConsumer with standard configuration
    fn create_base_consumer(&self, group_id: &str) -> BaseConsumer {
        ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("Failed to create BaseConsumer")
    }
}

// =============================================================================
// Common Benchmark Utilities
// =============================================================================

/// Result from warmup phase
struct WarmupResult {
    messages: usize,
    duration: Duration,
}

/// Async stream warmup with timeout handling - returns metrics
async fn warmup_async_stream<S, T, E>(stream: &mut S, target_count: usize) -> WarmupResult
where
    S: futures::Stream<Item = Result<T, E>> + Unpin,
    E: std::fmt::Debug,
{
    println!("Warming up...");
    let warmup_start = Instant::now();
    let mut warmup_count = 0;

    while warmup_count < target_count && warmup_start.elapsed() < WARMUP_TIMEOUT {
        match tokio::time::timeout(MESSAGE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(_))) => warmup_count += 1,
            Ok(Some(Err(e))) => {
                eprintln!("Warmup error: {:?}", e);
                break;
            }
            Ok(None) => break,
            Err(_) => {
                eprintln!("Warmup timeout waiting for message");
                break;
            }
        }
    }

    let duration = warmup_start.elapsed();
    println!(
        "Warmed up with {} messages in {:.2}s ({:.0} msg/s)",
        warmup_count,
        duration.as_secs_f64(),
        if duration.as_secs_f64() > 0.0 {
            warmup_count as f64 / duration.as_secs_f64()
        } else {
            0.0
        }
    );

    WarmupResult {
        messages: warmup_count,
        duration,
    }
}

/// Benchmark result from async stream consumption
struct BenchmarkResult {
    consumed: usize,
    duration: Duration,
    latencies: Vec<Duration>,
}

/// Run async stream benchmark with timeout handling
async fn benchmark_async_stream<S, T, E>(stream: &mut S, target_count: usize) -> BenchmarkResult
where
    S: futures::Stream<Item = Result<T, E>> + Unpin,
    E: std::fmt::Debug,
{
    println!("Starting benchmark...");
    let mut consumed = 0;
    let mut latencies = Vec::with_capacity(target_count);
    let start = Instant::now();
    let mut consecutive_timeouts = 0;

    while consumed < target_count && start.elapsed() < BENCHMARK_TIMEOUT {
        let msg_start = Instant::now();

        match tokio::time::timeout(MESSAGE_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(_))) => {
                latencies.push(msg_start.elapsed());
                consumed += 1;
                consecutive_timeouts = 0;

                if consumed % 1000 == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Ok(Some(Err(e))) => {
                eprintln!("Error consuming message: {:?}", e);
                break;
            }
            Ok(None) => break,
            Err(_) => {
                consecutive_timeouts += 1;
                if consecutive_timeouts >= 3 {
                    eprintln!("\nToo many consecutive timeouts, stopping benchmark");
                    break;
                }
            }
        }
    }

    let duration = start.elapsed();
    println!(
        "\nCompleted ({} messages in {:.2}s)",
        consumed,
        duration.as_secs_f64()
    );

    BenchmarkResult {
        consumed,
        duration,
        latencies,
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Benchmark FastConsumer with standard async stream (.stream())
///
/// This is the baseline benchmark using FastConsumer's default async stream.
async fn benchmark_async_stream_polling(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    println!("\n>>> Benchmarking: FastConsumer.stream() (async polling)");

    let base_consumer = env.create_base_consumer("benchmark-async-stream");
    let consumer = FastConsumer::<String, TestMessage, StringSerializer, JsonSerializer>::new(
        base_consumer,
        StringSerializer,
        JsonSerializer,
    );
    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    // Warmup
    let mut warmup_stream = consumer.stream();
    let warmup = warmup_async_stream(&mut warmup_stream, WARMUP_MESSAGES).await;

    // Benchmark
    let mut stream = consumer.stream();
    let result = benchmark_async_stream(&mut stream, message_count).await;

    PerformanceMetrics::new(
        "FastConsumer.stream()".to_string(),
        warmup.messages,
        warmup.duration,
        result.consumed,
        result.duration,
        result.latencies,
    )
}

/// Benchmark FastConsumer with buffered async stream (.buffered_stream(N))
async fn benchmark_buffered_stream_polling(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
    buffer_size: usize,
) -> PerformanceMetrics {
    println!(
        "\n>>> Benchmarking: FastConsumer.buffered_stream({}) (buffered async)",
        buffer_size
    );

    let base_consumer = env.create_base_consumer(&format!("benchmark-buffered-{}", buffer_size));
    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    let consumer: FastConsumer<String, TestMessage, StringSerializer, JsonSerializer> =
        FastConsumer::new(base_consumer, StringSerializer, JsonSerializer);

    // Warmup
    let mut warmup_stream = consumer.buffered_stream(buffer_size);
    let warmup = warmup_async_stream(&mut warmup_stream, WARMUP_MESSAGES).await;

    // Benchmark
    let mut stream = consumer.buffered_stream(buffer_size);
    let result = benchmark_async_stream(&mut stream, message_count).await;

    PerformanceMetrics::new(
        format!("FastConsumer.buffered_stream({})", buffer_size),
        warmup.messages,
        warmup.duration,
        result.consumed,
        result.duration,
        result.latencies,
    )
}

/// Benchmark FastConsumer with dedicated thread stream (.dedicated_stream())
fn benchmark_dedicated_thread_polling(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    println!("\n>>> Benchmarking: FastConsumer.dedicated_stream() (dedicated thread)");

    let base_consumer = env.create_base_consumer("benchmark-dedicated-thread");
    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    let consumer = Arc::new(FastConsumer::<
        String,
        TestMessage,
        StringSerializer,
        JsonSerializer,
    >::new(base_consumer, StringSerializer, JsonSerializer));

    // Warmup with timeout
    println!("Warming up...");
    let warmup_start = Instant::now();
    let mut warmup_stream = consumer.clone().dedicated_stream();
    let mut warmup_count = 0;
    while warmup_count < WARMUP_MESSAGES && warmup_start.elapsed() < WARMUP_TIMEOUT {
        match warmup_stream.next() {
            Some(Ok(_)) => warmup_count += 1,
            Some(Err(e)) => {
                eprintln!("Warmup error: {:?}", e);
                break;
            }
            None => {
                if warmup_start.elapsed() >= WARMUP_TIMEOUT {
                    eprintln!("Warmup timeout");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    let warmup_duration = warmup_start.elapsed();
    println!(
        "Warmed up with {} messages in {:.2}s ({:.0} msg/s)",
        warmup_count,
        warmup_duration.as_secs_f64(),
        if warmup_duration.as_secs_f64() > 0.0 {
            warmup_count as f64 / warmup_duration.as_secs_f64()
        } else {
            0.0
        }
    );

    // Actual benchmark with timeout
    println!("Starting benchmark...");
    let mut stream = consumer.dedicated_stream();
    let mut consumed = 0;
    let mut latencies = Vec::with_capacity(message_count);
    let start = Instant::now();
    let mut consecutive_empty = 0;

    while consumed < message_count && start.elapsed() < BENCHMARK_TIMEOUT {
        let msg_start = Instant::now();

        match stream.next() {
            Some(Ok(_msg)) => {
                latencies.push(msg_start.elapsed());
                consumed += 1;
                consecutive_empty = 0;

                if consumed % 1000 == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Some(Err(e)) => {
                eprintln!("Error consuming message: {:?}", e);
                break;
            }
            None => {
                consecutive_empty += 1;
                if consecutive_empty >= 100 {
                    // 100 * 10ms = 1 second of no messages
                    eprintln!("\nNo messages for 1 second, stopping benchmark");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    let duration = start.elapsed();
    println!(
        "\nCompleted ({} messages in {:.2}s)",
        consumed,
        duration.as_secs_f64()
    );

    PerformanceMetrics::new(
        "FastConsumer.dedicated_stream()".to_string(),
        warmup_count,
        warmup_duration,
        consumed,
        duration,
        latencies,
    )
}

/// Benchmark FastConsumer.poll_batch() (synchronous batch polling returning StreamRecords)
///
/// Note: poll_batch requires V = HashMap<String, FieldValue> and uses internal batch_size of 100
fn benchmark_poll_batch(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    println!("\n>>> Benchmarking: FastConsumer.poll_batch() (sync batch, batch_size=100)");

    let base_consumer = env.create_base_consumer("benchmark-poll-batch");
    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    // poll_batch requires V = HashMap<String, FieldValue>
    let mut consumer: FastConsumer<
        String,
        HashMap<String, FieldValue>,
        StringSerializer,
        JsonSerializer,
    > = FastConsumer::new(base_consumer, StringSerializer, JsonSerializer);

    // Warmup with timeout
    println!("Warming up...");
    let warmup_start = Instant::now();
    let mut warmup_count = 0;
    while warmup_count < WARMUP_MESSAGES && warmup_start.elapsed() < WARMUP_TIMEOUT {
        match consumer.poll_batch(Duration::from_millis(100), None) {
            Ok(batch) => {
                warmup_count += batch.len();
                if batch.is_empty() && warmup_start.elapsed() >= WARMUP_TIMEOUT {
                    eprintln!("Warmup timeout");
                    break;
                }
            }
            Err(e) => {
                eprintln!("Warmup error: {:?}", e);
                break;
            }
        }
    }
    let warmup_duration = warmup_start.elapsed();
    println!(
        "Warmed up with {} records in {:.2}s ({:.0} msg/s)",
        warmup_count,
        warmup_duration.as_secs_f64(),
        if warmup_duration.as_secs_f64() > 0.0 {
            warmup_count as f64 / warmup_duration.as_secs_f64()
        } else {
            0.0
        }
    );

    // Actual benchmark with timeout
    println!("Starting benchmark...");
    let mut consumed = 0;
    let mut latencies = Vec::with_capacity(message_count);
    let start = Instant::now();
    let mut consecutive_empty = 0;

    while consumed < message_count && start.elapsed() < BENCHMARK_TIMEOUT {
        let batch_start = Instant::now();

        match consumer.poll_batch(Duration::from_millis(100), None) {
            Ok(batch) => {
                if batch.is_empty() {
                    consecutive_empty += 1;
                    if consecutive_empty >= 10 {
                        eprintln!("\n10 consecutive empty batches, stopping benchmark");
                        break;
                    }
                    continue;
                }
                consecutive_empty = 0;

                let batch_latency = batch_start.elapsed();
                let per_record_latency = batch_latency / batch.len() as u32;

                for _ in &batch {
                    latencies.push(per_record_latency);
                }
                consumed += batch.len();

                if consumed % 1000 == 0 || consumed >= message_count {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Err(e) => {
                eprintln!("Error consuming batch: {:?}", e);
                break;
            }
        }
    }

    let duration = start.elapsed();
    println!(
        "\nCompleted ({} records in {:.2}s)",
        consumed,
        duration.as_secs_f64()
    );

    PerformanceMetrics::new(
        "FastConsumer.poll_batch()".to_string(),
        warmup_count,
        warmup_duration,
        consumed,
        duration,
        latencies,
    )
}

// =============================================================================
// Test Functions
// =============================================================================

/// Full performance comparison of all FastConsumer polling strategies
#[tokio::test]
#[ignore] // Run explicitly with: cargo test kafka_consumer_performance_comparison --release -- --ignored --nocapture
async fn kafka_consumer_performance_comparison() {
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "FastConsumer Polling Strategy Benchmark");
    println!("{}", "=".repeat(80));
    println!("Messages per test: {}", TEST_MESSAGE_COUNT);
    println!("Warmup messages:   {}", WARMUP_MESSAGES);
    println!("{}", "=".repeat(80));

    // Setup
    let env = KafkaTestEnv::new().await;
    let topic = "benchmark-topic";

    // Create topic
    env.create_topic(topic)
        .await
        .expect("Failed to create topic");

    // Produce test messages
    env.produce_messages(topic, TEST_MESSAGE_COUNT + WARMUP_MESSAGES)
        .await
        .expect("Failed to produce messages");

    // Wait for messages to be available
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut all_metrics = Vec::new();

    // Benchmark 1: FastConsumer.stream() - async polling (baseline)
    let metrics = benchmark_async_stream_polling(&env, topic, TEST_MESSAGE_COUNT).await;
    metrics.print();
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 2: FastConsumer.buffered_stream(32) - small buffer
    let metrics = benchmark_buffered_stream_polling(&env, topic, TEST_MESSAGE_COUNT, 32).await;
    metrics.print();
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 3: FastConsumer.buffered_stream(128) - larger buffer
    let metrics = benchmark_buffered_stream_polling(&env, topic, TEST_MESSAGE_COUNT, 128).await;
    metrics.print();
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 4: FastConsumer.dedicated_stream() - dedicated thread
    let metrics = benchmark_dedicated_thread_polling(&env, topic, TEST_MESSAGE_COUNT);
    metrics.print();
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 5: FastConsumer.poll_batch() - sync batch polling
    let metrics = benchmark_poll_batch(&env, topic, TEST_MESSAGE_COUNT);
    metrics.print();
    all_metrics.push(metrics);

    // Print comparison table
    print_comparison(&all_metrics);

    println!("\n{}", "=".repeat(80));
    println!("Benchmark complete!");
    println!("{}", "=".repeat(80));
}

/// Quick comparison with fewer messages for rapid iteration
#[tokio::test]
#[ignore] // Run explicitly with: cargo test kafka_consumer_quick_comparison --release -- --ignored --nocapture
async fn kafka_consumer_quick_comparison() {
    const QUICK_MESSAGE_COUNT: usize = 1_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Quick FastConsumer Polling Comparison");
    println!("{}", "=".repeat(80));
    println!("Messages per test: {}", QUICK_MESSAGE_COUNT);
    println!("{}", "=".repeat(80));

    let env = KafkaTestEnv::new().await;
    let topic = "quick-benchmark-topic";

    env.create_topic(topic)
        .await
        .expect("Failed to create topic");
    env.produce_messages(topic, QUICK_MESSAGE_COUNT + WARMUP_MESSAGES)
        .await
        .expect("Failed to produce messages");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut all_metrics = Vec::new();

    // Test all polling strategies with fewer messages
    let metrics = benchmark_async_stream_polling(&env, topic, QUICK_MESSAGE_COUNT).await;
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = benchmark_buffered_stream_polling(&env, topic, QUICK_MESSAGE_COUNT, 32).await;
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = benchmark_dedicated_thread_polling(&env, topic, QUICK_MESSAGE_COUNT);
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = benchmark_poll_batch(&env, topic, QUICK_MESSAGE_COUNT);
    all_metrics.push(metrics);

    print_comparison(&all_metrics);
}
