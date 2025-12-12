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

use chrono::Local;
use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdKafkaConsumer};
use rdkafka::producer::BaseRecord;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use velostream::velostream::kafka::kafka_fast_consumer::Consumer as FastConsumer;
use velostream::velostream::kafka::serialization::{JsonSerializer, StringSerializer};
use velostream::velostream::kafka::{AsyncPolledProducer, PolledProducer};
use velostream::velostream::serialization::avro_codec::AvroCodec;
use velostream::velostream::sql::execution::types::FieldValue;

/// Helper macro for timestamped println
macro_rules! tprintln {
    ($($arg:tt)*) => {
        println!("[{}] {}", Local::now().format("%H:%M:%S%.3f"), format!($($arg)*))
    };
}

// =============================================================================
// Configuration Constants
// =============================================================================

const TEST_MESSAGE_COUNT: usize = 1_000_000;
const WARMUP_MESSAGES: usize = 1000;
/// Maximum time to wait for a single message during warmup/benchmark
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time to wait for warmup phase to complete
const WARMUP_TIMEOUT: Duration = Duration::from_secs(120);
/// Maximum time for entire benchmark to complete (per consumer type)
const BENCHMARK_TIMEOUT: Duration = Duration::from_secs(600);

/// Avro schema for TestMessage structure (id: long, timestamp: long, data: string)
const AVRO_TEST_MESSAGE_SCHEMA: &str = r#"
{
    "type": "record",
    "name": "TestMessage",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "timestamp", "type": "long"},
        {"name": "data", "type": "string"}
    ]
}
"#;

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

    /// Create with a pre-computed timestamp (faster for bulk production)
    fn with_timestamp(id: u64, timestamp: i64) -> Self {
        Self {
            id,
            timestamp,
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
    producer: AsyncPolledProducer,
}

impl KafkaTestEnv {
    async fn new() -> Self {
        // Use local Kafka for testing
        // Note: Requires Kafka running on localhost:9092
        let bootstrap_servers = "localhost:9092".to_string();

        let producer = AsyncPolledProducer::with_high_throughput(&bootstrap_servers)
            .expect("Failed to create AsyncPolledProducer");

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

    fn produce_messages(
        &mut self,
        topic: &str,
        count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tprintln!("Producing {} messages to topic '{}'...", count, topic);
        let start = Instant::now();

        // Pre-compute timestamp once per batch for speed
        let batch_timestamp = chrono::Utc::now().timestamp_millis();

        // Reuse key buffer to avoid allocations
        let mut key_buf = itoa::Buffer::new();

        // Progress reporting interval: every 100K messages or 10% of total, whichever is smaller
        let progress_interval = (count / 10).max(100_000).min(count);

        for i in 0..count {
            let message = TestMessage::with_timestamp(i as u64, batch_timestamp);
            let payload = serde_json::to_string(&message)?;
            let key = key_buf.format(i);

            let record = BaseRecord::to(topic).payload(payload.as_bytes()).key(key);

            // Non-blocking send - poll thread handles callbacks
            let _ = self.producer.send(record);

            // Periodic flush every 10K messages to prevent memory exhaustion
            if (i + 1) % 10_000 == 0 {
                self.producer.flush(Duration::from_secs(10))?;
            }

            if (i + 1) % progress_interval == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = (i + 1) as f64 / elapsed;
                tprintln!(
                    "[JSON] Produced {}/{} messages ({:.1}%) - {:.0} msg/s",
                    i + 1,
                    count,
                    ((i + 1) as f64 / count as f64) * 100.0,
                    rate
                );
            }
        }

        // Final flush to ensure all messages are delivered
        tprintln!("Flushing remaining messages...");
        self.producer.flush(Duration::from_secs(30))?;

        let elapsed = start.elapsed();
        let rate = count as f64 / elapsed.as_secs_f64();
        tprintln!(
            "Produced {} JSON messages in {:.2}s ({:.0} msg/s)",
            count,
            elapsed.as_secs_f64(),
            rate
        );
        Ok(())
    }

    /// Produce Avro-encoded messages to a topic
    fn produce_avro_messages(
        &mut self,
        topic: &str,
        count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tprintln!(
            "Producing {} Avro-encoded messages to topic '{}'...",
            count,
            topic
        );
        let start = Instant::now();

        // Create Avro codec with schema matching TestMessage structure
        let avro_codec = AvroCodec::new(AVRO_TEST_MESSAGE_SCHEMA)?;

        // Pre-compute timestamp once per batch for speed
        let batch_timestamp = chrono::Utc::now().timestamp_millis();

        // Reuse key buffer to avoid allocations
        let mut key_buf = itoa::Buffer::new();

        // Pre-allocate HashMap with known capacity (3 fields)
        // Reuse HashMap by clearing instead of recreating
        let mut record_data: HashMap<String, FieldValue> = HashMap::with_capacity(3);

        // Pre-allocate field name strings (reused across iterations)
        let id_key = "id".to_string();
        let timestamp_key = "timestamp".to_string();
        let data_key = "data".to_string();

        // Progress reporting interval: every 100K messages or 10% of total, whichever is smaller
        let progress_interval = (count / 10).max(100_000).min(count);

        for i in 0..count {
            // Reuse HashMap by clearing and re-inserting
            record_data.clear();
            record_data.insert(id_key.clone(), FieldValue::Integer(i as i64));
            record_data.insert(timestamp_key.clone(), FieldValue::Integer(batch_timestamp));
            record_data.insert(
                data_key.clone(),
                FieldValue::String(format!("Test message {}", i)),
            );

            let payload = avro_codec.serialize(&record_data)?;
            let key = key_buf.format(i);

            let record = BaseRecord::to(topic).payload(payload.as_slice()).key(key);

            // Non-blocking send - poll thread handles callbacks
            let _ = self.producer.send(record);

            // Periodic flush every 10K messages to prevent memory exhaustion
            if (i + 1) % 10_000 == 0 {
                self.producer.flush(Duration::from_secs(10))?;
            }

            if (i + 1) % progress_interval == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = (i + 1) as f64 / elapsed;
                tprintln!(
                    "[Avro] Produced {}/{} messages ({:.1}%) - {:.0} msg/s",
                    i + 1,
                    count,
                    ((i + 1) as f64 / count as f64) * 100.0,
                    rate
                );
            }
        }

        // Final flush to ensure all messages are delivered
        tprintln!("Flushing remaining Avro messages...");
        self.producer.flush(Duration::from_secs(30))?;

        let elapsed = start.elapsed();
        let rate = count as f64 / elapsed.as_secs_f64();
        tprintln!(
            "Produced {} Avro messages in {:.2}s ({:.0} msg/s)",
            count,
            elapsed.as_secs_f64(),
            rate
        );
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
    tprintln!("Warming up...");
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
    tprintln!(
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
    tprintln!("Starting benchmark...");
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
    tprintln!(
        "Completed ({} messages in {:.2}s)",
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
    tprintln!(">>> Benchmarking: FastConsumer.stream() (async polling)");

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
    tprintln!(
        ">>> Benchmarking: FastConsumer.buffered_stream({}) (buffered async)",
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
    tprintln!(">>> Benchmarking: FastConsumer.dedicated_stream() (dedicated thread)");

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
    tprintln!("Warming up...");
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
    tprintln!(
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
    tprintln!("Starting benchmark...");
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
    tprintln!(
        "Completed ({} messages in {:.2}s)",
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
    tprintln!(">>> Benchmarking: FastConsumer.poll_batch() (sync batch, batch_size=100)");

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
    tprintln!("Warming up...");
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
    tprintln!(
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
    tprintln!("Starting benchmark...");
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
    tprintln!(
        "Completed ({} records in {:.2}s)",
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

/// Benchmark FastConsumer.poll_batch() with Avro serialization
///
/// This benchmark uses AvroCodec for deserializing Avro-encoded messages
/// to HashMap<String, FieldValue>, comparing performance against JSON.
fn benchmark_poll_batch_avro(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    tprintln!(">>> Benchmarking: FastConsumer.poll_batch() with Avro (sync batch, batch_size=100)");

    let base_consumer = env.create_base_consumer("benchmark-poll-batch-avro");
    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    // Create Avro codec with the test message schema
    let avro_codec = AvroCodec::new(AVRO_TEST_MESSAGE_SCHEMA).expect("Failed to create AvroCodec");

    // poll_batch requires V = HashMap<String, FieldValue>
    let mut consumer: FastConsumer<
        String,
        HashMap<String, FieldValue>,
        StringSerializer,
        AvroCodec,
    > = FastConsumer::new(base_consumer, StringSerializer, avro_codec);

    // Warmup with timeout
    tprintln!("Warming up...");
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
    tprintln!(
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
    tprintln!("Starting benchmark...");
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
    tprintln!(
        "Completed ({} records in {:.2}s)",
        consumed,
        duration.as_secs_f64()
    );

    PerformanceMetrics::new(
        "FastConsumer.poll_batch() [Avro]".to_string(),
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
    let mut env = KafkaTestEnv::new().await;
    let topic = "benchmark-topic";

    // Create topic
    env.create_topic(topic)
        .await
        .expect("Failed to create topic");

    // Produce test messages
    env.produce_messages(topic, TEST_MESSAGE_COUNT + WARMUP_MESSAGES)
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

    let mut env = KafkaTestEnv::new().await;
    let topic = "quick-benchmark-topic";

    env.create_topic(topic)
        .await
        .expect("Failed to create topic");
    env.produce_messages(topic, QUICK_MESSAGE_COUNT + WARMUP_MESSAGES)
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

/// Avro serialization benchmark comparing JSON vs Avro poll_batch performance
#[tokio::test]
#[ignore] // Run explicitly with: cargo test kafka_consumer_avro_benchmark --release -- --ignored --nocapture
async fn kafka_consumer_avro_benchmark() {
    const AVRO_MESSAGE_COUNT: usize = 50_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "JSON vs Avro Serialization Benchmark");
    println!("{}", "=".repeat(80));
    println!("Messages per test: {}", AVRO_MESSAGE_COUNT);
    println!("{}", "=".repeat(80));

    let mut env = KafkaTestEnv::new().await;

    // Create topics for JSON and Avro
    let json_topic = "benchmark-json-topic";
    let avro_topic = "benchmark-avro-topic";

    env.create_topic(json_topic)
        .await
        .expect("Failed to create JSON topic");
    env.create_topic(avro_topic)
        .await
        .expect("Failed to create Avro topic");

    // Produce messages in both formats
    env.produce_messages(json_topic, AVRO_MESSAGE_COUNT + WARMUP_MESSAGES)
        .expect("Failed to produce JSON messages");
    env.produce_avro_messages(avro_topic, AVRO_MESSAGE_COUNT + WARMUP_MESSAGES)
        .expect("Failed to produce Avro messages");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut all_metrics = Vec::new();

    // Benchmark JSON poll_batch
    let metrics = benchmark_poll_batch(&env, json_topic, AVRO_MESSAGE_COUNT);
    metrics.print();
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark Avro poll_batch
    let metrics = benchmark_poll_batch_avro(&env, avro_topic, AVRO_MESSAGE_COUNT);
    metrics.print();
    all_metrics.push(metrics);

    // Print comparison table
    print_comparison(&all_metrics);

    println!("\n{}", "=".repeat(80));
    println!("JSON vs Avro Benchmark complete!");
    println!("{}", "=".repeat(80));
}

/// Pure serialization microbenchmark - isolates deserialization overhead without Kafka
/// This test compares JSON vs Avro deserialization performance in a tight loop
#[test]
#[ignore] // Run explicitly with: cargo test serialization_microbenchmark --release -- --ignored --nocapture
fn serialization_microbenchmark() {
    use std::hint::black_box;

    const ITERATIONS: usize = 100_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Pure Serialization Microbenchmark");
    println!("{}", "=".repeat(80));
    println!("Iterations: {}", ITERATIONS);
    println!("{}", "=".repeat(80));

    // Create sample data
    let sample_json =
        r#"{"id":12345,"timestamp":1699999999999,"data":"test_message_with_some_realistic_data"}"#;

    // Create Avro codec and serialize the sample data
    let avro_codec = AvroCodec::new(AVRO_TEST_MESSAGE_SCHEMA).expect("Failed to create AvroCodec");
    let mut sample_record: HashMap<String, FieldValue> = HashMap::new();
    sample_record.insert("id".to_string(), FieldValue::Integer(12345));
    sample_record.insert("timestamp".to_string(), FieldValue::Integer(1699999999999));
    sample_record.insert(
        "data".to_string(),
        FieldValue::String("test_message_with_some_realistic_data".to_string()),
    );
    let sample_avro = avro_codec
        .serialize(&sample_record)
        .expect("Failed to serialize Avro");

    println!("\nMessage sizes:");
    println!("  JSON: {} bytes", sample_json.len());
    println!("  Avro: {} bytes", sample_avro.len());

    // Benchmark JSON deserialization
    println!("\n--- JSON Deserialization ---");
    let json_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: HashMap<String, FieldValue> =
            black_box(serde_json::from_str(sample_json).expect("JSON deser failed"));
        black_box(result);
    }
    let json_duration = json_start.elapsed();
    let json_ns_per_op = json_duration.as_nanos() / ITERATIONS as u128;
    let json_ops_per_sec = 1_000_000_000.0 / json_ns_per_op as f64;
    println!(
        "  Total time: {:?} ({} ns/op, {:.0} ops/sec)",
        json_duration, json_ns_per_op, json_ops_per_sec
    );

    // Benchmark Avro deserialization
    println!("\n--- Avro Deserialization ---");
    let avro_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result = black_box(
            avro_codec
                .deserialize(&sample_avro)
                .expect("Avro deser failed"),
        );
        black_box(result);
    }
    let avro_duration = avro_start.elapsed();
    let avro_ns_per_op = avro_duration.as_nanos() / ITERATIONS as u128;
    let avro_ops_per_sec = 1_000_000_000.0 / avro_ns_per_op as f64;
    println!(
        "  Total time: {:?} ({} ns/op, {:.0} ops/sec)",
        avro_duration, avro_ns_per_op, avro_ops_per_sec
    );

    // Breakdown: Benchmark just apache_avro::from_avro_datum (without FieldValue conversion)
    println!("\n--- Avro Datum Decode Only (no FieldValue conversion) ---");
    let schema = avro_codec.schema().clone();
    let datum_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result = black_box(
            apache_avro::from_avro_datum(&schema, &mut &sample_avro[..], None)
                .expect("Datum decode failed"),
        );
        black_box(result);
    }
    let datum_duration = datum_start.elapsed();
    let datum_ns_per_op = datum_duration.as_nanos() / ITERATIONS as u128;
    let datum_ops_per_sec = 1_000_000_000.0 / datum_ns_per_op as f64;
    println!(
        "  Total time: {:?} ({} ns/op, {:.0} ops/sec)",
        datum_duration, datum_ns_per_op, datum_ops_per_sec
    );

    // Summary
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Summary");
    println!("{}", "=".repeat(80));
    println!(
        "JSON:                    {:.0} ops/sec ({} ns/op)",
        json_ops_per_sec, json_ns_per_op
    );
    println!(
        "Avro (full):             {:.0} ops/sec ({} ns/op)",
        avro_ops_per_sec, avro_ns_per_op
    );
    println!(
        "Avro (datum only):       {:.0} ops/sec ({} ns/op)",
        datum_ops_per_sec, datum_ns_per_op
    );
    println!();
    println!(
        "Avro vs JSON ratio:      {:.2}x",
        json_ops_per_sec / avro_ops_per_sec
    );
    println!(
        "FieldValue conversion overhead: {:.2}x of datum decode",
        (avro_ns_per_op as f64 - datum_ns_per_op as f64) / datum_ns_per_op as f64
    );
    println!(
        "FieldValue conversion time: {} ns/op ({:.1}% of total Avro time)",
        avro_ns_per_op - datum_ns_per_op,
        (avro_ns_per_op - datum_ns_per_op) as f64 / avro_ns_per_op as f64 * 100.0
    );
    println!("{}", "=".repeat(80));
}
