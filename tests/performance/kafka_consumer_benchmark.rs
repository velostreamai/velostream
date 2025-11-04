//! Performance benchmarks comparing Kafka consumer implementations
//!
//! This test compares:
//! 1. StreamConsumer (kafka_consumer.rs) - Legacy async consumer
//! 2. BaseConsumer - Standard tier (KafkaStream)
//! 3. BaseConsumer - Buffered tier (BufferedKafkaStream)
//! 4. BaseConsumer - Dedicated tier (DedicatedKafkaStream)
//!
//! Run with: cargo test --test kafka_consumer_benchmark --release -- --nocapture

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
use velostream::velostream::kafka::serialization::{JsonSerializer, Serde, StringSerializer};
use velostream::velostream::sql::execution::types::FieldValue;

const TEST_MESSAGE_COUNT: usize = 10_000;
const WARMUP_MESSAGES: usize = 100;

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

/// Performance metrics for a consumer test
#[derive(Debug)]
struct PerformanceMetrics {
    consumer_type: String,
    messages_consumed: usize,
    total_duration: Duration,
    messages_per_second: f64,
    avg_latency_ms: f64,
    min_latency_ms: f64,
    max_latency_ms: f64,
    p50_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
}

impl PerformanceMetrics {
    fn new(
        consumer_type: String,
        messages_consumed: usize,
        total_duration: Duration,
        latencies: Vec<Duration>,
    ) -> Self {
        let total_secs = total_duration.as_secs_f64();
        let messages_per_second = messages_consumed as f64 / total_secs;

        let mut latency_ms: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
        latency_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg_latency_ms = latency_ms.iter().sum::<f64>() / latency_ms.len() as f64;
        let min_latency_ms = latency_ms.first().copied().unwrap_or(0.0);
        let max_latency_ms = latency_ms.last().copied().unwrap_or(0.0);

        let p50_idx = (latency_ms.len() as f64 * 0.50) as usize;
        let p95_idx = (latency_ms.len() as f64 * 0.95) as usize;
        let p99_idx = (latency_ms.len() as f64 * 0.99) as usize;

        let p50_latency_ms = latency_ms.get(p50_idx).copied().unwrap_or(0.0);
        let p95_latency_ms = latency_ms.get(p95_idx).copied().unwrap_or(0.0);
        let p99_latency_ms = latency_ms.get(p99_idx).copied().unwrap_or(0.0);

        Self {
            consumer_type,
            messages_consumed,
            total_duration,
            messages_per_second,
            avg_latency_ms,
            min_latency_ms,
            max_latency_ms,
            p50_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
        }
    }

    fn print(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Consumer Type: {}", self.consumer_type);
        println!("{}", "=".repeat(80));
        println!("Messages Consumed:  {}", self.messages_consumed);
        println!("Total Duration:     {:?}", self.total_duration);
        println!("Throughput:         {:.0} msg/s", self.messages_per_second);
        println!("\nLatency Statistics (ms):");
        println!("  Average:  {:.3}", self.avg_latency_ms);
        println!("  Min:      {:.3}", self.min_latency_ms);
        println!("  Max:      {:.3}", self.max_latency_ms);
        println!("  P50:      {:.3}", self.p50_latency_ms);
        println!("  P95:      {:.3}", self.p95_latency_ms);
        println!("  P99:      {:.3}", self.p99_latency_ms);
    }
}

/// Kafka test environment
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

        // FutureProducer automatically flushes when futures are awaited
        // No explicit flush() needed

        Ok(())
    }
}

/// Benchmark the old StreamConsumer-based consumer
async fn benchmark_stream_consumer(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    println!("\n>>> Benchmarking: FastConsumer (BaseConsumer-based)");

    let base_consumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("group.id", "benchmark-stream-consumer")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create BaseConsumer");

    let consumer = FastConsumer::<String, TestMessage, StringSerializer, JsonSerializer>::new(
        base_consumer,
        StringSerializer,
        JsonSerializer,
    );

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    // Warmup
    println!("Warming up...");
    let mut warmup_stream = consumer.stream();
    for _ in 0..WARMUP_MESSAGES {
        if warmup_stream.next().await.is_none() {
            break;
        }
    }

    // Actual benchmark
    println!("Starting benchmark...");
    let mut stream = consumer.stream();
    let mut consumed = 0;
    let mut latencies = Vec::new();
    let start = Instant::now();

    while consumed < message_count {
        let msg_start = Instant::now();

        match stream.next().await {
            Some(Ok(_msg)) => {
                latencies.push(msg_start.elapsed());
                consumed += 1;

                if consumed % 1000 == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Some(Err(e)) => {
                eprintln!("Error consuming message: {:?}", e);
                break;
            }
            None => break,
        }
    }

    let duration = start.elapsed();
    println!("\nCompleted");

    PerformanceMetrics::new(
        "StreamConsumer (Legacy)".to_string(),
        consumed,
        duration,
        latencies,
    )
}

/// Benchmark the new BaseConsumer with standard stream
async fn benchmark_standard_consumer(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    println!("\n>>> Benchmarking: BaseConsumer - Standard Stream");

    let base_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("group.id", "benchmark-standard-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create BaseConsumer");

    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    let consumer: FastConsumer<String, TestMessage, StringSerializer, JsonSerializer> =
        FastConsumer::new(base_consumer, StringSerializer, JsonSerializer);

    // Warmup
    println!("Warming up...");
    let mut warmup_stream = consumer.stream();
    for _ in 0..WARMUP_MESSAGES {
        if warmup_stream.next().await.is_none() {
            break;
        }
    }

    // Actual benchmark
    println!("Starting benchmark...");
    let mut stream = consumer.stream();
    let mut consumed = 0;
    let mut latencies = Vec::new();
    let start = Instant::now();

    while consumed < message_count {
        let msg_start = Instant::now();

        match stream.next().await {
            Some(Ok(_msg)) => {
                latencies.push(msg_start.elapsed());
                consumed += 1;

                if consumed % 1000 == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Some(Err(e)) => {
                eprintln!("Error consuming message: {:?}", e);
                break;
            }
            None => break,
        }
    }

    let duration = start.elapsed();
    println!("\nCompleted");

    PerformanceMetrics::new(
        "BaseConsumer - Standard".to_string(),
        consumed,
        duration,
        latencies,
    )
}

/// Benchmark the new BaseConsumer with buffered stream
async fn benchmark_buffered_consumer(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
    batch_size: usize,
) -> PerformanceMetrics {
    println!(
        "\n>>> Benchmarking: BaseConsumer - Buffered Stream (batch_size={})",
        batch_size
    );

    let base_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("group.id", "benchmark-buffered-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create BaseConsumer");

    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    let consumer: FastConsumer<String, TestMessage, StringSerializer, JsonSerializer> =
        FastConsumer::new(base_consumer, StringSerializer, JsonSerializer);

    // Warmup
    println!("Warming up...");
    let mut warmup_stream = consumer.buffered_stream(batch_size);
    for _ in 0..WARMUP_MESSAGES {
        if warmup_stream.next().await.is_none() {
            break;
        }
    }

    // Actual benchmark
    println!("Starting benchmark...");
    let mut stream = consumer.buffered_stream(batch_size);
    let mut consumed = 0;
    let mut latencies = Vec::new();
    let start = Instant::now();

    while consumed < message_count {
        let msg_start = Instant::now();

        match stream.next().await {
            Some(Ok(_msg)) => {
                latencies.push(msg_start.elapsed());
                consumed += 1;

                if consumed % 1000 == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Some(Err(e)) => {
                eprintln!("Error consuming message: {:?}", e);
                break;
            }
            None => break,
        }
    }

    let duration = start.elapsed();
    println!("\nCompleted");

    PerformanceMetrics::new(
        format!("BaseConsumer - Buffered ({})", batch_size),
        consumed,
        duration,
        latencies,
    )
}

/// Benchmark the new BaseConsumer with dedicated thread
fn benchmark_dedicated_consumer(
    env: &KafkaTestEnv,
    topic: &str,
    message_count: usize,
) -> PerformanceMetrics {
    println!("\n>>> Benchmarking: BaseConsumer - Dedicated Thread");

    let base_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("group.id", "benchmark-dedicated-consumer")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create BaseConsumer");

    base_consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe");

    let consumer = Arc::new(FastConsumer::<
        String,
        TestMessage,
        JsonSerializer,
        JsonSerializer,
    >::new(base_consumer, JsonSerializer, JsonSerializer));

    // Warmup
    println!("Warming up...");
    let mut warmup_stream = consumer.clone().dedicated_stream();
    for _ in 0..WARMUP_MESSAGES {
        if warmup_stream.next().is_none() {
            break;
        }
    }

    // Actual benchmark
    println!("Starting benchmark...");
    let mut stream = consumer.dedicated_stream();
    let mut consumed = 0;
    let mut latencies = Vec::new();
    let start = Instant::now();

    while consumed < message_count {
        let msg_start = Instant::now();

        match stream.next() {
            Some(Ok(_msg)) => {
                latencies.push(msg_start.elapsed());
                consumed += 1;

                if consumed % 1000 == 0 {
                    print!(".");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
            Some(Err(e)) => {
                eprintln!("Error consuming message: {:?}", e);
                break;
            }
            None => break,
        }
    }

    let duration = start.elapsed();
    println!("\nCompleted");

    PerformanceMetrics::new(
        "BaseConsumer - Dedicated".to_string(),
        consumed,
        duration,
        latencies,
    )
}

/// Print comparison table
fn print_comparison(metrics: &[PerformanceMetrics]) {
    println!("\n{}", "=".repeat(120));
    println!("{:^120}", "PERFORMANCE COMPARISON");
    println!("{}", "=".repeat(120));
    println!(
        "{:<30} {:>12} {:>15} {:>12} {:>12} {:>12} {:>12}",
        "Consumer Type", "Messages", "Duration (s)", "Throughput", "Avg Lat", "P95 Lat", "P99 Lat"
    );
    println!(
        "{:<30} {:>12} {:>15} {:>12} {:>12} {:>12} {:>12}",
        "", "", "", "(msg/s)", "(ms)", "(ms)", "(ms)"
    );
    println!("{}", "-".repeat(120));

    for metric in metrics {
        println!(
            "{:<30} {:>12} {:>15.3} {:>12.0} {:>12.3} {:>12.3} {:>12.3}",
            metric.consumer_type,
            metric.messages_consumed,
            metric.total_duration.as_secs_f64(),
            metric.messages_per_second,
            metric.avg_latency_ms,
            metric.p95_latency_ms,
            metric.p99_latency_ms,
        );
    }

    println!("{}", "=".repeat(120));

    // Calculate speedups relative to StreamConsumer
    if let Some(baseline) = metrics.first() {
        println!("\nSpeedup vs {} (baseline):", baseline.consumer_type);
        println!("{}", "-".repeat(80));
        for metric in metrics.iter().skip(1) {
            let speedup = metric.messages_per_second / baseline.messages_per_second;
            println!("  {:<40} {:>6.2}x faster", metric.consumer_type, speedup);
        }
    }

    println!();
}

#[tokio::test]
#[ignore] // Run explicitly with: cargo test --test kafka_consumer_benchmark --release -- --ignored --nocapture
async fn kafka_consumer_performance_comparison() {
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Kafka Consumer Performance Benchmark");
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

    // Benchmark 1: StreamConsumer (Legacy)
    let metrics = benchmark_stream_consumer(&env, topic, TEST_MESSAGE_COUNT).await;
    metrics.print();
    all_metrics.push(metrics);

    // Wait between tests
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 2: BaseConsumer - Standard
    let metrics = benchmark_standard_consumer(&env, topic, TEST_MESSAGE_COUNT).await;
    metrics.print();
    all_metrics.push(metrics);

    // Wait between tests
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 3: BaseConsumer - Buffered (batch_size=32)
    let metrics = benchmark_buffered_consumer(&env, topic, TEST_MESSAGE_COUNT, 32).await;
    metrics.print();
    all_metrics.push(metrics);

    // Wait between tests
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 4: BaseConsumer - Buffered (batch_size=128)
    let metrics = benchmark_buffered_consumer(&env, topic, TEST_MESSAGE_COUNT, 128).await;
    metrics.print();
    all_metrics.push(metrics);

    // Wait between tests
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Benchmark 5: BaseConsumer - Dedicated
    let metrics = benchmark_dedicated_consumer(&env, topic, TEST_MESSAGE_COUNT);
    metrics.print();
    all_metrics.push(metrics);

    // Print comparison table
    print_comparison(&all_metrics);

    println!("\n{}", "=".repeat(80));
    println!("Benchmark complete!");
    println!("{}", "=".repeat(80));
}

#[tokio::test]
#[ignore]
async fn kafka_consumer_quick_comparison() {
    // Shorter version with fewer messages for quick testing
    const QUICK_MESSAGE_COUNT: usize = 1_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Quick Kafka Consumer Comparison");
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

    // Test all consumer types with fewer messages
    let metrics = benchmark_stream_consumer(&env, topic, QUICK_MESSAGE_COUNT).await;
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = benchmark_standard_consumer(&env, topic, QUICK_MESSAGE_COUNT).await;
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = benchmark_buffered_consumer(&env, topic, QUICK_MESSAGE_COUNT, 32).await;
    all_metrics.push(metrics);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let metrics = benchmark_dedicated_consumer(&env, topic, QUICK_MESSAGE_COUNT);
    all_metrics.push(metrics);

    print_comparison(&all_metrics);
}
