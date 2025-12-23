//! Testcontainers-based Kafka Consumer/Producer Throughput Test
//!
//! This test measures end-to-end Kafka throughput by:
//! 1. Starting a Kafka container via testcontainers
//! 2. Populating a source topic with configurable record count (default 1M)
//! 3. Running a consumer-to-producer pipeline (read from source, write to sink)
//! 4. Measuring throughput with different payload types and consumer configurations
//!
//! Run with:
//! ```bash
//! # Quick test (10k records)
//! cargo test kafka_throughput_quick -- --ignored --nocapture
//!
//! # Full test (1M records) - requires ~5 minutes
//! cargo test kafka_throughput_1m -- --ignored --nocapture
//!
//! # Different payload sizes
//! cargo test kafka_throughput_payload_comparison -- --ignored --nocapture
//! ```

use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RdKafkaConsumer};
use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "test-support"))]
use testcontainers::ContainerAsync;
#[cfg(any(test, feature = "test-support"))]
use testcontainers::runners::AsyncRunner;
#[cfg(any(test, feature = "test-support"))]
use testcontainers_modules::kafka::KAFKA_PORT;

/// Payload types for testing different message sizes
#[derive(Clone, Copy, Debug)]
pub enum PayloadType {
    /// Small JSON payload (~50 bytes)
    Small,
    /// Medium JSON payload (~500 bytes)
    Medium,
    /// Large JSON payload (~5KB)
    Large,
    /// Financial trade data (~200 bytes)
    Financial,
}

impl PayloadType {
    fn generate_payload(&self, id: u64) -> String {
        match self {
            PayloadType::Small => {
                format!(
                    r#"{{"id":{},"ts":{},"v":"x"}}"#,
                    id,
                    chrono::Utc::now().timestamp_millis()
                )
            }
            PayloadType::Medium => {
                let data = "x".repeat(400);
                format!(
                    r#"{{"id":{},"timestamp":{},"data":"{}","extra":{{"nested":true}}}}"#,
                    id,
                    chrono::Utc::now().timestamp_millis(),
                    data
                )
            }
            PayloadType::Large => {
                let data = "x".repeat(4500);
                format!(
                    r#"{{"id":{},"timestamp":{},"data":"{}","metadata":{{"source":"test","version":"1.0"}}}}"#,
                    id,
                    chrono::Utc::now().timestamp_millis(),
                    data
                )
            }
            PayloadType::Financial => {
                format!(
                    r#"{{"trade_id":"TRD-{:08}","symbol":"AAPL","price":{},"quantity":{},"side":"{}","timestamp":{},"exchange":"NYSE","trader_id":"TRADER-{:04}"}}"#,
                    id,
                    150.0 + (id as f64 % 100.0) / 100.0,
                    100 + (id % 1000),
                    if id % 2 == 0 { "BUY" } else { "SELL" },
                    chrono::Utc::now().timestamp_millis(),
                    id % 100
                )
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            PayloadType::Small => "Small (~50B)",
            PayloadType::Medium => "Medium (~500B)",
            PayloadType::Large => "Large (~5KB)",
            PayloadType::Financial => "Financial (~200B)",
        }
    }
}

/// Consumer configuration presets for testing
#[derive(Clone, Copy, Debug)]
pub enum ConsumerConfig {
    /// Low latency settings (current test harness defaults)
    LowLatency,
    /// High throughput settings
    HighThroughput,
    /// Balanced settings
    Balanced,
}

impl ConsumerConfig {
    fn apply(&self, config: &mut ClientConfig) {
        match self {
            ConsumerConfig::LowLatency => {
                // Small fetches for fast response
                config.set("fetch.min.bytes", "1");
                config.set("fetch.wait.max.ms", "100");
                config.set("fetch.message.max.bytes", "1048576"); // 1MB per partition
                config.set("queued.min.messages", "100"); // Min messages before fetch
            }
            ConsumerConfig::HighThroughput => {
                // Large batch fetches for throughput
                config.set("fetch.min.bytes", "1048576"); // 1MB min before returning
                config.set("fetch.wait.max.ms", "500");
                config.set("fetch.message.max.bytes", "52428800"); // 50MB per partition
                config.set("queued.min.messages", "10000");
                config.set("queued.max.messages.kbytes", "65536"); // 64MB queue
                config.set("receive.message.max.bytes", "104857600"); // 100MB max
            }
            ConsumerConfig::Balanced => {
                // Balanced settings
                config.set("fetch.min.bytes", "16384"); // 16KB
                config.set("fetch.wait.max.ms", "200");
                config.set("fetch.message.max.bytes", "10485760"); // 10MB per partition
                config.set("queued.min.messages", "1000");
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ConsumerConfig::LowLatency => "Low Latency",
            ConsumerConfig::HighThroughput => "High Throughput",
            ConsumerConfig::Balanced => "Balanced",
        }
    }
}

/// Producer configuration presets
#[derive(Clone, Copy, Debug)]
pub enum ProducerConfig {
    /// Low latency settings
    LowLatency,
    /// High throughput settings
    HighThroughput,
    /// Balanced settings
    Balanced,
}

impl ProducerConfig {
    fn apply(&self, config: &mut ClientConfig) {
        match self {
            ProducerConfig::LowLatency => {
                config.set("linger.ms", "0");
                config.set("batch.size", "16384"); // 16KB
                config.set("acks", "1");
            }
            ProducerConfig::HighThroughput => {
                config.set("linger.ms", "50");
                config.set("batch.size", "1048576"); // 1MB
                config.set("acks", "1");
                config.set("compression.type", "lz4");
            }
            ProducerConfig::Balanced => {
                config.set("linger.ms", "5");
                config.set("batch.size", "65536"); // 64KB
                config.set("acks", "1");
            }
        }
    }

    fn name(&self) -> &'static str {
        match self {
            ProducerConfig::LowLatency => "Low Latency",
            ProducerConfig::HighThroughput => "High Throughput",
            ProducerConfig::Balanced => "Balanced",
        }
    }
}

/// Test result metrics
#[derive(Debug, Clone)]
pub struct ThroughputMetrics {
    pub test_name: String,
    pub payload_type: String,
    pub consumer_config: String,
    pub producer_config: String,
    pub total_records: u64,
    pub duration: Duration,
    pub records_per_second: f64,
    pub bytes_per_second: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
}

impl ThroughputMetrics {
    fn print(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Test: {}", self.test_name);
        println!("{}", "=".repeat(80));
        println!("Payload Type:    {}", self.payload_type);
        println!("Consumer Config: {}", self.consumer_config);
        println!("Producer Config: {}", self.producer_config);
        println!("{}", "-".repeat(80));
        println!("Total Records:   {:>12}", self.total_records);
        println!("Duration:        {:>12.2}s", self.duration.as_secs_f64());
        println!(
            "Throughput:      {:>12.0} records/sec",
            self.records_per_second
        );
        println!(
            "Bandwidth:       {:>12.2} MB/sec",
            self.bytes_per_second / 1_000_000.0
        );
        println!("{}", "-".repeat(80));
        println!("Latencies:");
        println!("  Average:       {:>12.3} ms", self.avg_latency_ms);
        println!("  P95:           {:>12.3} ms", self.p95_latency_ms);
        println!("  P99:           {:>12.3} ms", self.p99_latency_ms);
        println!("{}", "=".repeat(80));
    }
}

/// Kafka testcontainers environment
struct KafkaTestcontainersEnv {
    bootstrap_servers: String,
    #[allow(dead_code)]
    container: ContainerAsync<testcontainers_modules::kafka::Kafka>,
}

impl KafkaTestcontainersEnv {
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        println!("🚀 Starting Kafka container via testcontainers...");

        let container = testcontainers_modules::kafka::Kafka::default()
            .start()
            .await?;

        let host_port = container.get_host_port_ipv4(KAFKA_PORT).await?;
        let bootstrap_servers = format!("127.0.0.1:{}", host_port);

        println!("✅ Kafka container started: {}", bootstrap_servers);

        // Give Kafka time to fully initialize
        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(Self {
            bootstrap_servers,
            container,
        })
    }

    fn create_admin(
        &self,
    ) -> Result<AdminClient<DefaultClientContext>, Box<dyn std::error::Error + Send + Sync>> {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()?;
        Ok(admin)
    }

    async fn create_topic(
        &self,
        topic: &str,
        partitions: i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let admin = self.create_admin()?;
        let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        // Ignore error if topic already exists
        let _ = admin.create_topics(&[new_topic], &opts).await;

        // Wait for topic to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    fn create_producer(
        &self,
        config: ProducerConfig,
    ) -> Result<FutureProducer, Box<dyn std::error::Error + Send + Sync>> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &self.bootstrap_servers);
        client_config.set("message.timeout.ms", "30000");
        config.apply(&mut client_config);

        let producer: FutureProducer = client_config.create()?;
        Ok(producer)
    }

    fn create_consumer(
        &self,
        group_id: &str,
        config: ConsumerConfig,
    ) -> Result<BaseConsumer, Box<dyn std::error::Error + Send + Sync>> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &self.bootstrap_servers);
        client_config.set("group.id", group_id);
        client_config.set("auto.offset.reset", "earliest");
        client_config.set("enable.auto.commit", "false");
        config.apply(&mut client_config);

        let consumer: BaseConsumer = client_config.create()?;
        Ok(consumer)
    }
}

/// Populate a topic with test messages
async fn populate_topic(
    env: &KafkaTestcontainersEnv,
    topic: &str,
    record_count: u64,
    payload_type: PayloadType,
    producer_config: ProducerConfig,
) -> Result<(Duration, u64), Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "\n📝 Populating topic '{}' with {} records ({})...",
        topic,
        record_count,
        payload_type.name()
    );

    let producer = Arc::new(env.create_producer(producer_config)?);
    let start = Instant::now();
    let mut total_bytes: u64 = 0;
    let mut sent_count: u64 = 0;

    // Use smaller batches for testcontainers reliability
    let batch_size = 500;
    let send_timeout = Duration::from_secs(10);

    println!("  Starting message production (batch_size={})", batch_size);

    for batch_start in (0..record_count).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size as u64, record_count);
        let batch_count = batch_end - batch_start;

        // Pre-generate all payloads for this batch
        let batch_data: Vec<(String, String)> = (batch_start..batch_end)
            .map(|i| {
                let payload = payload_type.generate_payload(i);
                let key = format!("{}", i % 1000);
                (payload, key)
            })
            .collect();

        // Track bytes
        for (payload, _) in &batch_data {
            total_bytes += payload.len() as u64;
        }

        // Send messages and collect futures
        let mut handles = Vec::with_capacity(batch_data.len());
        for (payload, key) in batch_data {
            let producer_clone = Arc::clone(&producer);
            let topic_owned = topic.to_string();

            let handle = tokio::spawn(async move {
                let record = FutureRecord::to(&topic_owned)
                    .payload(payload.as_bytes())
                    .key(key.as_bytes());
                producer_clone.send(record, send_timeout).await
            });
            handles.push(handle);
        }

        // Wait for all sends in this batch with overall timeout
        let batch_timeout = Duration::from_secs(30);
        match tokio::time::timeout(batch_timeout, async {
            for handle in handles {
                match handle.await {
                    Ok(Ok(_)) => {}
                    Ok(Err((e, _))) => {
                        return Err(format!("Producer send failed: {:?}", e));
                    }
                    Err(e) => {
                        return Err(format!("Task join error: {:?}", e));
                    }
                }
            }
            Ok(())
        })
        .await
        {
            Ok(Ok(())) => {
                sent_count += batch_count;
            }
            Ok(Err(e)) => {
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)));
            }
            Err(_) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Batch timeout after {}s (sent {} records)",
                        batch_timeout.as_secs(),
                        sent_count
                    ),
                )));
            }
        }

        // Progress reporting every 10K or at batch boundaries
        if sent_count % 10000 < batch_size as u64 || batch_end == record_count {
            let elapsed = start.elapsed();
            let rate = if elapsed.as_secs_f64() > 0.0 {
                sent_count as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            println!("  Progress: {:>10} records ({:.0} rec/s)", sent_count, rate);
        }
    }

    let duration = start.elapsed();
    let rate = if duration.as_secs_f64() > 0.0 {
        record_count as f64 / duration.as_secs_f64()
    } else {
        record_count as f64
    };
    println!(
        "✅ Population complete: {} records in {:.2}s ({:.0} rec/s)",
        record_count,
        duration.as_secs_f64(),
        rate
    );

    Ok((duration, total_bytes))
}

/// Run consumer-to-producer throughput test
async fn run_throughput_test(
    env: &KafkaTestcontainersEnv,
    source_topic: &str,
    sink_topic: &str,
    record_count: u64,
    consumer_config: ConsumerConfig,
    producer_config: ProducerConfig,
) -> Result<ThroughputMetrics, Box<dyn std::error::Error + Send + Sync>> {
    println!("\n📊 Running throughput test...");
    println!("  Consumer: {}", consumer_config.name());
    println!("  Producer: {}", producer_config.name());

    let consumer = env.create_consumer("throughput-test-group", consumer_config)?;
    consumer.subscribe(&[source_topic])?;

    let producer = Arc::new(env.create_producer(producer_config)?);

    let mut consumed: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut latencies: Vec<Duration> = Vec::with_capacity(record_count as usize);
    let mut pending_produces: Vec<_> = Vec::with_capacity(1000);

    let start = Instant::now();
    let timeout = Duration::from_secs(300); // 5 minute timeout
    let no_progress_timeout = Duration::from_secs(30); // 30 seconds without progress = give up
    let mut last_progress: u64 = 0;
    let mut last_progress_time = Instant::now();

    while consumed < record_count {
        if start.elapsed() > timeout {
            println!("⚠️ Timeout reached after {:?}", timeout);
            break;
        }

        // Check for no-progress timeout
        if last_progress_time.elapsed() > no_progress_timeout {
            println!(
                "⚠️ No progress for {:?} (consumed {} of {}), stopping",
                no_progress_timeout, consumed, record_count
            );
            break;
        }

        let msg_start = Instant::now();

        // Poll for messages
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap_or(&[]);
                total_bytes += payload.len() as u64;
                consumed += 1;
                last_progress_time = Instant::now(); // Reset no-progress timer

                // Forward to sink topic - copy data to owned strings
                let key_owned: Option<String> =
                    msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let payload_owned = String::from_utf8_lossy(payload).to_string();
                let sink_topic_owned = sink_topic.to_string();
                let producer_clone = Arc::clone(&producer);

                // Queue the produce
                let produce_future = async move {
                    let record = if let Some(ref k) = key_owned {
                        FutureRecord::to(&sink_topic_owned)
                            .payload(payload_owned.as_bytes())
                            .key(k.as_bytes())
                    } else {
                        FutureRecord::to(&sink_topic_owned).payload(payload_owned.as_bytes())
                    };
                    producer_clone.send(record, Duration::from_secs(30)).await
                };

                pending_produces.push((msg_start, tokio::spawn(produce_future)));
            }
            Some(Err(e)) => {
                eprintln!("Consumer error: {:?}", e);
            }
            None => {
                // No message available, yield to allow pending produces to complete
                tokio::task::yield_now().await;
            }
        }

        // Periodically drain completed produces to avoid memory buildup
        if pending_produces.len() >= 500 {
            let mut new_pending = Vec::with_capacity(pending_produces.len());
            for (start_time, handle) in pending_produces.drain(..) {
                if handle.is_finished() {
                    if handle.await.is_ok() {
                        latencies.push(start_time.elapsed());
                    }
                } else {
                    new_pending.push((start_time, handle));
                }
            }
            pending_produces = new_pending;
        }

        // Progress reporting
        if consumed > 0 && consumed - last_progress >= 50000 {
            last_progress = consumed;
            let elapsed = start.elapsed();
            let rate = consumed as f64 / elapsed.as_secs_f64();
            println!("  Progress: {:>10} consumed ({:.0} rec/s)", consumed, rate);
        }
    }

    // Wait for all pending produces
    for (start_time, handle) in pending_produces {
        if handle.await.is_ok() {
            latencies.push(start_time.elapsed());
        }
    }

    let duration = start.elapsed();

    // Calculate latency percentiles
    latencies.sort();
    let avg_latency = if !latencies.is_empty() {
        latencies
            .iter()
            .map(|d| d.as_secs_f64() * 1000.0)
            .sum::<f64>()
            / latencies.len() as f64
    } else {
        0.0
    };

    let p95_idx = (latencies.len() as f64 * 0.95) as usize;
    let p99_idx = (latencies.len() as f64 * 0.99) as usize;

    let p95_latency = latencies
        .get(p95_idx)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);
    let p99_latency = latencies
        .get(p99_idx)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0);

    Ok(ThroughputMetrics {
        test_name: "Consumer→Producer Pipeline".to_string(),
        payload_type: "Various".to_string(),
        consumer_config: consumer_config.name().to_string(),
        producer_config: producer_config.name().to_string(),
        total_records: consumed,
        duration,
        records_per_second: consumed as f64 / duration.as_secs_f64(),
        bytes_per_second: total_bytes as f64 / duration.as_secs_f64(),
        avg_latency_ms: avg_latency,
        p95_latency_ms: p95_latency,
        p99_latency_ms: p99_latency,
    })
}

/// Print comparison table of multiple test results
fn print_comparison(results: &[ThroughputMetrics]) {
    println!("\n{}", "=".repeat(120));
    println!("{:^120}", "THROUGHPUT COMPARISON");
    println!("{}", "=".repeat(120));
    println!(
        "{:<25} {:>12} {:>15} {:>15} {:>12} {:>12} {:>12}",
        "Configuration", "Records", "Duration (s)", "Throughput", "MB/s", "P95 Lat", "P99 Lat"
    );
    println!(
        "{:<25} {:>12} {:>15} {:>15} {:>12} {:>12} {:>12}",
        "", "", "", "(rec/s)", "", "(ms)", "(ms)"
    );
    println!("{}", "-".repeat(120));

    for m in results {
        println!(
            "{:<25} {:>12} {:>15.2} {:>15.0} {:>12.2} {:>12.3} {:>12.3}",
            m.consumer_config,
            m.total_records,
            m.duration.as_secs_f64(),
            m.records_per_second,
            m.bytes_per_second / 1_000_000.0,
            m.p95_latency_ms,
            m.p99_latency_ms,
        );
    }
    println!("{}", "=".repeat(120));

    // Speedup comparison
    if let Some(baseline) = results.first() {
        println!("\nSpeedup vs {} (baseline):", baseline.consumer_config);
        println!("{}", "-".repeat(80));
        for m in results.iter().skip(1) {
            let speedup = m.records_per_second / baseline.records_per_second;
            println!("  {:<40} {:>6.2}x", m.consumer_config, speedup);
        }
    }
}

// =============================================================================
// Test Functions
// =============================================================================

/// Quick throughput test with 10k records
#[tokio::test]
#[ignore] // Run explicitly: cargo test kafka_throughput_quick -- --ignored --nocapture
async fn kafka_throughput_quick() {
    const RECORD_COUNT: u64 = 10_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Quick Kafka Throughput Test (10K records)");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let source_topic = "quick-source-topic";
    let sink_topic = "quick-sink-topic";

    env.create_topic(source_topic, 3)
        .await
        .expect("Failed to create source topic");
    env.create_topic(sink_topic, 3)
        .await
        .expect("Failed to create sink topic");

    // Populate source topic
    populate_topic(
        &env,
        source_topic,
        RECORD_COUNT,
        PayloadType::Financial,
        ProducerConfig::HighThroughput,
    )
    .await
    .expect("Failed to populate topic");

    // Run test with balanced settings
    let metrics = run_throughput_test(
        &env,
        source_topic,
        sink_topic,
        RECORD_COUNT,
        ConsumerConfig::Balanced,
        ProducerConfig::Balanced,
    )
    .await
    .expect("Test failed");

    metrics.print();

    println!("\n✅ Quick test completed successfully");
}

/// Full throughput test with 1M records
#[tokio::test]
#[ignore] // Run explicitly: cargo test kafka_throughput_1m -- --ignored --nocapture
async fn kafka_throughput_1m() {
    const RECORD_COUNT: u64 = 1_000_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Full Kafka Throughput Test (1M records)");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let source_topic = "full-source-topic";
    let sink_topic = "full-sink-topic";

    env.create_topic(source_topic, 6)
        .await
        .expect("Failed to create source topic");
    env.create_topic(sink_topic, 6)
        .await
        .expect("Failed to create sink topic");

    // Populate source topic with financial data
    populate_topic(
        &env,
        source_topic,
        RECORD_COUNT,
        PayloadType::Financial,
        ProducerConfig::HighThroughput,
    )
    .await
    .expect("Failed to populate topic");

    // Wait for messages to be fully committed
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Run test with high throughput settings
    let metrics = run_throughput_test(
        &env,
        source_topic,
        sink_topic,
        RECORD_COUNT,
        ConsumerConfig::HighThroughput,
        ProducerConfig::HighThroughput,
    )
    .await
    .expect("Test failed");

    metrics.print();

    println!("\n✅ Full 1M record test completed");
}

/// Compare different consumer configurations
#[tokio::test]
#[ignore] // Run explicitly: cargo test kafka_throughput_config_comparison -- --ignored --nocapture
async fn kafka_throughput_config_comparison() {
    const RECORD_COUNT: u64 = 50_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Kafka Configuration Comparison Test");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let configs = [
        (ConsumerConfig::LowLatency, ProducerConfig::LowLatency),
        (ConsumerConfig::Balanced, ProducerConfig::Balanced),
        (
            ConsumerConfig::HighThroughput,
            ProducerConfig::HighThroughput,
        ),
    ];

    let mut results = Vec::new();

    for (i, (consumer_config, producer_config)) in configs.iter().enumerate() {
        let source_topic = format!("config-source-{}", i);
        let sink_topic = format!("config-sink-{}", i);

        env.create_topic(&source_topic, 3)
            .await
            .expect("Failed to create source topic");
        env.create_topic(&sink_topic, 3)
            .await
            .expect("Failed to create sink topic");

        println!(
            "\n📦 Test {}: {} consumer, {} producer",
            i + 1,
            consumer_config.name(),
            producer_config.name()
        );

        populate_topic(
            &env,
            &source_topic,
            RECORD_COUNT,
            PayloadType::Financial,
            *producer_config,
        )
        .await
        .expect("Failed to populate topic");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let metrics = run_throughput_test(
            &env,
            &source_topic,
            &sink_topic,
            RECORD_COUNT,
            *consumer_config,
            *producer_config,
        )
        .await
        .expect("Test failed");

        metrics.print();
        results.push(metrics);

        // Small delay between tests
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    print_comparison(&results);

    println!("\n✅ Configuration comparison test completed");
}

/// Compare different payload sizes
#[tokio::test]
// #[ignore] // Run explicitly: cargo test kafka_throughput_payload_comparison -- --ignored --nocapture
async fn kafka_throughput_payload_comparison() {
    const RECORD_COUNT: u64 = 20_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "Kafka Payload Size Comparison Test");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let payload_types = [
        PayloadType::Small,
        PayloadType::Medium,
        PayloadType::Large,
        PayloadType::Financial,
    ];

    let mut results = Vec::new();

    for (i, payload_type) in payload_types.iter().enumerate() {
        let source_topic = format!("payload-source-{}", i);
        let sink_topic = format!("payload-sink-{}", i);

        env.create_topic(&source_topic, 3)
            .await
            .expect("Failed to create source topic");
        env.create_topic(&sink_topic, 3)
            .await
            .expect("Failed to create sink topic");

        println!("\n📦 Test {}: {} payload", i + 1, payload_type.name());

        populate_topic(
            &env,
            &source_topic,
            RECORD_COUNT,
            *payload_type,
            ProducerConfig::HighThroughput,
        )
        .await
        .expect("Failed to populate topic");

        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut metrics = run_throughput_test(
            &env,
            &source_topic,
            &sink_topic,
            RECORD_COUNT,
            ConsumerConfig::HighThroughput,
            ProducerConfig::HighThroughput,
        )
        .await
        .expect("Test failed");

        metrics.payload_type = payload_type.name().to_string();
        metrics.print();
        results.push(metrics);

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Print payload comparison
    println!("\n{}", "=".repeat(120));
    println!("{:^120}", "PAYLOAD SIZE COMPARISON");
    println!("{}", "=".repeat(120));
    println!(
        "{:<25} {:>12} {:>15} {:>15} {:>12}",
        "Payload Type", "Records", "Throughput", "MB/s", "Avg Lat"
    );
    println!("{}", "-".repeat(120));

    for m in &results {
        println!(
            "{:<25} {:>12} {:>15.0} {:>15.2} {:>12.3}",
            m.payload_type,
            m.total_records,
            m.records_per_second,
            m.bytes_per_second / 1_000_000.0,
            m.avg_latency_ms,
        );
    }
    println!("{}", "=".repeat(120));

    println!("\n✅ Payload comparison test completed");
}

// =============================================================================
// KafkaDataSource/KafkaDataSink Direct Throughput Tests
// =============================================================================

use velostream::velostream::datasource::kafka::data_sink::KafkaDataSink;
use velostream::velostream::datasource::kafka::data_source::KafkaDataSource;
use velostream::velostream::datasource::{DataSink, DataSource};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Simple throughput test using KafkaDataSource and KafkaDataSink directly
/// No time simulation, just raw read/write throughput measurement
#[tokio::test]
#[ignore] // Run explicitly: cargo test kafka_datasource_datasink_throughput -- --ignored --nocapture
async fn kafka_datasource_datasink_throughput() {
    const RECORD_COUNT: u64 = 50_000;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "KafkaDataSource/KafkaDataSink Direct Throughput Test"
    );
    println!("{}", "=".repeat(80));

    // Start Kafka container
    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let source_topic = "datasource-test-source";
    let sink_topic = "datasource-test-sink";

    env.create_topic(source_topic, 3)
        .await
        .expect("Failed to create source topic");
    env.create_topic(sink_topic, 3)
        .await
        .expect("Failed to create sink topic");

    // Phase 1: Populate source topic with raw messages
    println!("\n📝 Phase 1: Populating source topic...");
    let (populate_duration, total_bytes) = populate_topic(
        &env,
        source_topic,
        RECORD_COUNT,
        PayloadType::Financial,
        ProducerConfig::HighThroughput,
    )
    .await
    .expect("Failed to populate topic");

    println!(
        "   Populated {} records ({:.2} MB) in {:.2}s",
        RECORD_COUNT,
        total_bytes as f64 / 1_000_000.0,
        populate_duration.as_secs_f64()
    );

    // Wait for messages to be committed
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 2: Use KafkaDataSource to read and KafkaDataSink to write
    println!("\n📊 Phase 2: Running KafkaDataSource → KafkaDataSink pipeline...");

    let mut props = std::collections::HashMap::new();
    props.insert(
        "bootstrap.servers".to_string(),
        env.bootstrap_servers.clone(),
    );
    props.insert("auto.offset.reset".to_string(), "earliest".to_string());

    // Create KafkaDataSource with low-latency settings for testcontainers
    let mut data_source =
        KafkaDataSource::new(env.bootstrap_servers.clone(), source_topic.to_string())
            .with_group_id("datasource-throughput-test-group".to_string())
            .with_config("auto.offset.reset".to_string(), "earliest".to_string())
            .with_config("fetch.min.bytes".to_string(), "1".to_string()) // Return immediately
            .with_config("fetch.wait.max.ms".to_string(), "0".to_string()) // No waiting
            .with_config("max.poll.records".to_string(), "5000".to_string());

    data_source
        .self_initialize()
        .await
        .expect("Failed to initialize source");

    // Create KafkaDataSink
    let mut data_sink = KafkaDataSink::new(env.bootstrap_servers.clone(), sink_topic.to_string())
        .with_config("linger.ms".to_string(), "50".to_string())
        .with_config("batch.size".to_string(), "1048576".to_string()) // 1MB
        .with_config("compression.type".to_string(), "lz4".to_string())
        .with_config("acks".to_string(), "1".to_string());

    data_sink
        .initialize(velostream::velostream::datasource::SinkConfig::Kafka {
            brokers: env.bootstrap_servers.clone(),
            topic: sink_topic.to_string(),
            properties: std::collections::HashMap::new(),
        })
        .await
        .expect("Failed to initialize sink");

    // Create reader and writer
    let mut reader = data_source
        .create_reader()
        .await
        .expect("Failed to create reader");
    let mut writer = data_sink
        .create_writer()
        .await
        .expect("Failed to create writer");

    // Measure throughput
    let start = Instant::now();
    let mut records_read: u64 = 0;
    let mut records_written: u64 = 0;
    let mut bytes_processed: u64 = 0;
    let timeout = Duration::from_secs(120);
    let mut last_progress = 0u64;

    println!("   Starting read/write loop...");

    while records_read < RECORD_COUNT {
        if start.elapsed() > timeout {
            println!("⚠️ Timeout reached after {:?}", timeout);
            break;
        }

        // Read batch from source
        match reader.read().await {
            Ok(batch) if !batch.is_empty() => {
                let batch_size = batch.len();
                records_read += batch_size as u64;

                // Estimate bytes (use JSON size approximation)
                for record in &batch {
                    bytes_processed += estimate_record_size(record);
                }

                // Write to sink using Arc for zero-copy
                let arc_batch: Vec<Arc<StreamRecord>> = batch.into_iter().map(Arc::new).collect();
                writer.write_batch(arc_batch).await.expect("Write failed");
                records_written += batch_size as u64;
            }
            Ok(_) => {
                // Empty batch - yield and try again
                tokio::task::yield_now().await;
            }
            Err(e) => {
                eprintln!("Read error: {:?}", e);
                break;
            }
        }

        // Progress reporting
        if records_read - last_progress >= 10000 {
            last_progress = records_read;
            let elapsed = start.elapsed();
            let rate = records_read as f64 / elapsed.as_secs_f64();
            println!(
                "   Progress: {:>10} records ({:.0} rec/s)",
                records_read, rate
            );
        }
    }

    // Flush remaining writes
    writer.flush().await.expect("Flush failed");

    let duration = start.elapsed();

    // Print results
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "RESULTS");
    println!("{}", "=".repeat(80));
    println!("Records Read:      {:>12}", records_read);
    println!("Records Written:   {:>12}", records_written);
    println!("Duration:          {:>12.2}s", duration.as_secs_f64());
    println!(
        "Read Throughput:   {:>12.0} records/sec",
        records_read as f64 / duration.as_secs_f64()
    );
    println!(
        "Write Throughput:  {:>12.0} records/sec",
        records_written as f64 / duration.as_secs_f64()
    );
    println!(
        "Data Processed:    {:>12.2} MB",
        bytes_processed as f64 / 1_000_000.0
    );
    println!(
        "Bandwidth:         {:>12.2} MB/sec",
        bytes_processed as f64 / duration.as_secs_f64() / 1_000_000.0
    );
    println!("{}", "=".repeat(80));

    println!("\n✅ KafkaDataSource/KafkaDataSink throughput test completed");
}

/// Quick version with fewer records for CI
#[tokio::test]
#[ignore] // Run explicitly: cargo test kafka_datasource_quick -- --ignored --nocapture
async fn kafka_datasource_quick() {
    const RECORD_COUNT: u64 = 5_000;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "Quick KafkaDataSource/KafkaDataSink Test (5K records)"
    );
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let source_topic = "quick-datasource-source";
    let sink_topic = "quick-datasource-sink";

    env.create_topic(source_topic, 1)
        .await
        .expect("Failed to create source topic");
    env.create_topic(sink_topic, 1)
        .await
        .expect("Failed to create sink topic");

    // Populate
    populate_topic(
        &env,
        source_topic,
        RECORD_COUNT,
        PayloadType::Small,
        ProducerConfig::Balanced,
    )
    .await
    .expect("Failed to populate topic");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create source with high-throughput settings
    let mut data_source =
        KafkaDataSource::new(env.bootstrap_servers.clone(), source_topic.to_string())
            .with_group_id("quick-test-group".to_string())
            .with_config("auto.offset.reset".to_string(), "earliest".to_string())
            // High-throughput consumer settings
            .with_config("max.poll.records".to_string(), "5000".to_string()) // Large batch
            .with_config("fetch.min.bytes".to_string(), "1".to_string()) // Don't wait for data
            .with_config("fetch.max.wait.ms".to_string(), "100".to_string()); // Short wait

    data_source
        .self_initialize()
        .await
        .expect("Failed to init source");

    let mut data_sink = KafkaDataSink::new(env.bootstrap_servers.clone(), sink_topic.to_string());
    data_sink
        .initialize(velostream::velostream::datasource::SinkConfig::Kafka {
            brokers: env.bootstrap_servers.clone(),
            topic: sink_topic.to_string(),
            properties: std::collections::HashMap::new(),
        })
        .await
        .expect("Failed to init sink");

    let mut reader = data_source.create_reader().await.expect("Reader failed");
    let mut writer = data_sink.create_writer().await.expect("Writer failed");

    let start = Instant::now();
    let mut count = 0u64;
    let timeout = Duration::from_secs(60);

    while count < RECORD_COUNT && start.elapsed() < timeout {
        match reader.read().await {
            Ok(batch) if !batch.is_empty() => {
                count += batch.len() as u64;
                let arc_batch: Vec<Arc<StreamRecord>> = batch.into_iter().map(Arc::new).collect();
                writer.write_batch(arc_batch).await.expect("Write failed");
            }
            Ok(_) => tokio::task::yield_now().await,
            Err(e) => {
                eprintln!("Error: {:?}", e);
                break;
            }
        }
    }

    writer.flush().await.expect("Flush failed");
    let duration = start.elapsed();

    println!(
        "\n✅ Processed {} records in {:.2}s ({:.0} rec/s)",
        count,
        duration.as_secs_f64(),
        count as f64 / duration.as_secs_f64()
    );
}

/// Estimate record size in bytes
fn estimate_record_size(record: &StreamRecord) -> u64 {
    let mut size = 0u64;
    for (key, value) in &record.fields {
        size += key.len() as u64;
        size += match value {
            FieldValue::String(s) => s.len() as u64,
            FieldValue::Integer(_) => 8,
            FieldValue::Float(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::Timestamp(_) => 8,
            FieldValue::ScaledInteger(_, _) => 16,
            FieldValue::Decimal(_) => 16,
            FieldValue::Array(arr) => arr.len() as u64 * 8,
            FieldValue::Null => 0,
            _ => 8,
        };
    }
    size
}

// =============================================================================
// Performance Isolation Tests - Compare raw vs typed consumer
// =============================================================================

/// Test 1: Raw rdkafka polling (baseline - no deserialization)
#[tokio::test]
#[ignore]
async fn perf_isolation_raw_poll() {
    const RECORD_COUNT: u64 = 5_000;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "PERF TEST 1: Raw rdkafka polling (no deserialization)"
    );
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    let topic = "perf-raw-poll";
    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Populate
    populate_topic(
        &env,
        topic,
        RECORD_COUNT,
        PayloadType::Small,
        ProducerConfig::Balanced,
    )
    .await
    .expect("Failed to populate");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Raw consumer - no deserialization
    let consumer = env
        .create_consumer("perf-raw-group", ConsumerConfig::HighThroughput)
        .expect("Failed to create consumer");
    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let start = Instant::now();
    let mut count = 0u64;
    let timeout = Duration::from_secs(30);

    while count < RECORD_COUNT && start.elapsed() < timeout {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(msg)) => {
                let _ = msg.payload(); // Just access raw bytes
                count += 1;
            }
            Some(Err(_)) | None => {}
        }
    }

    let duration = start.elapsed();
    println!(
        "\n✅ Raw poll: {} records in {:.2}s ({:.0} rec/s)",
        count,
        duration.as_secs_f64(),
        count as f64 / duration.as_secs_f64()
    );
}

/// Test 2: FastConsumer with JSON deserialization (typed consumer)
#[tokio::test]
#[ignore]
async fn perf_isolation_typed_consumer() {
    use std::collections::HashMap;
    use velostream::velostream::kafka::kafka_fast_consumer::Consumer as FastConsumer;
    use velostream::velostream::kafka::serialization::StringSerializer;
    use velostream::velostream::serialization::SerializationCodec;

    const RECORD_COUNT: u64 = 5_000;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "PERF TEST 2: FastConsumer with JSON deserialization"
    );
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    let topic = "perf-typed-consumer";
    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Populate
    populate_topic(
        &env,
        topic,
        RECORD_COUNT,
        PayloadType::Small,
        ProducerConfig::Balanced,
    )
    .await
    .expect("Failed to populate");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create typed FastConsumer with JSON deserializer
    let mut config = velostream::velostream::kafka::consumer_config::ConsumerConfig::new(
        &env.bootstrap_servers,
        "perf-typed-group",
    );
    config.auto_offset_reset =
        velostream::velostream::kafka::consumer_config::OffsetReset::Earliest;

    let key_serializer = StringSerializer;
    let value_serializer = SerializationCodec::Json(
        velostream::velostream::serialization::json_codec::JsonCodec::new(),
    );

    let consumer: FastConsumer<String, HashMap<String, FieldValue>, _, _> =
        FastConsumer::with_config(config, key_serializer, value_serializer)
            .expect("Failed to create consumer");
    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let start = Instant::now();
    let mut count = 0u64;
    let timeout = Duration::from_secs(30);

    while count < RECORD_COUNT && start.elapsed() < timeout {
        match consumer.try_poll() {
            Ok(Some(_msg)) => {
                // Message is already deserialized to HashMap<String, FieldValue>
                count += 1;
            }
            Ok(None) => {
                // No message, do a blocking poll
                match consumer.poll_blocking(Duration::from_millis(100)) {
                    Ok(_msg) => count += 1,
                    Err(_) => {}
                }
            }
            Err(_) => {}
        }
    }

    let duration = start.elapsed();
    println!(
        "\n✅ Typed consumer: {} records in {:.2}s ({:.0} rec/s)",
        count,
        duration.as_secs_f64(),
        count as f64 / duration.as_secs_f64()
    );
}

/// Test 3: KafkaDataSource (full pipeline)
#[tokio::test]
#[ignore]
async fn perf_isolation_datasource() {
    const RECORD_COUNT: u64 = 5_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "PERF TEST 3: KafkaDataSource (full pipeline)");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    let topic = "perf-datasource";
    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Populate
    populate_topic(
        &env,
        topic,
        RECORD_COUNT,
        PayloadType::Small,
        ProducerConfig::Balanced,
    )
    .await
    .expect("Failed to populate");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // KafkaDataSource
    let mut data_source = KafkaDataSource::new(env.bootstrap_servers.clone(), topic.to_string())
        .with_group_id("perf-datasource-group".to_string())
        .with_config("auto.offset.reset".to_string(), "earliest".to_string())
        .with_config("max.poll.records".to_string(), "5000".to_string());

    data_source.self_initialize().await.expect("Failed to init");
    let mut reader = data_source
        .create_reader()
        .await
        .expect("Failed to create reader");

    let start = Instant::now();
    let mut count = 0u64;
    let timeout = Duration::from_secs(60);

    while count < RECORD_COUNT && start.elapsed() < timeout {
        match reader.read().await {
            Ok(batch) if !batch.is_empty() => {
                count += batch.len() as u64;
            }
            Ok(_) => tokio::task::yield_now().await,
            Err(_) => break,
        }
    }

    let duration = start.elapsed();
    println!(
        "\n✅ KafkaDataSource: {} records in {:.2}s ({:.0} rec/s)",
        count,
        duration.as_secs_f64(),
        count as f64 / duration.as_secs_f64()
    );
}

/// Run all isolation tests together for comparison
#[tokio::test]
#[ignore]
async fn perf_isolation_all() {
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "PERFORMANCE ISOLATION COMPARISON");
    println!("{}", "=".repeat(80));
    println!("\nRun individual tests:");
    println!("  cargo test perf_isolation_raw_poll -- --ignored --nocapture");
    println!("  cargo test perf_isolation_typed_consumer -- --ignored --nocapture");
    println!("  cargo test perf_isolation_datasource -- --ignored --nocapture");
}

/// Test 4: Compare polling strategies - different timeout values
#[tokio::test]
#[ignore]
async fn perf_isolation_polling_strategies() {
    const RECORD_COUNT: u64 = 5_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "PERF TEST 4: Polling Strategies Comparison");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    // Test A: Non-blocking poll (0ms timeout)
    {
        let topic = "perf-poll-zero";
        env.create_topic(topic, 1)
            .await
            .expect("Failed to create topic");
        populate_topic(
            &env,
            topic,
            RECORD_COUNT,
            PayloadType::Small,
            ProducerConfig::Balanced,
        )
        .await
        .expect("Failed to populate");
        tokio::time::sleep(Duration::from_secs(1)).await;

        let consumer = env
            .create_consumer("perf-poll-zero-group", ConsumerConfig::HighThroughput)
            .expect("Failed to create consumer");
        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let start = Instant::now();
        let mut count = 0u64;
        let timeout = Duration::from_secs(30);
        let mut empty_polls = 0u64;

        while count < RECORD_COUNT && start.elapsed() < timeout {
            // Non-blocking poll (0ms)
            match consumer.poll(Duration::ZERO) {
                Some(Ok(msg)) => {
                    let _ = msg.payload();
                    count += 1;
                    empty_polls = 0; // Reset empty counter
                }
                Some(Err(_)) | None => {
                    empty_polls += 1;
                    // If many empty polls, do a brief sleep to avoid busy-wait
                    if empty_polls > 100 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        }

        let duration = start.elapsed();
        println!(
            "\n✅ Non-blocking poll (0ms): {} records in {:.2}s ({:.0} rec/s)",
            count,
            duration.as_secs_f64(),
            count as f64 / duration.as_secs_f64()
        );
    }

    // Test B: Batch poll with 10ms timeout
    {
        let topic = "perf-poll-10ms";
        env.create_topic(topic, 1)
            .await
            .expect("Failed to create topic");
        populate_topic(
            &env,
            topic,
            RECORD_COUNT,
            PayloadType::Small,
            ProducerConfig::Balanced,
        )
        .await
        .expect("Failed to populate");
        tokio::time::sleep(Duration::from_secs(1)).await;

        let consumer = env
            .create_consumer("perf-poll-10ms-group", ConsumerConfig::HighThroughput)
            .expect("Failed to create consumer");
        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let start = Instant::now();
        let mut count = 0u64;
        let timeout = Duration::from_secs(30);

        while count < RECORD_COUNT && start.elapsed() < timeout {
            // Shorter timeout (10ms)
            match consumer.poll(Duration::from_millis(10)) {
                Some(Ok(msg)) => {
                    let _ = msg.payload();
                    count += 1;
                }
                Some(Err(_)) | None => {}
            }
        }

        let duration = start.elapsed();
        println!(
            "\n✅ Short timeout poll (10ms): {} records in {:.2}s ({:.0} rec/s)",
            count,
            duration.as_secs_f64(),
            count as f64 / duration.as_secs_f64()
        );
    }

    // Test C: Standard poll with 100ms timeout
    {
        let topic = "perf-poll-100ms";
        env.create_topic(topic, 1)
            .await
            .expect("Failed to create topic");
        populate_topic(
            &env,
            topic,
            RECORD_COUNT,
            PayloadType::Small,
            ProducerConfig::Balanced,
        )
        .await
        .expect("Failed to populate");
        tokio::time::sleep(Duration::from_secs(1)).await;

        let consumer = env
            .create_consumer("perf-poll-100ms-group", ConsumerConfig::HighThroughput)
            .expect("Failed to create consumer");
        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let start = Instant::now();
        let mut count = 0u64;
        let timeout = Duration::from_secs(30);

        while count < RECORD_COUNT && start.elapsed() < timeout {
            // Standard timeout (100ms)
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    let _ = msg.payload();
                    count += 1;
                }
                Some(Err(_)) | None => {}
            }
        }

        let duration = start.elapsed();
        println!(
            "\n✅ Standard poll (100ms): {} records in {:.2}s ({:.0} rec/s)",
            count,
            duration.as_secs_f64(),
            count as f64 / duration.as_secs_f64()
        );
    }

    // Test D: LowLatency consumer config (small fetch.min.bytes)
    {
        let topic = "perf-low-latency";
        env.create_topic(topic, 1)
            .await
            .expect("Failed to create topic");
        populate_topic(
            &env,
            topic,
            RECORD_COUNT,
            PayloadType::Small,
            ProducerConfig::Balanced,
        )
        .await
        .expect("Failed to populate");
        tokio::time::sleep(Duration::from_secs(1)).await;

        let consumer = env
            .create_consumer("perf-low-latency-group", ConsumerConfig::LowLatency)
            .expect("Failed to create consumer");
        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let start = Instant::now();
        let mut count = 0u64;
        let timeout = Duration::from_secs(30);

        while count < RECORD_COUNT && start.elapsed() < timeout {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    let _ = msg.payload();
                    count += 1;
                }
                Some(Err(_)) | None => {}
            }
        }

        let duration = start.elapsed();
        println!(
            "\n✅ LowLatency config (fetch.min.bytes=1): {} records in {:.2}s ({:.0} rec/s)",
            count,
            duration.as_secs_f64(),
            count as f64 / duration.as_secs_f64()
        );
    }

    // Test E: Custom ultra-low latency config
    {
        let topic = "perf-ultra-low";
        env.create_topic(topic, 1)
            .await
            .expect("Failed to create topic");
        populate_topic(
            &env,
            topic,
            RECORD_COUNT,
            PayloadType::Small,
            ProducerConfig::Balanced,
        )
        .await
        .expect("Failed to populate");
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Custom consumer with ultra-low latency settings
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &env.bootstrap_servers)
            .set("group.id", "perf-ultra-low-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            // Ultra-low latency settings
            .set("fetch.min.bytes", "1") // Return immediately with any data
            .set("fetch.wait.max.ms", "0") // Don't wait at all
            .set("fetch.message.max.bytes", "1048576")
            .set("queued.min.messages", "1") // Fetch as soon as 1 message available
            .create()
            .expect("Failed to create consumer");
        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let start = Instant::now();
        let mut count = 0u64;
        let timeout = Duration::from_secs(30);

        while count < RECORD_COUNT && start.elapsed() < timeout {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(msg)) => {
                    let _ = msg.payload();
                    count += 1;
                }
                Some(Err(_)) | None => {}
            }
        }

        let duration = start.elapsed();
        println!(
            "\n✅ Ultra-low latency (fetch.wait.max.ms=0): {} records in {:.2}s ({:.0} rec/s)",
            count,
            duration.as_secs_f64(),
            count as f64 / duration.as_secs_f64()
        );
    }

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "Summary: Consumer CONFIG affects throughput more than poll timeout!"
    );
    println!("{}", "=".repeat(80));
}

/// Test 5: KafkaDataSink write-only with synthetic data
#[tokio::test]
#[ignore]
async fn perf_isolation_datasink_write() {
    use chrono::Utc;
    use std::collections::HashMap;
    use velostream::velostream::datasource::kafka::KafkaDataSink;
    use velostream::velostream::datasource::{DataSink, SinkConfig};
    use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

    const RECORD_COUNT: u64 = 50_000;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "PERF TEST 5: KafkaDataSink WRITE-ONLY (50K synthetic records)"
    );
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    let topic = "perf-datasink-write";
    env.create_topic(topic, 3)
        .await
        .expect("Failed to create topic");

    // Create KafkaDataSink
    let mut data_sink = KafkaDataSink::new(env.bootstrap_servers.clone(), topic.to_string())
        .with_config("linger.ms".to_string(), "5".to_string())
        .with_config("batch.size".to_string(), "1048576".to_string())
        .with_config("compression.type".to_string(), "lz4".to_string())
        .with_config("acks".to_string(), "1".to_string());

    data_sink
        .initialize(SinkConfig::Kafka {
            brokers: env.bootstrap_servers.clone(),
            topic: topic.to_string(),
            properties: HashMap::new(),
        })
        .await
        .expect("Failed to initialize sink");

    let mut writer = data_sink
        .create_writer()
        .await
        .expect("Failed to create writer");

    // Generate synthetic records
    println!("📝 Generating {} synthetic records...", RECORD_COUNT);
    let records: Vec<Arc<StreamRecord>> = (0..RECORD_COUNT)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert(
                "price".to_string(),
                FieldValue::Float(150.0 + (i as f64 * 0.01)),
            );
            fields.insert("quantity".to_string(), FieldValue::Integer(100));
            fields.insert(
                "timestamp".to_string(),
                FieldValue::String(Utc::now().to_rfc3339()),
            );

            Arc::new(StreamRecord {
                fields,
                timestamp: Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: 0,
                headers: HashMap::new(),
                event_time: Some(Utc::now()),
                topic: None,
                key: None,
            })
        })
        .collect();
    println!("✅ Generated {} records", records.len());

    // Write in batches and measure throughput
    let batch_size = 1000;
    let start = Instant::now();
    let mut written = 0u64;
    let mut last_progress = 0u64;

    for chunk in records.chunks(batch_size) {
        let batch: Vec<Arc<StreamRecord>> = chunk.to_vec();
        writer.write_batch(batch).await.expect("Write failed");
        written += chunk.len() as u64;

        if written - last_progress >= 10000 {
            last_progress = written;
            let elapsed = start.elapsed();
            let rate = written as f64 / elapsed.as_secs_f64();
            println!("   Progress: {:>10} records ({:.0} rec/s)", written, rate);
        }
    }

    // Flush
    writer.flush().await.expect("Flush failed");

    let duration = start.elapsed();
    println!(
        "\n✅ KafkaDataSink (50K write-only): {} records in {:.2}s ({:.0} rec/s)",
        written,
        duration.as_secs_f64(),
        written as f64 / duration.as_secs_f64()
    );
}

/// Test 6: KafkaDataSource read-only with larger dataset
#[tokio::test]
#[ignore]
async fn perf_isolation_datasource_50k() {
    const RECORD_COUNT: u64 = 50_000;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "PERF TEST 5: KafkaDataSource READ-ONLY (50K records)"
    );
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    let topic = "perf-datasource-50k";
    env.create_topic(topic, 3)
        .await
        .expect("Failed to create topic");

    // Populate with financial data
    populate_topic(
        &env,
        topic,
        RECORD_COUNT,
        PayloadType::Financial,
        ProducerConfig::HighThroughput,
    )
    .await
    .expect("Failed to populate");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // KafkaDataSource with low-latency settings
    let mut data_source = KafkaDataSource::new(env.bootstrap_servers.clone(), topic.to_string())
        .with_group_id("perf-datasource-50k-group".to_string())
        .with_config("auto.offset.reset".to_string(), "earliest".to_string())
        .with_config("fetch.min.bytes".to_string(), "1".to_string())
        .with_config("fetch.wait.max.ms".to_string(), "0".to_string())
        .with_config("max.poll.records".to_string(), "5000".to_string());

    data_source.self_initialize().await.expect("Failed to init");
    let mut reader = data_source
        .create_reader()
        .await
        .expect("Failed to create reader");

    let start = Instant::now();
    let mut count = 0u64;
    let timeout = Duration::from_secs(120);
    let mut last_progress = 0u64;

    while count < RECORD_COUNT && start.elapsed() < timeout {
        match reader.read().await {
            Ok(batch) if !batch.is_empty() => {
                count += batch.len() as u64;
            }
            Ok(_) => tokio::task::yield_now().await,
            Err(_) => break,
        }

        if count - last_progress >= 10000 {
            last_progress = count;
            let elapsed = start.elapsed();
            let rate = count as f64 / elapsed.as_secs_f64();
            println!("   Progress: {:>10} records ({:.0} rec/s)", count, rate);
        }
    }

    let duration = start.elapsed();
    println!(
        "\n✅ KafkaDataSource (50K read-only): {} records in {:.2}s ({:.0} rec/s)",
        count,
        duration.as_secs_f64(),
        count as f64 / duration.as_secs_f64()
    );
}

/// Test 7: Raw Producer Configuration Benchmark
/// Compares different producer configurations to find optimal settings
/// Based on WarpStream recommendations: https://docs.warpstream.com/warpstream/kafka/configure-kafka-client/tuning-for-performance#librdkafka
#[tokio::test]
#[ignore]
async fn perf_producer_config_benchmark() {
    const RECORD_COUNT: u64 = 50_000;
    const BATCH_SIZE: usize = 500;

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "PRODUCER CONFIG BENCHMARK: Testing Different Producer Settings"
    );
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    /// Producer config presets to test
    #[derive(Clone, Copy, Debug)]
    enum TestConfig {
        Current,    // Current KafkaDataSink settings
        WarpStream, // WarpStream recommended settings
        Aggressive, // Maximum batching
    }

    impl TestConfig {
        fn name(&self) -> &'static str {
            match self {
                TestConfig::Current => "Current (linger=5ms, batch=1MB)",
                TestConfig::WarpStream => "WarpStream (linger=100ms, batch=16MB)",
                TestConfig::Aggressive => "Aggressive (linger=200ms, batch=32MB)",
            }
        }

        fn apply(&self, config: &mut ClientConfig) {
            match self {
                TestConfig::Current => {
                    // Current KafkaDataSink settings
                    config.set("linger.ms", "5");
                    config.set("batch.size", "1048576"); // 1MB
                    config.set("compression.type", "lz4");
                    config.set("acks", "1");
                }
                TestConfig::WarpStream => {
                    // WarpStream recommended settings
                    config.set("queue.buffering.max.kbytes", "1048576"); // 1GB buffer
                    config.set("queue.buffering.max.messages", "1000000");
                    config.set("batch.size", "16000000"); // 16MB
                    config.set("batch.num.messages", "100000");
                    config.set("linger.ms", "100");
                    config.set("max.in.flight.requests.per.connection", "1000000");
                    config.set("enable.idempotence", "false");
                    config.set("compression.type", "lz4");
                    config.set("acks", "1");
                    config.set("partitioner", "consistent_random");
                }
                TestConfig::Aggressive => {
                    // Maximum batching
                    config.set("queue.buffering.max.kbytes", "2097152"); // 2GB buffer
                    config.set("queue.buffering.max.messages", "2000000");
                    config.set("batch.size", "33554432"); // 32MB
                    config.set("batch.num.messages", "200000");
                    config.set("linger.ms", "200");
                    config.set("max.in.flight.requests.per.connection", "1000000");
                    config.set("enable.idempotence", "false");
                    config.set("compression.type", "lz4");
                    config.set("acks", "1");
                }
            }
        }
    }

    // Test each configuration
    for test_config in [
        TestConfig::Current,
        TestConfig::WarpStream,
        TestConfig::Aggressive,
    ] {
        let topic = format!("perf-producer-config-{:?}", test_config)
            .to_lowercase()
            .replace(" ", "-")
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
            .replace("=", "");
        let topic = &topic[..topic.len().min(50)]; // Kafka topic name limit

        env.create_topic(topic, 3)
            .await
            .expect("Failed to create topic");

        // Create producer with test config
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &env.bootstrap_servers);
        test_config.apply(&mut config);

        let producer: FutureProducer = config.create().expect("Failed to create producer");
        let producer = Arc::new(producer);

        println!("\n📊 Testing: {}", test_config.name());
        println!("   Topic: {}", topic);

        // Generate payloads
        let payloads: Vec<(String, String)> = (0..RECORD_COUNT)
            .map(|i| {
                let payload = format!(
                    r#"{{"id":{},"symbol":"AAPL","price":{},"qty":100,"ts":{}}}"#,
                    i,
                    150.0 + (i as f64 * 0.01),
                    chrono::Utc::now().timestamp_millis()
                );
                let key = format!("key-{}", i % 100);
                (payload, key)
            })
            .collect();

        // Send using fire-and-forget pattern (tokio::spawn)
        let start = Instant::now();
        let mut sent = 0u64;
        let send_timeout = Duration::from_secs(10);

        for batch_start in (0..RECORD_COUNT as usize).step_by(BATCH_SIZE) {
            let batch_end = (batch_start + BATCH_SIZE).min(RECORD_COUNT as usize);
            let batch_data: Vec<_> = payloads[batch_start..batch_end].to_vec();

            let mut handles = Vec::with_capacity(batch_data.len());
            for (payload, key) in batch_data {
                let producer_clone = Arc::clone(&producer);
                let topic_owned = topic.to_string();

                let handle = tokio::spawn(async move {
                    let record = FutureRecord::to(&topic_owned)
                        .payload(payload.as_bytes())
                        .key(key.as_bytes());
                    producer_clone.send(record, send_timeout).await
                });
                handles.push(handle);
            }

            // Wait for all sends in this batch
            for handle in handles {
                match handle.await {
                    Ok(Ok(_)) => sent += 1,
                    Ok(Err((e, _))) => {
                        eprintln!("Send error: {:?}", e);
                    }
                    Err(e) => {
                        eprintln!("Join error: {:?}", e);
                    }
                }
            }

            // Progress
            if sent % 10000 < BATCH_SIZE as u64 {
                let elapsed = start.elapsed();
                let rate = sent as f64 / elapsed.as_secs_f64();
                println!("   Progress: {:>10} records ({:.0} rec/s)", sent, rate);
            }
        }

        // Flush
        producer
            .flush(Duration::from_secs(30))
            .expect("Flush failed");

        let duration = start.elapsed();
        let rate = sent as f64 / duration.as_secs_f64();

        println!(
            "   ✅ {}: {} records in {:.2}s = {:.0} rec/s",
            test_config.name(),
            sent,
            duration.as_secs_f64(),
            rate
        );
    }

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "PRODUCER CONFIG BENCHMARK COMPLETE");
    println!("{}", "=".repeat(80));
}

/// Test 8: Sequential vs Parallel Send Pattern Comparison
/// Tests whether the send pattern or configuration matters more
#[tokio::test]
#[ignore]
async fn perf_send_pattern_comparison() {
    // Sequential is VERY slow (~100 rec/s), so use smaller count
    const SEQUENTIAL_COUNT: u64 = 500;
    // Parallel patterns are fast, use larger count
    const PARALLEL_COUNT: u64 = 50_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "SEND PATTERN COMPARISON: Sequential vs Parallel");
    println!("{}", "=".repeat(80));

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    // Create producer with WarpStream settings
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &env.bootstrap_servers);
    config.set("queue.buffering.max.kbytes", "1048576");
    config.set("queue.buffering.max.messages", "1000000");
    config.set("batch.size", "16000000");
    config.set("batch.num.messages", "100000");
    config.set("linger.ms", "100");
    config.set("max.in.flight.requests.per.connection", "1000000");
    config.set("enable.idempotence", "false");
    config.set("compression.type", "lz4");
    config.set("acks", "1");

    // Generate payloads for sequential test
    let sequential_payloads: Vec<(String, String)> = (0..SEQUENTIAL_COUNT)
        .map(|i| {
            let payload = format!(
                r#"{{"id":{},"symbol":"AAPL","price":{},"qty":100}}"#,
                i,
                150.0 + (i as f64 * 0.01)
            );
            let key = format!("key-{}", i % 100);
            (payload, key)
        })
        .collect();

    // Generate payloads for parallel tests
    let parallel_payloads: Vec<(String, String)> = (0..PARALLEL_COUNT)
        .map(|i| {
            let payload = format!(
                r#"{{"id":{},"symbol":"AAPL","price":{},"qty":100}}"#,
                i,
                150.0 + (i as f64 * 0.01)
            );
            let key = format!("key-{}", i % 100);
            (payload, key)
        })
        .collect();

    let mut results: Vec<(String, u64, f64, f64)> = Vec::new();

    // Test 1: Sequential await (current KafkaDataSink pattern)
    {
        let topic = "perf-sequential-send";
        env.create_topic(topic, 3)
            .await
            .expect("Failed to create topic");

        let producer: FutureProducer = config.clone().create().expect("Failed to create producer");
        let send_timeout = Duration::from_secs(5);

        println!("\n📊 Pattern 1: Sequential await (current KafkaDataSink)");
        println!(
            "   Testing with {} records (smaller due to slow sequential pattern)",
            SEQUENTIAL_COUNT
        );
        let start = Instant::now();
        let mut sent = 0u64;

        for (payload, key) in &sequential_payloads {
            let record = FutureRecord::to(topic)
                .payload(payload.as_bytes())
                .key(key.as_bytes());

            match producer.send(record, send_timeout).await {
                Ok(_) => sent += 1,
                Err((e, _)) => eprintln!("Send error: {:?}", e),
            }
        }

        producer
            .flush(Duration::from_secs(30))
            .expect("Flush failed");
        let duration = start.elapsed();
        let rate = sent as f64 / duration.as_secs_f64();

        println!(
            "   ✅ Sequential: {} records in {:.2}s = {:.0} rec/s",
            sent,
            duration.as_secs_f64(),
            rate
        );
        results.push((
            "Sequential await".to_string(),
            sent,
            duration.as_secs_f64(),
            rate,
        ));
    }

    // Test 2: Parallel with tokio::spawn
    {
        let topic = "perf-parallel-spawn";
        env.create_topic(topic, 3)
            .await
            .expect("Failed to create topic");

        let producer: FutureProducer = config.clone().create().expect("Failed to create producer");
        let producer = Arc::new(producer);
        let send_timeout = Duration::from_secs(5);

        println!("\n📊 Pattern 2: Parallel tokio::spawn");
        println!("   Testing with {} records", PARALLEL_COUNT);
        let start = Instant::now();

        let mut handles = Vec::with_capacity(parallel_payloads.len());
        for (payload, key) in parallel_payloads.iter().cloned() {
            let producer_clone = Arc::clone(&producer);
            let topic_owned = topic.to_string();

            let handle = tokio::spawn(async move {
                let record = FutureRecord::to(&topic_owned)
                    .payload(payload.as_bytes())
                    .key(key.as_bytes());
                producer_clone.send(record, send_timeout).await
            });
            handles.push(handle);
        }

        let mut sent = 0u64;
        for handle in handles {
            if handle.await.is_ok() {
                sent += 1;
            }
        }

        producer
            .flush(Duration::from_secs(30))
            .expect("Flush failed");
        let duration = start.elapsed();
        let rate = sent as f64 / duration.as_secs_f64();

        println!(
            "   ✅ Parallel spawn: {} records in {:.2}s = {:.0} rec/s",
            sent,
            duration.as_secs_f64(),
            rate
        );
        results.push((
            "Parallel tokio::spawn".to_string(),
            sent,
            duration.as_secs_f64(),
            rate,
        ));
    }

    // Test 3: Parallel with futures::join_all (no spawning)
    {
        let topic = "perf-parallel-join";
        env.create_topic(topic, 3)
            .await
            .expect("Failed to create topic");

        let producer: FutureProducer = config.create().expect("Failed to create producer");
        let send_timeout = Duration::from_secs(5);

        println!("\n📊 Pattern 3: Parallel futures::join_all (no spawn)");
        println!("   Testing with {} records", PARALLEL_COUNT);
        let start = Instant::now();

        // Collect all futures without awaiting
        let futures: Vec<_> = parallel_payloads
            .iter()
            .map(|(payload, key)| {
                let record = FutureRecord::to(topic)
                    .payload(payload.as_bytes())
                    .key(key.as_bytes());
                producer.send(record, send_timeout)
            })
            .collect();

        // Await all at once
        let results_vec = futures::future::join_all(futures).await;
        let sent = results_vec.iter().filter(|r| r.is_ok()).count() as u64;

        producer
            .flush(Duration::from_secs(30))
            .expect("Flush failed");
        let duration = start.elapsed();
        let rate = sent as f64 / duration.as_secs_f64();

        println!(
            "   ✅ join_all: {} records in {:.2}s = {:.0} rec/s",
            sent,
            duration.as_secs_f64(),
            rate
        );
        results.push((
            "Parallel join_all".to_string(),
            sent,
            duration.as_secs_f64(),
            rate,
        ));
    }

    // Print comparison summary
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "RESULTS SUMMARY");
    println!("{}", "=".repeat(80));
    println!(
        "{:<25} {:>12} {:>12} {:>15}",
        "Pattern", "Records", "Time (s)", "Rate (rec/s)"
    );
    println!("{}", "-".repeat(80));
    for (name, records, time, rate) in &results {
        println!("{:<25} {:>12} {:>12.2} {:>15.0}", name, records, time, rate);
    }
    println!("{}", "-".repeat(80));

    // Calculate speedup
    let sequential_rate = results[0].3;
    let spawn_rate = results[1].3;
    let join_rate = results[2].3;

    println!("\n📊 SPEEDUP vs Sequential:");
    println!(
        "   tokio::spawn:    {:.0}x faster ({:.0} vs {:.0} rec/s)",
        spawn_rate / sequential_rate,
        spawn_rate,
        sequential_rate
    );
    println!(
        "   futures::join_all: {:.0}x faster ({:.0} vs {:.0} rec/s)",
        join_rate / sequential_rate,
        join_rate,
        sequential_rate
    );

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "SEND PATTERN COMPARISON COMPLETE");
    println!("{}", "=".repeat(80));
    println!(
        "\n🎯 CONCLUSION: Use futures::join_all or tokio::spawn for KafkaDataSink.write_batch()"
    );
}

/// Test 9: Zero-Allocation Multi-Threaded BaseProducer Pattern
/// Uses Arc<BaseProducer> + dedicated poll thread + worker threads
/// This is the MAXIMUM THROUGHPUT pattern for Kafka producers
#[tokio::test]
#[ignore]
async fn perf_zero_alloc_multithreaded_producer() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::thread;

    const TARGET_RECORDS: u64 = 500_000;
    const NUM_WORKER_THREADS: usize = 1; // 1 thread + poll thread = optimal (see benchmarks)

    println!("\n{}", "=".repeat(80));
    println!(
        "{:^80}",
        "ZERO-ALLOCATION MULTI-THREADED PRODUCER BENCHMARK"
    );
    println!("{}", "=".repeat(80));
    println!("Configuration:");
    println!("  • Target records: {}", TARGET_RECORDS);
    println!("  • Worker threads: {}", NUM_WORKER_THREADS);
    println!("  • Pattern: Arc<BaseProducer> + dedicated poll thread");

    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");

    let topic = "perf-zero-alloc";
    env.create_topic(topic, 3)
        .await
        .expect("Failed to create topic");

    // Create high-performance BaseProducer
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("enable.idempotence", "false") // Disable for max speed
        .set("acks", "1") // Leader only for speed
        .set("max.in.flight.requests.per.connection", "5")
        .set("compression.type", "lz4")
        .set("batch.size", "1048576") // 1MB batches
        .set("linger.ms", "5")
        .set("queue.buffering.max.kbytes", "1048576") // 1GB queue
        .set("queue.buffering.max.messages", "500000")
        .create()
        .expect("Failed to create producer");

    let producer = Arc::new(producer);
    let topic_owned = topic.to_string();

    // Shared counters
    let total_sent = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Static payload (zero allocation per message)
    static PAYLOAD: &[u8] = b"{\"id\":0,\"symbol\":\"AAPL\",\"price\":150.00,\"qty\":100}";

    println!("\n🚀 Starting benchmark...");
    let start = Instant::now();

    // Dedicated poll thread (tight loop)
    let poll_producer = Arc::clone(&producer);
    let poll_stop = Arc::clone(&stop_flag);
    let poll_handle = thread::spawn(move || {
        while !poll_stop.load(Ordering::Relaxed) {
            poll_producer.poll(Duration::from_millis(0));
        }
    });

    // Worker threads
    let mut worker_handles = Vec::new();
    for worker_id in 0..NUM_WORKER_THREADS {
        let p = Arc::clone(&producer);
        let sent = Arc::clone(&total_sent);
        let stop = Arc::clone(&stop_flag);
        let topic_clone = topic_owned.clone();

        let handle = thread::spawn(move || {
            let mut local_sent = 0u64;
            let records_per_worker = TARGET_RECORDS / NUM_WORKER_THREADS as u64;

            while local_sent < records_per_worker && !stop.load(Ordering::Relaxed) {
                // Zero-allocation send: static payload, reused topic
                let key = format!("key-{}", local_sent % 100);
                let record = BaseRecord::to(&topic_clone)
                    .payload(PAYLOAD)
                    .key(key.as_str());

                match p.send(record) {
                    Ok(()) => {
                        local_sent += 1;
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err((err, _)) => {
                        if err.rdkafka_error_code()
                            == Some(rdkafka::types::RDKafkaErrorCode::QueueFull)
                        {
                            // Queue full - yield and retry
                            thread::yield_now();
                        } else {
                            eprintln!("Worker {}: Send error: {:?}", worker_id, err);
                        }
                    }
                }
            }
            local_sent
        });
        worker_handles.push(handle);
    }

    // Wait for workers to complete
    let mut worker_results = Vec::new();
    for handle in worker_handles {
        worker_results.push(handle.join().expect("Worker thread panicked"));
    }

    // Stop poll thread
    stop_flag.store(true, Ordering::Relaxed);
    poll_handle.join().expect("Poll thread panicked");

    // Flush remaining messages
    producer
        .flush(Duration::from_secs(30))
        .expect("Flush failed");

    let duration = start.elapsed();
    let total = total_sent.load(Ordering::Relaxed);
    let rate = total as f64 / duration.as_secs_f64();

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "RESULTS");
    println!("{}", "=".repeat(80));
    println!("  Total records sent: {}", total);
    println!("  Duration: {:.2}s", duration.as_secs_f64());
    println!("  Throughput: {:.0} rec/s", rate);
    println!(
        "  Per-worker throughput: {:.0} rec/s",
        rate / NUM_WORKER_THREADS as f64
    );

    // Compare with KafkaDataSink
    println!("\n📊 Comparison:");
    println!("  • KafkaDataSink (BaseProducer): ~7,500 rec/s");
    println!("  • Zero-alloc multi-threaded:   {:.0} rec/s", rate);
    if rate > 7500.0 {
        println!("  • Improvement: {:.1}x faster", rate / 7500.0);
    }

    println!("\n{}", "=".repeat(80));
}

/// Helper to convert FieldValue to JSON Value
fn field_value_to_json_value(
    fv: &velostream::velostream::sql::execution::types::FieldValue,
) -> serde_json::Value {
    use velostream::velostream::sql::execution::types::FieldValue;
    match fv {
        FieldValue::Integer(i) => serde_json::json!(i),
        FieldValue::Float(f) => serde_json::json!(f),
        FieldValue::String(s) => serde_json::json!(s),
        FieldValue::Boolean(b) => serde_json::json!(b),
        FieldValue::Null => serde_json::Value::Null,
        _ => serde_json::json!(format!("{:?}", fv)),
    }
}

/// Test 10: Bottleneck Isolation - identify where time is spent
#[tokio::test]
#[ignore]
async fn perf_bottleneck_isolation() {
    use chrono::Utc;
    use std::collections::HashMap;
    use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

    const RECORD_COUNT: usize = 100_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "BOTTLENECK ISOLATION ANALYSIS");
    println!("{}", "=".repeat(80));

    // Generate test records
    println!("\n📝 Generating {} StreamRecords...", RECORD_COUNT);
    let records: Vec<StreamRecord> = (0..RECORD_COUNT)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
            fields.insert(
                "price".to_string(),
                FieldValue::Float(150.0 + (i as f64 * 0.01)),
            );
            fields.insert("quantity".to_string(), FieldValue::Integer(100));
            StreamRecord {
                fields,
                timestamp: Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: 0,
                headers: HashMap::new(),
                event_time: Some(Utc::now()),
                topic: None,
                key: None,
            }
        })
        .collect();
    println!("✅ Generated {} records\n", records.len());

    // Test 1: JSON serialization only (no Kafka)
    println!("🔬 Test 1: JSON Serialization Only");
    let start = Instant::now();
    let mut total_bytes = 0usize;
    for record in &records {
        // Convert fields to JSON object
        let json_obj: serde_json::Map<String, serde_json::Value> = record
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), field_value_to_json_value(v)))
            .collect();
        let json = serde_json::to_vec(&json_obj).unwrap();
        total_bytes += json.len();
    }
    let json_time = start.elapsed();
    let json_rate = RECORD_COUNT as f64 / json_time.as_secs_f64();
    println!(
        "   JSON serialization: {} records in {:.2}ms = {:.0} rec/s ({} bytes avg)",
        RECORD_COUNT,
        json_time.as_millis(),
        json_rate,
        total_bytes / RECORD_COUNT
    );

    // Test 2: Key extraction only
    println!("\n🔬 Test 2: Key Extraction Only");
    let start = Instant::now();
    let mut keys_found = 0usize;
    for record in &records {
        if let Some(FieldValue::Integer(id)) = record.fields.get("id") {
            keys_found += 1;
            let _ = id.to_string();
        }
    }
    let key_time = start.elapsed();
    let key_rate = RECORD_COUNT as f64 / key_time.as_secs_f64();
    println!(
        "   Key extraction: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        key_time.as_millis(),
        key_rate
    );

    // Test 3: Arc cloning
    println!("\n🔬 Test 3: Arc<StreamRecord> Creation + Cloning");
    let start = Instant::now();
    let arc_records: Vec<std::sync::Arc<StreamRecord>> = records
        .iter()
        .map(|r| std::sync::Arc::new(r.clone()))
        .collect();
    let arc_time = start.elapsed();
    let arc_rate = RECORD_COUNT as f64 / arc_time.as_secs_f64();
    println!(
        "   Arc creation: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        arc_time.as_millis(),
        arc_rate
    );

    // Test 4: Combined (JSON + Key extraction) - simulates KafkaDataWriter
    println!("\n🔬 Test 4: Combined JSON + Key Extraction");
    let start = Instant::now();
    for record in &records {
        let json_obj: serde_json::Map<String, serde_json::Value> = record
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), field_value_to_json_value(v)))
            .collect();
        let _json = serde_json::to_vec(&json_obj).unwrap();
        let _key = record.fields.get("id").map(|v| format!("{:?}", v));
    }
    let combined_time = start.elapsed();
    let combined_rate = RECORD_COUNT as f64 / combined_time.as_secs_f64();
    println!(
        "   Combined: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        combined_time.as_millis(),
        combined_rate
    );

    // Now test with actual Kafka
    println!("\n🔬 Test 5: Raw BaseProducer with Static Payload");
    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    let topic = "perf-bottleneck";
    env.create_topic(topic, 3)
        .await
        .expect("Failed to create topic");

    // Create producer with same settings as KafkaDataSink
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("enable.idempotence", "false")
        .set("acks", "1")
        .set("compression.type", "lz4")
        .set("batch.size", "1048576")
        .set("linger.ms", "5")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("queue.buffering.max.messages", "500000")
        .create()
        .expect("Failed to create producer");

    let producer = std::sync::Arc::new(producer);
    static PAYLOAD: &[u8] = b"{\"id\":0,\"symbol\":\"AAPL\",\"price\":150.00,\"qty\":100}";

    // Spawn poll thread
    let poll_producer = std::sync::Arc::clone(&producer);
    let poll_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poll_stop_clone = std::sync::Arc::clone(&poll_stop);
    let poll_handle = std::thread::spawn(move || {
        while !poll_stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            poll_producer.poll(Duration::from_millis(0));
        }
    });

    let start = Instant::now();
    for i in 0..RECORD_COUNT {
        let key = format!("key-{}", i % 100);
        let record = BaseRecord::to(topic).payload(PAYLOAD).key(key.as_str());
        let _ = producer.send(record);
    }
    let queue_time = start.elapsed();

    // Stop poll thread and flush
    poll_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    poll_handle.join().unwrap();
    producer.flush(Duration::from_secs(30)).unwrap();
    let total_time = start.elapsed();

    let queue_rate = RECORD_COUNT as f64 / queue_time.as_secs_f64();
    let total_rate = RECORD_COUNT as f64 / total_time.as_secs_f64();
    println!(
        "   Static payload queue: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        queue_time.as_millis(),
        queue_rate
    );
    println!(
        "   Including flush: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        total_time.as_millis(),
        total_rate
    );

    // Test 6: BaseProducer with JSON serialization inline
    println!("\n🔬 Test 6: BaseProducer with JSON Serialization");
    let producer2: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &env.bootstrap_servers)
        .set("enable.idempotence", "false")
        .set("acks", "1")
        .set("compression.type", "lz4")
        .set("batch.size", "1048576")
        .set("linger.ms", "5")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("queue.buffering.max.messages", "500000")
        .create()
        .expect("Failed to create producer");

    let producer2 = std::sync::Arc::new(producer2);
    let poll_producer2 = std::sync::Arc::clone(&producer2);
    let poll_stop2 = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poll_stop_clone2 = std::sync::Arc::clone(&poll_stop2);
    let poll_handle2 = std::thread::spawn(move || {
        while !poll_stop_clone2.load(std::sync::atomic::Ordering::Relaxed) {
            poll_producer2.poll(Duration::from_millis(0));
        }
    });

    let start = Instant::now();
    for record in &records {
        let json_obj: serde_json::Map<String, serde_json::Value> = record
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), field_value_to_json_value(v)))
            .collect();
        let payload = serde_json::to_vec(&json_obj).unwrap();
        let key = record.fields.get("id").map(|v| format!("{:?}", v));
        let mut kafka_record = BaseRecord::to(topic).payload(&payload);
        if let Some(k) = &key {
            kafka_record = kafka_record.key(k.as_str());
        }
        let _ = producer2.send(kafka_record);
    }
    let queue_time2 = start.elapsed();

    poll_stop2.store(true, std::sync::atomic::Ordering::Relaxed);
    poll_handle2.join().unwrap();
    producer2.flush(Duration::from_secs(30)).unwrap();
    let total_time2 = start.elapsed();

    let queue_rate2 = RECORD_COUNT as f64 / queue_time2.as_secs_f64();
    let total_rate2 = RECORD_COUNT as f64 / total_time2.as_secs_f64();
    println!(
        "   JSON + queue: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        queue_time2.as_millis(),
        queue_rate2
    );
    println!(
        "   Including flush: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        total_time2.as_millis(),
        total_rate2
    );

    // Test 7: KafkaDataWriter.write_batch() - full pipeline
    // Use same topic as Tests 5/6 to avoid new topic warmup overhead
    println!("\n🔬 Test 7: KafkaDataWriter.write_batch() (Full Pipeline)");
    use velostream::velostream::datasource::DataWriter;
    use velostream::velostream::datasource::kafka::writer::KafkaDataWriter;

    let writer_topic = topic; // Reuse same topic (already warmed up)

    let mut writer = KafkaDataWriter::new_with_config(
        &env.bootstrap_servers,
        writer_topic.to_string(),
        velostream::velostream::kafka::serialization_format::SerializationFormat::Json,
        Some(vec!["id".to_string()]),
        None,
    )
    .await
    .expect("Failed to create KafkaDataWriter");

    // Create Arc batch (same as actual usage)
    let arc_batch: Vec<std::sync::Arc<StreamRecord>> = records
        .iter()
        .map(|r| std::sync::Arc::new(r.clone()))
        .collect();

    let start = Instant::now();
    writer
        .write_batch(arc_batch)
        .await
        .expect("write_batch failed");
    let write_batch_time = start.elapsed();
    let write_batch_rate = RECORD_COUNT as f64 / write_batch_time.as_secs_f64();
    println!(
        "   write_batch: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        write_batch_time.as_millis(),
        write_batch_rate
    );

    // Flush and measure total time
    let flush_start = Instant::now();
    writer.flush().await.expect("flush failed");
    let flush_time = flush_start.elapsed();
    let total_with_flush = write_batch_time + flush_time;
    let total_rate = RECORD_COUNT as f64 / total_with_flush.as_secs_f64();
    println!(
        "   Including flush: {} records in {:.2}ms = {:.0} rec/s",
        RECORD_COUNT,
        total_with_flush.as_millis(),
        total_rate
    );

    // Summary
    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "SUMMARY");
    println!("{}", "=".repeat(80));
    println!("  JSON serialization only:     {:>12.0} rec/s", json_rate);
    println!("  Key extraction only:         {:>12.0} rec/s", key_rate);
    println!("  Arc creation:                {:>12.0} rec/s", arc_rate);
    println!(
        "  Combined (JSON + key):       {:>12.0} rec/s",
        combined_rate
    );
    println!(
        "  BaseProducer (static):       {:>12.0} rec/s (queue)",
        queue_rate
    );
    println!(
        "  BaseProducer (JSON inline):  {:>12.0} rec/s (queue)",
        queue_rate2
    );
    println!(
        "  KafkaDataWriter.write_batch: {:>12.0} rec/s (queue)",
        write_batch_rate
    );
    println!("  KafkaDataWriter (w/ flush):  {:>12.0} rec/s", total_rate);
    println!(
        "\n  Bottleneck: {}",
        if write_batch_rate < queue_rate2 * 0.5 {
            "KafkaDataWriter overhead"
        } else if json_rate < queue_rate {
            "JSON serialization"
        } else {
            "Producer queue/network"
        }
    );
    println!("{}", "=".repeat(80));
}

/// Test 8: Compare SyncPolledProducer vs AsyncPolledProducer
///
/// This benchmark compares the two PolledProducer implementations:
/// - SyncPolledProducer: No poll thread, inline polling
/// - AsyncPolledProducer: Dedicated poll thread for async delivery
#[tokio::test]
#[ignore]
async fn perf_polled_producer_comparison() {
    use rdkafka::producer::BaseRecord;
    use std::time::Instant;
    use velostream::velostream::kafka::kafka_fast_producer::{
        AsyncPolledProducer, PolledProducer, SyncPolledProducer,
    };

    const RECORD_COUNT: usize = 100_000;

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "POLLED PRODUCER COMPARISON: Sync vs Async");
    println!("{}", "=".repeat(80));

    // Start Kafka container
    println!("\n🚀 Starting Kafka container via testcontainers...");
    let env = KafkaTestcontainersEnv::new()
        .await
        .expect("Failed to start Kafka");
    println!("✅ Kafka container started: {}", env.bootstrap_servers);

    let topic = format!("polled-producer-test-{}", uuid::Uuid::new_v4());

    // Create static payload
    let payload = b"{\"id\":12345,\"name\":\"test_user\",\"price\":99.99}";

    // ========== Test: SyncPolledProducer ==========
    println!("\n🔬 Test: SyncPolledProducer (no poll thread)");
    {
        let mut producer = SyncPolledProducer::with_high_throughput(&env.bootstrap_servers)
            .expect("Failed to create SyncPolledProducer");

        let start = Instant::now();
        for _ in 0..RECORD_COUNT {
            let record = BaseRecord::to(&topic).payload(&payload[..]);
            let _ = producer.send(record);
        }
        let queue_time = start.elapsed();

        producer
            .flush(Duration::from_secs(30))
            .expect("flush failed");
        let total_time = start.elapsed();

        let queue_rate = RECORD_COUNT as f64 / queue_time.as_secs_f64();
        let total_rate = RECORD_COUNT as f64 / total_time.as_secs_f64();
        println!(
            "   Queue: {} records in {:.2}ms = {:.0} rec/s",
            RECORD_COUNT,
            queue_time.as_millis(),
            queue_rate
        );
        println!(
            "   Total (with flush): {} records in {:.2}ms = {:.0} rec/s",
            RECORD_COUNT,
            total_time.as_millis(),
            total_rate
        );
    }

    // ========== Test: AsyncPolledProducer ==========
    println!("\n🔬 Test: AsyncPolledProducer (poll thread, stops during flush)");
    {
        let mut producer = AsyncPolledProducer::with_high_throughput(&env.bootstrap_servers)
            .expect("Failed to create AsyncPolledProducer");

        let start = Instant::now();
        for _ in 0..RECORD_COUNT {
            let record = BaseRecord::to(&topic).payload(&payload[..]);
            let _ = producer.send(record);
        }
        let queue_time = start.elapsed();

        // flush() now automatically stops poll thread (production-grade behavior)
        producer
            .flush(Duration::from_secs(30))
            .expect("flush failed");
        let total_time = start.elapsed();

        let queue_rate = RECORD_COUNT as f64 / queue_time.as_secs_f64();
        let total_rate = RECORD_COUNT as f64 / total_time.as_secs_f64();
        println!(
            "   Queue: {} records in {:.2}ms = {:.0} rec/s",
            RECORD_COUNT,
            queue_time.as_millis(),
            queue_rate
        );
        println!(
            "   Total (with flush): {} records in {:.2}ms = {:.0} rec/s",
            RECORD_COUNT,
            total_time.as_millis(),
            total_rate
        );
    }

    println!("\n{}", "=".repeat(80));
    println!("{:^80}", "POLLED PRODUCER COMPARISON COMPLETE");
    println!("{}", "=".repeat(80));
}
