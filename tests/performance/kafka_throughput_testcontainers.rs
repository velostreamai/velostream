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
use rdkafka::producer::{FutureProducer, FutureRecord};
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
    let mut last_progress: u64 = 0;

    while consumed < record_count {
        if start.elapsed() > timeout {
            println!("⚠️ Timeout reached after {:?}", timeout);
            break;
        }

        let msg_start = Instant::now();

        // Poll for messages
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap_or(&[]);
                total_bytes += payload.len() as u64;
                consumed += 1;

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
#[ignore] // Run explicitly: cargo test kafka_throughput_payload_comparison -- --ignored --nocapture
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

    // Create KafkaDataSource
    let mut data_source =
        KafkaDataSource::new(env.bootstrap_servers.clone(), source_topic.to_string())
            .with_group_id("datasource-throughput-test-group".to_string())
            .with_config("auto.offset.reset".to_string(), "earliest".to_string())
            .with_config("fetch.min.bytes".to_string(), "1048576".to_string()) // 1MB
            .with_config("fetch.max.wait.ms".to_string(), "500".to_string())
            .with_config("max.poll.records".to_string(), "1000".to_string());

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
