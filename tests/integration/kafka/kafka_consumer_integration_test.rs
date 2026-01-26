//! Kafka Consumer Integration Tests with Testcontainers
//!
//! This module provides integration tests for Kafka consumers using real Kafka
//! running in Docker containers via testcontainers.
//!
//! # Test Coverage
//!
//! - Consumer creation and configuration
//! - Topic subscription
//! - Message consumption (legacy StreamConsumer)
//! - Message consumption (fast BaseConsumer)
//! - Unified consumer trait behavior
//! - Performance tier selection
//!
//! # Running Tests
//!
//! These tests require Docker to be running:
//! ```bash
//! cargo test integration::kafka
//! ```

use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json;
use std::time::Duration;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::kafka::Kafka;
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, ConsumerTier};
use velostream::velostream::kafka::kafka_fast_consumer::Consumer as FastConsumer;
use velostream::velostream::kafka::serialization::JsonSerializer;
use velostream::velostream::kafka::unified_consumer::KafkaStreamConsumer;

/// Kafka test environment using testcontainers.
///
/// Provides a real Kafka instance running in Docker for integration testing.
pub struct KafkaTestEnv {
    _kafka_container: ContainerAsync<Kafka>,
    bootstrap_servers: String,
}

impl KafkaTestEnv {
    /// Creates a new Kafka test environment with Docker.
    ///
    /// Starts a Kafka container and returns the bootstrap servers address.
    pub async fn new() -> Self {
        // Start Kafka container (testcontainers 0.23 API)
        let kafka_container = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka container");

        // Get bootstrap servers from container
        let kafka_port = kafka_container
            .get_host_port_ipv4(9093)
            .await
            .expect("Failed to get Kafka port");
        let bootstrap_servers = format!("127.0.0.1:{}", kafka_port);

        // Wait for Kafka to be ready
        tokio::time::sleep(Duration::from_secs(5)).await;

        Self {
            _kafka_container: kafka_container,
            bootstrap_servers,
        }
    }

    /// Returns the bootstrap servers address.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Creates a topic in the Kafka cluster.
    pub async fn create_topic(&self, topic_name: &str, num_partitions: i32) -> Result<(), String> {
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .map_err(|e| format!("Failed to create admin client: {}", e))?;

        let new_topic = NewTopic::new(topic_name, num_partitions, TopicReplication::Fixed(1));
        let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));

        admin_client
            .create_topics(&[new_topic], &options)
            .await
            .map_err(|e| format!("Failed to create topic: {}", e))?;

        // Wait for topic to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok(())
    }

    /// Produces test messages to a topic.
    pub async fn produce_messages(
        &self,
        topic: &str,
        messages: Vec<(String, String)>, // (key, value) pairs - both should be JSON-encoded for JsonSerializer
    ) -> Result<(), String> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|e| format!("Failed to create producer: {}", e))?;

        for (key, value) in messages {
            let record = FutureRecord::to(topic).key(&key).payload(&value);

            producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(e, _)| format!("Failed to send message: {}", e))?;
        }

        // FutureProducer::send().await already waits for delivery, no explicit flush needed
        // Producer will be dropped at end of scope, ensuring all messages are sent

        Ok(())
    }
}

/// Helper to create JSON-encoded message pairs for String types.
/// Both keys and values are JSON-encoded since JsonSerializer is used for both.
fn json_messages(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| {
            // JSON-encode the key
            let key_bytes = serde_json::to_vec(k).unwrap();
            let key_str = String::from_utf8(key_bytes).unwrap();

            // JSON-encode the value
            let value_bytes = serde_json::to_vec(v).unwrap();
            let value_str = String::from_utf8(value_bytes).unwrap();

            (key_str, value_str)
        })
        .collect()
}

// ===== Integration Tests =====

#[tokio::test]
async fn test_kafka_consumer_basic_consumption() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-basic-consumption";

    // Create topic
    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Produce test messages (JSON-encoded for String type)
    let messages = json_messages(&[("key1", "value1"), ("key2", "value2"), ("key3", "value3")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create consumer (FR-081 Phase 2D: Using FastConsumer)
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-1");
    let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    // Consume messages
    let mut stream = consumer.stream();
    let mut received_count = 0;

    // Use timeout to avoid hanging if no messages
    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        received_count += 1;
                        println!("Received: key={:?}, value={:?}", message.key(), message.value());
                        if received_count >= 3 {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error consuming message: {}", e);
                        break;
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout waiting for messages");
        }
    }

    assert_eq!(received_count, 3, "Should have received 3 messages");
}

#[tokio::test]
async fn test_fast_consumer_with_config() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-fast-consumer";

    // Create topic
    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Produce test messages
    let messages = json_messages(&[("key1", "msg1"), ("key2", "msg2")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create fast consumer using with_config()
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-2");
    let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create fast consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    // Consume messages
    let mut stream = consumer.stream();
    let mut received_count = 0;

    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(_message) => {
                        received_count += 1;
                        if received_count >= 2 {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        break;
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout");
        }
    }

    assert_eq!(received_count, 2, "Should have received 2 messages");
}

#[tokio::test]
async fn test_unified_consumer_trait() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-unified-trait";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    let messages = json_messages(&[("k1", "v1")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Test via trait interface (FR-081 Phase 2D: Using FastConsumer)
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-3");
    let consumer: Box<dyn KafkaStreamConsumer<String, String>> = Box::new(
        FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
            config,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer"),
    );

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe via trait");

    let mut stream = consumer.stream();
    let mut received = false;

    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                if result.is_ok() {
                    received = true;
                    break;
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout");
        }
    }

    assert!(received, "Should have received message via trait interface");
}

#[tokio::test]
async fn test_performance_tier_configuration() {
    let env = KafkaTestEnv::new().await;

    // Test Standard tier configuration
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-4")
        .performance_tier(ConsumerTier::Standard);

    assert!(config.performance_tier.is_some());
    match config.performance_tier {
        Some(ConsumerTier::Standard) => {
            // Expected
        }
        _ => panic!("Expected Standard tier"),
    }

    // Test Buffered tier configuration
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-5")
        .performance_tier(ConsumerTier::Buffered { batch_size: 32 });

    match config.performance_tier {
        Some(ConsumerTier::Buffered { batch_size }) => {
            assert_eq!(batch_size, 32);
        }
        _ => panic!("Expected Buffered tier"),
    }

    // Test Dedicated tier configuration
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-6")
        .performance_tier(ConsumerTier::Dedicated);

    match config.performance_tier {
        Some(ConsumerTier::Dedicated) => {
            // Expected
        }
        _ => panic!("Expected Dedicated tier"),
    }
}

#[tokio::test]
async fn test_consumer_commit() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-commit";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    let messages = json_messages(&[("k1", "v1")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create consumer with manual commit (FR-081 Phase 2D: Using FastConsumer)
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-7")
        .auto_commit(false, Duration::from_secs(5));

    let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let mut stream = consumer.stream();

    tokio::select! {
        _ = async {
            if let Some(result) = stream.next().await {
                if result.is_ok() {
                    // Test manual commit
                    let commit_result = consumer.commit();
                    assert!(commit_result.is_ok(), "Commit should succeed");
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout");
        }
    }
}

/// Test that consumer offset commit actually persists the offset.
/// This verifies the fix for the "NoOffset" error where commit_consumer_state
/// fails because offsets weren't properly stored.
///
/// The test:
/// 1. Produces messages to a topic
/// 2. Consumes messages with manual commit (enable.auto.commit=false)
/// 3. Commits the offset after consuming
/// 4. Creates a NEW consumer with the same group ID
/// 5. Verifies the new consumer does NOT receive the already-committed messages
#[tokio::test]
async fn test_consumer_offset_commit_persists() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-offset-commit-persists";
    let group_id = "test-group-offset-persist";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Produce 5 test messages
    let messages = json_messages(&[
        ("k1", "v1"),
        ("k2", "v2"),
        ("k3", "v3"),
        ("k4", "v4"),
        ("k5", "v5"),
    ]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Consumer 1: Consume 3 messages and commit
    {
        let config = ConsumerConfig::new(env.bootstrap_servers(), group_id)
            .auto_commit(false, Duration::from_secs(5));

        let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
            config,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer 1");

        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let mut stream = consumer.stream();
        let mut received_count = 0;

        // Consume exactly 3 messages
        tokio::select! {
            _ = async {
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(msg) => {
                            received_count += 1;
                            println!("Consumer 1 received message {}: key={:?}", received_count, msg.key());
                            if received_count >= 3 {
                                break;
                            }
                        }
                        Err(e) => {
                            panic!("Consumer 1 error: {}", e);
                        }
                    }
                }
            } => {}
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                panic!("Timeout waiting for messages in consumer 1");
            }
        }

        assert_eq!(
            received_count, 3,
            "Consumer 1 should have received 3 messages"
        );

        // Commit the offset - this should persist offset 3 (next offset to read)
        let commit_result = consumer.commit();
        assert!(
            commit_result.is_ok(),
            "Commit should succeed, got error: {:?}",
            commit_result.err()
        );
        println!("Consumer 1: Offset committed successfully");

        // Drop consumer 1 - stream is already dropped
        drop(stream);
    }

    // Wait a bit for the commit to propagate
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Consumer 2: Same group ID - should start from committed offset (after message 3)
    {
        let config = ConsumerConfig::new(env.bootstrap_servers(), group_id)
            .auto_commit(false, Duration::from_secs(5));

        let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
            config,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer 2");

        consumer.subscribe(&[topic]).expect("Failed to subscribe");

        let mut stream = consumer.stream();
        let mut received_messages: Vec<String> = Vec::new();

        // Consumer 2 should only receive the remaining 2 messages (k4, k5)
        tokio::select! {
            _ = async {
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(msg) => {
                            let key = msg.key().cloned().unwrap_or_default();
                            println!("Consumer 2 received: key={}", key);
                            received_messages.push(key);
                            if received_messages.len() >= 2 {
                                break;
                            }
                        }
                        Err(e) => {
                            panic!("Consumer 2 error: {}", e);
                        }
                    }
                }
            } => {}
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                // Timeout is OK if we received 2 messages or 0 (if commit worked perfectly)
                println!("Consumer 2 timed out after receiving {} messages", received_messages.len());
            }
        }

        // Verify consumer 2 only received the uncommitted messages
        assert_eq!(
            received_messages.len(),
            2,
            "Consumer 2 should only receive 2 remaining messages (k4, k5), but got: {:?}",
            received_messages
        );

        // Verify it's the correct messages (k4, k5)
        assert!(
            received_messages.iter().any(|k| k.contains("k4")),
            "Consumer 2 should have received k4, got: {:?}",
            received_messages
        );
        assert!(
            received_messages.iter().any(|k| k.contains("k5")),
            "Consumer 2 should have received k5, got: {:?}",
            received_messages
        );

        // Should NOT have received k1, k2, k3 (already committed)
        assert!(
            !received_messages.iter().any(|k| k.contains("k1")),
            "Consumer 2 should NOT have received k1 (already committed), got: {:?}",
            received_messages
        );
    }

    println!("✅ Offset commit persistence test passed!");
}

/// Test that verifies the consumer commit behavior with transactional settings.
/// This specifically tests the scenario where enable.auto.commit=false and
/// we rely on manual commit after processing.
#[tokio::test]
async fn test_transactional_consumer_manual_commit() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-txn-manual-commit";
    let group_id = "test-group-txn-commit";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Produce messages
    let messages = json_messages(&[("txn-k1", "txn-v1"), ("txn-k2", "txn-v2")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create consumer with transactional-like settings
    let config = ConsumerConfig::new(env.bootstrap_servers(), group_id)
        .auto_commit(false, Duration::from_secs(5));

    let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let mut stream = consumer.stream();
    let mut received_count = 0;

    // Consume all messages
    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(msg) => {
                        received_count += 1;
                        println!("Received message {}: key={:?}", received_count, msg.key());
                        if received_count >= 2 {
                            break;
                        }
                    }
                    Err(e) => {
                        panic!("Consumer error: {}", e);
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout waiting for messages");
        }
    }

    assert_eq!(received_count, 2, "Should have received 2 messages");

    // Now commit - this should NOT fail with "NoOffset"
    let commit_result = consumer.commit();

    // Log the result for debugging
    match &commit_result {
        Ok(()) => println!("✅ Commit succeeded"),
        Err(e) => println!("❌ Commit failed: {:?}", e),
    }

    // The commit should succeed
    assert!(
        commit_result.is_ok(),
        "Manual commit should succeed after consuming messages. Error: {:?}",
        commit_result.err()
    );
}

// ===== Performance Tier Integration Tests =====

#[tokio::test]
async fn test_standard_tier_adapter() {
    use velostream::velostream::kafka::consumer_factory::ConsumerFactory;

    let env = KafkaTestEnv::new().await;
    let topic = "test-standard-tier";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    let messages = json_messages(&[("key1", "value1"), ("key2", "value2")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create consumer using ConsumerFactory with Standard tier
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-standard")
        .performance_tier(ConsumerTier::Standard);

    let consumer =
        ConsumerFactory::create::<String, String, _, _>(config, JsonSerializer, JsonSerializer)
            .expect("Failed to create Standard tier consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let mut stream = consumer.stream();
    let mut received_count = 0;

    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        received_count += 1;
                        println!("Standard tier received: {:?}", message.key());
                        if received_count >= 2 {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        break;
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout waiting for messages");
        }
    }

    assert_eq!(
        received_count, 2,
        "Standard tier should have received 2 messages"
    );
}

#[tokio::test]
async fn test_buffered_tier_adapter() {
    use velostream::velostream::kafka::consumer_factory::ConsumerFactory;

    let env = KafkaTestEnv::new().await;
    let topic = "test-buffered-tier";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    let messages = json_messages(&[("key1", "value1"), ("key2", "value2"), ("key3", "value3")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create consumer using ConsumerFactory with Buffered tier
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-buffered")
        .performance_tier(ConsumerTier::Buffered { batch_size: 32 });

    let consumer =
        ConsumerFactory::create::<String, String, _, _>(config, JsonSerializer, JsonSerializer)
            .expect("Failed to create Buffered tier consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let mut stream = consumer.stream();
    let mut received_count = 0;

    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        received_count += 1;
                        println!("Buffered tier received: {:?}", message.key());
                        if received_count >= 3 {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        break;
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout waiting for messages");
        }
    }

    assert_eq!(
        received_count, 3,
        "Buffered tier should have received 3 messages"
    );
}

#[tokio::test]
async fn test_dedicated_tier_adapter() {
    use velostream::velostream::kafka::consumer_factory::ConsumerFactory;

    let env = KafkaTestEnv::new().await;
    let topic = "test-dedicated-tier";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    let messages = json_messages(&[
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
        ("key4", "value4"),
    ]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    // Create consumer using ConsumerFactory with Dedicated tier
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-dedicated")
        .performance_tier(ConsumerTier::Dedicated);

    let consumer =
        ConsumerFactory::create::<String, String, _, _>(config, JsonSerializer, JsonSerializer)
            .expect("Failed to create Dedicated tier consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    let mut stream = consumer.stream();
    let mut received_count = 0;

    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        received_count += 1;
                        println!("Dedicated tier received: {:?}", message.key());
                        if received_count >= 4 {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        break;
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout waiting for messages");
        }
    }

    assert_eq!(
        received_count, 4,
        "Dedicated tier should have received 4 messages"
    );
}

// ===== Transactional Edge Case Tests =====

/// Test that a consumer with `isolation.level=read_committed` can read messages
/// produced by a NON-TRANSACTIONAL producer on testcontainers.
///
/// This tests the fix for the bug where:
/// - Consumer position stayed `Invalid` because LSO wasn't advancing properly
/// - The workaround seeks to beginning when Invalid positions are detected
///
/// Without the fix in `kafka_fast_consumer.rs`, this test would hang or timeout
/// because the consumer would never receive any messages.
///
/// Related fix: `kafka_fast_consumer.rs` - seek to beginning when detecting
/// Invalid positions with read_committed isolation level.
#[tokio::test]
async fn test_read_committed_consumer_with_non_transactional_messages() {
    let env = KafkaTestEnv::new().await;
    let topic = "test-read-committed-nontxn";

    env.create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    // Produce messages with NON-TRANSACTIONAL producer (like test harness does)
    // This is key: messages have no transaction markers
    let messages = json_messages(&[("key1", "value1"), ("key2", "value2"), ("key3", "value3")]);
    env.produce_messages(topic, messages)
        .await
        .expect("Failed to produce messages");

    println!("Produced 3 non-transactional messages to topic '{}'", topic);

    // Create consumer with read_committed isolation level
    // This is what transactional processors use
    let config = ConsumerConfig::new(env.bootstrap_servers(), "test-group-read-committed")
        .isolation_level(
            velostream::velostream::kafka::consumer_config::IsolationLevel::ReadCommitted,
        );

    let consumer = FastConsumer::<String, String, JsonSerializer, JsonSerializer>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[topic]).expect("Failed to subscribe");

    // Consume messages - without the fix this would hang because position stays Invalid
    let mut stream = consumer.stream();
    let mut received_count = 0;

    tokio::select! {
        _ = async {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        received_count += 1;
                        println!("Received message {}: key={:?}", received_count, message.key());
                        if received_count >= 3 {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error consuming message: {}", e);
                        break;
                    }
                }
            }
        } => {}
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            panic!("Timeout waiting for messages - read_committed consumer may be stuck with Invalid position");
        }
    }

    assert_eq!(
        received_count, 3,
        "read_committed consumer should receive all 3 non-transactional messages. \
         If this fails, the Invalid position seek workaround may not be working."
    );

    println!("✅ read_committed consumer successfully read non-transactional messages");
}
