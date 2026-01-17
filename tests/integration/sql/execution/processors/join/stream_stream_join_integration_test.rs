//! Stream-Stream Join Integration Tests with Testcontainers
//!
//! FR-085: These tests verify the full stream-stream join pipeline using real Kafka
//! running in Docker containers via testcontainers.
//!
//! # Test Coverage
//!
//! - JoinCoordinator processing with concurrent streams
//! - JoinStateStore watermark-based expiration
//! - Full pipeline with orders/shipments interval join
//! - SourceCoordinator concurrent stream reading
//!
//! # Running Tests
//!
//! These tests require Docker to be running. To skip Docker tests:
//! ```bash
//! SKIP_DOCKER_TESTS=1 cargo test stream_stream_join_integration
//! ```

use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::kafka::Kafka;
use velostream::velostream::kafka::serialization::StringSerializer;
use velostream::velostream::sql::execution::StreamRecord;
use velostream::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinSide, JoinStateStore,
};
use velostream::velostream::sql::execution::types::FieldValue;

/// Check if Docker tests should be skipped
fn should_skip_docker_tests() -> bool {
    std::env::var("SKIP_DOCKER_TESTS").is_ok()
}

/// Check if an error indicates Docker is not available
fn is_docker_unavailable_error(err: &str) -> bool {
    err.contains("Docker")
        || err.contains("docker")
        || err.contains("container")
        || err.contains("daemon")
        || err.contains("connection refused")
}

/// Kafka test environment using testcontainers.
struct JoinTestEnv {
    _kafka_container: ContainerAsync<Kafka>,
    bootstrap_servers: String,
}

impl JoinTestEnv {
    /// Creates a new Kafka test environment with Docker.
    /// Returns Err if Docker is not available.
    async fn new() -> Result<Self, String> {
        let kafka_container = Kafka::default()
            .start()
            .await
            .map_err(|e| format!("Failed to start Kafka container: {}", e))?;

        let kafka_port = kafka_container
            .get_host_port_ipv4(9093)
            .await
            .map_err(|e| format!("Failed to get Kafka port: {}", e))?;
        let bootstrap_servers = format!("127.0.0.1:{}", kafka_port);

        // Wait for Kafka to be ready
        tokio::time::sleep(Duration::from_secs(5)).await;

        Ok(Self {
            _kafka_container: kafka_container,
            bootstrap_servers,
        })
    }

    fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    async fn create_topic(&self, topic_name: &str, num_partitions: i32) -> Result<(), String> {
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

        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    async fn produce_json_messages(
        &self,
        topic: &str,
        messages: Vec<(String, String)>,
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

        Ok(())
    }
}

/// Create an order record for testing
fn create_order_record(
    order_id: &str,
    customer_id: i64,
    amount: i64,
    event_time_ms: i64,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "order_id".to_string(),
        FieldValue::String(order_id.to_string()),
    );
    fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
    fields.insert("amount".to_string(), FieldValue::Integer(amount));
    fields.insert("event_time".to_string(), FieldValue::Integer(event_time_ms));

    StreamRecord {
        fields,
        timestamp: event_time_ms,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: Some(FieldValue::String("orders".to_string())),
        key: Some(FieldValue::String(order_id.to_string())),
    }
}

/// Create a shipment record for testing
fn create_shipment_record(
    shipment_id: &str,
    order_id: &str,
    carrier: &str,
    event_time_ms: i64,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "shipment_id".to_string(),
        FieldValue::String(shipment_id.to_string()),
    );
    fields.insert(
        "order_id".to_string(),
        FieldValue::String(order_id.to_string()),
    );
    fields.insert(
        "carrier".to_string(),
        FieldValue::String(carrier.to_string()),
    );
    fields.insert("event_time".to_string(), FieldValue::Integer(event_time_ms));

    StreamRecord {
        fields,
        timestamp: event_time_ms,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: Some(FieldValue::String("shipments".to_string())),
        key: Some(FieldValue::String(shipment_id.to_string())),
    }
}

// ===== Unit-Level Integration Tests (No Kafka) =====

#[test]
fn test_join_state_store_basic_operations() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));

    // Store some records
    store.store(
        "order_1",
        create_order_record("order_1", 100, 5000, 1000),
        1000,
    );
    store.store(
        "order_2",
        create_order_record("order_2", 101, 7500, 2000),
        2000,
    );
    store.store(
        "order_1",
        create_order_record("order_1b", 100, 3000, 1500),
        1500,
    );

    // Verify counts
    assert_eq!(store.record_count(), 3, "Should have 3 records stored");
    assert_eq!(store.key_count(), 2, "Should have 2 unique keys");

    // Lookup with time range
    let matches = store.lookup("order_1", 900, 1600);
    assert_eq!(
        matches.len(),
        2,
        "Should find 2 records for order_1 in time range"
    );

    // Verify stats
    let stats = store.stats();
    assert_eq!(stats.records_stored, 3);
}

#[test]
fn test_join_state_store_watermark_expiration() {
    let mut store = JoinStateStore::with_retention_ms(1000);

    // Store records at different event times
    store.store("k1", create_order_record("o1", 1, 100, 1000), 1000);
    store.store("k2", create_order_record("o2", 2, 200, 2000), 2000);
    store.store("k3", create_order_record("o3", 3, 300, 3000), 3000);

    assert_eq!(store.record_count(), 3);

    // Advance watermark - should expire records older than watermark - retention
    let expired = store.advance_watermark(2500);
    assert_eq!(
        expired, 1,
        "Should expire 1 record (event_time 1000 < 2500 - 1000)"
    );
    assert_eq!(store.record_count(), 2);

    // Advance further
    let expired = store.advance_watermark(4500);
    assert_eq!(expired, 2, "Should expire remaining 2 records");
    assert_eq!(store.record_count(), 0);
}

#[test]
fn test_join_coordinator_creation() {
    // Create join config with 5 required arguments
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -3_600_000, // 1 hour before
        86_400_000, // 24 hours after
    )
    .with_retention(Duration::from_secs(3600));

    let coordinator = JoinCoordinator::new(config);
    let stats = coordinator.stats();

    assert_eq!(stats.left_records_processed, 0);
    assert_eq!(stats.right_records_processed, 0);
    assert_eq!(stats.matches_emitted, 0);
}

#[test]
fn test_join_coordinator_basic_join() {
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -1000, // 1 second before
        5000,  // 5 seconds after
    )
    .with_retention(Duration::from_secs(3600));

    let mut coordinator = JoinCoordinator::new(config);

    // Process an order at t=1000
    let order = create_order_record("ORD-001", 100, 5000, 1000);
    let results = coordinator
        .process_left(order)
        .expect("process_left failed");
    assert!(results.is_empty(), "No matches yet - no shipments in store");

    // Process a shipment at t=2000 (within interval: 1000-1000 to 1000+5000)
    let shipment = create_shipment_record("SHIP-001", "ORD-001", "FedEx", 2000);
    let results = coordinator
        .process_right(shipment)
        .expect("process_right failed");
    assert_eq!(results.len(), 1, "Should match with the order");

    // Verify joined record has fields from both sides
    let joined = &results[0];
    assert!(joined.fields.contains_key("order_id"));
    assert!(joined.fields.contains_key("amount"));
    assert!(joined.fields.contains_key("shipment_id"));
    assert!(joined.fields.contains_key("carrier"));

    // Verify stats
    let stats = coordinator.stats();
    assert_eq!(stats.left_records_processed, 1);
    assert_eq!(stats.right_records_processed, 1);
    assert_eq!(stats.matches_emitted, 1);
}

#[test]
fn test_join_coordinator_no_match_outside_interval() {
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -1000, // 1 second before
        1000,  // 1 second after
    )
    .with_retention(Duration::from_secs(3600));

    let mut coordinator = JoinCoordinator::new(config);

    // Process an order at t=1000
    let order = create_order_record("ORD-002", 100, 5000, 1000);
    let _ = coordinator
        .process_left(order)
        .expect("process_left failed");

    // Process a shipment at t=5000 (outside interval: should be within 0 to 2000)
    let shipment = create_shipment_record("SHIP-002", "ORD-002", "UPS", 5000);
    let results = coordinator
        .process_right(shipment)
        .expect("process_right failed");
    assert!(
        results.is_empty(),
        "Shipment at t=5000 is outside interval [0, 2000]"
    );
}

#[test]
fn test_join_coordinator_multiple_matches() {
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("customer_id".to_string(), "order_id".to_string())],
        -5000, // 5 seconds before
        5000,  // 5 seconds after
    )
    .with_retention(Duration::from_secs(3600));

    let mut coordinator = JoinCoordinator::new(config);

    // Store multiple orders for customer 100
    let order1 = create_order_record("ORD-A", 100, 1000, 1000);
    let order2 = create_order_record("ORD-B", 100, 2000, 2000);
    let order3 = create_order_record("ORD-C", 100, 3000, 3000);

    let _ = coordinator
        .process_left(order1)
        .expect("process_left failed");
    let _ = coordinator
        .process_left(order2)
        .expect("process_left failed");
    let _ = coordinator
        .process_left(order3)
        .expect("process_left failed");

    // Process a shipment that should match all orders (by customer_id key)
    let mut shipment_fields = HashMap::new();
    shipment_fields.insert(
        "shipment_id".to_string(),
        FieldValue::String("SHIP-X".to_string()),
    );
    shipment_fields.insert("order_id".to_string(), FieldValue::Integer(100)); // Match customer_id
    shipment_fields.insert("carrier".to_string(), FieldValue::String("DHL".to_string()));
    shipment_fields.insert("event_time".to_string(), FieldValue::Integer(2500));

    let shipment = StreamRecord {
        fields: shipment_fields,
        timestamp: 2500,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: Some(FieldValue::String("shipments".to_string())),
        key: Some(FieldValue::String("SHIP-X".to_string())),
    };

    let results = coordinator
        .process_right(shipment)
        .expect("process_right failed");
    // Should match all 3 orders since they're all within the time interval
    assert_eq!(
        results.len(),
        3,
        "Should match all 3 orders within time interval"
    );
}

#[test]
fn test_join_side_opposite() {
    assert!(matches!(JoinSide::Left.opposite(), JoinSide::Right));
    assert!(matches!(JoinSide::Right.opposite(), JoinSide::Left));
}

// ===== Kafka Integration Tests (Require Docker) =====

#[tokio::test]
async fn test_kafka_env_creation() {
    if should_skip_docker_tests() {
        println!("Skipping Docker test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    match JoinTestEnv::new().await {
        Ok(env) => {
            assert!(!env.bootstrap_servers().is_empty());
            println!("Kafka bootstrap servers: {}", env.bootstrap_servers());
        }
        Err(e) => {
            if is_docker_unavailable_error(&e) {
                println!("Skipping test - Docker not available: {}", e);
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_kafka_topic_creation_for_joins() {
    if should_skip_docker_tests() {
        println!("Skipping Docker test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    match JoinTestEnv::new().await {
        Ok(env) => {
            // Create orders topic
            env.create_topic("test-orders", 1)
                .await
                .expect("Failed to create orders topic");

            // Create shipments topic
            env.create_topic("test-shipments", 1)
                .await
                .expect("Failed to create shipments topic");

            println!("Created orders and shipments topics successfully");
        }
        Err(e) => {
            if is_docker_unavailable_error(&e) {
                println!("Skipping test - Docker not available: {}", e);
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_kafka_produce_order_messages() {
    if should_skip_docker_tests() {
        println!("Skipping Docker test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    match JoinTestEnv::new().await {
        Ok(env) => {
            let topic = "test-orders-produce";

            env.create_topic(topic, 1)
                .await
                .expect("Failed to create topic");

            // Produce order messages as JSON
            let orders = vec![
                (
                    "ORD-001".to_string(),
                    r#"{"order_id":"ORD-001","customer_id":100,"amount":5000}"#.to_string(),
                ),
                (
                    "ORD-002".to_string(),
                    r#"{"order_id":"ORD-002","customer_id":101,"amount":7500}"#.to_string(),
                ),
                (
                    "ORD-003".to_string(),
                    r#"{"order_id":"ORD-003","customer_id":100,"amount":3000}"#.to_string(),
                ),
            ];

            env.produce_json_messages(topic, orders)
                .await
                .expect("Failed to produce orders");

            println!("Produced 3 order messages successfully");
        }
        Err(e) => {
            if is_docker_unavailable_error(&e) {
                println!("Skipping test - Docker not available: {}", e);
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_kafka_produce_and_consume_for_join() {
    if should_skip_docker_tests() {
        println!("Skipping Docker test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    match JoinTestEnv::new().await {
        Ok(env) => {
            use velostream::velostream::kafka::consumer_config::ConsumerConfig;
            use velostream::velostream::kafka::kafka_fast_consumer::Consumer as FastConsumer;

            let orders_topic = "test-join-orders";
            let shipments_topic = "test-join-shipments";

            // Create topics
            env.create_topic(orders_topic, 1)
                .await
                .expect("Failed to create orders topic");
            env.create_topic(shipments_topic, 1)
                .await
                .expect("Failed to create shipments topic");

            // Produce test data
            let orders = vec![
                (
                    "ORD-001".to_string(),
                    r#"{"order_id":"ORD-001","customer_id":100,"amount":5000}"#.to_string(),
                ),
                (
                    "ORD-002".to_string(),
                    r#"{"order_id":"ORD-002","customer_id":101,"amount":7500}"#.to_string(),
                ),
            ];
            let shipments = vec![(
                "SHIP-001".to_string(),
                r#"{"shipment_id":"SHIP-001","order_id":"ORD-001","carrier":"FedEx"}"#.to_string(),
            )];

            env.produce_json_messages(orders_topic, orders)
                .await
                .expect("Failed to produce orders");
            env.produce_json_messages(shipments_topic, shipments)
                .await
                .expect("Failed to produce shipments");

            // Create consumer for orders - use StringSerializer since we produce raw JSON strings
            let config = ConsumerConfig::new(env.bootstrap_servers(), "join-test-group-orders");
            let consumer =
                FastConsumer::<String, String, StringSerializer, StringSerializer>::with_config(
                    config,
                    StringSerializer,
                    StringSerializer,
                )
                .expect("Failed to create consumer");

            consumer
                .subscribe(&[orders_topic])
                .expect("Failed to subscribe");

            // Consume orders
            let mut stream = consumer.stream();
            let mut order_count = 0;

            tokio::select! {
                _ = async {
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(msg) => {
                                order_count += 1;
                                println!("Received order: key={:?}, value={:?}", msg.key(), msg.value());
                                if order_count >= 2 {
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
                    panic!("Timeout waiting for orders");
                }
            }

            assert_eq!(order_count, 2, "Should have received 2 orders");
            println!("Successfully consumed {} orders from Kafka", order_count);
        }
        Err(e) => {
            if is_docker_unavailable_error(&e) {
                println!("Skipping test - Docker not available: {}", e);
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_full_join_pipeline_with_kafka() {
    if should_skip_docker_tests() {
        println!("Skipping Docker test (SKIP_DOCKER_TESTS is set)");
        return;
    }

    match JoinTestEnv::new().await {
        Ok(env) => {
            use velostream::velostream::kafka::consumer_config::ConsumerConfig;
            use velostream::velostream::kafka::kafka_fast_consumer::Consumer as FastConsumer;

            let orders_topic = "test-full-join-orders";
            let shipments_topic = "test-full-join-shipments";

            // Create topics
            env.create_topic(orders_topic, 1)
                .await
                .expect("Failed to create orders topic");
            env.create_topic(shipments_topic, 1)
                .await
                .expect("Failed to create shipments topic");

            // Produce test data with timestamps for join matching
            let base_time = chrono::Utc::now().timestamp_millis();

            let orders = vec![
                (
                    "ORD-100".to_string(),
                    format!(
                        r#"{{"order_id":"ORD-100","customer_id":100,"amount":5000,"event_time":{}}}"#,
                        base_time
                    ),
                ),
                (
                    "ORD-101".to_string(),
                    format!(
                        r#"{{"order_id":"ORD-101","customer_id":101,"amount":7500,"event_time":{}}}"#,
                        base_time + 1000
                    ),
                ),
            ];

            let shipments = vec![
                (
                    "SHIP-100".to_string(),
                    format!(
                        r#"{{"shipment_id":"SHIP-100","order_id":"ORD-100","carrier":"FedEx","event_time":{}}}"#,
                        base_time + 2000
                    ),
                ),
                (
                    "SHIP-101".to_string(),
                    format!(
                        r#"{{"shipment_id":"SHIP-101","order_id":"ORD-101","carrier":"UPS","event_time":{}}}"#,
                        base_time + 3000
                    ),
                ),
            ];

            env.produce_json_messages(orders_topic, orders)
                .await
                .expect("Failed to produce orders");
            env.produce_json_messages(shipments_topic, shipments)
                .await
                .expect("Failed to produce shipments");

            // Create join coordinator
            let config = JoinConfig::interval_ms(
                "orders",
                "shipments",
                vec![("order_id".to_string(), "order_id".to_string())],
                -3_600_000, // -1h
                86_400_000, // +24h
            )
            .with_retention(Duration::from_secs(3600));

            let mut coordinator = JoinCoordinator::new(config);

            // Consume orders and feed to join coordinator - use StringSerializer for raw JSON
            let order_config = ConsumerConfig::new(env.bootstrap_servers(), "full-join-orders");
            let order_consumer =
                FastConsumer::<String, String, StringSerializer, StringSerializer>::with_config(
                    order_config,
                    StringSerializer,
                    StringSerializer,
                )
                .expect("Failed to create order consumer");

            order_consumer
                .subscribe(&[orders_topic])
                .expect("Failed to subscribe to orders");

            let mut order_stream = order_consumer.stream();
            let mut orders_processed = 0;

            // Process orders
            tokio::select! {
                _ = async {
                    while let Some(result) = order_stream.next().await {
                        if let Ok(msg) = result {
                            let value = msg.value();
                            // Parse JSON and create StreamRecord
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(value) {
                                let order_id = json["order_id"].as_str().unwrap_or("");
                                let customer_id = json["customer_id"].as_i64().unwrap_or(0);
                                let amount = json["amount"].as_i64().unwrap_or(0);
                                let event_time = json["event_time"].as_i64().unwrap_or(base_time);

                                let record = create_order_record(order_id, customer_id, amount, event_time);
                                let _ = coordinator.process_left(record);
                                orders_processed += 1;

                                if orders_processed >= 2 {
                                    break;
                                }
                            }
                        }
                    }
                } => {}
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    panic!("Timeout processing orders");
                }
            }

            // Consume shipments and feed to join coordinator - use StringSerializer for raw JSON
            let shipment_config =
                ConsumerConfig::new(env.bootstrap_servers(), "full-join-shipments");
            let shipment_consumer =
                FastConsumer::<String, String, StringSerializer, StringSerializer>::with_config(
                    shipment_config,
                    StringSerializer,
                    StringSerializer,
                )
                .expect("Failed to create shipment consumer");

            shipment_consumer
                .subscribe(&[shipments_topic])
                .expect("Failed to subscribe to shipments");

            let mut shipment_stream = shipment_consumer.stream();
            let mut total_matches = 0;
            let mut shipments_processed = 0;

            // Process shipments and collect join results
            tokio::select! {
                _ = async {
                    while let Some(result) = shipment_stream.next().await {
                        if let Ok(msg) = result {
                            let value = msg.value();
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(value) {
                                let shipment_id = json["shipment_id"].as_str().unwrap_or("");
                                let order_id = json["order_id"].as_str().unwrap_or("");
                                let carrier = json["carrier"].as_str().unwrap_or("");
                                let event_time = json["event_time"].as_i64().unwrap_or(base_time);

                                let record = create_shipment_record(shipment_id, order_id, carrier, event_time);
                                if let Ok(results) = coordinator.process_right(record) {
                                    println!("Shipment {} matched {} orders", shipment_id, results.len());
                                    total_matches += results.len();
                                }
                                shipments_processed += 1;

                                if shipments_processed >= 2 {
                                    break;
                                }
                            }
                        }
                    }
                } => {}
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    panic!("Timeout processing shipments");
                }
            }

            // Verify results
            let stats = coordinator.stats();
            println!("\n=== Join Statistics ===");
            println!("Left records processed: {}", stats.left_records_processed);
            println!("Right records processed: {}", stats.right_records_processed);
            println!("Join matches: {}", stats.matches_emitted);
            println!("Total matches found: {}", total_matches);

            assert_eq!(orders_processed, 2, "Should have processed 2 orders");
            assert_eq!(shipments_processed, 2, "Should have processed 2 shipments");
            assert_eq!(
                total_matches, 2,
                "Each shipment should match its corresponding order"
            );

            println!("\nFull join pipeline test passed!");
        }
        Err(e) => {
            if is_docker_unavailable_error(&e) {
                println!("Skipping test - Docker not available: {}", e);
            } else {
                panic!("Unexpected error: {}", e);
            }
        }
    }
}

/// Test to verify the watermark-based state cleanup works correctly
#[tokio::test]
async fn test_join_state_cleanup_with_watermarks() {
    // This test doesn't actually require Docker, but it's async so keeping it here
    let config = JoinConfig::interval_ms(
        "orders",
        "shipments",
        vec![("order_id".to_string(), "order_id".to_string())],
        -1000, // 1 second before
        5000,  // 5 seconds after
    )
    .with_retention(Duration::from_millis(2000)); // 2 second retention

    let mut coordinator = JoinCoordinator::new(config);

    // Add records at different times
    let order1 = create_order_record("O1", 1, 100, 1000);
    let order2 = create_order_record("O2", 2, 200, 3000);
    let order3 = create_order_record("O3", 3, 300, 5000);

    let _ = coordinator.process_left(order1);
    let _ = coordinator.process_left(order2);
    let _ = coordinator.process_left(order3);

    // Advance watermark to 4000 - should expire records before 2000 (4000 - 2000 retention)
    let (left_expired, right_expired) = coordinator.advance_watermark(JoinSide::Left, 4000);

    println!(
        "After watermark 4000: left_expired={}, right_expired={}",
        left_expired, right_expired
    );

    // Order1 at t=1000 should be expired (1000 < 4000 - 2000)
    // Order2 at t=3000 should still be there (3000 >= 2000)
    // Order3 at t=5000 should still be there (5000 >= 2000)
    assert_eq!(left_expired, 1, "Should have expired 1 old order");

    let stats = coordinator.stats();
    println!("Stats: {:?}", stats);

    println!("Watermark-based cleanup test passed!");
}
