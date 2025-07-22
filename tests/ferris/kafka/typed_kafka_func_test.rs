use crate::ferris::kafka::test_utils::{is_kafka_running, init_logger};
use ferrisstreams::{
    TypedKafkaProducer, TypedKafkaConsumer, TypedProducerBuilder, TypedConsumerBuilder,
    JsonSerializer, TypedMessage, KafkaConsumable, SerializationError
};
use serde::{Deserialize, Serialize};
use serial_test::serial;
use std::time::Duration;
use uuid::Uuid;

const TEST_TOPIC: &str = "typed-kafka-func-test";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestUser {
    id: u32,
    name: String,
    email: String,
    active: bool,
}

impl TestUser {
    fn new(id: u32, name: &str, email: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            email: email.to_string(),
            active: true,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestOrder {
    order_id: String,
    user_id: u32,
    amount: f64,
    status: OrderStatus,
    timestamp: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
enum OrderStatus {
    Created,
    Paid,
    Shipped,
    Delivered,
    Cancelled,
}

impl TestOrder {
    fn new(order_id: &str, user_id: u32, amount: f64) -> Self {
        Self {
            order_id: order_id.to_string(),
            user_id,
            amount,
            status: OrderStatus::Created,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }
}

#[tokio::test]
#[serial]
async fn test_typed_producer_consumer_basic() {
    init_logger();
    if !is_kafka_running() { return; }

    let test_user = TestUser::new(1, "Alice Smith", "alice@example.com");
    let user_key = format!("user-{}", test_user.id);

    // Create typed producer
    let producer = TypedKafkaProducer::<TestUser, _>::new(
        "localhost:9092",
        TEST_TOPIC,
        JsonSerializer,
    ).expect("Failed to create typed producer");

    // Send typed message
    let send_result = producer.send(Some(&user_key), &test_user, None).await;
    assert!(send_result.is_ok(), "Failed to send typed message: {:?}", send_result.err());

    // Flush producer
    producer.flush(5000).expect("Failed to flush producer");

    // Create typed consumer
    let group_id = format!("typed-test-group-{}", Uuid::new_v4());
    let consumer = TypedKafkaConsumer::<TestUser, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
    );

    consumer.subscribe(&[TEST_TOPIC]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Poll for typed message
    let mut found = false;
    for _ in 0..20 {
        match consumer.poll_message(Duration::from_secs(2)).await {
            Ok(typed_message) => {
                let received_user = typed_message.value();
                let received_key = typed_message.key();
                
                println!("Received user: {:?}", received_user);
                println!("Received key: {:?}", received_key);
                
                if *received_user == test_user && received_key == Some(&user_key) {
                    found = true;
                    
                    // Test TypedMessage methods
                    assert_eq!(received_user.id, 1);
                    assert_eq!(received_user.name, "Alice Smith");
                    assert_eq!(received_user.email, "alice@example.com");
                    assert_eq!(received_user.active, true);
                    break;
                }
            }
            Err(e) => {
                println!("Consumer error: {:?}", e);
            }
        }
    }
    
    assert!(found, "Did not receive the expected typed message");
    
    // Test commit
    consumer.commit().expect("Failed to commit consumer state");
}

#[tokio::test]
#[serial]
async fn test_typed_producer_builder() {
    init_logger();
    if !is_kafka_running() { return; }

    let test_order = TestOrder::new("order-123", 1, 99.99);
    let order_key = format!("order-{}", test_order.order_id);

    // Create producer using builder pattern
    let producer = TypedProducerBuilder::<TestOrder, _>::new(
        "localhost:9092",
        TEST_TOPIC,
        JsonSerializer,
    ).build().expect("Failed to build typed producer");

    // Send with current timestamp
    let send_result = producer.send_with_current_timestamp(Some(&order_key), &test_order).await;
    assert!(send_result.is_ok(), "Failed to send order: {:?}", send_result.err());

    producer.flush(5000).expect("Failed to flush producer");

    // Create consumer using builder pattern
    let group_id = format!("typed-builder-test-{}", Uuid::new_v4());
    let consumer = TypedConsumerBuilder::<TestOrder, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
    ).build();

    consumer.subscribe(&[TEST_TOPIC]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify received order
    let mut found = false;
    for _ in 0..20 {
        match consumer.poll_message(Duration::from_secs(2)).await {
            Ok(typed_message) => {
                let received_order = typed_message.value();
                
                if received_order.order_id == test_order.order_id {
                    assert_eq!(received_order.user_id, 1);
                    assert_eq!(received_order.amount, 99.99);
                    assert_eq!(received_order.status, OrderStatus::Created);
                    found = true;
                    break;
                }
            }
            Err(_) => continue,
        }
    }
    
    assert!(found, "Did not receive the expected order");
}

#[tokio::test]
#[serial]
async fn test_consumable_trait() {
    init_logger();
    if !is_kafka_running() { return; }

    let test_user = TestUser::new(42, "Bob Johnson", "bob@test.com");

    // Create producer
    let producer = TypedKafkaProducer::<TestUser, _>::new(
        "localhost:9092",
        TEST_TOPIC,
        JsonSerializer,
    ).expect("Failed to create producer");

    // Send message
    producer.send(Some("bob-key"), &test_user, None).await
        .expect("Failed to send message");
    
    producer.flush(5000).expect("Failed to flush");

    // Create consumer using trait method
    let group_id = format!("consumable-test-{}", Uuid::new_v4());
    let consumer = TestUser::consumer("localhost:9092", &group_id, JsonSerializer);
    
    consumer.subscribe(&[TEST_TOPIC]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify message received
    let mut found = false;
    for _ in 0..20 {
        if let Ok(typed_message) = consumer.poll_message(Duration::from_secs(1)).await {
            let user = typed_message.value();
            if user.id == 42 && user.name == "Bob Johnson" {
                found = true;
                break;
            }
        }
    }
    
    assert!(found, "KafkaConsumable trait test failed");
}

#[tokio::test]
#[serial]
async fn test_multiple_message_types() {
    init_logger();
    if !is_kafka_running() { return; }

    // Send different message types to the same topic
    let user = TestUser::new(100, "Multi Test", "multi@test.com");
    let order = TestOrder::new("multi-order", 100, 199.99);

    // User producer/consumer
    let user_producer = TypedKafkaProducer::<TestUser, _>::new(
        "localhost:9092", TEST_TOPIC, JsonSerializer
    ).expect("Failed to create user producer");
    
    let user_group = format!("multi-user-{}", Uuid::new_v4());
    let user_consumer = TypedKafkaConsumer::<TestUser, _>::new(
        "localhost:9092", &user_group, JsonSerializer
    );

    // Order producer/consumer  
    let order_producer = TypedKafkaProducer::<TestOrder, _>::new(
        "localhost:9092", TEST_TOPIC, JsonSerializer
    ).expect("Failed to create order producer");
    
    let order_group = format!("multi-order-{}", Uuid::new_v4());
    let order_consumer = TypedKafkaConsumer::<TestOrder, _>::new(
        "localhost:9092", &order_group, JsonSerializer
    );

    // Subscribe consumers
    user_consumer.subscribe(&[TEST_TOPIC]);
    order_consumer.subscribe(&[TEST_TOPIC]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Send messages
    user_producer.send(Some("multi-user"), &user, None).await
        .expect("Failed to send user");
    order_producer.send(Some("multi-order"), &order, None).await
        .expect("Failed to send order");
    
    // Flush both producers
    user_producer.flush(5000).expect("Failed to flush user producer");
    order_producer.flush(5000).expect("Failed to flush order producer");

    // Verify each consumer receives the correct type
    let mut user_received = false;
    let mut order_received = false;

    // Check user consumer (should only deserialize user messages successfully)
    for _ in 0..10 {
        match user_consumer.poll_message(Duration::from_secs(1)).await {
            Ok(typed_message) => {
                if typed_message.value().id == 100 {
                    user_received = true;
                    break;
                }
            }
            Err(_) => continue, // May fail to deserialize order messages, which is expected
        }
    }

    // Check order consumer (should only deserialize order messages successfully) 
    for _ in 0..10 {
        match order_consumer.poll_message(Duration::from_secs(1)).await {
            Ok(typed_message) => {
                if typed_message.value().order_id == "multi-order" {
                    order_received = true;
                    break;
                }
            }
            Err(_) => continue, // May fail to deserialize user messages, which is expected
        }
    }

    assert!(user_received, "User consumer did not receive user message");
    assert!(order_received, "Order consumer did not receive order message");
}

#[tokio::test] 
#[serial]
async fn test_send_to_specific_topic() {
    init_logger();
    if !is_kafka_running() { return; }

    let test_user = TestUser::new(200, "Topic Test", "topic@test.com");
    let custom_topic = "typed-custom-topic-test";

    let producer = TypedKafkaProducer::<TestUser, _>::new(
        "localhost:9092",
        TEST_TOPIC, // Default topic
        JsonSerializer,
    ).expect("Failed to create producer");

    // Send to custom topic (not the default)
    producer.send_to_topic(custom_topic, Some("topic-key"), &test_user, None).await
        .expect("Failed to send to custom topic");
    
    producer.flush(5000).expect("Failed to flush");

    // Consumer for custom topic
    let group_id = format!("custom-topic-test-{}", Uuid::new_v4());
    let consumer = TypedKafkaConsumer::<TestUser, _>::new(
        "localhost:9092",
        &group_id, 
        JsonSerializer
    );
    
    consumer.subscribe(&[custom_topic]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify message in custom topic
    let mut found = false;
    for _ in 0..20 {
        if let Ok(typed_message) = consumer.poll_message(Duration::from_secs(1)).await {
            if typed_message.value().id == 200 {
                found = true;
                break;
            }
        }
    }
    
    assert!(found, "Message not found in custom topic");
}

#[tokio::test]
#[serial]
async fn test_typed_message_methods() {
    init_logger();
    if !is_kafka_running() { return; }

    let test_user = TestUser::new(300, "Method Test", "method@test.com");
    let test_key = "method-test-key";

    let producer = TypedKafkaProducer::<TestUser, _>::new(
        "localhost:9092", TEST_TOPIC, JsonSerializer
    ).expect("Failed to create producer");

    producer.send(Some(test_key), &test_user, None).await
        .expect("Failed to send message");
    producer.flush(5000).expect("Failed to flush");

    let group_id = format!("method-test-{}", Uuid::new_v4());
    let consumer = TypedKafkaConsumer::<TestUser, _>::new(
        "localhost:9092", &group_id, JsonSerializer
    );
    
    consumer.subscribe(&[TEST_TOPIC]);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test TypedMessage methods
    for _ in 0..20 {
        if let Ok(typed_message) = consumer.poll_message(Duration::from_secs(1)).await {
            if typed_message.value().id == 300 {
                // Test reference methods
                assert_eq!(typed_message.value().name, "Method Test");
                assert_eq!(typed_message.key(), Some(test_key));
                
                // Test consumption methods
                let (user, key) = typed_message.into_parts();
                assert_eq!(user.id, 300);
                assert_eq!(user.name, "Method Test");
                assert_eq!(key, Some(test_key.to_string()));
                return; // Success
            }
        }
    }
    
    panic!("TypedMessage methods test failed - no matching message found");
}

#[tokio::test]
#[serial]
async fn test_error_scenarios() {
    init_logger();
    
    // Test with invalid broker (should fail gracefully)
    let result = TypedKafkaProducer::<TestUser, _>::new(
        "invalid-broker:9999",
        TEST_TOPIC,
        JsonSerializer,
    );
    
    // This should fail, but not panic
    assert!(result.is_err(), "Should fail with invalid broker");
    
    if !is_kafka_running() { return; }

    // Test timeout scenarios
    let group_id = format!("timeout-test-{}", Uuid::new_v4());
    let consumer = TypedKafkaConsumer::<TestUser, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer
    );
    
    consumer.subscribe(&[TEST_TOPIC]);
    
    // Should timeout quickly on empty topic
    let start = std::time::Instant::now();
    let result = consumer.poll_message(Duration::from_millis(100)).await;
    let elapsed = start.elapsed();
    
    // Should respect timeout
    assert!(elapsed < Duration::from_secs(2), "Timeout not respected");
    assert!(result.is_err(), "Should timeout with no messages");
}