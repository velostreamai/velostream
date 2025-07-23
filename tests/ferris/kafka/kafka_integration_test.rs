use crate::ferris::kafka::test_utils::is_kafka_running;
use ferrisstreams::{
    KafkaProducer, KafkaConsumer, ProducerBuilder, ConsumerBuilder,
    JsonSerializer, KafkaConsumable, Serializer
};
use futures::StreamExt;
use serde::{Serialize, Deserialize};
use serial_test::serial;
use std::time::Duration;
use uuid::Uuid;
use chrono::Utc;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestMessage {
    id: u32,
    content: String,
    timestamp: Option<String>,
}

impl TestMessage {
    fn new(id: u32, content: &str) -> Self {
        Self {
            id,
            content: content.to_string(),
            timestamp: Some(Utc::now().to_rfc3339()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct SimpleMessage {
    content: String,
}

// Core Integration Tests

#[tokio::test]
#[serial]
async fn test_basic_producer_consumer() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-basic-{}", Uuid::new_v4());
    let group_id = format!("basic-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(1, "Basic integration test");

    // Test producer
    producer.send(Some("test-key"), &test_message, None).await
        .expect("Failed to send message");
    
    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test consumer
    match consumer.poll_message(Duration::from_secs(3)).await {
        Ok(message) => {
            assert_eq!(*message.value(), test_message);
            assert_eq!(message.key(), Some("test-key"));
        }
        Err(e) => panic!("Failed to receive message: {:?}", e),
    }

    consumer.commit().expect("Failed to commit");
}

#[tokio::test]
#[serial]
async fn test_multiple_messages() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-multi-{}", Uuid::new_v4());
    let group_id = format!("multi-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send multiple messages
    let messages = vec![
        TestMessage::new(1, "Message 1"),
        TestMessage::new(2, "Message 2"), 
        TestMessage::new(3, "Message 3"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("key-{}", i + 1);
        producer.send(Some(&key), message, None).await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Receive messages using stream with implicit deserialization
    let mut stream = consumer.stream();
    let mut received = Vec::new();
    let timeout_duration = Duration::from_secs(10);
    let start_time = std::time::Instant::now();

    while received.len() < messages.len() && start_time.elapsed() < timeout_duration {
        if let Some(message_result) = stream.next().await {
            match message_result {
                Ok(test_msg) => {
                    // Message is already deserialized!
                    received.push(test_msg);
                }
                Err(e) => {
                    eprintln!("Stream error: {:?}", e);
                    break;
                }
            }
        } else {
            break;
        }
    }

    consumer.commit().expect("Failed to commit");
    assert_eq!(received.len(), messages.len());
    
    for message in messages {
        assert!(received.contains(&message));
    }
}

#[tokio::test]
#[serial]
async fn test_different_message_types() {
    if !is_kafka_running() { return; }

    let test_topic = format!("integration-test-type-{}", Uuid::new_v4());
    let simple_topic = format!("integration-simple-type-{}", Uuid::new_v4());
    let group_id = format!("types-group-{}", Uuid::new_v4());

    let test_producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &test_topic, JsonSerializer)
        .expect("Failed to create test producer");
    
    let simple_producer = KafkaProducer::<SimpleMessage, _>::new("localhost:9092", &simple_topic, JsonSerializer)
        .expect("Failed to create simple producer");

    let test_consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create test consumer");
    
    let simple_consumer = KafkaConsumer::<SimpleMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create simple consumer");

    test_consumer.subscribe(&[&test_topic]).expect("Failed to subscribe to test topic");
    simple_consumer.subscribe(&[&simple_topic]).expect("Failed to subscribe to simple topic");

    let test_message = TestMessage::new(100, "Type test message");
    let simple_message = SimpleMessage { content: "Simple type test".to_string() };

    test_producer.send(Some("test-key"), &test_message, None).await
        .expect("Failed to send test message");
    
    simple_producer.send(Some("simple-key"), &simple_message, None).await
        .expect("Failed to send simple message");

    test_producer.flush(5000).expect("Failed to flush test producer");
    simple_producer.flush(5000).expect("Failed to flush simple producer");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut test_received = false;
    let mut simple_received = false;

    for _ in 0..10 {
        if let Ok(message) = test_consumer.poll_message(Duration::from_millis(500)).await {
            if message.value().id == 100 {
                test_received = true;
            }
        }
        
        if let Ok(message) = simple_consumer.poll_message(Duration::from_millis(500)).await {
            if message.value().content == "Simple type test" {
                simple_received = true;
            }
        }

        if test_received && simple_received {
            break;
        }
    }

    test_consumer.commit().expect("Failed to commit test consumer");
    simple_consumer.commit().expect("Failed to commit simple consumer");

    assert!(test_received && simple_received);
}

#[tokio::test]
#[serial]
async fn test_builder_pattern() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-builder-{}", Uuid::new_v4());
    let group_id = format!("builder-group-{}", Uuid::new_v4());

    // Test builder pattern
    let producer = ProducerBuilder::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .build()
        .expect("Failed to build producer");

    let consumer = ConsumerBuilder::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .build()
        .expect("Failed to build consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(99, "Builder pattern test");

    producer.send(Some("builder-key"), &test_message, None).await
        .expect("Failed to send message");

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    match consumer.poll_message(Duration::from_secs(3)).await {
        Ok(message) => {
            assert_eq!(*message.value(), test_message);
        }
        Err(e) => panic!("Failed to receive builder message: {:?}", e),
    }

    consumer.commit().expect("Failed to commit");
}

#[tokio::test]
#[serial]
async fn test_convenience_trait() {
    if !is_kafka_running() { return; }

    let group_id = format!("trait-group-{}", Uuid::new_v4());
    
    // Test KafkaConsumable trait
    let consumer = TestMessage::consumer("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer via trait");

    let trait_topic = format!("trait-test-topic-{}", Uuid::new_v4());
    let result = consumer.subscribe(&[&trait_topic]);
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
async fn test_serialization_roundtrip() {
    // Test JSON serialization without Kafka
    let serializer = JsonSerializer;
    
    let original = TestMessage::new(42, "Serialization test");
    let bytes = serializer.serialize(&original).expect("Failed to serialize");
    let deserialized: TestMessage = serializer.deserialize(&bytes).expect("Failed to deserialize");
    
    assert_eq!(original, deserialized);

    // Test with None timestamp
    let original_none = TestMessage { id: 43, content: "None test".to_string(), timestamp: None };
    let bytes = serializer.serialize(&original_none).expect("Failed to serialize none");
    let deserialized_none: TestMessage = serializer.deserialize(&bytes).expect("Failed to deserialize none");
    
    assert_eq!(original_none, deserialized_none);
}

#[tokio::test]
#[serial] 
async fn test_error_handling() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-errors-{}", Uuid::new_v4());
    let group_id = format!("error-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(999, "Error handling test");

    // Test successful operations
    producer.send(Some("error-key"), &test_message, None).await
        .expect("Send should succeed");

    producer.flush(5000).expect("Flush should succeed");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test timeout behavior
    let empty_consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &format!("empty-{}", Uuid::new_v4()), JsonSerializer)
        .expect("Failed to create empty consumer");
    
    let empty_topic = format!("empty-topic-{}", Uuid::new_v4());
    empty_consumer.subscribe(&[&empty_topic]).expect("Failed to subscribe to empty topic");

    let result = empty_consumer.poll_message(Duration::from_millis(100)).await;
    assert!(result.is_err(), "Should timeout on empty topic");

    consumer.commit().expect("Final commit should succeed");
}

#[tokio::test]
#[serial]
async fn test_message_with_timestamp() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-timestamp-{}", Uuid::new_v4());
    let group_id = format!("timestamp-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let current_time = Utc::now().timestamp_millis();
    let test_message = TestMessage::new(123, "Timestamp test message");

    // Send with Kafka timestamp
    producer.send(Some("timestamp-key"), &test_message, Some(current_time)).await
        .expect("Failed to send timestamped message");

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    match consumer.poll_message(Duration::from_secs(3)).await {
        Ok(message) => {
            assert_eq!(*message.value(), test_message);
            // Message should contain the timestamp in the content
            assert!(message.value().timestamp.is_some());
        }
        Err(e) => panic!("Failed to receive timestamped message: {:?}", e),
    }

    consumer.commit().expect("Failed to commit");
}

#[tokio::test]
#[serial]
async fn test_performance_comparison() {
    if !is_kafka_running() { return; }

    let topic_prefix = "perf-test";

    // Test direct constructor performance
    let start = std::time::Instant::now();
    for i in 0..5 {
        let _producer = KafkaProducer::<TestMessage, _>::new(
            "localhost:9092",
            &format!("{}-direct-{}", topic_prefix, i),
            JsonSerializer,
        ).expect("Failed to create direct producer");
    }
    let direct_duration = start.elapsed();

    // Test builder performance
    let start = std::time::Instant::now();
    for i in 0..5 {
        let _producer = ProducerBuilder::<TestMessage, _>::new(
            "localhost:9092",
            &format!("{}-builder-{}", topic_prefix, i),
            JsonSerializer,
        )
        .build()
        .expect("Failed to build producer");
    }
    let builder_duration = start.elapsed();

    // Both should be reasonably fast
    assert!(direct_duration < Duration::from_secs(1));
    assert!(builder_duration < Duration::from_secs(2));
    
    println!("Direct: {:?}, Builder: {:?}, Overhead: {:?}", 
        direct_duration, builder_duration, builder_duration.saturating_sub(direct_duration));
}

#[tokio::test]
#[serial]
async fn test_consumer_stream() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-stream-{}", Uuid::new_v4());
    let group_id = format!("stream-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send a few test messages
    let messages = vec![
        TestMessage::new(1, "Stream message 1"),
        TestMessage::new(2, "Stream message 2"), 
        TestMessage::new(3, "Stream message 3"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("stream-key-{}", i + 1);
        producer.send(Some(&key), message, None).await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test the stream functionality with implicit deserialization
    let mut stream = consumer.stream();
    let mut received_messages = Vec::new();
    
    // Use timeout to avoid hanging if no messages arrive
    let timeout_duration = Duration::from_secs(5);
    let start_time = std::time::Instant::now();

    while received_messages.len() < messages.len() && start_time.elapsed() < timeout_duration {
        if let Some(message_result) = stream.next().await {
            match message_result {
                Ok(test_msg) => {
                    // Message is already deserialized!
                    received_messages.push(test_msg);
                }
                Err(e) => {
                    eprintln!("Stream error: {:?}", e);
                    break;
                }
            }
        } else {
            // Stream ended
            break;
        }
    }

    consumer.commit().expect("Failed to commit");
    
    assert!(!received_messages.is_empty(), "Should receive messages via stream");
    assert!(received_messages.len() <= messages.len(), "Should not receive more messages than sent");
    
    // Verify at least some of our messages were received
    for received in &received_messages {
        assert!(messages.contains(received), "Received message should match one of the sent messages");
    }
}

#[tokio::test]
#[serial]
async fn test_fluent_consumer_style() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-fluent-{}", Uuid::new_v4());
    let group_id = format!("fluent-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send test messages
    let messages = vec![
        TestMessage::new(1, "Fluent message 1"),
        TestMessage::new(2, "Fluent message 2"), 
        TestMessage::new(3, "Fluent message 3"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("fluent-key-{}", i + 1);
        producer.send(Some(&key), message, None).await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Demonstrate fluent-style processing with implicit deserialization
    let result = consumer.stream()
        .take(messages.len())  // Take only as many as we sent
        .filter_map(|result| async move {
            // Automatic deserialization - much simpler!
            result.ok()
        })
        .collect::<Vec<TestMessage>>()
        .await;

    consumer.commit().expect("Failed to commit");
    
    assert!(!result.is_empty(), "Should receive messages via fluent style");
    assert!(result.len() <= messages.len(), "Should not receive more messages than sent");
    
    // Verify messages match
    for received in &result {
        assert!(messages.contains(received), "Received message should match sent messages");
    }
}

#[tokio::test]
#[serial]
async fn test_fluent_api_patterns() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-fluent-patterns-{}", Uuid::new_v4());
    let group_id = format!("fluent-patterns-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send test messages
    let messages = vec![
        TestMessage::new(1, "Pattern message 1"),
        TestMessage::new(2, "Pattern message 2"), 
        TestMessage::new(3, "Pattern message 3"),
        TestMessage::new(4, "Pattern message 4"),
        TestMessage::new(5, "Pattern message 5"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("pattern-key-{}", i + 1);
        producer.send(Some(&key), message, None).await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pattern 1: Collect all messages with implicit deserialization
    let collected_messages = consumer.stream()
        .take(messages.len())
        .filter_map(|result| async move {
            // Automatic deserialization - much cleaner!
            result.ok()
        })
        .collect::<Vec<TestMessage>>()
        .await;

    assert_eq!(collected_messages.len(), messages.len(), "Should collect all messages");

    // Pattern 2: Take only first N messages using take()
    let consumer2 = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &format!("{}-2", group_id), JsonSerializer)
        .expect("Failed to create consumer2");
    consumer2.subscribe(&[&topic]).expect("Failed to subscribe");
    
    let first_three = consumer2.stream()
        .take(3) // Only take first 3 messages
        .filter_map(|result| async move {
            // Implicit deserialization
            result.ok()
        })
        .collect::<Vec<TestMessage>>()
        .await;

    assert_eq!(first_three.len(), 3, "Should take exactly 3 messages");

    // Pattern 3: Filter by content and map to different type
    let consumer3 = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &format!("{}-3", group_id), JsonSerializer)
        .expect("Failed to create consumer3");
    consumer3.subscribe(&[&topic]).expect("Failed to subscribe");

    let filtered_ids = consumer3.stream()
        .take(messages.len())
        .filter_map(|result| async move {
            // Implicit deserialization
            result.ok()
        })
        .filter(|message: &TestMessage| {
            // Filter messages with even IDs 
            futures::future::ready(message.id % 2 == 0)
        })
        .map(|message| message.id) // Map to just the ID
        .collect::<Vec<u32>>()
        .await;

    assert!(!filtered_ids.is_empty(), "Should have filtered some messages");
    
    // Verify all IDs are even
    for id in &filtered_ids {
        assert_eq!(id % 2, 0, "All filtered IDs should be even");
    }

    consumer.commit().expect("Failed to commit");
    consumer2.commit().expect("Failed to commit");
    consumer3.commit().expect("Failed to commit");
}

#[tokio::test]
#[serial]
async fn test_implicit_deserialization() {
    if !is_kafka_running() { return; }

    let topic = format!("integration-implicit-{}", Uuid::new_v4());
    let group_id = format!("implicit-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<TestMessage, _>::new("localhost:9092", &topic, JsonSerializer)
        .expect("Failed to create producer");
    
    let consumer = KafkaConsumer::<TestMessage, _>::new("localhost:9092", &group_id, JsonSerializer)
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send test messages
    let messages = vec![
        TestMessage::new(1, "Implicit test 1"),
        TestMessage::new(2, "Implicit test 2"), 
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("implicit-key-{}", i + 1);
        producer.send(Some(&key), message, None).await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The new API is beautifully simple - no manual deserialization needed!
    let received: Vec<TestMessage> = consumer.stream()
        .take(messages.len())
        .filter_map(|result| async move { result.ok() }) // Just extract successful results
        .collect()
        .await;

    consumer.commit().expect("Failed to commit");
    
    assert_eq!(received.len(), messages.len(), "Should receive all messages");
    for message in &messages {
        assert!(received.contains(message), "Should contain sent message");
    }
}