use super::*; // Use the re-exported items from integration::mod
use futures::StreamExt;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct SimpleMessage {
    content: String,
}

// Core Integration Tests

#[tokio::test]
#[serial]
async fn test_basic_producer_consumer() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-basic-{}", Uuid::new_v4());
    let group_id = format!("basic-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(1, "Basic integration test");

    // Test producer
    producer
        .send(
            Some(&"test-key".to_string()),
            &test_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send message");

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test consumer
    match consumer.poll(Duration::from_secs(3)).await {
        Ok(message) => {
            assert_eq!(*message.value(), test_message);
            assert_eq!(message.key(), Some(&"test-key".to_string()));
        }
        Err(e) => panic!("Failed to receive message: {:?}", e),
    }

    consumer.commit().expect("Failed to commit");
}

#[tokio::test]
#[serial]
async fn test_multiple_messages() {
    if !is_kafka_running() {
        return;
    }

    // Add delay for CI environment to reduce resource contention
    tokio::time::sleep(Duration::from_secs(2)).await;

    let topic = format!("integration-multi-{}", Uuid::new_v4());
    let group_id = format!("multi-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
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
        producer
            .send(Some(&key), message, Headers::new(), None)
            .await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Receive messages using stream with implicit deserialization
    let mut stream = consumer.stream();
    let mut received = Vec::new();
    let timeout_duration = Duration::from_secs(20);
    let start_time = std::time::Instant::now();

    while received.len() < messages.len() && start_time.elapsed() < timeout_duration {
        if let Some(message_result) = stream.next().await {
            match message_result {
                Ok(message) => {
                    // Message is already deserialized! Extract the value
                    received.push(message.into_value());
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
    if !is_kafka_running() {
        return;
    }

    let test_topic = format!("integration-test-type-{}", Uuid::new_v4());
    let simple_topic = format!("integration-simple-type-{}", Uuid::new_v4());
    let group_id = format!("types-group-{}", Uuid::new_v4());

    let test_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &test_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create test producer");

    let simple_producer = KafkaProducer::<String, SimpleMessage, _, _>::new(
        "localhost:9092",
        &simple_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create simple producer");

    let test_consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create test consumer");

    let simple_consumer = KafkaConsumer::<String, SimpleMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create simple consumer");

    test_consumer
        .subscribe(&[&test_topic])
        .expect("Failed to subscribe to test topic");
    simple_consumer
        .subscribe(&[&simple_topic])
        .expect("Failed to subscribe to simple topic");

    let test_message = TestMessage::new(100, "Type test message");
    let simple_message = SimpleMessage {
        content: "Simple type test".to_string(),
    };

    test_producer
        .send(
            Some(&"test-key".to_string()),
            &test_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send test message");

    simple_producer
        .send(
            Some(&"simple-key".to_string()),
            &simple_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send simple message");

    test_producer
        .flush(5000)
        .expect("Failed to flush test producer");
    simple_producer
        .flush(5000)
        .expect("Failed to flush simple producer");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut test_received = false;
    let mut simple_received = false;

    for _ in 0..10 {
        if let Ok(message) = test_consumer.poll(Duration::from_millis(500)).await {
            if message.value().id == 100 {
                test_received = true;
            }
        }

        if let Ok(message) = simple_consumer.poll(Duration::from_millis(500)).await {
            if message.value().content == "Simple type test" {
                simple_received = true;
            }
        }

        if test_received && simple_received {
            break;
        }
    }

    test_consumer
        .commit()
        .expect("Failed to commit test consumer");
    simple_consumer
        .commit()
        .expect("Failed to commit simple consumer");

    assert!(test_received && simple_received);
}

#[tokio::test]
#[serial]
async fn test_builder_pattern() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-builder-{}", Uuid::new_v4());
    let group_id = format!("builder-group-{}", Uuid::new_v4());

    // Test builder pattern
    let producer = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .build()
    .expect("Failed to build producer");

    let consumer = ConsumerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .build()
    .expect("Failed to build consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(99, "Builder pattern test");

    producer
        .send(
            Some(&"builder-key".to_string()),
            &test_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send message");

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    match consumer.poll(Duration::from_secs(3)).await {
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
    if !is_kafka_running() {
        return;
    }

    let group_id = format!("trait-group-{}", Uuid::new_v4());

    // Test KafkaConsumable trait
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
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
    let bytes = serializer
        .serialize(&original)
        .expect("Failed to serialize");
    let deserialized: TestMessage = serializer
        .deserialize(&bytes)
        .expect("Failed to deserialize");

    assert_eq!(original, deserialized);

    // Test with None timestamp
    let original_none = TestMessage {
        id: 43,
        content: "None test".to_string(),
        timestamp: None,
    };
    let bytes = serializer
        .serialize(&original_none)
        .expect("Failed to serialize none");
    let deserialized_none: TestMessage = serializer
        .deserialize(&bytes)
        .expect("Failed to deserialize none");

    assert_eq!(original_none, deserialized_none);
}

#[tokio::test]
#[serial]
async fn test_error_handling() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-errors-{}", Uuid::new_v4());
    let group_id = format!("error-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(999, "Error handling test");

    // Test successful operations
    producer
        .send(
            Some(&"error-key".to_string()),
            &test_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Send should succeed");

    producer.flush(5000).expect("Flush should succeed");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test timeout behavior
    let empty_consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &format!("empty-{}", Uuid::new_v4()),
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create empty consumer");

    let empty_topic = format!("empty-topic-{}", Uuid::new_v4());
    empty_consumer
        .subscribe(&[&empty_topic])
        .expect("Failed to subscribe to empty topic");

    let result = empty_consumer.poll(Duration::from_millis(100)).await;
    assert!(result.is_err(), "Should timeout on empty topic");

    // Try to consume the message we sent to get an offset for committing
    if let Ok(_message) = consumer.poll(Duration::from_secs(2)).await {
        // Only commit if we successfully consumed a message
        let _ = consumer.commit(); // Make commit optional since we may not have consumed anything
    }
}

#[tokio::test]
#[serial]
async fn test_message_with_timestamp() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-timestamp-{}", Uuid::new_v4());
    let group_id = format!("timestamp-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let current_time = Utc::now().timestamp_millis();
    let test_message = TestMessage::new(123, "Timestamp test message");

    // Send with Kafka timestamp
    producer
        .send(
            Some(&"timestamp-key".to_string()),
            &test_message,
            Headers::new(),
            Some(current_time),
        )
        .await
        .expect("Failed to send timestamped message");

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    match consumer.poll(Duration::from_secs(3)).await {
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
    if !is_kafka_running() {
        return;
    }

    let topic_prefix = "perf-test";

    // Test direct constructor performance
    let start = std::time::Instant::now();
    for i in 0..5 {
        let _producer = KafkaProducer::<String, TestMessage, _, _>::new(
            "localhost:9092",
            &format!("{}-direct-{}", topic_prefix, i),
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create direct producer");
    }
    let direct_duration = start.elapsed();

    // Test builder performance
    let start = std::time::Instant::now();
    for i in 0..5 {
        let _producer = ProducerBuilder::<String, TestMessage, _, _>::new(
            "localhost:9092",
            &format!("{}-builder-{}", topic_prefix, i),
            JsonSerializer,
            JsonSerializer,
        )
        .build()
        .expect("Failed to build producer");
    }
    let builder_duration = start.elapsed();

    // Both should be reasonably fast
    assert!(direct_duration < Duration::from_secs(1));
    assert!(builder_duration < Duration::from_secs(2));

    println!(
        "Direct: {:?}, Builder: {:?}, Overhead: {:?}",
        direct_duration,
        builder_duration,
        builder_duration.saturating_sub(direct_duration)
    );
}

#[tokio::test]
#[serial]
async fn test_consumer_stream() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-stream-{}", Uuid::new_v4());
    let group_id = format!("stream-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send a few test messages
    let messages = [
        TestMessage::new(1, "Stream message 1"),
        TestMessage::new(2, "Stream message 2"),
        TestMessage::new(3, "Stream message 3"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("stream-key-{}", i + 1);
        producer
            .send(Some(&key), message, Headers::new(), None)
            .await
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
                Ok(message) => {
                    // Message is already deserialized! Extract the value
                    received_messages.push(message.into_value());
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

    assert!(
        !received_messages.is_empty(),
        "Should receive messages via stream"
    );
    assert!(
        received_messages.len() <= messages.len(),
        "Should not receive more messages than sent"
    );

    // Verify at least some of our messages were received
    for received in &received_messages {
        assert!(
            messages.contains(received),
            "Received message should match one of the sent messages"
        );
    }
}

#[tokio::test]
#[serial]
async fn test_fluent_consumer_style() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-fluent-{}", Uuid::new_v4());
    let group_id = format!("fluent-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send test messages
    let messages = [
        TestMessage::new(1, "Fluent message 1"),
        TestMessage::new(2, "Fluent message 2"),
        TestMessage::new(3, "Fluent message 3"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("fluent-key-{}", i + 1);
        producer
            .send(Some(&key), message, Headers::new(), None)
            .await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Demonstrate fluent-style processing with implicit deserialization
    let result = consumer
        .stream()
        .take(messages.len()) // Take only as many as we sent
        .filter_map(|result| async move {
            // Automatic deserialization - much simpler! Extract just the value
            result.ok().map(|message| message.into_value())
        })
        .collect::<Vec<TestMessage>>()
        .await;

    consumer.commit().expect("Failed to commit");

    assert!(
        !result.is_empty(),
        "Should receive messages via fluent style"
    );
    assert!(
        result.len() <= messages.len(),
        "Should not receive more messages than sent"
    );

    // Verify messages match
    for received in &result {
        assert!(
            messages.contains(received),
            "Received message should match sent messages"
        );
    }
}

#[tokio::test]
#[serial]
async fn test_fluent_api_patterns() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-fluent-patterns-{}", Uuid::new_v4());
    let group_id = format!("fluent-patterns-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
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
        producer
            .send(Some(&key), message, Headers::new(), None)
            .await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Pattern 1: Collect all messages with implicit deserialization
    let collected_messages = consumer
        .stream()
        .take(messages.len())
        .filter_map(|result| async move {
            // Automatic deserialization - much cleaner! Extract the value
            result.ok().map(|message| message.into_value())
        })
        .collect::<Vec<TestMessage>>()
        .await;

    assert_eq!(
        collected_messages.len(),
        messages.len(),
        "Should collect all messages"
    );

    // Pattern 2: Take only first N messages using take()
    let consumer2 = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &format!("{}-2", group_id),
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer2");
    consumer2.subscribe(&[&topic]).expect("Failed to subscribe");

    let first_three = consumer2
        .stream()
        .take(3) // Only take first 3 messages
        .filter_map(|result| async move {
            // Implicit deserialization - extract the value
            result.ok().map(|message| message.into_value())
        })
        .collect::<Vec<TestMessage>>()
        .await;

    assert_eq!(first_three.len(), 3, "Should take exactly 3 messages");

    // Pattern 3: Filter by content and map to different type
    let consumer3 = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &format!("{}-3", group_id),
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer3");
    consumer3.subscribe(&[&topic]).expect("Failed to subscribe");

    let filtered_ids = consumer3
        .stream()
        .take(messages.len())
        .filter_map(|result| async move {
            // Implicit deserialization - extract the value
            result.ok().map(|message| message.into_value())
        })
        .filter(|message: &TestMessage| {
            // Filter messages with even IDs
            futures::future::ready(message.id % 2 == 0)
        })
        .map(|message| message.id) // Map to just the ID
        .collect::<Vec<u32>>()
        .await;

    assert!(
        !filtered_ids.is_empty(),
        "Should have filtered some messages"
    );

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
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-implicit-{}", Uuid::new_v4());
    let group_id = format!("implicit-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send test messages
    let messages = vec![
        TestMessage::new(1, "Implicit test 1"),
        TestMessage::new(2, "Implicit test 2"),
    ];

    for (i, message) in messages.iter().enumerate() {
        let key = format!("implicit-key-{}", i + 1);
        producer
            .send(Some(&key), message, Headers::new(), None)
            .await
            .expect("Failed to send message");
    }

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The new API is beautifully simple - no manual deserialization needed!
    let received: Vec<TestMessage> = consumer
        .stream()
        .take(messages.len())
        .filter_map(|result| async move {
            // Just extract successful results and get the value
            result.ok().map(|message| message.into_value())
        })
        .collect()
        .await;

    consumer.commit().expect("Failed to commit");

    assert_eq!(
        received.len(),
        messages.len(),
        "Should receive all messages"
    );
    for message in &messages {
        assert!(received.contains(message), "Should contain sent message");
    }
}

#[tokio::test]
#[serial]
async fn test_headers_functionality() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("integration-headers-{}", Uuid::new_v4());
    let group_id = format!("headers-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let test_message = TestMessage::new(100, "Headers test message");

    // Create headers with various metadata
    let headers = Headers::new()
        .insert("source", "test-suite")
        .insert("version", "1.0.0")
        .insert("trace-id", "test-trace-123")
        .insert("content-type", "application/json");

    // Send message with headers
    producer
        .send(
            Some(&"headers-key".to_string()),
            &test_message,
            headers,
            None,
        )
        .await
        .expect("Failed to send message with headers");

    producer.flush(5000).expect("Failed to flush");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Consume and verify headers
    match consumer.poll(Duration::from_secs(3)).await {
        Ok(message) => {
            // Verify key and value
            assert_eq!(*message.value(), test_message);
            assert_eq!(message.key(), Some(&"headers-key".to_string()));

            // Verify headers
            let received_headers = message.headers();
            assert_eq!(received_headers.get("source"), Some("test-suite"));
            assert_eq!(received_headers.get("version"), Some("1.0.0"));
            assert_eq!(received_headers.get("trace-id"), Some("test-trace-123"));
            assert_eq!(
                received_headers.get("content-type"),
                Some("application/json")
            );

            // Verify header count
            assert_eq!(received_headers.len(), 4);
            assert!(!received_headers.is_empty());

            // Verify non-existent header
            assert_eq!(received_headers.get("non-existent"), None);
            assert!(received_headers.contains_key("source"));
            assert!(!received_headers.contains_key("non-existent"));
        }
        Err(e) => panic!("Failed to receive message with headers: {:?}", e),
    }

    consumer.commit().expect("Failed to commit");
}
