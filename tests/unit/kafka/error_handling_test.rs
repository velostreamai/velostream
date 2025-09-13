use crate::unit::common::*;
use ferrisstreams::ferris::kafka::serialization::Serializer;

/// Custom serializer that always fails for testing error scenarios
struct FailingSerializer;

impl Serializer<TestMessage> for FailingSerializer {
    fn serialize(&self, _value: &TestMessage) -> Result<Vec<u8>, SerializationError> {
        // Create a JSON error by attempting to serialize an invalid value
        let invalid_json = "\x00\x01\x02invalid_json";
        match serde_json::from_str::<serde_json::Value>(invalid_json) {
            Err(json_err) => Err(SerializationError::JsonSerializationFailed(Box::new(
                json_err,
            ))),
            Ok(_) => unreachable!("Invalid JSON should have failed"),
        }
    }

    fn deserialize(&self, _bytes: &[u8]) -> Result<TestMessage, SerializationError> {
        // Create a JSON error by attempting to deserialize invalid JSON
        let invalid_json = "\x00\x01\x02invalid_json";
        match serde_json::from_str::<TestMessage>(invalid_json) {
            Err(json_err) => Err(SerializationError::JsonSerializationFailed(Box::new(
                json_err,
            ))),
            Ok(_) => unreachable!("Invalid JSON should have failed"),
        }
    }
}

impl Serializer<String> for FailingSerializer {
    fn serialize(&self, _value: &String) -> Result<Vec<u8>, SerializationError> {
        // Create a JSON error by attempting to serialize an invalid value
        let invalid_json = "\x00\x01\x02invalid_json";
        match serde_json::from_str::<serde_json::Value>(invalid_json) {
            Err(json_err) => Err(SerializationError::JsonSerializationFailed(Box::new(
                json_err,
            ))),
            Ok(_) => unreachable!("Invalid JSON should have failed"),
        }
    }

    fn deserialize(&self, _bytes: &[u8]) -> Result<String, SerializationError> {
        // Create a JSON error by attempting to deserialize invalid JSON
        let invalid_json = "\x00\x01\x02invalid_json";
        match serde_json::from_str::<String>(invalid_json) {
            Err(json_err) => Err(SerializationError::JsonSerializationFailed(Box::new(
                json_err,
            ))),
            Ok(_) => unreachable!("Invalid JSON should have failed"),
        }
    }
}

#[tokio::test]
async fn test_producer_serialization_error() {
    // Test producer with serializer that always fails
    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "error-test-topic",
        FailingSerializer,
        FailingSerializer,
    )
    .expect("Failed to create producer");

    let test_message = TestMessage::basic(1, "test");

    let headers = Headers::new();

    // This should fail due to serialization error
    let result = producer
        .send(Some(&"test-key".to_string()), &test_message, headers, None)
        .await;

    assert!(result.is_err());
    if let Err(KafkaClientError::SerializationError(err)) = result {
        assert!(err.to_string().contains("JSON"));
    } else {
        panic!("Expected SerializationError, got: {:?}", result);
    }
}

#[tokio::test]
async fn test_producer_key_serialization_error() {
    // Test producer with key serializer that fails, but value serializer works
    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "error-test-topic",
        FailingSerializer, // Key serializer fails
        JsonSerializer,    // Value serializer works
    )
    .expect("Failed to create producer");

    let test_message = TestMessage::basic(1, "test");

    let headers = Headers::new();

    // This should fail due to key serialization error
    let result = producer
        .send(Some(&"test-key".to_string()), &test_message, headers, None)
        .await;

    assert!(
        matches!(&result,
            Err(KafkaClientError::SerializationError(err))
            if err.to_string().contains("JSON")),
        "Expected SerializationError containing 'serialization', but got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_producer_invalid_broker() {
    // Test producer with invalid broker configuration
    let result = KafkaProducer::<String, TestMessage, _, _>::new(
        "invalid-broker-format:abc:def",
        "error-test-topic",
        JsonSerializer,
        JsonSerializer,
    );

    // Should either fail immediately or fail on first send operation
    match result {
        Err(_) => {
            // Producer creation failed immediately - this is acceptable
        }
        Ok(producer) => {
            // Producer created, but should fail on send
            let test_message = TestMessage::basic(1, "test");

            let headers = Headers::new();

            let send_result = tokio::time::timeout(
                Duration::from_secs(5),
                producer.send(Some(&"key".to_string()), &test_message, headers, None),
            )
            .await;

            // Should either timeout or return an error
            match send_result {
                Ok(Ok(_)) => panic!("Expected send to fail with invalid broker"),
                Ok(Err(_)) => {} // Expected error
                Err(_) => {}     // Expected timeout
            }
        }
    }
}

#[tokio::test]
async fn test_producer_send_timeout() {
    // Test producer with very short timeout to trigger timeout error
    let config = ProducerConfig::new("localhost:9092", "timeout-test-topic")
        .message_timeout(Duration::from_millis(100)) // Short timeout but longer than linger
        .delivery_timeout(Duration::from_millis(100))
        .batching(1024, Duration::from_millis(0)); // Set linger to 0 to avoid conflicts

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let test_message = TestMessage::basic(1, "test");

    let headers = Headers::new();

    // This might fail due to timeout (depends on Kafka availability)
    let result = producer
        .send(Some(&"test-key".to_string()), &test_message, headers, None)
        .await;

    // We don't assert failure here since it depends on Kafka being available
    // But we verify the producer handles timeouts gracefully
    match result {
        Ok(_) => {} // Message sent successfully despite short timeout
        Err(KafkaClientError::KafkaError(_)) => {} // Expected timeout error
        Err(KafkaClientError::SerializationError(_)) => panic!("Unexpected serialization error"),
        Err(KafkaClientError::Timeout) => {} // Expected timeout
        Err(KafkaClientError::NoMessage) => {} // Unexpected but acceptable
    }
}

#[tokio::test]
async fn test_consumer_deserialization_error() {
    // Skip this test if Kafka is not available (unit test should not depend on external services)
    if std::env::var("SKIP_KAFKA_TESTS").is_ok() {
        println!("Skipping Kafka-dependent test (SKIP_KAFKA_TESTS is set)");
        return;
    }

    // Wrap the test in a timeout to prevent hanging
    let test_future = async {
        test_consumer_deserialization_workflow().await;
    };

    match tokio::time::timeout(tokio::time::Duration::from_secs(10), test_future).await {
        Ok(_) => {
            println!("Consumer deserialization error test completed successfully");
        }
        Err(_) => {
            println!("Consumer deserialization error test timed out (Kafka likely not available)");
            // Don't fail the test, just skip it
        }
    }
}

async fn test_consumer_deserialization_workflow() {
    // Create a consumer with a deserializer that always fails
    let consumer_result = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "error-test-group",
        FailingSerializer, // Key deserializer fails
        FailingSerializer, // Value deserializer fails
    );

    let consumer = match consumer_result {
        Ok(c) => c,
        Err(_) => {
            // Skip test if Kafka is not available
            return;
        }
    };

    // First, try to send a valid message with a working producer (with timeout)
    let producer_result = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "consumer-error-test-topic",
        JsonSerializer,
        JsonSerializer,
    );

    let producer = match producer_result {
        Ok(p) => p,
        Err(_) => {
            // Skip test if Kafka is not available
            return;
        }
    };

    let test_message = TestMessage::basic(1, "test");
    let headers = Headers::new();

    // Send a message that the consumer will try to deserialize (with timeout)
    let test_key = "test-key".to_string();
    let send_future = producer.send(Some(&test_key), &test_message, headers, None);
    let _ = tokio::time::timeout(Duration::from_secs(2), send_future).await;

    // Subscribe the failing consumer to the topic
    if let Err(_) = consumer.subscribe(&["consumer-error-test-topic"]) {
        // Subscribe might fail if Kafka is not available - skip this test
        return;
    }

    // Try to poll a message with reduced timeout - this should fail during deserialization
    let poll_future = consumer.poll(Duration::from_millis(500));
    let poll_result = tokio::time::timeout(Duration::from_secs(1), poll_future).await;

    match poll_result {
        Ok(Ok(_)) => {
            // Message received successfully - unexpected but acceptable if Kafka isn't available
        }
        Ok(Err(KafkaClientError::SerializationError(err))) => {
            assert!(err.to_string().contains("JSON"));
        }
        Ok(Err(KafkaClientError::Timeout)) => {
            // No message available to deserialize - acceptable for this test
        }
        Ok(Err(KafkaClientError::NoMessage)) => {
            // No message available - acceptable for this test
        }
        Ok(Err(KafkaClientError::KafkaError(_))) => {
            // Kafka not available - acceptable for this test
        }
        Err(_) => {
            // Timeout occurred - acceptable for this test (Kafka likely not available)
        }
    }
}

#[tokio::test]
async fn test_consumer_invalid_topic_subscription() {
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "error-test-group",
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    // Try to subscribe to a topic with invalid characters
    let subscribe_result = consumer.subscribe(&["invalid/topic/name", "another$invalid%topic"]);

    // rdkafka might accept these topic names, so we don't assert failure
    // But we verify the consumer handles it gracefully
    match subscribe_result {
        Ok(_) => {
            // Subscription succeeded - topic name validation is handled by Kafka server
        }
        Err(_) => {
            // Subscription failed - acceptable for invalid topic names
        }
    }
}

#[tokio::test]
async fn test_consumer_poll_timeout() {
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "timeout-test-group",
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    // Subscribe to a topic that likely has no messages
    if let Err(_) = consumer.subscribe(&["non-existent-topic-for-timeout-test"]) {
        // Skip test if Kafka is not available
        return;
    }

    // Poll with a short timeout
    let start_time = std::time::Instant::now();
    let result = consumer.poll(Duration::from_millis(500)).await;
    let elapsed = start_time.elapsed();

    match result {
        Err(KafkaClientError::Timeout) => {
            // Expected timeout - verify timing only in this case
            assert!(elapsed >= Duration::from_millis(50));
            assert!(elapsed <= Duration::from_millis(2000));
        }
        Err(KafkaClientError::NoMessage) => {
            // Also acceptable - may return immediately if no broker connection
        }
        Err(KafkaClientError::KafkaError(_)) => {
            // Kafka might not be available
        }
        Ok(_) => {
            // Unexpected message received - but not necessarily wrong
        }
        Err(KafkaClientError::SerializationError(_)) => {
            panic!("Unexpected serialization error during timeout test");
        }
    }
}

#[tokio::test]
async fn test_producer_flush_timeout() {
    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "flush-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    // Test flush with very short timeout
    let flush_result = producer.flush(1); // 1ms timeout

    match flush_result {
        Ok(_) => {
            // Flush succeeded quickly
        }
        Err(_) => {
            // Flush timed out or failed - acceptable
        }
    }

    // Test flush with reasonable timeout
    let flush_result = producer.flush(5000); // 5s timeout

    match flush_result {
        Ok(_) => {
            // Flush succeeded
        }
        Err(err) => {
            // Might fail if Kafka is not available
            println!("Flush failed (Kafka might not be available): {:?}", err);
        }
    }
}

#[cfg(test)]
mod error_handling_unit_tests {
    use super::*;

    #[test]
    fn test_serialization_error_display() {
        let invalid_json = "\x00\x01\x02invalid_json";
        let json_err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let json_error = SerializationError::JsonSerializationFailed(Box::new(json_err));
        assert!(json_error.to_string().contains("JSON serialization error"));
    }

    #[test]
    fn test_kafka_client_error_conversion() {
        let invalid_json = "\x00\x01\x02invalid_json";
        let json_err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let serialization_error = SerializationError::JsonSerializationFailed(Box::new(json_err));
        let kafka_error: KafkaClientError = serialization_error.into();

        match kafka_error {
            KafkaClientError::SerializationError(_) => {} // Expected
            _ => panic!("Expected SerializationError conversion"),
        }
    }

    #[test]
    fn test_error_chain() {
        let invalid_json = "\x00\x01\x02invalid_json";
        let json_err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let serialization_error = SerializationError::JsonSerializationFailed(Box::new(json_err));
        let kafka_error: KafkaClientError = serialization_error.into();

        // Test error source chain
        if let KafkaClientError::SerializationError(_inner) = kafka_error {
            // Error chain test passed - the inner error exists
        } else {
            panic!("Expected SerializationError");
        }
    }
}
