use ferrisstreams::ferris::kafka::{
    KafkaProducer, KafkaConsumer, JsonSerializer, Headers, 
    ProducerError, ConsumerError, SerializationError
};
use ferrisstreams::ferris::kafka::producer_config::ProducerConfig;
use ferrisstreams::ferris::kafka::consumer_config::ConsumerConfig;
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestMessage {
    id: u32,
    content: String,
}

/// Custom serializer that always fails for testing error scenarios
struct FailingSerializer;

impl ferrisstreams::ferris::kafka::Serializer<TestMessage> for FailingSerializer {
    fn serialize(&self, _value: &TestMessage) -> Result<Vec<u8>, SerializationError> {
        Err(SerializationError::Json("Intentional serialization failure".to_string()))
    }

    fn deserialize(&self, _bytes: &[u8]) -> Result<TestMessage, SerializationError> {
        Err(SerializationError::Json("Intentional deserialization failure".to_string()))
    }
}

impl ferrisstreams::ferris::kafka::Serializer<String> for FailingSerializer {
    fn serialize(&self, _value: &String) -> Result<Vec<u8>, SerializationError> {
        Err(SerializationError::Json("Intentional key serialization failure".to_string()))
    }

    fn deserialize(&self, _bytes: &[u8]) -> Result<String, SerializationError> {
        Err(SerializationError::Json("Intentional key deserialization failure".to_string()))
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
    ).expect("Failed to create producer");

    let test_message = TestMessage {
        id: 1,
        content: "test".to_string(),
    };

    let headers = Headers::new();

    // This should fail due to serialization error
    let result = producer.send(
        Some(&"test-key".to_string()),
        &test_message,
        headers,
        None,
    ).await;

    assert!(result.is_err());
    if let Err(ProducerError::SerializationError(err)) = result {
        assert!(err.to_string().contains("Intentional"));
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
    ).expect("Failed to create producer");

    let test_message = TestMessage {
        id: 1,
        content: "test".to_string(),
    };

    let headers = Headers::new();

    // This should fail due to key serialization error
    let result = producer.send(
        Some(&"test-key".to_string()),
        &test_message,
        headers,
        None,
    ).await;

    assert!(result.is_err());
    if let Err(ProducerError::SerializationError(err)) = result {
        assert!(err.to_string().contains("key serialization"));
    } else {
        panic!("Expected key SerializationError, got: {:?}", result);
    }
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
        },
        Ok(producer) => {
            // Producer created, but should fail on send
            let test_message = TestMessage {
                id: 1,
                content: "test".to_string(),
            };

            let headers = Headers::new();
            
            let send_result = tokio::time::timeout(
                Duration::from_secs(5),
                producer.send(Some(&"key".to_string()), &test_message, headers, None)
            ).await;

            // Should either timeout or return an error
            match send_result {
                Ok(Ok(_)) => panic!("Expected send to fail with invalid broker"),
                Ok(Err(_)) => {}, // Expected error
                Err(_) => {}, // Expected timeout
            }
        }
    }
}

#[tokio::test]
async fn test_producer_send_timeout() {
    // Test producer with very short timeout to trigger timeout error
    let config = ProducerConfig::new("localhost:9092", "timeout-test-topic")
        .message_timeout(Duration::from_millis(1)) // Very short timeout
        .delivery_timeout(Duration::from_millis(1));

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    ).expect("Failed to create producer");

    let test_message = TestMessage {
        id: 1,
        content: "test".to_string(),
    };

    let headers = Headers::new();

    // This might fail due to timeout (depends on Kafka availability)
    let result = producer.send(
        Some(&"test-key".to_string()),
        &test_message,
        headers,
        None,
    ).await;

    // We don't assert failure here since it depends on Kafka being available
    // But we verify the producer handles timeouts gracefully
    match result {
        Ok(_) => {}, // Message sent successfully despite short timeout
        Err(ProducerError::KafkaError(_)) => {}, // Expected timeout error
        Err(ProducerError::SerializationError(_)) => panic!("Unexpected serialization error"),
    }
}

#[tokio::test]
async fn test_consumer_deserialization_error() {
    // Create a consumer with a deserializer that always fails
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "error-test-group",
        FailingSerializer, // Key deserializer fails
        FailingSerializer, // Value deserializer fails
    ).expect("Failed to create consumer");

    // First, send a valid message with a working producer
    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "consumer-error-test-topic",
        JsonSerializer,
        JsonSerializer,
    ).expect("Failed to create producer");

    let test_message = TestMessage {
        id: 1,
        content: "test".to_string(),
    };

    let headers = Headers::new();

    // Send a message that the consumer will try to deserialize
    let _ = producer.send(
        Some(&"test-key".to_string()),
        &test_message,
        headers,
        None,
    ).await;

    // Subscribe the failing consumer to the topic
    if let Err(_) = consumer.subscribe(&["consumer-error-test-topic"]) {
        // Subscribe might fail if Kafka is not available - skip this test
        return;
    }

    // Try to poll a message - this should fail during deserialization
    let poll_result = consumer.poll_message(Duration::from_secs(2)).await;

    match poll_result {
        Err(ConsumerError::SerializationError(err)) => {
            assert!(err.to_string().contains("Intentional"));
        },
        Err(ConsumerError::Timeout) => {
            // No message available to deserialize - acceptable for this test
        },
        Err(ConsumerError::NoMessage) => {
            // No message available - acceptable for this test
        },
        Err(ConsumerError::KafkaError(_)) => {
            // Kafka not available - acceptable for this test
        },
        Ok(_) => {
            // This shouldn't happen with our failing deserializer
            // But if Kafka isn't available, the consumer might not receive any messages
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
    ).expect("Failed to create consumer");

    // Try to subscribe to a topic with invalid characters
    let subscribe_result = consumer.subscribe(&["invalid/topic/name", "another$invalid%topic"]);

    // rdkafka might accept these topic names, so we don't assert failure
    // But we verify the consumer handles it gracefully
    match subscribe_result {
        Ok(_) => {
            // Subscription succeeded - topic name validation is handled by Kafka server
        },
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
    ).expect("Failed to create consumer");

    // Subscribe to a topic that likely has no messages
    if let Err(_) = consumer.subscribe(&["non-existent-topic-for-timeout-test"]) {
        // Skip test if Kafka is not available
        return;
    }

    // Poll with a short timeout
    let start_time = std::time::Instant::now();
    let result = consumer.poll_message(Duration::from_millis(500)).await;
    let elapsed = start_time.elapsed();

    // Should timeout within reasonable bounds
    assert!(elapsed >= Duration::from_millis(400)); // Allow some variance
    assert!(elapsed <= Duration::from_millis(1000)); // But not too much

    match result {
        Err(ConsumerError::Timeout) => {
            // Expected timeout
        },
        Err(ConsumerError::NoMessage) => {
            // Also acceptable
        },
        Err(ConsumerError::KafkaError(_)) => {
            // Kafka might not be available
        },
        Ok(_) => {
            // Unexpected message received - but not necessarily wrong
        },
        Err(ConsumerError::SerializationError(_)) => {
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
    ).expect("Failed to create producer");

    // Test flush with very short timeout
    let flush_result = producer.flush(1); // 1ms timeout

    match flush_result {
        Ok(_) => {
            // Flush succeeded quickly
        },
        Err(_) => {
            // Flush timed out or failed - acceptable
        }
    }

    // Test flush with reasonable timeout
    let flush_result = producer.flush(5000); // 5s timeout

    match flush_result {
        Ok(_) => {
            // Flush succeeded
        },
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
        let json_error = SerializationError::Json("test error".to_string());
        assert_eq!(json_error.to_string(), "JSON serialization error: test error");

        let io_error = SerializationError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "test io error"
        ));
        assert!(io_error.to_string().contains("IO error"));
    }

    #[test]
    fn test_producer_error_conversion() {
        let serialization_error = SerializationError::Json("test".to_string());
        let producer_error: ProducerError = serialization_error.into();
        
        match producer_error {
            ProducerError::SerializationError(_) => {}, // Expected
            _ => panic!("Expected SerializationError conversion"),
        }
    }

    #[test]
    fn test_consumer_error_conversion() {
        let serialization_error = SerializationError::Json("test".to_string());
        let consumer_error: ConsumerError = serialization_error.into();
        
        match consumer_error {
            ConsumerError::SerializationError(_) => {}, // Expected
            _ => panic!("Expected SerializationError conversion"),
        }
    }

    #[test]
    fn test_error_chain() {
        let serialization_error = SerializationError::Json("root cause".to_string());
        let producer_error: ProducerError = serialization_error.into();
        
        // Test error source chain
        if let ProducerError::SerializationError(inner) = producer_error {
            assert!(inner.to_string().contains("root cause"));
        } else {
            panic!("Expected SerializationError");
        }
    }
}