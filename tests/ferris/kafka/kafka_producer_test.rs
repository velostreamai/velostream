use std::thread;
use std::time::Duration;

/// This test class demonstrates how to use the KafkaProducer
/// 
/// IMPORTANT: These tests require a running Kafka instance.
/// You can start one using the provided docker-compose.yml:
/// ```
/// docker-compose up -d
/// ```
/// 
/// Wait for Kafka to start (about 60 seconds):
/// ```
/// ./test-kafka.sh
/// ```
/// 
/// To run these tests:
/// ```
/// cargo test -- --nocapture
/// ```
/// 
/// If Kafka is not running, the tests that require sending messages
/// will be skipped with a note indicating that Kafka is required.
#[cfg(test)]
mod kafka_producer_tests {
    use super::*;
    use crate::ferris::kafka::test_utils::{init};
    use chrono::{Local, Utc};
    use ferrisstreams::ferris::kafka::{KafkaProducer};
    use ferrisstreams::ferris::kafka::LoggingProducerContext;
    use logtest::Logger;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::ClientContext;
    use rdkafka::config::RDKafkaLogLevel;
    use rdkafka::message::DeliveryResult;
    use rdkafka::producer::ProducerContext;

    #[tokio::test]
    async fn test_kakfa_producer_new_with_context() {
        if !init() { return; }

        println!("Testing KafkaProducer creation with custom context...");

        // Create a KafkaProducer instance with LoggingProducerContext
        let context = MyProducerContext;
        let producer = match KafkaProducer::<MyProducerContext>::new_with_context("localhost:9092", "test-topic", context) {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };
        let result = producer.send(Some("test-key"), "Test message with key", None).await;

        // Check if the message was sent successfully
        assert!(result.is_ok(), "Failed to send message: {:?}", result.err());


        // Check if the producer was created successfully
        println!("KafkaProducer created successfully with custom context!");
    }

    #[test]
    fn test_create_producer() {

        if !init() { return; }

        println!("Testing KafkaProducer creation...");

        // Create a KafkaProducer instance
        let producer = KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic");

        // Check if the producer was created successfully
        assert!(producer.is_ok(), "Failed to create KafkaProducer: {:?}", producer.err());

        println!("KafkaProducer created successfully!");
    }



    /// Test sending a message with a key
    #[tokio::test]
    async fn test_send_with_key() {
        if !init() { return; }


        println!("Testing sending a message with a key...");


        // Create a KafkaProducer instance
        let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        // Send a message with a key (without timestamp)
        let result = producer.send(Some("test-key"), "Test message with key", None).await;

        // Check if the message was sent successfully
        assert!(result.is_ok(), "Failed to send message: {:?}", result.err());

        println!("Message with key sent successfully!");
    }

    /// Test sending a message without a key
    #[tokio::test]
    async fn test_send_without_key() {
        if !init() { return; }

        println!("Testing sending a message without a key...");

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        // Send a message without a key (without timestamp)
        let result = producer.send(None, "Test message without key", None).await;

        // Check if the message was sent successfully
        assert!(result.is_ok(), "Failed to send message: {:?}", result.err());

        tokio::time::sleep(Duration::from_secs(2)).await;

        println!("Message without key sent successfully!");
    }

    /// Test sending a message to a specific topic
    #[tokio::test]
    async fn test_send_to_topic() {
        if !init() { return; }

        println!("Testing sending a message to a specific topic...");

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        // Send a message to a specific topic (without timestamp)
        let result = producer.send_to_topic("another-topic", Some("test-key"), "Test message to another topic", None).await;

        // Check if the message was sent successfully
        assert!(result.is_ok(), "Failed to send message to topic: {:?}", result.err());

        println!("Message to specific topic sent successfully!");
    }

    /// Test flushing the producer
    #[test]
    fn test_flush() {
        if !init() { return; }

        println!("Testing flushing the producer...");

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        // Flush the producer
        let result = producer.flush(5000);

        // Check if the flush was successful
        assert!(result.is_ok(), "Failed to flush producer: {:?}", result.err());

        println!("Producer flushed successfully!");
    }

    /// Test sending a message with a timestamp
    #[tokio::test]
    async fn test_send_with_timestamp() {
        if !init() { return; }

        println!("Testing sending a message with a timestamp...");

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        let current_time = Utc::now().timestamp_millis();
        let now_local = Local::now();
        let now_string = now_local.format("%Y-%m-%d %H:%M:%S").to_string();

        // Send a message with a specific timestamp
        let result = producer.send(Some("timestamp-key"),
                                               format!("Test message with timestamp {}", now_string).as_str(),
                                               Some(current_time)).await;

        // Check if the message was sent successfully
        assert!(result.is_ok(), "Failed to send message with timestamp: {:?}", result.err());

        println!("Message with timestamp {} sent successfully!", current_time);
    }

    /// Test a complete workflow
    #[tokio::test]
    async fn test_complete_workflow() {
        if !init() { return; }

        println!("Testing complete workflow...");

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        // Send multiple messages
        let messages = vec![
            ("key1", "Message 1"),
            ("key2", "Message 2"),
            ("key3", "Message 3"),
        ];

        for (key, payload) in messages {
            let result = producer.send(Some(key), payload, None).await;
            assert!(result.is_ok(), "Failed to send message with key {}: {:?}", key, result.err());
            println!("Sent message with key: {}", key);

            // Add a small delay between messages
            thread::sleep(Duration::from_millis(100));
        }

        // Flush the producer
        let result = producer.flush(5000);
        assert!(result.is_ok(), "Failed to flush producer: {:?}", result.err());

        println!("Complete workflow executed successfully!");
    }


    #[test]
    fn test_logging_producer_context_error_logs() {
        let mut logger = Logger::start();
        let ctx = LoggingProducerContext::default();

        let error_reason = "things broke";
        let error_variant = KafkaError::MessageProduction(RDKafkaErrorCode::MessageTimedOut);

        ctx.error(error_variant, error_reason);

        let log_entry = logger.pop().expect("No log entry was captured");
        assert_eq!(
            log_entry.level(),
            log::Level::Error,
            "Expected log level ERROR"
        );
        assert!(
            log_entry.args().contains("Kafka client error"),
            "Log message should contain 'Kafka client error'"
        );
        assert!(
            log_entry.args().contains(error_reason),
            "Log message should contain the error reason: {}",
            error_reason
        );
        assert!(
            log_entry.args().contains("Message production error"),
            "Log message should mention the error variant"
        );
    }

    struct MyProducerContext;

    impl ClientContext for MyProducerContext {
        fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
            println!(" LOGGGGGING MSG: {:?} - {} - {}", level, fac, log_message);
        }

    }

    impl ProducerContext for MyProducerContext {
        type DeliveryOpaque = ();
        fn delivery(&self, delivery_result: &DeliveryResult, _: Self::DeliveryOpaque) {
            println!("***** --->>> Delivery result: {:?}", delivery_result);
        }
    }
}
