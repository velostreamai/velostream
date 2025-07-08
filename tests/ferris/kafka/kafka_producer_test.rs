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
    use crate::ferris::kafka::test_utils::{init, is_kafka_running};
    use chrono::{Local, Utc};
    use ferrisstreams::ferris::kafka::{KafkaProducer};
    use ferrisstreams::ferris::kafka::LoggingProducerContext;
    use logtest::Logger;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};
    use rdkafka::ClientContext;

    // #[test]
    // fn test_kafka_producer_new_with_context() {
    //     use rdkafka::producer::{ProducerContext, NoCustomPartitioner};
    //     use rdkafka::client::ClientContext;
    //     use rdkafka::message::DeliveryResult;
    //     use rdkafka::config::RDKafkaLogLevel;
    //     use rdkafka::error::KafkaError;
    //
    //     // Use the custom context defined in kafka_producer.rs
    //     let brokers = "localhost:9092";
    //     let topic = "test-topic";
    //     let custom_context = MyCustomProducerContext::default();
    //     let context = Some(custom_context);
    //
    //     // You must create the producer manually with the correct type
    //     let producer: Result<KafkaProducer<MyCustomProducerContext>, _> =
    //         KafkaProducer::new_with_context(brokers, topic, context);
    //
    //     assert!(producer.is_ok() || producer.is_err());
    // }


    #[test]
    fn test_logging_producer_context_error_logs() {
        let mut logger = Logger::start();
        let ctx = LoggingProducerContext;

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


    /// Test creating a new KafkaProducer
    #[test]
    fn test_create_producer() {

        if !init() { return; }

        println!("Testing KafkaProducer creation...");

        // Create a KafkaProducer instance
        let producer = KafkaProducer::new("localhost:9092", "test-topic");

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
        let producer = match KafkaProducer::new("localhost:9092", "test-topic") {
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

        // Skip test if Kafka is not running
        if !is_kafka_running() {
            println!("Skipping test_send_without_key because Kafka is not running");
            return;
        }

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::new("localhost:9092", "test-topic") {
            Ok(p) => p,
            Err(e) => {
                panic!("Failed to create KafkaProducer: {}", e);
            }
        };

        // Send a message without a key (without timestamp)
        let result = producer.send(None, "Test message without key", None).await;

        // Check if the message was sent successfully
        assert!(result.is_ok(), "Failed to send message: {:?}", result.err());

        println!("Message without key sent successfully!");
    }

    /// Test sending a message to a specific topic
    #[tokio::test]
    async fn test_send_to_topic() {
        if !init() { return; }

        println!("Testing sending a message to a specific topic...");

        // Skip test if Kafka is not running
        if !is_kafka_running() {
            println!("Skipping test_send_to_topic because Kafka is not running");
            return;
        }

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::new("localhost:9092", "test-topic") {
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
        let producer = match KafkaProducer::new("localhost:9092", "test-topic") {
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

        // Skip test if Kafka is not running
        if !is_kafka_running() {
            println!("Skipping test_send_with_timestamp because Kafka is not running");
            return;
        }

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::new("localhost:9092", "test-topic") {
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

        // Skip test if Kafka is not running
        if !is_kafka_running() {
            println!("Skipping test_complete_workflow because Kafka is not running");
            return;
        }

        // Create a KafkaProducer instance
        let producer = match KafkaProducer::new("localhost:9092", "test-topic") {
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
}
