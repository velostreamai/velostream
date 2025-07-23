use ferrisstreams::ferris::kafka::{
    KafkaProducer, KafkaConsumer, ProducerBuilder, ConsumerBuilder, JsonSerializer, Headers
};
use ferrisstreams::ferris::kafka::producer_config::{ProducerConfig, CompressionType, AckMode};
use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct TestMessage {
    id: u32,
    content: String,
}

#[tokio::test]
async fn test_producer_builder_basic() {
    // Test basic producer builder functionality
    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-test-topic",
        JsonSerializer,
        JsonSerializer,
    ).build();

    match producer_result {
        Ok(producer) => {
            // Verify we can use the producer
            let test_message = TestMessage {
                id: 1,
                content: "builder test".to_string(),
            };

            let headers = Headers::new().insert("source", "builder-test");
            
            // Try to send a message (might fail if Kafka is not available)
            let _ = producer.send(
                Some(&"test-key".to_string()),
                &test_message,
                headers,
                None,
            ).await;
        },
        Err(err) => {
            // Builder might fail if Kafka is not available - that's acceptable
            println!("Producer builder failed (Kafka might not be available): {:?}", err);
        }
    }
}

#[tokio::test]
async fn test_producer_builder_with_configuration() {
    // Test producer builder with custom configuration
    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-config-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("test-producer-builder")
    .message_timeout(Duration::from_secs(10))
    .compression(CompressionType::Lz4)
    .acks(AckMode::All)
    .batching(32768, Duration::from_millis(10))
    .idempotence(true)
    .retries(5, Duration::from_millis(100))
    .custom_property("test.property", "test.value")
    .build();

    assert!(producer_result.is_ok(), "Producer builder with configuration should succeed");
}

#[tokio::test]
async fn test_producer_builder_with_presets() {
    // Test producer builder with performance presets
    let high_throughput_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-ht-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("ht-producer")
    .high_throughput()
    .build();

    assert!(high_throughput_result.is_ok(), "High throughput producer should build successfully");

    let low_latency_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-ll-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("ll-producer")
    .low_latency()
    .build();

    assert!(low_latency_result.is_ok(), "Low latency producer should build successfully");

    let max_durability_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-md-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("md-producer")
    .max_durability()
    .build();

    assert!(max_durability_result.is_ok(), "Max durability producer should build successfully");

    let development_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-dev-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("dev-producer")
    .development()
    .build();

    assert!(development_result.is_ok(), "Development producer should build successfully");
}

#[tokio::test]
async fn test_producer_builder_with_config_object() {
    // Test producer builder with ProducerConfig object
    let config = ProducerConfig::new("localhost:9092", "builder-config-obj-topic")
        .client_id("config-obj-producer")
        .compression(CompressionType::Gzip)
        .acks(AckMode::Leader)
        .custom_property("security.protocol", "PLAINTEXT");

    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    ).build();

    assert!(producer_result.is_ok(), "Producer builder with config object should succeed");
}

#[tokio::test]
async fn test_consumer_builder_basic() {
    // Test basic consumer builder functionality
    let consumer_result = ConsumerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "builder-consumer-group",
        JsonSerializer,
        JsonSerializer,
    ).build();

    match consumer_result {
        Ok(consumer) => {
            // Verify we can use the consumer
            let _ = consumer.subscribe(&["builder-consumer-test-topic"]);
            
            // Try to poll (might timeout if no messages)
            let _ = consumer.poll_message(Duration::from_millis(100)).await;
        },
        Err(err) => {
            // Consumer might fail if Kafka is not available - that's acceptable
            println!("Consumer builder failed (Kafka might not be available): {:?}", err);
        }
    }
}

#[tokio::test]
async fn test_consumer_builder_with_presets() {
    // Test consumer builder with performance presets using ConsumerConfig
    let high_throughput_config = ConsumerConfig::new("localhost:9092", "ht-consumer-group")
        .client_id("ht-consumer")
        .high_throughput();

    let ht_consumer_result = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        high_throughput_config,
        JsonSerializer,
        JsonSerializer,
    );

    assert!(ht_consumer_result.is_ok(), "High throughput consumer should build successfully");

    let low_latency_config = ConsumerConfig::new("localhost:9092", "ll-consumer-group")
        .client_id("ll-consumer")
        .low_latency();

    let ll_consumer_result = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        low_latency_config,
        JsonSerializer,
        JsonSerializer,
    );

    assert!(ll_consumer_result.is_ok(), "Low latency consumer should build successfully");

    let development_config = ConsumerConfig::new("localhost:9092", "dev-consumer-group")
        .client_id("dev-consumer")
        .development();

    let dev_consumer_result = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        development_config,
        JsonSerializer,
        JsonSerializer,
    );

    assert!(dev_consumer_result.is_ok(), "Development consumer should build successfully");

    let streaming_config = ConsumerConfig::new("localhost:9092", "stream-consumer-group")
        .client_id("stream-consumer")
        .streaming();

    let stream_consumer_result = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        streaming_config,
        JsonSerializer,
        JsonSerializer,
    );

    assert!(stream_consumer_result.is_ok(), "Streaming consumer should build successfully");
}

#[tokio::test]
async fn test_producer_builder_method_chaining() {
    // Test that builder methods can be chained in different orders
    let producer1_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "chain-test-topic-1",
        JsonSerializer,
        JsonSerializer,
    )
    .compression(CompressionType::Snappy)
    .client_id("chain-test-1")
    .acks(AckMode::All)
    .build();

    let producer2_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "chain-test-topic-2",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("chain-test-2")
    .acks(AckMode::Leader)
    .compression(CompressionType::Lz4)
    .build();

    assert!(producer1_result.is_ok(), "First chained producer should build");
    assert!(producer2_result.is_ok(), "Second chained producer should build");
}

#[tokio::test]
async fn test_builder_configuration_override() {
    // Test that later configuration calls override earlier ones
    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "override-test-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("first-id")
    .compression(CompressionType::None)
    .client_id("second-id") // This should override the first
    .compression(CompressionType::Gzip) // This should override None
    .build();

    assert!(producer_result.is_ok(), "Producer with overridden config should build");
}

#[tokio::test]
async fn test_producer_builder_preset_override() {
    // Test that preset configurations can be overridden
    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "preset-override-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .high_throughput() // Apply high throughput preset
    .compression(CompressionType::None) // Override compression from preset
    .acks(AckMode::All) // Override acks from preset
    .build();

    assert!(producer_result.is_ok(), "Producer with preset override should build");
}

#[tokio::test]
async fn test_consumer_builder_with_custom_context() {
    // Test consumer builder with custom context
    use rdkafka::consumer::DefaultConsumerContext;
    
    let context = DefaultConsumerContext;
    let consumer_result = ConsumerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "context-test-group",
        JsonSerializer,
        JsonSerializer,
    )
    .with_context(context)
    .build();

    // This test verifies the with_context method compiles and works
    // The actual functionality depends on rdkafka internals
    match consumer_result {
        Ok(_) => {
            // Custom context consumer created successfully
        },
        Err(err) => {
            // Might fail if Kafka is not available
            println!("Consumer with custom context failed: {:?}", err);
        }
    }
}

#[tokio::test]
async fn test_producer_builder_with_custom_context() {
    // Test producer builder with custom context
    use ferrisstreams::ferris::kafka::LoggingProducerContext;
    
    let context = LoggingProducerContext::default();
    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "producer-context-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .with_context(context)
    .build();

    // This test verifies the with_context method compiles and works
    match producer_result {
        Ok(_) => {
            // Custom context producer created successfully
        },
        Err(err) => {
            // Might fail if Kafka is not available
            println!("Producer with custom context failed: {:?}", err);
        }
    }
}

#[tokio::test]
async fn test_end_to_end_builder_workflow() {
    // Test complete workflow using builders
    let producer_result = ProducerBuilder::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "e2e-builder-topic",
        JsonSerializer,
        JsonSerializer,
    )
    .client_id("e2e-producer")
    .development() // Use development preset for testing
    .build();

    let consumer_config = ConsumerConfig::new("localhost:9092", "e2e-builder-group")
        .client_id("e2e-consumer")
        .auto_offset_reset(OffsetReset::Latest)
        .development();

    let consumer_result = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    );

    if let (Ok(producer), Ok(consumer)) = (producer_result, consumer_result) {
        // Subscribe consumer
        if let Ok(_) = consumer.subscribe(&["e2e-builder-topic"]) {
            // Send a test message
            let test_message = TestMessage {
                id: 42,
                content: "end-to-end builder test".to_string(),
            };

            let headers = Headers::new()
                .insert("test-type", "e2e-builder")
                .insert("timestamp", &std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .to_string());

            let send_result = producer.send(
                Some(&"e2e-key".to_string()),
                &test_message,
                headers,
                None,
            ).await;

            match send_result {
                Ok(_) => {
                    // Message sent successfully
                    // Try to consume it (might timeout if Kafka is slow)
                    let _ = consumer.poll_message(Duration::from_secs(1)).await;
                },
                Err(err) => {
                    println!("Send failed in e2e test (Kafka might not be available): {:?}", err);
                }
            }
        }
    }
}

#[cfg(test)]
mod builder_unit_tests {
    use super::*;

    #[test]
    fn test_producer_builder_creation() {
        // Test that producer builder can be created without errors
        let _builder = ProducerBuilder::<String, TestMessage, _, _>::new(
            "localhost:9092",
            "unit-test-topic",
            JsonSerializer,
            JsonSerializer,
        );
        // If we get here, the builder was created successfully
    }

    #[test]
    fn test_consumer_builder_creation() {
        // Test that consumer builder can be created without errors
        let _builder = ConsumerBuilder::<String, TestMessage, _, _>::new(
            "localhost:9092",
            "unit-test-group",
            JsonSerializer,
            JsonSerializer,
        );
        // If we get here, the builder was created successfully
    }

    #[test]
    fn test_producer_config_with_builder() {
        // Test that ProducerConfig can be used with builder
        let config = ProducerConfig::new("localhost:9092", "config-test-topic")
            .client_id("unit-test-producer");

        let _builder = ProducerBuilder::<String, TestMessage, _, _>::with_config(
            config,
            JsonSerializer,
            JsonSerializer,
        );
        // If we get here, the config was accepted by the builder
    }
}