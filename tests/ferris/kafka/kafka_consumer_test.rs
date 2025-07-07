use std::time::Duration;
use uuid::Uuid;
/// Helper function to check if Kafka is running

#[cfg(test)]
mod kafka_consumer_tests {
    use uuid::Uuid;
    use ferrisstreams::KafkaConsumer;
    use crate::ferris::kafka::test_utils::init;

    use super::*;

    #[test]
    fn test_consume_message() {
        if !init() { return; }

        let topic = "test-topic";
        let group_id = format!("test-group-{}", Uuid::new_v4());
        let consumer = KafkaConsumer::new("localhost:9092", &group_id)
;

        consumer.subscribe(&[topic]);

        // need to wait for rebalance to allocate otherwise nothing happens
        std::thread::sleep(Duration::from_secs(2));

        // Try to poll a message (may be none if topic is empty)
        let result = consumer.poll_message(Duration::from_secs(2));

        assert!(
            result.is_some(),
            "No messages received from Kafka topic '{}'. Consider producing a message before running this test.",
            topic
        );

        if let Some((payload, headers)) = result {
            let payload_str = String::from_utf8_lossy(&payload);
            println!("Message payload: {}", payload_str);

            match headers {
                Some(h) => println!("Headers: {}", String::from_utf8_lossy(&h)),
                None => println!("No headers."),
            }
        } else {
            println!("No messages received.");
        }
    }
}

