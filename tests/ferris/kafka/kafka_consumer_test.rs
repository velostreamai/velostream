use std::time::Duration;

use futures::StreamExt;

#[cfg(test)]
mod kafka_consumer_tests {
    use rdkafka::Message;
    use rdkafka::message::{BorrowedMessage, Headers};
    use crate::ferris::kafka::test_utils::init;
    use ferrisstreams::KafkaConsumer;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn test_consume_message() {
        if !init() { return; }

        let topic = "test-topic";
        let group_id = format!("test-group-{}", Uuid::new_v4());
        let consumer = KafkaConsumer::new("localhost:9092", &group_id);

        consumer.subscribe(&[topic]);

        // need to wait for rebalance to allocate otherwise nothing happens
        std::thread::sleep(Duration::from_secs(2));

        // Try to poll a message (may be none if topic is empty)
        let mut stream = consumer.stream();
        let result = stream.next().await;

        assert!(
            result.is_some(),
            "No messages received from Kafka topic '{}'. Consider producing a message before running this test.",
            topic
        );
        if let Some(Ok(message)) = result {
            print_msg(&message);
            print_headers(message);
        } else {
            println!("No messages received.");
        }
    }

    fn print_msg(message: &BorrowedMessage) {
        if let Some(payload) = message.payload() {
            print!("Message payload: {}", String::from_utf8_lossy(payload));
        } else {
            print!("No payload in message.");
        }
    }

    fn print_headers(message: BorrowedMessage) {
        match message.headers() {
            Some(headers) => {
                let hh = headers.detach();
                hh.iter().for_each(|header| {
                    let key = header.key;
                    let value = header.value.map(|v| String::from_utf8_lossy(v)).unwrap_or_else(|| "None".into());
                    println!("Header: {} = {}", key, value);
                });
            }
            None => println!("No headers."),
        }
    }
}
