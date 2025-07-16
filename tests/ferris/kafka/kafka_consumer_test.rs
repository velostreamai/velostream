use std::time::Duration;

use futures::StreamExt;
use tokio::time::{timeout, Duration as TokioDuration};

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
        let first_result = stream.next().await;

        assert!(
            first_result.is_some(),
            "No messages received from Kafka topic '{}'. Consider producing a message before running this test.",
            topic
        );
        if let Some(Ok(message)) = first_result {
            print_msg(&message);
            print_headers(message);
        } else {
            println!("No messages received.");
        }

        // Print all remaining messages in the stream until none are left, with a timeout for each
        loop {
            match timeout(TokioDuration::from_secs(1), stream.next()).await {
                Ok(Some(Ok(message))) => {
                    print_msg(&message);
                    print_headers(message);
                }
                Ok(Some(Err(e))) => {
                    println!("Kafka error: {}", e);
                }
                Ok(None) => break, // Stream ended
                Err(_) => {
                    println!("No more messages within timeout, exiting.");
                    break;
                }
            }
        }
    }

    fn print_msg(message: &BorrowedMessage) {
        use chrono::Local;
        let now = Local::now();
        if let Some(payload) = message.payload() {
            print!("[{}] Message payload: {}", now.format("%Y-%m-%d %H:%M:%S"), String::from_utf8_lossy(payload));
        } else {
            print!("[{}] No payload in message.", now.format("%Y-%m-%d %H:%M:%S"));
        }
    }

    fn print_headers(message: BorrowedMessage) {
        match message.headers() {
            Some(headers) => {
                let hh = headers.detach();
                headers.iter().for_each(|header| {
                    let key = header.key;
                    let value = header.value.map(|v| String::from_utf8_lossy(v)).unwrap_or_else(|| "None".into());
                    println!(" Header: {} = {}", key, value);
                });
            }
            None => println!(" Header: None"),
        }
    }
}
