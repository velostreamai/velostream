use test_setup1::simple::kafka::app::KafkaConsumer;
use std::time::Duration;
use std::net::TcpStream;
use uuid::Uuid;

/// Helper function to check if Kafka is running
fn is_kafka_running() -> bool {
    TcpStream::connect("localhost:9092").is_ok()
}

#[cfg(test)]
mod kafka_consumer_tests {
    use uuid::Uuid;
    use super::*;

    #[test]
    fn test_consume_message() {
        if !is_kafka_running() {
            println!("Kafka is not running. Skipping test.");
            return;
        }
        let topic = "test-topic";
        let group_id = format!("test-group-{}", Uuid::new_v4());
        let consumer = KafkaConsumer::new("localhost:9092", &group_id)
;

        consumer.subscribe(&[topic]);

        // need to wait for rebalance to allocate otherwise nothing happens
        std::thread::sleep(Duration::from_secs(2));

        // Try to poll a message (may be none if topic is empty)
        let result = consumer.poll_message(Duration::from_secs(2));
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

        // // We don't assert on result, as topic may be empty
        // println!("Polled message: {:?}", result);
        // if let Some((payload, headers)) = result {
        //
        //     println!("Message payload: {:?}, headers: {:?}", payload, headers);
        // } else {
        //     println!("No messages received.");
        // }



    }
}

