use std::time::Duration;
use std::net::TcpStream;
use chrono::Utc;
use uuid::Uuid;
use ferrisstreams::{KafkaConsumer, KafkaProducer};
use crate::ferris::kafka::test_utils::is_kafka_running;

const TOPIC: &str = "functional-test-topic";


#[tokio::test]
async fn test_produce_and_consume() {
    if !is_kafka_running() { return; }

    let test_key = "test-key";
    let timestamp = Utc::now().to_rfc3339();
    let test_payload = format!("hello, kafka! @ {}", timestamp);

    let producer = KafkaProducer::new("localhost:9092", TOPIC).expect("Failed to create KafkaProducer");
    let result = producer.send_to_topic(TOPIC, Some(test_key), &test_payload, None).await;
    assert!(result.is_ok(), "Failed to send message: {:?}", result.err());

    let group_id = format!("test-functional-group-{}", Uuid::new_v4());
    let consumer = KafkaConsumer::new("localhost:9092", &group_id);
    consumer.subscribe(&[TOPIC]);
    std::thread::sleep(Duration::from_secs(2));

    let mut found = false;
    for _ in 0..5 {
        if let Some((payload, key)) = consumer.poll_message(Duration::from_secs(2)) {
            let payload_str = String::from_utf8_lossy(&payload);
            let key_match = key.as_deref().map(|k| String::from_utf8_lossy(k)) == Some(test_key.into());
            println!("Received message: key={:?}, payload={}", key, payload_str);
            if payload_str == test_payload && key_match {
                found = true;
                break;
            }
        }
    }
    assert!(found, "Did not find the produced message in Kafka");
}