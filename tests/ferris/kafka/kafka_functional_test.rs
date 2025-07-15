use crate::ferris::kafka::test_utils::is_kafka_running;
use chrono::Utc;
use ferrisstreams::{KafkaConsumer, KafkaProducer};
use std::time::Duration;
use rdkafka::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{DefaultProducerContext, ProducerContext};
use uuid::Uuid;
use ferrisstreams::ferris::kafka::LoggingProducerContext;

const TOPIC: &str = "kafka-functional-test-topic";


#[tokio::test]
async fn test_produce_and_consume() {
    if !is_kafka_running() { return; }

    let test_key = "test-key";
    let timestamp = Utc::now().to_rfc3339();
    let test_payload = format!("hello, kafka! @ {}", timestamp);

    let context = MyProducerContext::new("CLIENT_ID".to_string(), "CONTEXT_ID".to_string());


    let producer = match KafkaProducer::<MyProducerContext>::new_with_context("localhost:9092", TOPIC, context) {
        Ok(p) => {
            println!("Created KafkaProducer with custom context:");
            p
        },
        Err(e) => {
            panic!("Failed to create KafkaProducer: {}", e);
        }
    };
        //.expect("Failed to create KafkaProducer");
    let result = producer.send_to_topic(TOPIC, Some(test_key), &test_payload, None).await;
    assert!(result.is_ok(), "Failed to send message: {:?}", result.err());

    let group_id = format!("test-functional-group-{}", Uuid::new_v4());
    let consumer = KafkaConsumer::new_with_context("localhost:9092", &group_id, DefaultConsumerContext);
    consumer.subscribe(&[TOPIC]);
    std::thread::sleep(Duration::from_secs(2));

    let mut found = false;
    for _ in 0..100 {
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

pub struct MyProducerContext {
    client_id: String,
    context_id: String,

}
impl MyProducerContext {
    pub fn new(client_id: String, client_context: String) -> Self {
        MyProducerContext {
            client_id: client_id,
            context_id: client_context,
        }
    }
}

impl ClientContext for MyProducerContext {

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        println!("{} [{:?}] {}: {}", self.client_id, level, fac, log_message);
    }
}

impl ProducerContext for MyProducerContext {
    type DeliveryOpaque = ();
    fn delivery(&self, delivery_result: &DeliveryResult, _: Self::DeliveryOpaque) {
        println!("{} ***** --->>> Delivery result: {:?}", self.context_id, delivery_result);
    }
}