use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::Message;
use std::time::Duration;

pub struct KafkaConsumer {
    consumer: BaseConsumer,
}

impl KafkaConsumer {
    pub fn new(brokers: &str, group_id: &str) -> Self {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            // .set("enable.partition.eof", "false")
            // .set("session.timeout.ms", "60 * 1000")
            // .set("enable.auto.commit", "true")
            //.set("statistics.interval.ms", "30000")
            .create()
            .expect("Consumer creation failed");
        KafkaConsumer { consumer }
    }

    pub fn subscribe(&self, topics: &[&str]) {
        self.consumer.subscribe(topics).expect("Can't subscribe to specified topics");
    }

    pub fn poll_message(&self, timeout: Duration) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        match self.consumer.poll(timeout) {
            Some(Ok(msg)) => {
                let payload = msg.payload().map(|p| p.to_vec()).unwrap_or_default();
                let key = msg.key().map(|k| k.to_vec());
                Some((payload, key))
            },
            _ => None,
        }
    }
}

