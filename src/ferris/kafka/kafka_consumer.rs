use std::task::Context;
use rdkafka::config::{ClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};
use std::time::Duration;
use log::{error, info, log, Level};
use rdkafka::bindings::rd_kafka_event_debug_contexts;
use rdkafka::error::KafkaError;

pub struct KafkaConsumer<C: ConsumerContext + 'static> {
    consumer: StreamConsumer<C>,
}

impl KafkaConsumer<DefaultConsumerContext> {
    pub fn new(brokers: &str, group_id: &str) -> Self {
        KafkaConsumer::new_with_context(
            brokers,
            group_id,
            DefaultConsumerContext::default(),
        )
    }
}

impl<C: ConsumerContext + 'static> KafkaConsumer<C> {
    pub fn new_with_context(brokers: &str,
               group_id: &str,
               context: C) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", brokers);
        config.set("group.id", group_id);
        config.set("auto.offset.reset", "earliest");

        let result = StreamConsumer::from_config_and_context(&config, context);
        match result {
            Ok(consumer) => {
                info!("Created KafkaConsumer connected to {} with group ID {}", brokers, group_id);
                KafkaConsumer { consumer }
            },
            Err(e) => {
                error!("Failed to create KafkaConsumer: {}", e);
                panic!("KafkaConsumer creation failed: {}", e);
            }
        }
    }
    pub fn subscribe(&self, topics: &[&str]) {
        self.consumer.subscribe(topics).expect("Can't subscribe to specified topics");
    }
    pub fn stream(&self) -> MessageStream<C> {
        self.consumer.stream()
    }

    pub fn commit(&self) -> Result<(), rdkafka::error::KafkaError> {
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }


    pub fn poll_message(&self, timeout: Duration) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        // match self.consumer.poll() {
        //     Some(Ok(msg)) => {
        //         let payload = msg.payload().map(|p| p.to_vec()).unwrap_or_default();
        //         let key = msg.key().map(|k| k.to_vec());
        //         Some((payload, key))
        //     },
        //     _ => None,
        // }
        panic!("Not implemented yet");
        // None
    }
}
