use futures::StreamExt;
use log::{error, info, log, Level};
use rdkafka::bindings::rd_kafka_event_debug_contexts;
use rdkafka::config::{ClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Message};
use std::task::Context;
use std::time::Duration;

/// KafkaConsumer is a wrapper around rdkafka's StreamConsumer, providing
/// convenient methods for consuming messages from Kafka topics asynchronously.
///
/// # Type Parameters
/// * `C` - The consumer context, which must implement `ConsumerContext`.
///
/// # Example
/// ```rust
/// use ferrisstreams::KafkaConsumer;
/// let consumer = KafkaConsumer::new("localhost:9092", "my-group");
/// consumer.subscribe(&["my-topic"]);
/// ```
pub struct KafkaConsumer<C: ConsumerContext + 'static> {
    /// The underlying rdkafka StreamConsumer.
    consumer: StreamConsumer<C>,
}

impl KafkaConsumer<DefaultConsumerContext> {
    /// Create a new KafkaConsumer with the default context.
    ///
    /// # Arguments
    /// * `brokers` - The Kafka broker list (e.g., "localhost:9092").
    /// * `group_id` - The consumer group ID.
    pub fn new(brokers: &str, group_id: &str) -> Self {
        KafkaConsumer::new_with_context(
            brokers,
            group_id,
            DefaultConsumerContext::default(),
        )
    }
}

impl<C: ConsumerContext + 'static> KafkaConsumer<C> {
    /// Create a new KafkaConsumer with a custom context.
    ///
    /// # Arguments
    /// * `brokers` - The Kafka broker list.
    /// * `group_id` - The consumer group ID.
    /// * `context` - The consumer context.
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
    /// Subscribe to a list of topics.
    ///
    /// # Arguments
    /// * `topics` - A slice of topic names to subscribe to.
    pub fn subscribe(&self, topics: &[&str]) {
        self.consumer.subscribe(topics).expect("Can't subscribe to specified topics");
    }
    /// Get an async stream of messages from the consumer.
    pub fn stream(&self) -> MessageStream<C> {
        self.consumer.stream()
    }
    /// Commit the current consumer state synchronously.
    pub fn commit(&self) -> Result<(), rdkafka::error::KafkaError> {
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }


    /// fake a poll_message function that waits for a message with a timeout
    pub async fn poll_message(&self, timeout: Duration) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        use tokio::time;
        let mut stream = self.consumer.stream();
        match time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().map(|p| p.to_vec()).unwrap_or_default();
                let key = msg.key().map(|k| k.to_vec());
                Some((payload, key))
            }
            _ => None,
        }
    }
}
