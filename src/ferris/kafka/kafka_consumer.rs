use crate::ferris::kafka::serialization::{Serializer, SerializationError};
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message as KafkaMessage;
use std::marker::PhantomData;
use std::time::Duration;

/// A Kafka consumer that handles deserialization automatically
pub struct KafkaConsumer<T, S, C = DefaultConsumerContext>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    consumer: StreamConsumer<C>,
    serializer: S,
    _phantom: PhantomData<T>,
}

/// Error type for consumer operations
#[derive(Debug)]
pub enum ConsumerError {
    KafkaError(KafkaError),
    SerializationError(SerializationError),
    Timeout,
    NoMessage,
}

impl std::fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerError::KafkaError(e) => write!(f, "Kafka error: {}", e),
            ConsumerError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            ConsumerError::Timeout => write!(f, "Timeout waiting for message"),
            ConsumerError::NoMessage => write!(f, "No message available"),
        }
    }
}

impl std::error::Error for ConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsumerError::KafkaError(e) => Some(e),
            ConsumerError::SerializationError(e) => Some(e),
            ConsumerError::Timeout | ConsumerError::NoMessage => None,
        }
    }
}

impl From<KafkaError> for ConsumerError {
    fn from(err: KafkaError) -> Self {
        ConsumerError::KafkaError(err)
    }
}

impl From<SerializationError> for ConsumerError {
    fn from(err: SerializationError) -> Self {
        ConsumerError::SerializationError(err)
    }
}

/// A message containing deserialized value and optional key
#[derive(Debug)]
pub struct Message<T> {
    pub value: T,
    pub key: Option<String>,
}

impl<T> Message<T> {
    pub fn new(value: T, key: Option<String>) -> Self {
        Self { value, key }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    pub fn into_value(self) -> T {
        self.value
    }

    pub fn into_parts(self) -> (T, Option<String>) {
        (self.value, self.key)
    }
}

impl<T, S> KafkaConsumer<T, S, DefaultConsumerContext>
where
    S: Serializer<T>,
{
    /// Creates a new KafkaConsumer with default context
    pub fn new(brokers: &str, group_id: &str, serializer: S) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create()?;

        Ok(KafkaConsumer {
            consumer,
            serializer,
            _phantom: PhantomData,
        })
    }
}

impl<T, S, C> KafkaConsumer<T, S, C>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    /// Creates a new KafkaConsumer with custom context
    pub fn new_with_context(brokers: &str, group_id: &str, serializer: S, context: C) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer<C> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create_with_context(context)?;

        Ok(KafkaConsumer {
            consumer,
            serializer,
            _phantom: PhantomData,
        })
    }

    /// Subscribe to topics
    pub fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        self.consumer.subscribe(topics)
    }

    /// Poll for a message with timeout
    pub async fn poll_message(&self, timeout: Duration) -> Result<Message<T>, ConsumerError> {
        use tokio::time;
        let mut stream = self.consumer.stream();
        
        match time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().ok_or(ConsumerError::NoMessage)?;
                let key = msg.key().and_then(|k| String::from_utf8(k.to_vec()).ok());
                let value = self.serializer.deserialize(payload)?;
                Ok(Message::new(value, key))
            }
            Ok(Some(Err(e))) => Err(ConsumerError::KafkaError(e)),
            Ok(None) => Err(ConsumerError::NoMessage),
            Err(_) => Err(ConsumerError::Timeout),
        }
    }

    /// Get a stream of raw Kafka messages (for advanced use cases)
    pub fn raw_stream(&self) -> MessageStream<C> {
        self.consumer.stream()
    }

    /// Get a stream that yields deserialized typed messages
    pub fn stream(&self) -> impl futures::Stream<Item = Result<T, ConsumerError>> + '_ {
        self.consumer.stream().map(|msg_result| {
            match msg_result {
                Ok(borrowed_message) => {
                    if let Some(payload) = borrowed_message.payload() {
                        self.serializer
                            .deserialize(payload)
                            .map_err(ConsumerError::SerializationError)
                    } else {
                        Err(ConsumerError::NoMessage)
                    }
                }
                Err(e) => Err(ConsumerError::KafkaError(e)),
            }
        })
    }


    /// Commit the current consumer state
    pub fn commit(&self) -> Result<(), KafkaError> {
        use rdkafka::consumer::{CommitMode, Consumer};
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }

    /// Access the serializer
    pub fn serializer(&self) -> &S {
        &self.serializer
    }
}

/// Metadata associated with a Kafka message
#[derive(Debug, Clone)]
pub struct MessageMetadata {
    pub key: Option<String>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

/// Builder for creating KafkaConsumer with configuration options
pub struct ConsumerBuilder<T, S, C = DefaultConsumerContext>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    brokers: String,
    group_id: String,
    serializer: S,
    context: Option<C>,
    _phantom: PhantomData<T>,
}

impl<T, S> ConsumerBuilder<T, S, DefaultConsumerContext>
where
    S: Serializer<T>,
{
    /// Creates a new builder with required parameters
    pub fn new(brokers: &str, group_id: &str, serializer: S) -> Self {
        Self {
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            serializer,
            context: None,
            _phantom: PhantomData,
        }
    }

    /// Builds the KafkaConsumer
    pub fn build(self) -> Result<KafkaConsumer<T, S, DefaultConsumerContext>, KafkaError> {
        KafkaConsumer::new(&self.brokers, &self.group_id, self.serializer)
    }
}

impl<T, S, C> ConsumerBuilder<T, S, C>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    /// Sets a custom consumer context
    pub fn with_context<NewC>(self, context: NewC) -> ConsumerBuilder<T, S, NewC>
    where
        NewC: ConsumerContext + 'static,
    {
        ConsumerBuilder {
            brokers: self.brokers,
            group_id: self.group_id,
            serializer: self.serializer,
            context: Some(context),
            _phantom: PhantomData,
        }
    }

}

/// Convenience trait for types that can be consumed from Kafka with a specific serializer
pub trait KafkaConsumable<S>: Sized
where
    S: Serializer<Self>,
{
    /// Creates a consumer for this type
    fn consumer(brokers: &str, group_id: &str, serializer: S) -> Result<KafkaConsumer<Self, S>, KafkaError> {
        KafkaConsumer::new(brokers, group_id, serializer)
    }

    /// Creates a consumer builder for this type
    fn consumer_builder(brokers: &str, group_id: &str, serializer: S) -> ConsumerBuilder<Self, S> {
        ConsumerBuilder::new(brokers, group_id, serializer)
    }
}

// Implement for any type that can be serialized/deserialized
impl<T, S> KafkaConsumable<S> for T
where
    S: Serializer<T>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::kafka::serialization::JsonSerializer;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestMessage {
        id: u32,
        content: String,
    }

    #[tokio::test]
    async fn test_consumer_builder() {
        let serializer = JsonSerializer;
        let builder = ConsumerBuilder::<TestMessage, _>::new(
            "localhost:9092",
            "test-group",
            serializer,
        );

        // This would fail if Kafka isn't running, but demonstrates the API
        let _result = builder.build();
    }

    #[tokio::test]
    async fn test_consumable_trait() {
        let serializer = JsonSerializer;
        let _consumer = TestMessage::consumer("localhost:9092", "test-group", serializer);
    }
}