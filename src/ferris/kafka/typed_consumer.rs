use crate::ferris::kafka::kafka_consumer::{KafkaConsumer, ConsumerError};
use crate::ferris::kafka::serialization::Serializer;
use futures::StreamExt;
use rdkafka::consumer::{ConsumerContext, DefaultConsumerContext, MessageStream};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use std::marker::PhantomData;
use std::time::Duration;

/// A type-safe Kafka consumer that handles deserialization automatically
pub struct TypedKafkaConsumer<T, S, C = DefaultConsumerContext>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    consumer: KafkaConsumer<C>,
    serializer: S,
    _phantom: PhantomData<T>,
}

impl<T, S> TypedKafkaConsumer<T, S, DefaultConsumerContext>
where
    S: Serializer<T>,
{
    /// Creates a new TypedKafkaConsumer with default context
    pub fn new(brokers: &str, group_id: &str, serializer: S) -> Self {
        let consumer = KafkaConsumer::new(brokers, group_id);
        TypedKafkaConsumer {
            consumer,
            serializer,
            _phantom: PhantomData,
        }
    }
}

impl<T, S, C> TypedKafkaConsumer<T, S, C>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    /// Creates a new TypedKafkaConsumer with custom context
    pub fn new_with_context(brokers: &str, group_id: &str, serializer: S, context: C) -> Self {
        let consumer = KafkaConsumer::new_with_context(brokers, group_id, context);
        TypedKafkaConsumer {
            consumer,
            serializer,
            _phantom: PhantomData,
        }
    }

    /// Subscribe to topics
    pub fn subscribe(&self, topics: &[&str]) {
        self.consumer.subscribe(topics);
    }

    /// Poll for a typed message with timeout
    pub async fn poll_message(&self, timeout: Duration) -> Result<TypedMessage<T>, ConsumerError> {
        match self.consumer.poll_with_serializer(timeout, &self.serializer).await {
            Ok((value, key)) => Ok(TypedMessage {
                value,
                key: key.and_then(|k| String::from_utf8(k).ok()),
            }),
            Err(e) => Err(e),
        }
    }

    /// Get a stream of typed messages
    pub fn typed_stream(&self) -> TypedMessageStream<T, S, C> {
        TypedMessageStream {
            stream: self.consumer.stream(),
            serializer: &self.serializer,
            _phantom: PhantomData,
        }
    }

    /// Commit the current consumer state
    pub fn commit(&self) -> Result<(), KafkaError> {
        self.consumer.commit()
    }

    /// Access the underlying consumer for advanced operations
    pub fn inner(&self) -> &KafkaConsumer<C> {
        &self.consumer
    }

    /// Access the serializer
    pub fn serializer(&self) -> &S {
        &self.serializer
    }
}

/// A typed message containing deserialized value and optional key
#[derive(Debug)]
pub struct TypedMessage<T> {
    pub value: T,
    pub key: Option<String>,
}

impl<T> TypedMessage<T> {
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

/// A stream of typed messages
pub struct TypedMessageStream<'a, T, S, C>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    stream: MessageStream<'a, C>,
    serializer: &'a S,
    _phantom: PhantomData<T>,
}

impl<'a, T, S, C> TypedMessageStream<'a, T, S, C>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    /// Get the next typed message from the stream
    pub async fn next_typed(&mut self) -> Option<Result<TypedMessage<T>, ConsumerError>> {
        match self.stream.next().await {
            Some(Ok(message)) => {
                let payload = message.payload().unwrap_or(&[]);
                let key = message.key().and_then(|k| String::from_utf8(k.to_vec()).ok());
                
                match self.serializer.deserialize(payload) {
                    Ok(value) => Some(Ok(TypedMessage::new(value, key))),
                    Err(e) => Some(Err(ConsumerError::SerializationError(e))),
                }
            }
            Some(Err(e)) => Some(Err(ConsumerError::KafkaError(e))),
            None => None,
        }
    }
}

/// Builder for creating TypedKafkaConsumer with configuration options
pub struct TypedConsumerBuilder<T, S, C = DefaultConsumerContext>
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

impl<T, S> TypedConsumerBuilder<T, S, DefaultConsumerContext>
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
}

impl<T, S, C> TypedConsumerBuilder<T, S, C>
where
    S: Serializer<T>,
    C: ConsumerContext + 'static,
{
    /// Sets a custom consumer context
    pub fn with_context<NewC>(self, context: NewC) -> TypedConsumerBuilder<T, S, NewC>
    where
        NewC: ConsumerContext + 'static,
    {
        TypedConsumerBuilder {
            brokers: self.brokers,
            group_id: self.group_id,
            serializer: self.serializer,
            context: Some(context),
            _phantom: PhantomData,
        }
    }

    /// Builds the TypedKafkaConsumer
    pub fn build(self) -> TypedKafkaConsumer<T, S, DefaultConsumerContext> {
        let consumer = KafkaConsumer::new(&self.brokers, &self.group_id);
        TypedKafkaConsumer {
            consumer,
            serializer: self.serializer,
            _phantom: PhantomData,
        }
    }
}

/// Convenience trait for types that can be consumed from Kafka with a specific serializer
pub trait KafkaConsumable<S>: Sized
where
    S: Serializer<Self>,
{
    /// Creates a typed consumer for this type
    fn consumer(brokers: &str, group_id: &str, serializer: S) -> TypedKafkaConsumer<Self, S> {
        TypedKafkaConsumer::new(brokers, group_id, serializer)
    }

    /// Creates a typed consumer builder for this type
    fn consumer_builder(brokers: &str, group_id: &str, serializer: S) -> TypedConsumerBuilder<Self, S> {
        TypedConsumerBuilder::new(brokers, group_id, serializer)
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
    async fn test_typed_consumer_builder() {
        let serializer = JsonSerializer;
        let consumer = TypedConsumerBuilder::<TestMessage, _>::new(
            "localhost:9092",
            "test-group",
            serializer,
        ).build();

        // Test the API without actually connecting
        assert!(!std::ptr::eq(consumer.serializer(), std::ptr::null()));
    }

    #[tokio::test]
    async fn test_consumable_trait() {
        let serializer = JsonSerializer;
        let _consumer = TestMessage::consumer("localhost:9092", "test-group", serializer);
    }
}