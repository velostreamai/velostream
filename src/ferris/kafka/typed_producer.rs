use crate::ferris::kafka::kafka_producer::{KafkaProducer, ProducerError};
use crate::ferris::kafka::kafka_producer_def_context::LoggingProducerContext;
use crate::ferris::kafka::serialization::Serializer;
use rdkafka::error::KafkaError;
use rdkafka::producer::ProducerContext;
use std::marker::PhantomData;

/// A type-safe Kafka producer that handles serialization automatically
pub struct TypedKafkaProducer<T, S, C = LoggingProducerContext> 
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    producer: KafkaProducer<C>,
    serializer: S,
    _phantom: PhantomData<T>,
}

impl<T, S> TypedKafkaProducer<T, S, LoggingProducerContext>
where
    S: Serializer<T>,
{
    /// Creates a new TypedKafkaProducer with default context
    pub fn new(
        brokers: &str,
        default_topic: &str,
        serializer: S,
    ) -> Result<Self, KafkaError> {
        let producer = KafkaProducer::<LoggingProducerContext>::new(brokers, default_topic)?;
        Ok(TypedKafkaProducer {
            producer,
            serializer,
            _phantom: PhantomData,
        })
    }
}

impl<T, S, C> TypedKafkaProducer<T, S, C>
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    /// Creates a new TypedKafkaProducer with custom context
    pub fn new_with_context(
        brokers: &str,
        default_topic: &str,
        serializer: S,
        context: C,
    ) -> Result<Self, KafkaError> {
        let producer = KafkaProducer::new_with_context(brokers, default_topic, context)?;
        Ok(TypedKafkaProducer {
            producer,
            serializer,
            _phantom: PhantomData,
        })
    }

    /// Sends a typed message to the default topic
    pub async fn send(
        &self,
        key: Option<&str>,
        value: &T,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        self.producer
            .send_with_serializer(key, value, &self.serializer, timestamp)
            .await
    }

    /// Sends a typed message to the default topic with current timestamp
    pub async fn send_with_current_timestamp(
        &self,
        key: Option<&str>,
        value: &T,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        self.send(key, value, Some(timestamp)).await
    }

    /// Sends a typed message to a specific topic
    pub async fn send_to_topic(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &T,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        self.producer
            .send_to_topic_with_serializer(topic, key, value, &self.serializer, timestamp)
            .await
    }

    /// Flushes any pending messages
    pub fn flush(&self, timeout_ms: u64) -> Result<(), KafkaError> {
        self.producer.flush(timeout_ms)
    }

    /// Access the underlying producer for advanced operations
    pub fn inner(&self) -> &KafkaProducer<C> {
        &self.producer
    }

    /// Access the serializer
    pub fn serializer(&self) -> &S {
        &self.serializer
    }
}

/// Builder for creating TypedKafkaProducer with configuration options
pub struct TypedProducerBuilder<T, S, C = LoggingProducerContext> 
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    brokers: String,
    default_topic: String,
    serializer: S,
    context: Option<C>,
    _phantom: PhantomData<T>,
}

impl<T, S> TypedProducerBuilder<T, S, LoggingProducerContext>
where
    S: Serializer<T>,
{
    /// Creates a new builder with required parameters
    pub fn new(brokers: &str, default_topic: &str, serializer: S) -> Self {
        Self {
            brokers: brokers.to_string(),
            default_topic: default_topic.to_string(),
            serializer,
            context: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, S, C> TypedProducerBuilder<T, S, C>
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    /// Sets a custom producer context
    pub fn with_context<NewC>(self, context: NewC) -> TypedProducerBuilder<T, S, NewC>
    where
        NewC: ProducerContext + 'static,
    {
        TypedProducerBuilder {
            brokers: self.brokers,
            default_topic: self.default_topic,
            serializer: self.serializer,
            context: Some(context),
            _phantom: PhantomData,
        }
    }

    /// Builds the TypedKafkaProducer
    pub fn build(self) -> Result<TypedKafkaProducer<T, S, LoggingProducerContext>, KafkaError> {
        let producer = KafkaProducer::<LoggingProducerContext>::new(&self.brokers, &self.default_topic)?;
        Ok(TypedKafkaProducer {
            producer,
            serializer: self.serializer,
            _phantom: PhantomData,
        })
    }
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
    async fn test_typed_producer_builder() {
        let serializer = JsonSerializer;
        let builder = TypedProducerBuilder::<TestMessage, _>::new(
            "localhost:9092",
            "test-topic",
            serializer,
        );

        // This would fail if Kafka isn't running, but demonstrates the API
        let _result = builder.build();
    }
}