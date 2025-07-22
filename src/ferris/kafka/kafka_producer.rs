use crate::ferris::kafka::kafka_producer_def_context::LoggingProducerContext;
use crate::ferris::kafka::serialization::{Serializer, SerializationError};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer, ProducerContext};
use rdkafka::util::Timeout;
use std::marker::PhantomData;
use std::time::Duration;

/// A Kafka producer that handles serialization automatically
pub struct KafkaProducer<T, S, C = LoggingProducerContext> 
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    producer: FutureProducer<C>,
    serializer: S,
    default_topic: String,
    _phantom: PhantomData<T>,
}

/// Error type for producer operations
#[derive(Debug)]
pub enum ProducerError {
    KafkaError(KafkaError),
    SerializationError(SerializationError),
}

impl std::fmt::Display for ProducerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProducerError::KafkaError(e) => write!(f, "Kafka error: {}", e),
            ProducerError::SerializationError(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl std::error::Error for ProducerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProducerError::KafkaError(e) => Some(e),
            ProducerError::SerializationError(e) => Some(e),
        }
    }
}

impl From<KafkaError> for ProducerError {
    fn from(err: KafkaError) -> Self {
        ProducerError::KafkaError(err)
    }
}

impl From<SerializationError> for ProducerError {
    fn from(err: SerializationError) -> Self {
        ProducerError::SerializationError(err)
    }
}

const SEND_WAIT_SECS: u64 = 30;

impl<T, S> KafkaProducer<T, S, LoggingProducerContext>
where
    S: Serializer<T>,
{
    /// Creates a new KafkaProducer with default context
    pub fn new(
        brokers: &str,
        default_topic: &str,
        serializer: S,
    ) -> Result<Self, KafkaError> {
        let producer: FutureProducer<LoggingProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("log_level", "7")
            .create_with_context(LoggingProducerContext::default())?;

        Ok(KafkaProducer {
            producer,
            serializer,
            default_topic: default_topic.to_string(),
            _phantom: PhantomData,
        })
    }
}

impl<T, S, C> KafkaProducer<T, S, C>
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    /// Creates a new KafkaProducer with custom context
    pub fn new_with_context(
        brokers: &str,
        default_topic: &str,
        serializer: S,
        context: C,
    ) -> Result<Self, KafkaError> {
        let producer: FutureProducer<C> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("log_level", "7")
            .create_with_context(context)?;

        Ok(KafkaProducer {
            producer,
            serializer,
            default_topic: default_topic.to_string(),
            _phantom: PhantomData,
        })
    }

    /// Sends a message to the default topic
    pub async fn send(
        &self,
        key: Option<&str>,
        value: &T,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        self.send_to_topic(&self.default_topic, key, value, timestamp).await
    }

    /// Sends a message to the default topic with current timestamp
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

    /// Sends a message to a specific topic
    pub async fn send_to_topic(
        &self,
        topic: &str,
        key: Option<&str>,
        value: &T,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        let payload = self.serializer.serialize(value)?;
        
        let mut record = FutureRecord::to(topic).payload(&payload);
        
        if let Some(k) = key {
            record = record.key(k);
        }
        
        if let Some(ts) = timestamp {
            record = record.timestamp(ts);
        }

        match self.producer.send(record, Timeout::After(Duration::from_secs(SEND_WAIT_SECS))).await {
            Ok(delivery) => Ok(delivery),
            Err((err, _)) => Err(ProducerError::KafkaError(err)),
        }
    }

    /// Flushes any pending messages
    pub fn flush(&self, timeout_ms: u64) -> Result<(), KafkaError> {
        self.producer.flush(Timeout::After(Duration::from_millis(timeout_ms)))
    }

    /// Access the serializer
    pub fn serializer(&self) -> &S {
        &self.serializer
    }
}

/// Builder for creating KafkaProducer with configuration options
pub struct ProducerBuilder<T, S, C = LoggingProducerContext> 
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

impl<T, S> ProducerBuilder<T, S, LoggingProducerContext>
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

    /// Builds the KafkaProducer
    pub fn build(self) -> Result<KafkaProducer<T, S, LoggingProducerContext>, KafkaError> {
        if let Some(context) = self.context {
            // This shouldn't happen for default context builders, but just in case
            KafkaProducer::new_with_context(&self.brokers, &self.default_topic, self.serializer, context)
        } else {
            KafkaProducer::new(&self.brokers, &self.default_topic, self.serializer)
        }
    }
}

impl<T, S, C> ProducerBuilder<T, S, C>
where
    S: Serializer<T>,
    C: ProducerContext + 'static,
{
    /// Sets a custom producer context
    pub fn with_context<NewC>(self, context: NewC) -> ProducerBuilder<T, S, NewC>
    where
        NewC: ProducerContext + 'static,
    {
        ProducerBuilder {
            brokers: self.brokers,
            default_topic: self.default_topic,
            serializer: self.serializer,
            context: Some(context),
            _phantom: PhantomData,
        }
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
    async fn test_producer_builder() {
        let serializer = JsonSerializer;
        let builder = ProducerBuilder::<TestMessage, _>::new(
            "localhost:9092",
            "test-topic",
            serializer,
        );

        // This would fail if Kafka isn't running, but demonstrates the API
        let _result = builder.build();
    }
}