use crate::ferris::kafka::kafka_producer_def_context::LoggingProducerContext;
use crate::ferris::kafka::serialization::{Serializer, SerializationError};
use crate::ferris::kafka::kafka_consumer::Headers; // Import Headers from consumer module
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer, ProducerContext};
use rdkafka::util::Timeout;
use std::marker::PhantomData;
use std::time::Duration;

/// A Kafka producer that handles serialization automatically for keys, values, and headers
/// 
/// This producer supports sending typed messages with:
/// - **Keys**: Optional typed keys with dedicated serializer
/// - **Values**: Typed message payloads with dedicated serializer  
/// - **Headers**: Rich metadata support via the `Headers` type
/// 
/// # Examples
/// 
/// ## Basic Usage
/// ```rust,no_run
/// use ferrisstreams::{KafkaProducer, JsonSerializer};
/// use ferrisstreams::ferris::kafka::Headers;
/// 
/// let producer = KafkaProducer::<String, MyMessage, _, _>::new(
///     "localhost:9092",
///     "my-topic",
///     JsonSerializer,  // Key serializer
///     JsonSerializer   // Value serializer
/// )?;
/// 
/// let headers = Headers::new()
///     .insert("source", "web-api")
///     .insert("version", "1.0.0");
/// 
/// producer.send(Some(&key), &message, headers, None).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// ## Headers for Message Routing
/// ```rust,no_run
/// # use ferrisstreams::{KafkaProducer, JsonSerializer};
/// # use ferrisstreams::ferris::kafka::Headers;
/// # let producer = KafkaProducer::<String, String, _, _>::new("localhost:9092", "topic", JsonSerializer, JsonSerializer)?;
/// // Add routing and tracing headers
/// let headers = Headers::new()
///     .insert("event-type", "order-created")
///     .insert("source-service", "order-api")
///     .insert("trace-id", "abc-123-def")
///     .insert("user-id", "user-456");
/// 
/// producer.send(
///     Some(&"order-123".to_string()),
///     &order_data,
///     headers,
///     None
/// ).await?;
/// # let order_data = "data";
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct KafkaProducer<K, V, KS, VS, C = LoggingProducerContext> 
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ProducerContext + 'static,
{
    producer: FutureProducer<C>,
    key_serializer: KS,
    value_serializer: VS,
    default_topic: String,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
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

impl<K, V, KS, VS> KafkaProducer<K, V, KS, VS, LoggingProducerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    /// Creates a new KafkaProducer with default context
    pub fn new(
        brokers: &str,
        default_topic: &str,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Self, KafkaError> {
        let producer: FutureProducer<LoggingProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("log_level", "7")
            .create_with_context(LoggingProducerContext::default())?;

        Ok(KafkaProducer {
            producer,
            key_serializer,
            value_serializer,
            default_topic: default_topic.to_string(),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }
}

impl<K, V, KS, VS, C> KafkaProducer<K, V, KS, VS, C>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ProducerContext + 'static,
{
    /// Creates a new KafkaProducer with custom context
    pub fn new_with_context(
        brokers: &str,
        default_topic: &str,
        key_serializer: KS,
        value_serializer: VS,
        context: C,
    ) -> Result<Self, KafkaError> {
        let producer: FutureProducer<C> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("log_level", "7")
            .create_with_context(context)?;

        Ok(KafkaProducer {
            producer,
            key_serializer,
            value_serializer,
            default_topic: default_topic.to_string(),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }

    /// Sends a message to the default topic
    pub async fn send(
        &self,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        self.send_to_topic(&self.default_topic, key, value, headers, timestamp).await
    }

    /// Sends a message to the default topic with current timestamp
    pub async fn send_with_current_timestamp(
        &self,
        key: Option<&K>,
        value: &V,
        headers: Headers,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        self.send(key, value, headers, Some(timestamp)).await
    }

    /// Sends a message to a specific topic
    pub async fn send_to_topic(
        &self,
        topic: &str,
        key: Option<&K>,
        value: &V,
        headers: Headers,
        timestamp: Option<i64>,
    ) -> Result<rdkafka::producer::future_producer::Delivery, ProducerError> {
        let payload = self.value_serializer.serialize(value)?;
        
        let mut record = FutureRecord::to(topic).payload(&payload);
        
        let key_bytes = if let Some(k) = key {
            Some(self.key_serializer.serialize(k)?)
        } else {
            None
        };
        
        if let Some(ref kb) = key_bytes {
            record = record.key(kb);
        }

        // Add headers if not empty
        if !headers.is_empty() {
            let rdkafka_headers = headers.to_rdkafka_headers();
            record = record.headers(rdkafka_headers);
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

    /// Access the key serializer
    pub fn key_serializer(&self) -> &KS {
        &self.key_serializer
    }

    /// Access the value serializer
    pub fn value_serializer(&self) -> &VS {
        &self.value_serializer
    }
}

/// Builder for creating KafkaProducer with configuration options
pub struct ProducerBuilder<K, V, KS, VS, C = LoggingProducerContext> 
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ProducerContext + 'static,
{
    brokers: String,
    default_topic: String,
    key_serializer: KS,
    value_serializer: VS,
    context: Option<C>,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V, KS, VS> ProducerBuilder<K, V, KS, VS, LoggingProducerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    /// Creates a new builder with required parameters
    pub fn new(brokers: &str, default_topic: &str, key_serializer: KS, value_serializer: VS) -> Self {
        Self {
            brokers: brokers.to_string(),
            default_topic: default_topic.to_string(),
            key_serializer,
            value_serializer,
            context: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Builds the KafkaProducer
    pub fn build(self) -> Result<KafkaProducer<K, V, KS, VS, LoggingProducerContext>, KafkaError> {
        if let Some(context) = self.context {
            // This shouldn't happen for default context builders, but just in case
            KafkaProducer::new_with_context(&self.brokers, &self.default_topic, self.key_serializer, self.value_serializer, context)
        } else {
            KafkaProducer::new(&self.brokers, &self.default_topic, self.key_serializer, self.value_serializer)
        }
    }
}

impl<K, V, KS, VS, C> ProducerBuilder<K, V, KS, VS, C>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ProducerContext + 'static,
{
    /// Sets a custom producer context
    pub fn with_context<NewC>(self, context: NewC) -> ProducerBuilder<K, V, KS, VS, NewC>
    where
        NewC: ProducerContext + 'static,
    {
        ProducerBuilder {
            brokers: self.brokers,
            default_topic: self.default_topic,
            key_serializer: self.key_serializer,
            value_serializer: self.value_serializer,
            context: Some(context),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
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
        let key_serializer = JsonSerializer;
        let value_serializer = JsonSerializer;
        let builder = ProducerBuilder::<String, TestMessage, _, _>::new(
            "localhost:9092",
            "test-topic",
            key_serializer,
            value_serializer,
        );

        // This would fail if Kafka isn't running, but demonstrates the API
        let _result = builder.build();
    }
}