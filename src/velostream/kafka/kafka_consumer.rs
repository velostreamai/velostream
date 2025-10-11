use crate::velostream::kafka::client_config_builder::ClientConfigBuilder;
use crate::velostream::kafka::consumer_config::ConsumerConfig;
use crate::velostream::kafka::headers::Headers;
use crate::velostream::kafka::kafka_error::ConsumerError;
use crate::velostream::kafka::message::Message;
use crate::velostream::kafka::serialization::Serde;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{
    Consumer, ConsumerContext, DefaultConsumerContext, MessageStream, StreamConsumer,
};
use rdkafka::error::KafkaError;
use rdkafka::message::Message as KafkaMessage;
use std::marker::PhantomData;
use std::time::Duration;

/// A Kafka consumer that handles deserialization automatically for keys, values, and headers
///
/// This consumer returns `Message<K, V>` structs containing:
/// - `key: Option<K>` - Deserialized message key
/// - `value: V` - Deserialized message value  
/// - `headers: Headers` - Message headers with metadata
///
/// # Examples
///
/// ## Basic Usage
/// ```rust,no_run
/// use velostream::{KafkaConsumer, JsonSerializer};
/// use serde::{Serialize, Deserialize};
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize, Debug)]
/// struct MyMessage {
///     id: u32,
///     content: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let consumer = KafkaConsumer::<String, MyMessage, _, _>::new(
///         "localhost:9092",
///         "my-group",
///         JsonSerializer,
///         JsonSerializer
///     )?;
///
///     consumer.subscribe(&["my-topic"])?;
///
///     // Poll for messages
///     let message = consumer.poll(Duration::from_secs(5)).await?;
///     println!("Key: {:?}", message.key());
///     println!("Value: {:?}", message.value());
///     println!("Headers: {:?}", message.headers());
///     Ok(())
/// }
/// ```
///
/// ## Stream Processing
/// ```rust,no_run
/// use velostream::{KafkaConsumer, JsonSerializer};
/// use futures::StreamExt;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let consumer = KafkaConsumer::<String, String, _, _>::new(
///         "localhost:9092",
///         "group",
///         JsonSerializer,
///         JsonSerializer
///     )?;
///     
///     consumer.subscribe(&["my-topic"])?;
///
///     consumer.stream()
///         .for_each(|result| async move {
///             if let Ok(message) = result {
///                 // Access headers for routing/filtering
///                 if let Some(source) = message.headers().get("source") {
///                     println!("Message from: {}", source);
///                 }
///                 
///                 // Process the value
///                 println!("Processing: {:?}", message.value());
///             }
///         })
///         .await;
///     Ok(())
/// }
/// ```
pub struct KafkaConsumer<K, V, KS, VS, C = DefaultConsumerContext>
where
    KS: Serde<K>,
    VS: Serde<V>,
    C: ConsumerContext + 'static,
{
    consumer: StreamConsumer<C>,
    key_serde: KS,
    value_serde: VS,
    group_id: String,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

// ConsumerError is now a type alias defined in kafka_error.rs

impl<K, V, KS, VS> KafkaConsumer<K, V, KS, VS, DefaultConsumerContext>
where
    KS: Serde<K>,
    VS: Serde<V>,
{
    /// Creates a new KafkaConsumer with default context and simple configuration
    pub fn new(
        brokers: &str,
        group_id: &str,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Self, KafkaError> {
        let config = ConsumerConfig::new(brokers, group_id);
        Self::with_config(config, key_serializer, value_serializer)
    }

    /// Creates a new KafkaConsumer with custom configuration
    pub fn with_config(
        config: ConsumerConfig,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfigBuilder::new()
            .bootstrap_servers(&config.common.brokers)
            .client_id(config.common.client_id.as_deref())
            .request_timeout(config.common.request_timeout)
            .retry_backoff(config.common.retry_backoff)
            .custom_properties(&config.common.custom_config)
            .build();

        // Set consumer-specific configuration
        client_config
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", config.auto_offset_reset.as_str())
            .set("enable.auto.commit", config.enable_auto_commit.to_string())
            .set(
                "auto.commit.interval.ms",
                config.auto_commit_interval.as_millis().to_string(),
            )
            .set(
                "session.timeout.ms",
                config.session_timeout.as_millis().to_string(),
            )
            .set(
                "heartbeat.interval.ms",
                config.heartbeat_interval.as_millis().to_string(),
            )
            .set("fetch.min.bytes", config.fetch_min_bytes.to_string())
            .set(
                "fetch.message.max.bytes",
                config.max_partition_fetch_bytes.to_string(),
            )
            .set("isolation.level", config.isolation_level.as_str());

        let consumer: StreamConsumer = client_config.create()?;

        Ok(KafkaConsumer {
            consumer,
            key_serde: key_serializer,
            value_serde: value_serializer,
            group_id: config.group_id.clone(),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }
}

impl<K, V, KS, VS, C> KafkaConsumer<K, V, KS, VS, C>
where
    KS: Serde<K>,
    VS: Serde<V>,
    C: ConsumerContext + 'static,
{
    /// Creates a new KafkaConsumer with custom context
    pub fn new_with_context(
        brokers: &str,
        group_id: &str,
        key_serializer: KS,
        value_serializer: VS,
        context: C,
    ) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer<C> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create_with_context(context)?;

        Ok(KafkaConsumer {
            consumer,
            key_serde: key_serializer,
            value_serde: value_serializer,
            group_id: group_id.to_string(),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }

    /// Subscribe to topics
    pub fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        self.consumer.subscribe(topics)
    }

    /// Poll for a message with timeout
    ///
    /// Returns a `Message<K, V>` containing the deserialized key, value, and headers.
    /// This method blocks until a message is available or the timeout expires.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use velostream::{KafkaConsumer, JsonSerializer};
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let consumer = KafkaConsumer::<String, String, _, _>::new(
    ///         "localhost:9092",
    ///         "group",
    ///         JsonSerializer,
    ///         JsonSerializer
    ///     )?;
    ///     consumer.subscribe(&["my-topic"])?;
    ///     
    ///     match consumer.poll(Duration::from_secs(5)).await {
    ///         Ok(message) => {
    ///             println!("Key: {:?}", message.key());
    ///             println!("Value: {}", message.value());
    ///             
    ///             // Process headers
    ///             if let Some(source) = message.headers().get("source") {
    ///                 println!("Message from: {}", source);
    ///             }
    ///         }
    ///         Err(e) => println!("No message received: {}", e),
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn poll(&self, timeout: Duration) -> Result<Message<K, V>, ConsumerError> {
        use tokio::time;
        let mut stream = self.consumer.stream();

        match time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().ok_or(ConsumerError::NoMessage)?;

                // Attempt value deserialization
                let value = match self.value_serde.deserialize(payload) {
                    Ok(v) => {
                        log::debug!(
                            target: "kafka_consumer",
                            topic = msg.topic(),
                            partition = msg.partition(),
                            offset = msg.offset(),
                            payload_size = payload.len();
                            "Message received"
                        );
                        v
                    }
                    Err(e) => {
                        log::error!(
                            target: "kafka_consumer",
                            "Failed to deserialize message value: topic={} partition={} offset={} payload_size={} error={:?}",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                            payload.len(),
                            e
                        );
                        return Err(ConsumerError::SerializationError(e));
                    }
                };

                let key = if let Some(key_bytes) = msg.key() {
                    Some(self.key_serde.deserialize(key_bytes)?)
                } else {
                    None
                };

                let headers = if let Some(kafka_headers) = msg.headers() {
                    Headers::from_rdkafka_headers(kafka_headers)
                } else {
                    Headers::new()
                };

                let partition = msg.partition();
                let offset = msg.offset();
                let timestamp = match msg.timestamp() {
                    rdkafka::Timestamp::NotAvailable => None,
                    rdkafka::Timestamp::CreateTime(t) | rdkafka::Timestamp::LogAppendTime(t) => {
                        Some(t)
                    }
                };

                Ok(Message::new(
                    key, value, headers, partition, offset, timestamp,
                ))
            }
            Ok(Some(Err(e))) => Err(ConsumerError::KafkaError(e)),
            Ok(None) => Err(ConsumerError::NoMessage),
            Err(_) => Err(ConsumerError::Timeout),
        }
    }

    /// Get a stream of raw Kafka messages (for advanced use cases)
    pub fn raw_stream(&self) -> MessageStream<'_, C> {
        self.consumer.stream()
    }

    /// Get a stream that yields deserialized typed messages
    ///
    /// Returns a stream of `Result<Message<K, V>, ConsumerError>` where each message
    /// contains the deserialized key, value, and headers. This enables reactive
    /// processing patterns and functional composition.
    ///
    /// # Examples
    ///
    /// ## Basic Stream Processing
    /// ```rust,no_run
    /// use velostream::{KafkaConsumer, JsonSerializer};
    /// use futures::StreamExt;
    ///
    /// fn handle_user_created(_: velostream::Message<String, String>) {}
    /// fn handle_user_updated(_: velostream::Message<String, String>) {}
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let consumer = KafkaConsumer::<String, String, _, _>::new(
    ///         "localhost:9092",
    ///         "group",
    ///         JsonSerializer,
    ///         JsonSerializer
    ///     )?;
    ///     
    ///     consumer.stream()
    ///         .for_each(|result| async move {
    ///             match result {
    ///                 Ok(message) => {
    ///                     println!("Processing: {:?}", message.value());
    ///                     
    ///                     // Route based on headers
    ///                     match message.headers().get("event-type") {
    ///                         Some("user-created") => handle_user_created(message),
    ///                         Some("user-updated") => handle_user_updated(message),
    ///                         _ => println!("Unknown event type"),
    ///                     }
    ///                 }
    ///                 Err(e) => eprintln!("Error: {}", e),
    ///             }
    ///         })
    ///         .await;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Filtering by Headers
    /// ```rust,no_run
    /// use velostream::{KafkaConsumer, JsonSerializer};
    /// use futures::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let consumer = KafkaConsumer::<String, String, _, _>::new(
    ///         "localhost:9092",
    ///         "group",
    ///         JsonSerializer,
    ///         JsonSerializer
    ///     )?;
    ///     
    ///     let important_messages: Vec<_> = consumer.stream()
    ///         .filter_map(|result| async move { result.ok() })
    ///         .filter(|message| {
    ///             futures::future::ready(
    ///                 message.headers().get("priority") == Some("high")
    ///             )
    ///         })
    ///         .take(10)
    ///         .collect()
    ///         .await;
    ///     Ok(())
    /// }
    /// ```
    pub fn stream(&self) -> impl futures::Stream<Item = Result<Message<K, V>, ConsumerError>> + '_ {
        self.consumer.stream().map(|msg_result| match msg_result {
            Ok(borrowed_message) => {
                if let Some(payload) = borrowed_message.payload() {
                    let value = self
                        .value_serde
                        .deserialize(payload)
                        .map_err(ConsumerError::SerializationError)?;

                    let key = if let Some(key_bytes) = borrowed_message.key() {
                        Some(
                            self.key_serde
                                .deserialize(key_bytes)
                                .map_err(ConsumerError::SerializationError)?,
                        )
                    } else {
                        None
                    };

                    let headers = if let Some(kafka_headers) = borrowed_message.headers() {
                        Headers::from_rdkafka_headers(kafka_headers)
                    } else {
                        Headers::new()
                    };

                    let partition = borrowed_message.partition();
                    let offset = borrowed_message.offset();
                    let timestamp = match borrowed_message.timestamp() {
                        rdkafka::Timestamp::NotAvailable => None,
                        rdkafka::Timestamp::CreateTime(t)
                        | rdkafka::Timestamp::LogAppendTime(t) => Some(t),
                    };

                    Ok(Message::new(
                        key, value, headers, partition, offset, timestamp,
                    ))
                } else {
                    Err(ConsumerError::NoMessage)
                }
            }
            Err(e) => Err(ConsumerError::KafkaError(e)),
        })
    }

    /// Commit the current consumer state
    pub fn commit(&self) -> Result<(), KafkaError> {
        use rdkafka::consumer::{CommitMode, Consumer};
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }

    /// Access the key serializer
    pub fn key_serializer(&self) -> &KS {
        &self.key_serde
    }

    /// Access the value serializer
    pub fn value_serializer(&self) -> &VS {
        &self.value_serde
    }

    /// Get current consumer offsets for transaction coordination
    pub fn current_offsets(&self) -> Result<rdkafka::TopicPartitionList, KafkaError> {
        use rdkafka::consumer::Consumer;
        self.consumer.assignment()
    }

    /// Get consumer group ID for transaction coordination
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
}

/// Metadata associated with a Kafka message
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MessageMetadata {
    pub key: Option<String>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

/// Builder for creating KafkaConsumer with configuration options
pub struct ConsumerBuilder<K, V, KS, VS, C = DefaultConsumerContext>
where
    KS: Serde<K>,
    VS: Serde<V>,
    C: ConsumerContext + 'static,
{
    brokers: String,
    group_id: String,
    key_serializer: KS,
    value_serializer: VS,
    _context: Option<C>,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V, KS, VS> ConsumerBuilder<K, V, KS, VS, DefaultConsumerContext>
where
    KS: Serde<K>,
    VS: Serde<V>,
{
    /// Creates a new builder with required parameters
    pub fn new(brokers: &str, group_id: &str, key_serializer: KS, value_serializer: VS) -> Self {
        Self {
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            key_serializer,
            value_serializer,
            _context: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Builds the KafkaConsumer
    pub fn build(self) -> Result<KafkaConsumer<K, V, KS, VS, DefaultConsumerContext>, KafkaError> {
        KafkaConsumer::new(
            &self.brokers,
            &self.group_id,
            self.key_serializer,
            self.value_serializer,
        )
    }
}

impl<K, V, KS, VS, C> ConsumerBuilder<K, V, KS, VS, C>
where
    KS: Serde<K>,
    VS: Serde<V>,
    C: ConsumerContext + 'static,
{
    /// Sets a custom consumer context
    pub fn with_context<NewC>(self, context: NewC) -> ConsumerBuilder<K, V, KS, VS, NewC>
    where
        NewC: ConsumerContext + 'static,
    {
        ConsumerBuilder {
            brokers: self.brokers,
            group_id: self.group_id,
            key_serializer: self.key_serializer,
            value_serializer: self.value_serializer,
            _context: Some(context),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }
}

/// Convenience trait for types that can be consumed from Kafka with specific key and value serializers
pub trait KafkaConsumable<K, KS, VS>: Sized
where
    KS: Serde<K>,
    VS: Serde<Self>,
{
    /// Creates a consumer for this type
    fn consumer(
        brokers: &str,
        group_id: &str,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<KafkaConsumer<K, Self, KS, VS>, KafkaError> {
        KafkaConsumer::new(brokers, group_id, key_serializer, value_serializer)
    }

    /// Creates a consumer builder for this type
    fn consumer_builder(
        brokers: &str,
        group_id: &str,
        key_serializer: KS,
        value_serializer: VS,
    ) -> ConsumerBuilder<K, Self, KS, VS> {
        ConsumerBuilder::new(brokers, group_id, key_serializer, value_serializer)
    }
}

// Implement for any type that can be serialized/deserialized
impl<K, V, KS, VS> KafkaConsumable<K, KS, VS> for V
where
    KS: Serde<K>,
    VS: Serde<V>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::kafka::serialization::JsonSerializer;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestMessage {
        id: u32,
        content: String,
    }

    #[tokio::test]
    async fn test_consumer_builder() {
        let key_serializer = JsonSerializer;
        let value_serializer = JsonSerializer;
        let builder = ConsumerBuilder::<String, TestMessage, _, _>::new(
            "localhost:9092",
            "test-group",
            key_serializer,
            value_serializer,
        );

        // This would fail if Kafka isn't running, but demonstrates the API
        let _result = builder.build();
    }

    #[tokio::test]
    async fn test_consumable_trait() {
        let key_serializer = JsonSerializer;
        let value_serializer = JsonSerializer;
        let _consumer: Result<KafkaConsumer<String, TestMessage, _, _>, _> = TestMessage::consumer(
            "localhost:9092",
            "test-group",
            key_serializer,
            value_serializer,
        );
    }
}
