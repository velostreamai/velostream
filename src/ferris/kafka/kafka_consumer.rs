use crate::ferris::kafka::serialization::{Serializer, SerializationError};
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Message as KafkaMessage, Headers as KafkaHeaders};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

/// Custom headers type that provides a clean API for Kafka message headers
/// 
/// `Headers` wraps a `HashMap<String, Option<String>>` to provide an ergonomic interface
/// for working with Kafka message headers. It supports both valued headers and null headers,
/// and provides builder-pattern methods for easy construction.
/// 
/// # Examples
/// 
/// ## Creating Headers
/// ```rust
/// # use ferrisstreams::ferris::kafka::Headers;
/// let headers = Headers::new()
///     .insert("source", "web-api")
///     .insert("version", "1.2.3")
///     .insert("trace-id", "abc-123-def")
///     .insert_null("optional-field");
/// ```
/// 
/// ## Querying Headers
/// ```rust
/// # use ferrisstreams::ferris::kafka::Headers;
/// # let headers = Headers::new().insert("source", "web-api");
/// // Get a header value
/// if let Some(source) = headers.get("source") {
///     println!("Source: {}", source);
/// }
/// 
/// // Check if header exists
/// if headers.contains_key("source") {
///     println!("Has source header");
/// }
/// 
/// // Iterate over all headers
/// for (key, value) in headers.iter() {
///     match value {
///         Some(v) => println!("{}: {}", key, v),
///         None => println!("{}: <null>", key),
///     }
/// }
/// ```
/// 
/// ## Integration with Messages
/// ```rust,no_run
/// # use ferrisstreams::{KafkaConsumer, JsonSerializer};
/// # use std::time::Duration;
/// # let consumer = KafkaConsumer::<String, String, _, _>::new("localhost:9092", "group", JsonSerializer, JsonSerializer)?;
/// let message = consumer.poll_message(Duration::from_secs(5)).await?;
/// 
/// // Access message headers
/// let headers = message.headers();
/// if let Some(event_type) = headers.get("event-type") {
///     match event_type {
///         "user-created" => handle_user_created(message.value()),
///         "user-updated" => handle_user_updated(message.value()),
///         _ => println!("Unknown event type: {}", event_type),
///     }
/// }
/// # fn handle_user_created(_: &String) {}
/// # fn handle_user_updated(_: &String) {}
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Headers {
    inner: HashMap<String, Option<String>>,
}

impl Headers {
    /// Creates a new empty headers collection
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Creates a new headers collection with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(capacity),
        }
    }

    /// Inserts a header with a value
    pub fn insert(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner.insert(key.into(), Some(value.into()));
        self
    }

    /// Inserts a header with no value (null header)
    pub fn insert_null(mut self, key: impl Into<String>) -> Self {
        self.inner.insert(key.into(), None);
        self
    }

    /// Gets a header value by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).and_then(|v| v.as_deref())
    }

    /// Gets a header value by key, including null values
    pub fn get_optional(&self, key: &str) -> Option<&Option<String>> {
        self.inner.get(key)
    }

    /// Checks if a header exists (regardless of value)
    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    /// Returns the number of headers
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if there are no headers
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterates over all headers
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Option<String>)> {
        self.inner.iter()
    }

    /// Converts to rdkafka OwnedHeaders for internal use
    pub(crate) fn to_rdkafka_headers(&self) -> rdkafka::message::OwnedHeaders {
        let mut headers = rdkafka::message::OwnedHeaders::new_with_capacity(self.inner.len());
        
        for (key, value) in &self.inner {
            let header = rdkafka::message::Header {
                key,
                value: value.as_deref(),
            };
            headers = headers.insert(header);
        }
        
        headers
    }

    /// Creates Headers from rdkafka headers
    pub(crate) fn from_rdkafka_headers<H: KafkaHeaders>(kafka_headers: &H) -> Self {
        let mut headers = HashMap::with_capacity(kafka_headers.count());
        
        for i in 0..kafka_headers.count() {
            let header = kafka_headers.get(i);
            let key = header.key.to_string();
            let value = header.value.map(|v| {
                // Convert bytes to string, using lossy conversion if needed
                String::from_utf8_lossy(v).into_owned()
            });
            headers.insert(key, value);
        }
        
        Self { inner: headers }
    }
}

impl Default for Headers {
    fn default() -> Self {
        Self::new()
    }
}

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
/// use ferrisstreams::{KafkaConsumer, JsonSerializer};
/// use std::time::Duration;
/// 
/// let consumer = KafkaConsumer::<String, MyMessage, _, _>::new(
///     "localhost:9092",
///     "my-group", 
///     JsonSerializer,
///     JsonSerializer
/// )?;
/// 
/// consumer.subscribe(&["my-topic"])?;
/// 
/// // Poll for messages
/// let message = consumer.poll_message(Duration::from_secs(5)).await?;
/// println!("Key: {:?}", message.key());
/// println!("Value: {:?}", message.value());
/// println!("Headers: {:?}", message.headers());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// ## Stream Processing
/// ```rust,no_run
/// # use ferrisstreams::{KafkaConsumer, JsonSerializer};
/// # let consumer = KafkaConsumer::<String, String, _, _>::new("localhost:9092", "group", JsonSerializer, JsonSerializer)?;
/// use futures::StreamExt;
/// 
/// consumer.stream()
///     .for_each(|result| async move {
///         if let Ok(message) = result {
///             // Access headers for routing/filtering
///             if let Some(source) = message.headers().get("source") {
///                 println!("Message from: {}", source);
///             }
///             
///             // Process the value
///             println!("Processing: {:?}", message.value());
///         }
///     })
///     .await;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct KafkaConsumer<K, V, KS, VS, C = DefaultConsumerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ConsumerContext + 'static,
{
    consumer: StreamConsumer<C>,
    key_serializer: KS,
    value_serializer: VS,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
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

/// A message containing deserialized key, value, and headers
/// 
/// This struct represents a complete Kafka message with type-safe access to all components:
/// - **Key**: Optional deserialized key of type `K`
/// - **Value**: Deserialized message payload of type `V`
/// - **Headers**: Message metadata as a `Headers` collection
/// 
/// # Examples
/// 
/// ```rust,no_run
/// # use ferrisstreams::ferris::kafka::{Message, Headers};
/// # let message = Message::new(Some("key".to_string()), "value".to_string(), Headers::new());
/// // Access by reference (borrowing)
/// println!("Key: {:?}", message.key());
/// println!("Value: {}", message.value());
/// println!("Headers: {:?}", message.headers());
/// 
/// // Check for specific headers
/// if let Some(source) = message.headers().get("source") {
///     println!("Message originated from: {}", source);
/// }
/// 
/// // Consume the message (take ownership)
/// let (key, value, headers) = message.into_parts();
/// // Now you own the key, value, and headers
/// ```
/// 
/// See the [consumer with headers example](https://github.com/your-repo/examples/consumer_with_headers.rs) 
/// for a comprehensive demonstration.
#[derive(Debug)]
pub struct Message<K, V> {
    pub key: Option<K>,
    pub value: V,
    pub headers: Headers,
}

impl<K, V> Message<K, V> {
    pub fn new(key: Option<K>, value: V, headers: Headers) -> Self {
        Self { key, value, headers }
    }

    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    pub fn into_key(self) -> Option<K> {
        self.key
    }

    pub fn into_value(self) -> V {
        self.value
    }

    pub fn into_headers(self) -> Headers {
        self.headers
    }

    pub fn into_parts(self) -> (Option<K>, V, Headers) {
        (self.key, self.value, self.headers)
    }
}

impl<K, V, KS, VS> KafkaConsumer<K, V, KS, VS, DefaultConsumerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    /// Creates a new KafkaConsumer with default context
    pub fn new(brokers: &str, group_id: &str, key_serializer: KS, value_serializer: VS) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create()?;

        Ok(KafkaConsumer {
            consumer,
            key_serializer,
            value_serializer,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }
}

impl<K, V, KS, VS, C> KafkaConsumer<K, V, KS, VS, C>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ConsumerContext + 'static,
{
    /// Creates a new KafkaConsumer with custom context
    pub fn new_with_context(brokers: &str, group_id: &str, key_serializer: KS, value_serializer: VS, context: C) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer<C> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .create_with_context(context)?;

        Ok(KafkaConsumer {
            consumer,
            key_serializer,
            value_serializer,
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
    /// # use ferrisstreams::{KafkaConsumer, JsonSerializer};
    /// # use std::time::Duration;
    /// # let consumer = KafkaConsumer::<String, String, _, _>::new("localhost:9092", "group", JsonSerializer, JsonSerializer)?;
    /// match consumer.poll_message(Duration::from_secs(5)).await {
    ///     Ok(message) => {
    ///         println!("Key: {:?}", message.key());
    ///         println!("Value: {}", message.value());
    ///         
    ///         // Process headers
    ///         if let Some(source) = message.headers().get("source") {
    ///             println!("Message from: {}", source);
    ///         }
    ///     }
    ///     Err(e) => println!("No message received: {}", e),
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn poll_message(&self, timeout: Duration) -> Result<Message<K, V>, ConsumerError> {
        use tokio::time;
        let mut stream = self.consumer.stream();
        
        match time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let payload = msg.payload().ok_or(ConsumerError::NoMessage)?;
                let value = self.value_serializer.deserialize(payload)?;
                
                let key = if let Some(key_bytes) = msg.key() {
                    Some(self.key_serializer.deserialize(key_bytes)?)
                } else {
                    None
                };

                let headers = if let Some(kafka_headers) = msg.headers() {
                    Headers::from_rdkafka_headers(kafka_headers)
                } else {
                    Headers::new()
                };
                
                Ok(Message::new(key, value, headers))
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
    /// 
    /// Returns a stream of `Result<Message<K, V>, ConsumerError>` where each message
    /// contains the deserialized key, value, and headers. This enables reactive
    /// processing patterns and functional composition.
    /// 
    /// # Examples
    /// 
    /// ## Basic Stream Processing
    /// ```rust,no_run
    /// # use ferrisstreams::{KafkaConsumer, JsonSerializer};
    /// # use futures::StreamExt;
    /// # let consumer = KafkaConsumer::<String, String, _, _>::new("localhost:9092", "group", JsonSerializer, JsonSerializer)?;
    /// consumer.stream()
    ///     .for_each(|result| async move {
    ///         match result {
    ///             Ok(message) => {
    ///                 println!("Processing: {:?}", message.value());
    ///                 
    ///                 // Route based on headers
    ///                 match message.headers().get("event-type") {
    ///                     Some("user-created") => handle_user_created(message),
    ///                     Some("user-updated") => handle_user_updated(message),
    ///                     _ => println!("Unknown event type"),
    ///                 }
    ///             }
    ///             Err(e) => eprintln!("Error: {}", e),
    ///         }
    ///     })
    ///     .await;
    /// # fn handle_user_created(_: ferrisstreams::ferris::kafka::Message<String, String>) {}
    /// # fn handle_user_updated(_: ferrisstreams::ferris::kafka::Message<String, String>) {}
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    /// 
    /// ## Filtering by Headers
    /// ```rust,no_run
    /// # use ferrisstreams::{KafkaConsumer, JsonSerializer};
    /// # use futures::StreamExt;
    /// # let consumer = KafkaConsumer::<String, String, _, _>::new("localhost:9092", "group", JsonSerializer, JsonSerializer)?;
    /// let important_messages: Vec<_> = consumer.stream()
    ///     .filter_map(|result| async move { result.ok() })
    ///     .filter(|message| {
    ///         futures::future::ready(
    ///             message.headers().get("priority") == Some("high")
    ///         )
    ///     })
    ///     .take(10)
    ///     .collect()
    ///     .await;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn stream(&self) -> impl futures::Stream<Item = Result<Message<K, V>, ConsumerError>> + '_ {
        self.consumer.stream().map(|msg_result| {
            match msg_result {
                Ok(borrowed_message) => {
                    if let Some(payload) = borrowed_message.payload() {
                        let value = self.value_serializer
                            .deserialize(payload)
                            .map_err(ConsumerError::SerializationError)?;
                        
                        let key = if let Some(key_bytes) = borrowed_message.key() {
                            Some(self.key_serializer
                                .deserialize(key_bytes)
                                .map_err(ConsumerError::SerializationError)?)
                        } else {
                            None
                        };

                        let headers = if let Some(kafka_headers) = borrowed_message.headers() {
                            Headers::from_rdkafka_headers(kafka_headers)
                        } else {
                            Headers::new()
                        };
                        
                        Ok(Message::new(key, value, headers))
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

    /// Access the key serializer
    pub fn key_serializer(&self) -> &KS {
        &self.key_serializer
    }

    /// Access the value serializer
    pub fn value_serializer(&self) -> &VS {
        &self.value_serializer
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
pub struct ConsumerBuilder<K, V, KS, VS, C = DefaultConsumerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ConsumerContext + 'static,
{
    brokers: String,
    group_id: String,
    key_serializer: KS,
    value_serializer: VS,
    context: Option<C>,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<K, V, KS, VS> ConsumerBuilder<K, V, KS, VS, DefaultConsumerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    /// Creates a new builder with required parameters
    pub fn new(brokers: &str, group_id: &str, key_serializer: KS, value_serializer: VS) -> Self {
        Self {
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            key_serializer,
            value_serializer,
            context: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Builds the KafkaConsumer
    pub fn build(self) -> Result<KafkaConsumer<K, V, KS, VS, DefaultConsumerContext>, KafkaError> {
        KafkaConsumer::new(&self.brokers, &self.group_id, self.key_serializer, self.value_serializer)
    }
}

impl<K, V, KS, VS, C> ConsumerBuilder<K, V, KS, VS, C>
where
    KS: Serializer<K>,
    VS: Serializer<V>, 
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
            context: Some(context),
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

}

/// Convenience trait for types that can be consumed from Kafka with specific key and value serializers
pub trait KafkaConsumable<K, KS, VS>: Sized
where
    KS: Serializer<K>,
    VS: Serializer<Self>,
{
    /// Creates a consumer for this type
    fn consumer(brokers: &str, group_id: &str, key_serializer: KS, value_serializer: VS) -> Result<KafkaConsumer<K, Self, KS, VS>, KafkaError> {
        KafkaConsumer::new(brokers, group_id, key_serializer, value_serializer)
    }

    /// Creates a consumer builder for this type
    fn consumer_builder(brokers: &str, group_id: &str, key_serializer: KS, value_serializer: VS) -> ConsumerBuilder<K, Self, KS, VS> {
        ConsumerBuilder::new(brokers, group_id, key_serializer, value_serializer)
    }
}

// Implement for any type that can be serialized/deserialized
impl<K, V, KS, VS> KafkaConsumable<K, KS, VS> for V
where
    KS: Serializer<K>,
    VS: Serializer<V>,
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
        let _consumer: Result<KafkaConsumer<String, TestMessage, _, _>, _> = TestMessage::consumer("localhost:9092", "test-group", key_serializer, value_serializer);
    }
}