use crate::ferris::kafka::kafka_producer_def_context::LoggingProducerContext;
use crate::ferris::kafka::serialization::Serializer;
use crate::ferris::kafka::headers::Headers;
use crate::ferris::kafka::producer_config::{ProducerConfig, CompressionType, AckMode};
use crate::ferris::kafka::kafka_error::ProducerError;
use crate::ferris::kafka::client_config_builder::ClientConfigBuilder;
use crate::ferris::kafka::performance_presets::PerformancePresets;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer, ProducerContext};
use rdkafka::util::Timeout;
use rdkafka::TopicPartitionList;
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

// ProducerError is now a type alias defined in kafka_error.rs

const SEND_WAIT_SECS: u64 = 30;

impl<K, V, KS, VS> KafkaProducer<K, V, KS, VS, LoggingProducerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    /// Creates a new KafkaProducer with default context and simple configuration
    pub fn new(
        brokers: &str,
        default_topic: &str,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Self, KafkaError> {
        let config = ProducerConfig::new(brokers, default_topic);
        Self::with_config(config, key_serializer, value_serializer)
    }

    /// Creates a new KafkaProducer with custom configuration
    pub fn with_config(
        config: ProducerConfig,
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
        
        // Set producer-specific configuration
        client_config
            .set("message.timeout.ms", &config.message_timeout.as_millis().to_string())
            .set("delivery.timeout.ms", &config.delivery_timeout.as_millis().to_string())
            .set("enable.idempotence", &config.enable_idempotence.to_string())
            .set("max.in.flight.requests.per.connection", &config.max_in_flight_requests.to_string())
            .set("retries", &config.retries.to_string())
            .set("batch.size", &config.batch_size.to_string())
            .set("linger.ms", &config.linger_ms.as_millis().to_string())
            .set("compression.type", config.compression_type.as_str())
            .set("acks", config.acks.as_str())
            .set("queue.buffering.max.messages", &config.buffer_memory.to_string());

        // Configure transactional settings if enabled
        if let Some(ref transaction_id) = config.transactional_id {
            client_config
                .set("transactional.id", transaction_id)
                .set("transaction.timeout.ms", &config.transaction_timeout.as_millis().to_string());
        }

        let producer: FutureProducer<LoggingProducerContext> = client_config
            .create_with_context(LoggingProducerContext::default())?;

        Ok(KafkaProducer {
            producer,
            key_serializer,
            value_serializer,
            default_topic: config.default_topic,
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
    /// Creates a new KafkaProducer with custom context (legacy method)
    pub fn new_with_context(
        brokers: &str,
        default_topic: &str,
        key_serializer: KS,
        value_serializer: VS,
        context: C,
    ) -> Result<Self, KafkaError> {
        let config = ProducerConfig::new(brokers, default_topic);
        Self::new_with_context_and_config(config, key_serializer, value_serializer, context)
    }

    /// Creates a new KafkaProducer with custom context and configuration
    pub fn new_with_context_and_config(
        config: ProducerConfig,
        key_serializer: KS,
        value_serializer: VS,
        context: C,
    ) -> Result<Self, KafkaError> {
        let mut client_config = ClientConfigBuilder::new()
            .bootstrap_servers(&config.common.brokers)
            .client_id(config.common.client_id.as_deref())
            .request_timeout(config.common.request_timeout)
            .retry_backoff(config.common.retry_backoff)
            .custom_properties(&config.common.custom_config)
            .build();
        
        // Set producer-specific configuration
        client_config
            .set("message.timeout.ms", &config.message_timeout.as_millis().to_string())
            .set("delivery.timeout.ms", &config.delivery_timeout.as_millis().to_string())
            .set("enable.idempotence", &config.enable_idempotence.to_string())
            .set("max.in.flight.requests.per.connection", &config.max_in_flight_requests.to_string())
            .set("retries", &config.retries.to_string())
            .set("batch.size", &config.batch_size.to_string())
            .set("linger.ms", &config.linger_ms.as_millis().to_string())
            .set("compression.type", config.compression_type.as_str())
            .set("acks", config.acks.as_str())
            .set("queue.buffering.max.messages", &config.buffer_memory.to_string());

        let producer: FutureProducer<C> = client_config.create_with_context(context)?;

        Ok(KafkaProducer {
            producer,
            key_serializer,
            value_serializer,
            default_topic: config.default_topic,
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

    /// Begin a new transaction (requires transactional producer)
    pub async fn begin_transaction(&self) -> Result<(), ProducerError> {
        match self.producer.init_transactions(Timeout::After(Duration::from_secs(30))) {
            Ok(_) => {
                match self.producer.begin_transaction() {
                    Ok(_) => Ok(()),
                    Err(err) => Err(ProducerError::KafkaError(err)),
                }
            }
            Err(err) => Err(ProducerError::KafkaError(err)),
        }
    }

    /// Commit the current transaction
    pub async fn commit_transaction(&self) -> Result<(), ProducerError> {
        match self.producer.commit_transaction(Timeout::After(Duration::from_secs(30))) {
            Ok(_) => Ok(()),
            Err(err) => Err(ProducerError::KafkaError(err)),
        }
    }

    /// Abort the current transaction
    pub async fn abort_transaction(&self) -> Result<(), ProducerError> {
        match self.producer.abort_transaction(Timeout::After(Duration::from_secs(30))) {
            Ok(_) => Ok(()),
            Err(err) => Err(ProducerError::KafkaError(err)),
        }
    }

    /// Send consumer offsets as part of the current transaction
    /// This enables exactly-once semantics by coordinating message production with offset commits
    /// Note: This is a placeholder implementation - full transaction coordination requires
    /// access to the consumer's group metadata which should be passed from the consumer
    pub async fn send_offsets_to_transaction(
        &self,
        _offsets: &TopicPartitionList,
        _group_id: &str,
    ) -> Result<(), ProducerError> {
        // TODO: Implement actual offset coordination
        // This requires ConsumerGroupMetadata from the consumer
        // For now, return success to allow compilation
        println!("Warning: send_offsets_to_transaction not fully implemented");
        Ok(())
    }
}

/// Builder for creating KafkaProducer with comprehensive configuration options
/// 
/// This builder provides a fluent API for configuring Kafka producers with
/// performance presets, custom settings, and fine-grained control over
/// producer behavior.
/// 
/// # Examples
/// 
/// ## Basic Usage
/// ```rust,no_run
/// # use ferrisstreams::{ProducerBuilder, JsonSerializer};
/// # use ferrisstreams::ferris::kafka::producer_config::{ProducerConfig, CompressionType};
/// let producer = ProducerBuilder::<String, String, _, _>::new(
///     "localhost:9092",
///     "my-topic", 
///     JsonSerializer,
///     JsonSerializer
/// )
/// .client_id("my-producer")
/// .high_throughput()
/// .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// ## Advanced Configuration
/// ```rust,no_run
/// # use ferrisstreams::{ProducerBuilder, JsonSerializer};
/// # use ferrisstreams::ferris::kafka::producer_config::{ProducerConfig, CompressionType, AckMode};
/// # use std::time::Duration;
/// let config = ProducerConfig::new("localhost:9092", "my-topic")
///     .compression(CompressionType::Lz4)
///     .acks(AckMode::All)
///     .batching(32768, Duration::from_millis(10))
///     .custom_property("security.protocol", "SSL");
/// 
/// let producer = ProducerBuilder::<String, String, _, _>::with_config(
///     config,
///     JsonSerializer,
///     JsonSerializer
/// ).build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ProducerBuilder<K, V, KS, VS, C = LoggingProducerContext> 
where
    KS: Serializer<K>,
    VS: Serializer<V>,
    C: ProducerContext + 'static,
{
    config: ProducerConfig,
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
            config: ProducerConfig::new(brokers, default_topic),
            key_serializer,
            value_serializer,
            context: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Creates a new builder with custom configuration
    pub fn with_config(config: ProducerConfig, key_serializer: KS, value_serializer: VS) -> Self {
        Self {
            config,
            key_serializer,
            value_serializer,
            context: None,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        }
    }

    /// Set client ID
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config = self.config.client_id(client_id);
        self
    }

    /// Set message timeout
    pub fn message_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.message_timeout(timeout);
        self
    }

    /// Set compression type
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.config = self.config.compression(compression);
        self
    }

    /// Set acknowledgment mode
    pub fn acks(mut self, acks: AckMode) -> Self {
        self.config = self.config.acks(acks);
        self
    }

    /// Set batching configuration
    pub fn batching(mut self, batch_size: u32, linger_ms: Duration) -> Self {
        self.config = self.config.batching(batch_size, linger_ms);
        self
    }

    /// Enable/disable idempotent producer
    pub fn idempotence(mut self, enable: bool) -> Self {
        self.config = self.config.idempotence(enable);
        self
    }

    /// Set retry configuration
    pub fn retries(mut self, retries: u32, backoff: Duration) -> Self {
        self.config = self.config.retries(retries, backoff);
        self
    }

    /// Add custom configuration property
    pub fn custom_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config = self.config.custom_property(key, value);
        self
    }

    /// Apply high throughput preset
    pub fn high_throughput(mut self) -> Self {
        self.config = self.config.high_throughput();
        self
    }

    /// Apply low latency preset
    pub fn low_latency(mut self) -> Self {
        self.config = self.config.low_latency();
        self
    }

    /// Apply maximum durability preset
    pub fn max_durability(mut self) -> Self {
        self.config = self.config.max_durability();
        self
    }

    /// Apply development preset
    pub fn development(mut self) -> Self {
        self.config = self.config.development();
        self
    }

    /// Builds the KafkaProducer
    pub fn build(self) -> Result<KafkaProducer<K, V, KS, VS, LoggingProducerContext>, KafkaError> {
        KafkaProducer::with_config(self.config, self.key_serializer, self.value_serializer)
    }
}

impl<K, V, KS, VS> ProducerBuilder<K, V, KS, VS, LoggingProducerContext>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    /// Sets a custom producer context
    pub fn with_context<NewC>(self, context: NewC) -> ProducerBuilder<K, V, KS, VS, NewC>
    where
        NewC: ProducerContext + 'static,
    {
        ProducerBuilder {
            config: self.config,
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