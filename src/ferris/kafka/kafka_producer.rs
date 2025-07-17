use crate::ferris::kafka::kafka_producer_def_context::LoggingProducerContext;
use log::{error, info, debug, log, Level};
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, NoCustomPartitioner, Producer, ProducerContext};
use rdkafka::util::Timeout;
use std::time::Duration;

/// A wrapper around rdkafka's FutureProducer to simplify Kafka message production
pub struct KafkaProducer<C: ProducerContext + 'static> {
    producer: FutureProducer<C>,
    default_topic: String
}
const SEND_WAIT: u64 = 30;

impl<C: ProducerContext + 'static> KafkaProducer<C> {
    /// Creates a new KafkaProducer with an optional custom context
    ///
    /// # Arguments
    ///
    /// * `brokers` - Comma-separated list of broker addresses (e.g., "localhost:9092")
    /// * `default_topic` - The default topic to produce messages to
    /// * `context` - Optional ProducerContext (if None, a default is used)
    ///
    /// # Returns
    ///
    /// A Result containing the KafkaProducer or an error
    pub fn new_with_context(
        brokers: &str,
        default_topic: &str,
        context: C,
    ) -> Result<KafkaProducer<C>, KafkaError> {
        let producer: FutureProducer<C> = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")

            // 7: Debug, 6:Info, 3:Error
            .set("log_level", "7")
            .create_with_context(context)?;

        info!("Created KafkaProducer connected to {} with default topic {}", brokers, default_topic);

        Ok(KafkaProducer {
            producer,
            default_topic: default_topic.to_string()
        })
    }

    /// Creates a new KafkaProducer (backward compatible, uses default context)
    pub fn new(brokers: &str, default_topic: &str) -> Result<KafkaProducer<LoggingProducerContext>, KafkaError> {
        KafkaProducer::new_with_context(brokers, default_topic, LoggingProducerContext::default())
    }

    /// Sends a message to the default topic
    ///
    /// # Arguments
    ///
    /// * `key` - Optional message key
    /// * `payload` - Message content as bytes
    /// * `timestamp` - Optional timestamp in milliseconds since the Unix epoch
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    pub async fn send(&self, key: Option<&str>, payload: &[u8], timestamp: Option<i64>) -> Result<rdkafka::producer::future_producer::Delivery, KafkaError> {
        self.send_to_topic(&self.default_topic, key, payload, timestamp).await
    }

    /// Sends a message to the default topic with the current system time as the timestamp
    ///
    /// # Arguments
    ///
    /// * `key` - Optional message key
    /// * `payload` - Message content as bytes
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    pub async fn send_with_current_timestamp(&self, key: Option<&str>, payload: &[u8]) -> Result<rdkafka::producer::future_producer::Delivery, KafkaError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        self.send_to_topic(&self.default_topic, key, payload, Some(timestamp)).await
    }

    /// Sends a message to a specific topic
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to send the message to
    /// * `key` - Optional message key
    /// * `payload` - Message content as bytes
    /// * `timestamp` - Optional timestamp in milliseconds since the Unix epoch
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    pub async fn send_to_topic(&self, topic: &str, key: Option<&str>, payload:  &[u8], timestamp: Option<i64>) -> Result<rdkafka::producer::future_producer::Delivery, rdkafka::error::KafkaError> {
        let mut record = FutureRecord::to(topic)
            .payload(payload)
            .key(key.unwrap_or(""));

        // Add timestamp if provided
        if let Some(ts) = timestamp {
            record = record.timestamp(ts);
        }

        match self.producer.send(record, Timeout::After(Duration::from_secs(SEND_WAIT))).await{
            Ok(delivery) => {
                debug!("Message sent to topic '{}'", topic);
                Ok(delivery)
            }
            Err((err, _)) => {
                error!("Failed to send message to topic '{}': {}", topic, err);
                Err(err)
            }
        }
    }



    /// Flushes any pending messages
    ///
    /// # Arguments
    ///
    /// * `timeout_ms` - Maximum time to wait for the flush to complete in milliseconds
    ///
    /// # Returns
    ///
    /// A Result indicating
    /// success or failure
    pub fn flush(&self, timeout_ms: u64) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.flush(Timeout::After(Duration::from_millis(timeout_ms)))
    }
}
