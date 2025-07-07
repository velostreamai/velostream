use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;
use log::{info, error};

/// A wrapper around rdkafka's FutureProducer to simplify Kafka message production
pub struct KafkaProducer {
    producer: FutureProducer,
    default_topic: String,
}

impl KafkaProducer {
    /// Creates a new KafkaProducer
    ///
    /// # Arguments
    ///
    /// * `brokers` - Comma-separated list of broker addresses (e.g., "localhost:9092")
    /// * `default_topic` - The default topic to produce messages to
    ///
    /// # Returns
    ///
    /// A Result containing the KafkaProducer or an error
    pub fn new(brokers: &str, default_topic: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        info!("Created KafkaProducer connected to {} with default topic {}", brokers, default_topic);

        Ok(KafkaProducer {
            producer,
            default_topic: default_topic.to_string(),
        })
    }

    /// Sends a message to the default topic
    ///
    /// # Arguments
    ///
    /// * `key` - Optional message key
    /// * `payload` - Message content
    /// * `timestamp` - Optional timestamp in milliseconds since the Unix epoch
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    pub async fn send(&self, key: Option<&str>, payload: &str, timestamp: Option<i64>) -> Result<(), rdkafka::error::KafkaError> {
        self.send_to_topic(&self.default_topic, key, payload, timestamp).await
    }

    /// Sends a message to the default topic without specifying a timestamp
    ///
    /// # Arguments
    ///
    /// * `key` - Optional message key
    /// * `payload` - Message content
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    pub async fn send_without_timestamp(&self, key: Option<&str>, payload: &str) -> Result<(), rdkafka::error::KafkaError> {
        self.send(key, payload, None).await
    }

    /// Sends a message to a specific topic
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to send the message to
    /// * `key` - Optional message key
    /// * `payload` - Message content
    /// * `timestamp` - Optional timestamp in milliseconds since the Unix epoch
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure
    pub async fn send_to_topic(&self, topic: &str, key: Option<&str>, payload: &str, timestamp: Option<i64>) -> Result<(), rdkafka::error::KafkaError> {
        let mut record = FutureRecord::to(topic)
            .payload(payload)
            .key(key.unwrap_or(""));

        // Add timestamp if provided
        if let Some(ts) = timestamp {
            record = record.timestamp(ts);
        }

        match self.producer.send(record, Timeout::After(Duration::from_secs(5))).await {
            Ok((partition, offset)) => {
                info!("Message sent to topic '{}', partition {}, offset {}", topic, partition, offset);
                Ok(())
            },
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
    /// A Result indicating success or failure
    pub fn flush(&self, timeout_ms: u64) -> Result<(), rdkafka::error::KafkaError> {
        self.producer.flush(Timeout::After(Duration::from_millis(timeout_ms)))
    }
}