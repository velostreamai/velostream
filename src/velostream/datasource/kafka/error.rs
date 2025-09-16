//! Kafka data source error types

use crate::velostream::kafka::kafka_error::{ConsumerError, ProducerError};
use rdkafka::error::KafkaError;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum KafkaDataSourceError {
    /// Kafka client error
    Kafka(KafkaError),
    /// Consumer error
    Consumer(ConsumerError),
    /// Producer error
    Producer(ProducerError),
    /// Configuration error
    Configuration(String),
    /// Serialization error
    Serialization(String),
    /// Schema error
    Schema(String),
}

impl fmt::Display for KafkaDataSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaDataSourceError::Kafka(err) => write!(f, "Kafka error: {}", err),
            KafkaDataSourceError::Consumer(err) => write!(f, "Consumer error: {}", err),
            KafkaDataSourceError::Producer(err) => write!(f, "Producer error: {}", err),
            KafkaDataSourceError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            KafkaDataSourceError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            KafkaDataSourceError::Schema(msg) => write!(f, "Schema error: {}", msg),
        }
    }
}

impl Error for KafkaDataSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            KafkaDataSourceError::Kafka(err) => Some(err),
            KafkaDataSourceError::Consumer(err) => Some(err),
            KafkaDataSourceError::Producer(err) => Some(err),
            _ => None,
        }
    }
}

impl From<KafkaError> for KafkaDataSourceError {
    fn from(err: KafkaError) -> Self {
        KafkaDataSourceError::Kafka(err)
    }
}

// Note: ConsumerError and ProducerError are both type aliases for KafkaClientError
impl From<ConsumerError> for KafkaDataSourceError {
    fn from(err: ConsumerError) -> Self {
        KafkaDataSourceError::Consumer(err)
    }
}
