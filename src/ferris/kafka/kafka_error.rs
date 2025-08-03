use crate::ferris::kafka::serialization::SerializationError;
use rdkafka::error::KafkaError;

/// Unified error type for Kafka producer and consumer operations
///
/// This consolidated error type eliminates duplication between ProducerError
/// and ConsumerError while providing all necessary error variants.
#[derive(Debug)]
pub enum KafkaClientError {
    /// Underlying Kafka library error
    KafkaError(KafkaError),
    /// Serialization/deserialization error
    SerializationError(SerializationError),
    /// Operation timed out
    Timeout,
    /// No message available
    NoMessage,
}

impl std::fmt::Display for KafkaClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaClientError::KafkaError(e) => write!(f, "Kafka error: {}", e),
            KafkaClientError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            KafkaClientError::Timeout => write!(f, "Timeout waiting for operation"),
            KafkaClientError::NoMessage => write!(f, "No message available"),
        }
    }
}

impl std::error::Error for KafkaClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KafkaClientError::KafkaError(e) => Some(e),
            KafkaClientError::SerializationError(e) => Some(e),
            KafkaClientError::Timeout | KafkaClientError::NoMessage => None,
        }
    }
}

impl From<KafkaError> for KafkaClientError {
    fn from(err: KafkaError) -> Self {
        KafkaClientError::KafkaError(err)
    }
}

impl From<SerializationError> for KafkaClientError {
    fn from(err: SerializationError) -> Self {
        KafkaClientError::SerializationError(err)
    }
}

/// Type alias for producer operations - maintains backward compatibility
pub type ProducerError = KafkaClientError;

/// Type alias for consumer operations - maintains backward compatibility
pub type ConsumerError = KafkaClientError;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_error_display() {
        let timeout_error = KafkaClientError::Timeout;
        assert_eq!(timeout_error.to_string(), "Timeout waiting for operation");

        let no_message_error = KafkaClientError::NoMessage;
        assert_eq!(no_message_error.to_string(), "No message available");
    }

    #[test]
    fn test_error_source() {
        let timeout_error = KafkaClientError::Timeout;
        assert!(timeout_error.source().is_none());

        let no_message_error = KafkaClientError::NoMessage;
        assert!(no_message_error.source().is_none());
    }

    #[test]
    fn test_type_aliases() {
        let _producer_error: ProducerError = KafkaClientError::Timeout;
        let _consumer_error: ConsumerError = KafkaClientError::NoMessage;
    }
}
