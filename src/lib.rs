//! Library for Kafka integration
//! 
//! This library provides a ferris wrapper around rdkafka for producing messages to Kafka.

// Export the ferris.kafka.app module structure
pub mod ferris;

// Re-export main API
pub use ferris::kafka::{
    KafkaProducer,
    KafkaConsumer, 
    ProducerBuilder,
    ConsumerBuilder,
    ProducerError,
    ConsumerError,
    Message,
    KafkaConsumable,
    JsonSerializer,
    Serializer,
    SerializationError,
};
