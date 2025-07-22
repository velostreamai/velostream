//! Library for Kafka integration
//! 
//! This library provides a ferris wrapper around rdkafka for producing messages to Kafka.

// Export the ferris.kafka.app module structure
pub mod ferris;

// Re-export for backward compatibility and convenience
pub use ferris::kafka::{
    KafkaProducer, 
    KafkaConsumer,
    // Type-safe versions
    TypedKafkaProducer,
    TypedKafkaConsumer,
    TypedProducerBuilder,
    TypedConsumerBuilder,
    TypedMessage,
    KafkaConsumable,
    // Serialization
    JsonSerializer,
    Serializer,
    SerializationError,
};
