//! Library for Kafka integration
//! 
//! This library provides a ferris wrapper around rdkafka for producing messages to Kafka.

// Export the ferris.kafka.app module structure
pub mod ferris;

// Re-export the KafkaProducer for backward compatibility
pub use ferris::kafka::KafkaProducer;
pub use ferris::kafka::KafkaConsumer;
