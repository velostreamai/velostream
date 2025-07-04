//! Library for Kafka integration
//! 
//! This library provides a simple wrapper around rdkafka for producing messages to Kafka.

// Export the simple.kafka.app module structure
pub mod simple;

// Re-export the KafkaProducer for backward compatibility
pub use simple::kafka::app::KafkaProducer;
