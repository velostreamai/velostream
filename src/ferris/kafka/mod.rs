// Kafka modules
pub mod admin_client;
mod kafka_consumer;
mod kafka_producer;
mod kafka_producer_def_context;
pub mod ktable;
pub mod serialization;
mod utils;

// Common types
pub mod headers;
pub mod message;

// Configuration modules
pub mod consumer_config;
pub mod producer_config;

// Shared utilities
pub mod client_config_builder;
pub mod common_config;
pub mod kafka_error;
pub mod performance_presets;

// Re-export main API
pub use kafka_consumer::{ConsumerBuilder, KafkaConsumable, KafkaConsumer};
pub use kafka_producer::{KafkaProducer, ProducerBuilder};
pub use ktable::KTable;
pub use serialization::{BytesSerializer, JsonSerializer, SerializationError, Serializer};
pub use utils::convert_kafka_log_level;

// Re-export common types at root level for easier access
pub use headers::Headers;
pub use message::Message;

// Re-export admin client
pub use admin_client::KafkaAdminClient;

// Re-export errors
pub use kafka_error::{ConsumerError, ProducerError};

// Conditional exports for feature-gated serializers

