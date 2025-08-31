// Kafka modules
pub mod admin_client;
pub mod configurable_consumer;
pub mod configurable_producer;
mod kafka_consumer;
mod kafka_producer;
mod kafka_producer_def_context;
pub mod ktable;
pub mod serialization;
pub mod serialization_format;
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

// Re-export Phase 2 Enhanced APIs
pub use configurable_consumer::{ConfigurableKafkaConsumer, ConfigurableKafkaConsumerBuilder};
pub use configurable_producer::{ConfigurableKafkaProducer, ConfigurableKafkaProducerBuilder};
pub use kafka_producer_def_context::LoggingProducerContext;
pub use ktable::KTable;
pub use serialization::{
    BytesSerializer, JsonSerializer, SerializationError, Serializer, StringSerializer,
};
pub use serialization_format::{SerializationConfig, SerializationFactory, SerializationFormat};

// Feature-gated exports
pub use serialization::AvroSerializer;
pub use serialization::ProtoSerializer;
pub use utils::convert_kafka_log_level;

// Re-export common types at root level for easier access
pub use headers::Headers;
pub use message::Message;

// Re-export admin client
pub use admin_client::KafkaAdminClient;

// Re-export errors
pub use kafka_error::{ConsumerError, KafkaClientError, ProducerError};

// Conditional exports for feature-gated serializers
