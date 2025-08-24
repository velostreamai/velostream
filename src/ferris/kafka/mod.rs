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
pub use admin_client::KafkaAdminClient;
pub use kafka_consumer::{ConsumerBuilder, KafkaConsumable, KafkaConsumer};
pub use kafka_error::{ConsumerError, KafkaClientError, ProducerError};
pub use kafka_producer::{KafkaProducer, ProducerBuilder};
pub use kafka_producer_def_context::LoggingProducerContext;
pub use ktable::{ChangeEvent, KTable, KTableStats};
pub use serialization::{BytesSerializer, JsonSerializer, SerializationError, Serializer};
pub use utils::convert_kafka_log_level;

// Re-export common types at root level for easier access
pub use client_config_builder::ClientConfigBuilder;
pub use common_config::{CommonKafkaConfig, HasCommonConfig};
pub use headers::Headers;
pub use message::Message;
pub use performance_presets::{presets, PerformancePresets};

// Conditional exports for feature-gated serializers
#[cfg(feature = "protobuf")]
pub use serialization::ProtoSerializer;

#[cfg(feature = "avro")]
pub use serialization::AvroSerializer;
