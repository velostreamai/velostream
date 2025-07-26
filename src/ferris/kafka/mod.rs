// Kafka modules
mod kafka_producer;
mod kafka_consumer;
mod kafka_producer_def_context;
pub mod serialization;
mod utils;
pub mod admin_client;
pub mod ktable;

// Common types
pub mod headers;
pub mod message;

// Configuration modules
pub mod producer_config;
pub mod consumer_config;

// Shared utilities
pub mod client_config_builder;
pub mod kafka_error;
pub mod common_config;
pub mod performance_presets;

// Re-export main API
pub use kafka_producer::{KafkaProducer, ProducerBuilder};
pub use kafka_consumer::{KafkaConsumer, ConsumerBuilder, KafkaConsumable};
pub use kafka_error::{KafkaClientError, ProducerError, ConsumerError};
pub use kafka_producer_def_context::LoggingProducerContext;
pub use serialization::{Serializer, SerializationError, JsonSerializer, BytesSerializer};
pub use utils::convert_kafka_log_level;
pub use admin_client::KafkaAdminClient;
pub use ktable::{KTable, KTableStats, ChangeEvent};

// Re-export common types at root level for easier access
pub use headers::Headers;
pub use message::Message;
pub use common_config::{CommonKafkaConfig, HasCommonConfig};
pub use performance_presets::{PerformancePresets, presets};
pub use client_config_builder::ClientConfigBuilder;

// Conditional exports for feature-gated serializers
#[cfg(feature = "protobuf")]
pub use serialization::ProtoSerializer;

#[cfg(feature = "avro")]
pub use serialization::AvroSerializer;
