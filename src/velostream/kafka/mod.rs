// Kafka modules
pub mod admin_client;
pub mod configurable_consumer;
pub mod configurable_producer;
pub mod consumer_adapters;
pub mod consumer_factory;
// FR-081 Phase 2D: kafka_consumer.rs removed - use FastConsumer
mod kafka_producer;
mod kafka_producer_def_context;
pub mod serialization;
pub mod serialization_format;
pub mod unified_consumer;
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
pub mod kafka_fast_consumer;
pub mod kafka_fast_producer;
pub mod performance_presets;

// Re-export main API (FR-081 Phase 2D: FastConsumer is the primary consumer)
pub use kafka_fast_consumer::Consumer as FastConsumer;
pub use kafka_producer::{KafkaProducer, ProducerBuilder};

// FR-081 Phase 2B+2D: Re-export tier-based consumer system
pub use consumer_factory::ConsumerFactory;
pub use unified_consumer::KafkaStreamConsumer;

// Re-export Phase 2 Enhanced APIs
pub use serialization::JsonSerializer;

// Feature-gated exports
pub use utils::convert_kafka_log_level;

// Re-export common types at root level for easier access
pub use headers::Headers;
pub use message::Message;

// Re-export broker address family configuration utilities
pub use common_config::{
    BROKER_ADDRESS_FAMILY_ENV, BrokerAddressFamily, apply_broker_address_family,
    get_broker_address_family,
};

// Re-export polled producer types
pub use kafka_fast_producer::{
    AsyncPolledProducer, PolledProducer, SyncPolledProducer, TransactionalPolledProducer,
};

// Re-export admin client for examples and tests

// Conditional exports for feature-gated serializers
