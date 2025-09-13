// Common imports and re-exports for Kafka tests
// This module consolidates all commonly used imports to reduce duplication across test files

// Re-export commonly used Kafka items
pub use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
pub use ferrisstreams::ferris::kafka::kafka_error::KafkaClientError;
pub use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;
pub use ferrisstreams::ferris::kafka::producer_config::{AckMode, CompressionType, ProducerConfig};
pub use ferrisstreams::ferris::kafka::{
    Headers, JsonSerializer, KafkaConsumer, KafkaProducer, Message, ProducerBuilder,
};
// Import from correct modules
pub use ferrisstreams::ferris::kafka::serialization::Serializer;
pub use ferrisstreams::ferris::serialization::SerializationError;

// Re-export commonly used external crates
pub use chrono::Utc;
pub use serde::{Deserialize, Serialize};
pub use serial_test::serial;
pub use std::time::Duration;
pub use uuid::Uuid;

// Re-export test utilities and messages
pub use crate::unit::test_messages::*;
pub use crate::unit::test_utils::*;
